import fetch from "node-fetch";
import * as cheerio from "cheerio";
import { createClient } from "@supabase/supabase-js";
import "dotenv/config";

/**
 * ================= CONFIG =================
 */

const BASE_URL =
  "https://store.steampowered.com/search/results/?query&count=100&ignore_preferences=1";

const MAX_PAGES = 100;
const FRONT_PAGE_SIZE = 10;
const PAGE_DELAY_MS = 30;

const TIMEOUT_MS = 40000;
const MAX_ATTEMPTS = 6;
const CHUNK_SIZE = 500;

const MIN_VALID_ITEMS_REGION = 500;

/**
 * 🔁 REGION ROTATION GROUPS (10 хв / група)
 */
const REGION_GROUPS = [
  ["us", "gb", "de", "fr", "pl", "ru"],
  ["uk", "tr", "es", "it", "nl", "th"],
  ["ca", "au", "jp", "kr", "br", "nz"],
  ["se", "dk", "no", "fi", "ch", "tw"],
  ["at", "be", "cz", "hk", "sg"],
];

/**
 * ================= SUPABASE =================
 */

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE,
  {
    db: { schema: "analytics" },
    auth: { persistSession: false },
  }
);

/**
 * ================= HELPERS =================
 */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

function extractAppId(url) {
  const m = url?.match(/\/app\/(\d+)/);
  return m ? Number(m[1]) : null;
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

/**
 * 🕒 визначаємо групу країн по UTC-хвилинах
 */
function getRegionGroup() {
  const m = new Date().getUTCMinutes();
  const idx = Math.floor(m / 10) % REGION_GROUPS.length;
  return { idx, ccs: REGION_GROUPS[idx] };
}

/**
 * ================= DB HELPERS =================
 */

async function insertChunked(table, rows) {
  if (!rows.length) return;

  const chunks = chunkArray(rows, CHUNK_SIZE);
  log(`INSERT ${table}: ${rows.length} rows (${chunks.length} chunks)`);

  for (const chunk of chunks) {
    const { error } = await supabase.from(table).insert(chunk);
    if (error) throw error;
  }
}

async function upsertPages(rows) {
  if (!rows.length) return;

  const { error } = await supabase
    .from("steam_topsellers_pages_region")
    .upsert(rows, { onConflict: "cc" });

  if (error) throw error;
}

/**
 * 🔴 КРИТИЧНО: очищення current для одного регіону
 */
async function clearCurrentRegion(cc) {
  const { error } = await supabase
    .from("steam_topsellers_current_region")
    .delete()
    .eq("cc", cc);

  if (error) throw error;
}

/**
 * ================= SCRAPER =================
 */

async function scrapePage({ cc, page }) {
  const url = `${BASE_URL}&filter=topsellers&cc=${cc}&page=${page}`;

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), TIMEOUT_MS);

      const res = await fetch(url, {
        signal: controller.signal,
        headers: { "User-Agent": "Mozilla/5.0" },
      });

      clearTimeout(timeout);
      if (!res.ok) throw new Error();

      const $ = cheerio.load(await res.text());
      const rows = [];

      $(".search_result_row").each((_, el) => {
        const appid = extractAppId($(el).attr("href"));
        if (appid) rows.push({ appid });
      });

      return rows;
    } catch {
      if (attempt === MAX_ATTEMPTS) return [];
      await sleep(3000);
    }
  }
}

/**
 * ================= REGION =================
 */

async function runRegion({ cc, ts }) {
  let rows = [];
  let rank = 1;

  for (let page = 1; page <= MAX_PAGES; page++) {
    const pageRows = await scrapePage({ cc, page });
    if (!pageRows.length) break;

    for (const r of pageRows) {
      rows.push({ appid: r.appid, cc, rank: rank++, ts });
    }

    await sleep(PAGE_DELAY_MS);
  }

  const unique = [...new Map(rows.map((r) => [r.appid, r])).values()];

  if (unique.length < MIN_VALID_ITEMS_REGION) return null;

  const current = unique.map((r, i) => ({
    cc,
    appid: r.appid,
    rank: i + 1,
    updated_ts: ts,
  }));

  return {
    history: rows,
    current,
    unique,
    totalPages: Math.ceil(unique.length / FRONT_PAGE_SIZE),
  };
}

/**
 * ================= SNAPSHOT =================
 */

async function runSnapshot() {
  const ts = Math.floor(Date.now() / 1000);
  const { idx, ccs } = getRegionGroup();

  log(`===== SNAPSHOT START ts=${ts} | group=${idx} =====`);

  let history = [];
  let pages = [];

  for (const cc of ccs) {
    const res = await runRegion({ cc, ts });
    if (!res) continue;

    history.push(...res.history);

    await clearCurrentRegion(cc);
    await insertChunked("steam_topsellers_current_region", res.current);

    pages.push({
      cc,
      total_pages: res.totalPages,
      updated_ts: ts,
    });

    /* ================= NEW: REGION HOURLY ================= */

    const hourly = res.unique.map((r, i) => ({
      cc,
      appid: r.appid,
      ts,
      rank: i + 1,
    }));

    for (let i = 0; i < hourly.length; i += 1000) {
      await supabase
        .from("steam_app_topsellers_stats_region")
        .upsert(hourly.slice(i, i + 1000), {
          onConflict: "cc,appid,ts",
          ignoreDuplicates: true,
        });
    }

    await supabase
      .from("steam_app_topsellers_stats_region")
      .delete()
      .eq("cc", cc)
      .lt("ts", ts - 48 * 3600);

    /* ================= NEW: REGION STATS ================= */

    const appids = hourly.map((r) => r.appid);
    const prevMap = new Map();

    for (let i = 0; i < appids.length; i += 1000) {
      const { data } = await supabase
        .from("steam_app_topsellers_region_stats")
        .select("*")
        .eq("cc", cc)
        .in("appid", appids.slice(i, i + 1000));

      for (const r of data || []) prevMap.set(r.appid, r);
    }

    const stats = hourly.map((r) => {
      const prev = prevMap.get(r.appid);

      let bestAll = prev?.best_all_time_rank ?? r.rank;
      let bestAllTs = prev?.best_all_time_rank_ts ?? ts;

      if (r.rank < bestAll) {
        bestAll = r.rank;
        bestAllTs = ts;
      }

      return {
        cc,
        appid: r.appid,

        rank_now: r.rank,

        best_24h_rank: r.rank,
        best_24h_rank_ts: ts,

        best_all_time_rank: bestAll,
        best_all_time_rank_ts: bestAllTs,

        updated_ts: ts,
      };
    });

    for (let i = 0; i < stats.length; i += 500) {
      const slice = stats.slice(i, i + 500);

      const { data } = await supabase.rpc("get_region_app_24h_best_ranks", {
        cc,
        appids: slice.map((x) => x.appid),
      });

      for (const row of data || []) {
        const u = slice.find((x) => x.appid === row.appid);
        if (u && row.best_rank < u.best_rank_24h) {
          u.best_rank_24h = row.best_rank;
          u.best_rank_24h_ts = row.ts;
        }
      }
    }

    for (let i = 0; i < stats.length; i += 1000) {
      await supabase
        .from("steam_app_topsellers_stats_region")
        .upsert(stats.slice(i, i + 1000), { onConflict: "cc,appid" });
    }
  }

  if (history.length) {
    await insertChunked("steam_topsellers_history_region", history);
    await upsertPages(pages);
  }

  log(`===== SNAPSHOT DONE ts=${ts} =====`);
}

/**
 * ================= LOOP =================
 */

async function main() {
  while (true) {
    const now = new Date();
    const next = new Date(now);
    next.setUTCMinutes(Math.ceil(now.getUTCMinutes() / 10) * 10);
    next.setUTCSeconds(0);

    await sleep(next - now);

    try {
      await runSnapshot();
    } catch (e) {
      log(`❌ SNAPSHOT FAILED`);
    }
  }
}

main().catch(console.error);
