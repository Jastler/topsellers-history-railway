import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";
import "dotenv/config";

/**
 * ================= CONFIG =================
 * Top sellers по регіонах через IStoreQueryService/Query.
 * Коли й які країни — REGION_GROUPS (ротація кожні 10 хв).
 * Запис у БД — як раніше (history, current, pages, hourly, stats).
 */

const STEAM_KEY = process.env.STEAM_KEY;
const API_BASE = "https://api.steampowered.com/IStoreQueryService/Query/v1/";

const BATCH_SIZE = 1000;
const TOTAL_PER_REGION = 10000;
const DELAY_BETWEEN_PAGES_MS = 400;
const DELAY_BETWEEN_REGIONS_MS = 600;

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

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE,
  {
    db: { schema: "analytics" },
    auth: { persistSession: false },
  },
);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

/**
 * 🕒 GROUP BY UTC MINUTES — яка група країн зараз
 * :00 → 0, :10 → 1, :20 → 2, :30 → 3, :40 → 4, :50 → 0 (група 0 ще раз)
 */
function getRegionGroup() {
  const m = new Date().getUTCMinutes();
  const idx = Math.floor(m / 10) % REGION_GROUPS.length;
  return { idx, ccs: REGION_GROUPS[idx] };
}

/**
 * Один запит до Query API (одна сторінка пагінації)
 */
function buildInput(cc, start) {
  return {
    query: {
      start,
      count: BATCH_SIZE,
      sort: 11,
      filters: { global_top_n_sellers: TOTAL_PER_REGION },
    },
    context: { language: "en", country_code: cc.toUpperCase() },
    data_request: { include_basic_info: true },
  };
}

async function fetchStoreQueryPage(input) {
  const url =
    API_BASE +
    "?key=" +
    encodeURIComponent(STEAM_KEY) +
    "&input_json=" +
    encodeURIComponent(JSON.stringify(input));

  const res = await fetch(url, {
    method: "GET",
    headers: { Accept: "application/json" },
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${text.slice(0, 200)}`);
  return JSON.parse(text);
}

/**
 * Завантажити топ по регіону через Query API (з пагінацією)
 */
async function fetchRegionViaQuery(cc) {
  const rows = [];
  for (let start = 0; start < TOTAL_PER_REGION; start += BATCH_SIZE) {
    const input = buildInput(cc, start);
    const data = await fetchStoreQueryPage(input);
    const items = data?.response?.store_items ?? [];
    for (let i = 0; i < items.length; i++) {
      const appid = items[i].appid ?? items[i].id;
      if (appid) rows.push({ appid, rank: start + i + 1 });
    }
    if (items.length === 0 || items.length < BATCH_SIZE) break;
    await sleep(DELAY_BETWEEN_PAGES_MS);
  }
  return rows;
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

async function clearCurrentRegion(cc) {
  const { error } = await supabase
    .from("steam_topsellers_current_region")
    .delete()
    .eq("cc", cc);
  if (error) throw error;
}

/**
 * Зібрати дані по одному регіону (API замість скрапінгу)
 */
async function runRegion({ cc, ts }) {
  log(`Fetch ${cc} via Query API...`);
  const rows = await fetchRegionViaQuery(cc);
  if (rows.length < MIN_VALID_ITEMS_REGION) {
    log(`Skip ${cc}: too few items (${rows.length})`);
    return null;
  }

  const uniqueByAppid = new Map();
  for (const r of rows) {
    if (!uniqueByAppid.has(r.appid)) uniqueByAppid.set(r.appid, r);
  }
  const unique = [...uniqueByAppid.values()];

  return {
    history: rows.map((r) => ({ appid: r.appid, cc, rank: r.rank, ts })),
    unique,
    current: unique.map((r, i) => ({
      cc,
      appid: r.appid,
      rank: i + 1,
      updated_ts: ts,
    })),
    totalPages: Math.ceil(unique.length / 10),
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
    try {
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

      const hourly = res.unique.map((r, i) => ({
        cc,
        appid: r.appid,
        ts,
        rank: i + 1,
      }));

      for (let i = 0; i < hourly.length; i += 1000) {
        await supabase
          .from("steam_app_topsellers_hourly_region")
          .upsert(hourly.slice(i, i + 1000), {
            onConflict: "cc,appid,ts",
            ignoreDuplicates: true,
          });
      }

      await supabase
        .from("steam_app_topsellers_hourly_region")
        .delete()
        .eq("cc", cc)
        .lt("ts", ts - 48 * 3600);

      const appids = hourly.map((r) => r.appid);
      const prevMap = new Map();
      for (let i = 0; i < appids.length; i += 1000) {
        const { data } = await supabase
          .from("steam_app_topsellers_stats_region")
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

      for (let i = 0; i < stats.length; i += 1000) {
        await supabase
          .from("steam_app_topsellers_stats_region")
          .upsert(stats.slice(i, i + 1000), { onConflict: "cc,appid" });
      }
    } catch (e) {
      log(`Region ${cc} failed: ${e?.message ?? e}`);
    }

    await sleep(DELAY_BETWEEN_REGIONS_MS);
  }

  if (history.length) {
    await insertChunked("steam_topsellers_history_region", history);
    await upsertPages(pages);
  }

  log(`===== SNAPSHOT DONE ts=${ts} =====`);
}

/**
 * ================= MAIN =================
 */
async function main() {
  if (!STEAM_KEY) {
    console.error("Missing STEAM_KEY");
    process.exit(1);
  }

  while (true) {
    const now = new Date();
    const next = new Date(now);
    next.setUTCMinutes(Math.ceil(now.getUTCMinutes() / 10) * 10);
    next.setUTCSeconds(0);
    const wait = next - now;
    log(`Next run at ${next.toISOString()} (in ${Math.round(wait / 1000)}s)`);
    await sleep(wait);

    try {
      await runSnapshot();
    } catch (e) {
      log("❌ SNAPSHOT FAILED: " + (e?.message ?? e));
    }
  }
}

main().catch(console.error);
