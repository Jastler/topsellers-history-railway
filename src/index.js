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
  ["us", "uk", "fr", "de", "pl", "es", "it"],
  ["nl", "be", "at", "ch", "dk", "se", "no"],
  ["fi", "tr", "cz", "sk", "hu", "ro", "bg"],
  ["jp", "kr", "tw", "hk", "sg", "th", "au"],
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

  for (let i = 0; i < chunks.length; i++) {
    const { error } = await supabase.from(table).insert(chunks[i]);
    if (error) throw error;
  }

  log(`✔ INSERT ${table} done`);
}

async function upsertPages(rows) {
  if (!rows.length) return;

  const { error } = await supabase
    .from("steam_topsellers_pages_region")
    .upsert(rows, { onConflict: "cc" });

  if (error) throw error;

  log(`✔ pages updated (${rows.length})`);
}

/**
 * ================= SCRAPER =================
 */

async function scrapePage({ cc, page, ts, rankRef }) {
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
      if (!res.ok) throw new Error(`HTTP ${res.status}`);

      const $ = cheerio.load(await res.text());
      const rows = [];

      $(".search_result_row").each((_, el) => {
        const appid = extractAppId($(el).attr("href"));
        if (appid) {
          rows.push({
            appid,
            cc,
            rank: rankRef.value++,
            ts,
          });
        }
      });

      return rows;
    } catch {
      if (attempt === MAX_ATTEMPTS) {
        log(`❌ ${cc} page ${page} failed`);
        return [];
      }
      await sleep(3000);
    }
  }
}

/**
 * ================= REGION =================
 */

async function runRegion({ cc, ts }) {
  let rows = [];
  let rankRef = { value: 1 };

  for (let page = 1; page <= MAX_PAGES; page++) {
    const pageRows = await scrapePage({ cc, page, ts, rankRef });
    if (!pageRows.length) break;

    rows.push(...pageRows);
    await sleep(PAGE_DELAY_MS);
  }

  const unique = [...new Map(rows.map((r) => [r.appid, r])).values()];

  if (unique.length < MIN_VALID_ITEMS_REGION) {
    log(`⚠️ ${cc}: too few items (${unique.length}), skipped`);
    return null;
  }

  return {
    rows,
    totalPages: Math.ceil(unique.length / FRONT_PAGE_SIZE),
  };
}

/**
 * ================= SNAPSHOT =================
 */

async function runSnapshot() {
  const ts = Math.floor(Date.now() / 1000);
  const { idx, ccs } = getRegionGroup();

  log(
    `===== SNAPSHOT START ts=${ts} | group=${idx} | ccs=[${ccs.join(
      ", "
    )}] =====`
  );

  let history = [];
  let pages = [];

  for (const cc of ccs) {
    log(`→ scraping ${cc}`);

    const res = await runRegion({ cc, ts });
    if (!res) continue;

    history.push(...res.rows);
    pages.push({
      cc,
      total_pages: res.totalPages,
      updated_ts: ts,
    });
  }

  if (!history.length) {
    log("❌ No valid regions scraped, aborting snapshot");
    return;
  }

  /**
   * HISTORY
   */
  await insertChunked("steam_topsellers_history_region", history);

  /**
   * PAGES
   */
  await upsertPages(pages);

  /**
   * CURRENT (partial refresh for this group)
   */
  log(`🔄 refreshing current for ${ccs.length} countries`);

  const { error } = await supabase.rpc(
    "refresh_topsellers_current_region_for_ccs",
    {
      ccs,
      snapshot_ts: ts,
    }
  );

  if (error) throw error;

  log(`✔ current refreshed for group`);

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

    const wait = next - now;
    log(`Waiting ${Math.round(wait / 1000)}s`);
    await sleep(wait);

    try {
      await runSnapshot();
    } catch (e) {
      log(`❌ SNAPSHOT FAILED: ${e.message}`);
    }
  }
}

main().catch(console.error);
