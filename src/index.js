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
const CONCURRENCY = 4;
const INSERT_CONCURRENCY = 2; // Micro-safe

const TIMEOUT_MS = 40000;
const MAX_ATTEMPTS = 6;
const CHUNK_SIZE = 1000;

const MIN_VALID_ITEMS_REGION = 500;

/**
 * SECONDARY REGIONS
 */
const REGIONS = [
  { cc: "us" },
  { cc: "at" },
  { cc: "au" },
  { cc: "be" },
  { cc: "br" },
  { cc: "ch" },
  { cc: "cz" },
  { cc: "dk" },
  { cc: "gb" },
  { cc: "hk" },
  { cc: "jp" },
  { cc: "kr" },
  { cc: "nz" },
  { cc: "se" },
  { cc: "sg" },
  { cc: "tw" },
];

/**
 * ================= SUPABASE =================
 */

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE,
  { db: { schema: "analytics" } }
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
 * ================= UTC SCHEDULER (05 15 25 35 45 55) =================
 */

function getNextRunAtUTC(now = new Date()) {
  const minutes = now.getUTCMinutes();
  const hours = now.getUTCHours();
  const slots = [0, 10, 20, 30, 40, 50];

  for (const m of slots) {
    if (minutes < m) {
      return Date.UTC(
        now.getUTCFullYear(),
        now.getUTCMonth(),
        now.getUTCDate(),
        hours,
        m,
        0,
        0
      );
    }
  }

  return Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate(),
    hours + 1,
    5,
    0,
    0
  );
}

async function sleepUntil(ts) {
  const ms = ts - Date.now();
  if (ms > 0) {
    log(
      `Waiting ${Math.round(ms / 1000)}s until ${new Date(ts).toISOString()}`
    );
    await sleep(ms);
  }
}

/**
 * ================= INSERT WITH CONCURRENCY =================
 */

async function insertWithConcurrency(table, chunks) {
  if (!chunks.length) {
    log(`INSERT ${table}: nothing to insert`);
    return;
  }

  log(
    `INSERT ${table}: ${chunks.length} chunks (concurrency=${INSERT_CONCURRENCY})`
  );

  for (let i = 0; i < chunks.length; i += INSERT_CONCURRENCY) {
    const batch = chunks.slice(i, i + INSERT_CONCURRENCY);
    log(`→ INSERT ${table}: chunks ${i + 1} – ${i + batch.length}`);

    const results = await Promise.all(
      batch.map((chunk) => supabase.from(table).insert(chunk))
    );

    for (const r of results) {
      if (r?.error) {
        log(`❌ INSERT FAILED ${table}: ${r.error.message}`);
        throw r.error;
      }
    }
  }

  log(`✔ INSERT ${table} done`);
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

      const html = await res.text();
      const $ = cheerio.load(html);

      const rows = [];
      $(".search_result_row").each((_, el) => {
        const appid = extractAppId($(el).attr("href"));
        if (!appid) return;
        rows.push({ appid, cc, rank: rankRef.value++, ts });
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
  log(`Region ${cc}: start`);

  let rows = [];
  let rankRef = { value: 1 };

  for (let page = 1; page <= MAX_PAGES; page++) {
    await sleep(PAGE_DELAY_MS);
    const pageRows = await scrapePage({ cc, page, ts, rankRef });
    if (!pageRows.length) break;
    rows.push(...pageRows);
  }

  const unique = [...new Map(rows.map((r) => [r.appid, r])).values()];

  if (unique.length < MIN_VALID_ITEMS_REGION) {
    log(`Region ${cc}: skipped (${unique.length})`);
    return null;
  }

  const totalPages = Math.ceil(unique.length / FRONT_PAGE_SIZE);

  log(`Region ${cc}: items=${unique.length}, totalPages=${totalPages}`);

  return { rows, unique, totalPages };
}

/**
 * ================= SNAPSHOT =================
 */

async function runSnapshot() {
  const ts = Math.floor(Date.now() / 1000);
  log(`===== SNAPSHOT START ts=${ts} =====`);

  let history = [];
  let current = [];
  let pages = [];

  for (let i = 0; i < REGIONS.length; i += CONCURRENCY) {
    const batch = REGIONS.slice(i, i + CONCURRENCY);
    log(`Processing regions: ${batch.map((r) => r.cc).join(", ")}`);

    const results = await Promise.all(
      batch.map((r) => runRegion({ cc: r.cc, ts }))
    );

    for (let j = 0; j < results.length; j++) {
      const res = results[j];
      if (!res) continue;

      const cc = batch[j].cc;

      history.push(...res.rows);

      current.push(
        ...res.unique.map((r) => ({
          appid: r.appid,
          cc,
          rank: r.rank,
          updated_ts: ts,
        }))
      );

      pages.push({ cc, total_pages: res.totalPages, updated_ts: ts });
    }
  }

  log(`UPSERT pages: ${pages.length} rows`);
  if (pages.length) {
    const p = await supabase
      .from("steam_topsellers_pages_region")
      .upsert(pages, { onConflict: "cc" });
    if (p.error) throw p.error;
  }

  await insertWithConcurrency(
    "steam_topsellers_history_region",
    chunkArray(history, CHUNK_SIZE)
  );

  log(`UPSERT current_region: ${current.length} rows`);
  if (current.length) {
    const c = await supabase
      .from("steam_topsellers_current_region")
      .upsert(current, { onConflict: "appid,cc" });
    if (c.error) throw c.error;
  }

  log(`===== SNAPSHOT DONE =====`);
}

/**
 * ================= LOOP =================
 */

async function main() {
  let running = false;

  while (true) {
    await sleepUntil(getNextRunAtUTC());

    if (running) {
      log("Previous snapshot still running — skipping");
      continue;
    }

    running = true;
    try {
      await runSnapshot();
    } catch (e) {
      log(`SNAPSHOT FAILED: ${e.message}`);
    } finally {
      running = false;
    }
  }
}

main().catch(console.error);
