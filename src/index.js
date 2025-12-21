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
const INSERT_CONCURRENCY = 1; 

const TIMEOUT_MS = 40000;
const MAX_ATTEMPTS = 6;
const CHUNK_SIZE = 500;

const MIN_VALID_ITEMS_REGION = 500;

/**
 * CLEANUP tuning (КЛЮЧОВЕ)
 */
const CLEANUP_BATCH_SIZE = 300;
const CLEANUP_DELAY_MS = 200;
const CLEANUP_MAX_LOOPS = 500;

/**
 * SECONDARY REGIONS ONLY
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
  {
    db: { schema: "analytics" },
    auth: { persistSession: false },
  }
);

/**
 * ================= HELPERS =================
 */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (msg) => console.log(`[${new Date().toISOString()}] ${msg}`);

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
 * ================= UTC SCHEDULER =================
 * 00 10 20 30 40 50
 */

function getNextRunAtUTC(now = new Date()) {
  const m = now.getUTCMinutes();
  const h = now.getUTCHours();
  const slots = [0, 10, 20, 30, 40, 50];

  for (const s of slots) {
    if (m < s) {
      return Date.UTC(
        now.getUTCFullYear(),
        now.getUTCMonth(),
        now.getUTCDate(),
        h,
        s,
        0,
        0
      );
    }
  }

  return Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate(),
    h + 1,
    0,
    0,
    0
  );
}

async function sleepUntil(ts) {
  const ms = ts - Date.now();
  if (ms > 0) {
    log(`Waiting ${Math.round(ms / 1000)}s`);
    await sleep(ms);
  }
}

/**
 * cleanup тільки раз на годину
 */
function shouldCleanup(ts) {
  return ts % 3600 < 300; // перші 5 хв години
}

/**
 * ================= DB HELPERS =================
 */

async function insertWithConcurrency(table, rows) {
  if (!rows.length) return;

  const chunks = chunkArray(rows, CHUNK_SIZE);
  log(`INSERT ${table}: ${rows.length} rows (${chunks.length} chunks)`);

  for (let i = 0; i < chunks.length; i += INSERT_CONCURRENCY) {
    const batch = chunks.slice(i, i + INSERT_CONCURRENCY);
    const res = await Promise.all(
      batch.map((c) => supabase.from(table).insert(c))
    );
    for (const r of res) if (r?.error) throw r.error;
  }
}

async function upsertWithConcurrency(table, rows, conflict) {
  if (!rows.length) return;

  const chunks = chunkArray(rows, CHUNK_SIZE);
  log(`UPSERT ${table}: ${rows.length} rows (${chunks.length} chunks)`);

  for (let i = 0; i < chunks.length; i += INSERT_CONCURRENCY) {
    const batch = chunks.slice(i, i + INSERT_CONCURRENCY);
    const res = await Promise.all(
      batch.map((c) => supabase.from(table).upsert(c, { onConflict: conflict }))
    );
    for (const r of res) if (r?.error) throw r.error;
  }
}

/**
 * ================= CLEANUP (CHUNKED) =================
 */

async function cleanupOldSnapshotsChunked({ ccs, ts }) {
  log(`CLEANUP chunked: secondary current_region < ${ts}`);

  let loops = 0;
  let total = 0;

  while (loops < CLEANUP_MAX_LOOPS) {
    const { data, error } = await supabase
      .from("steam_topsellers_current_region")
      .select("id")
      .in("cc", ccs)
      .lt("updated_ts", ts)
      .limit(CLEANUP_BATCH_SIZE);

    if (error) throw error;
    if (!data || data.length === 0) break;

    const ids = data.map((r) => r.id);

    const del = await supabase
      .from("steam_topsellers_current_region")
      .delete()
      .in("id", ids);

    if (del.error) throw del.error;

    total += ids.length;
    loops++;

    log(`CLEANUP deleted ${ids.length}, total=${total}`);

    await sleep(CLEANUP_DELAY_MS);
  }

  log(`✔ CLEANUP DONE, total deleted=${total}`);
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

      if (res.status === 429 || res.status === 503) {
        await sleep(3000);
        continue;
      }

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

  return [];
}

/**
 * ================= REGION =================
 */

async function runRegion({ cc, ts }) {
  let rows = [];
  let rankRef = { value: 1 };

  for (let page = 1; page <= MAX_PAGES; page++) {
    await sleep(PAGE_DELAY_MS);
    const pageRows = await scrapePage({ cc, page, ts, rankRef });
    if (!pageRows.length) break;
    rows.push(...pageRows);
  }

  const unique = [...new Map(rows.map((r) => [r.appid, r])).values()];
  if (unique.length < MIN_VALID_ITEMS_REGION) return null;

  const totalPages = Math.ceil(unique.length / FRONT_PAGE_SIZE);

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

  const regionCcs = REGIONS.map((r) => r.cc);

  for (let i = 0; i < REGIONS.length; i += CONCURRENCY) {
    const batch = REGIONS.slice(i, i + CONCURRENCY);
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

      pages.push({
        cc,
        total_pages: res.totalPages,
        updated_ts: ts,
      });
    }
  }

  await insertWithConcurrency("steam_topsellers_history_region", history);
  await insertWithConcurrency("steam_topsellers_current_region", current);
  await upsertWithConcurrency("steam_topsellers_pages_region", pages, "cc");

  if (shouldCleanup(ts)) {
    await cleanupOldSnapshotsChunked({ ccs: regionCcs, ts });
  } else {
    log("CLEANUP skipped (not scheduled)");
  }

  log(`===== SNAPSHOT DONE ts=${ts} =====`);
}

/**
 * ================= LOOP =================
 */

async function main() {
  let running = false;

  while (true) {
    await sleepUntil(getNextRunAtUTC());

    if (running) continue;

    running = true;
    try {
      await runSnapshot();
    } catch (e) {
      log(`SNAPSHOT FAILED: ${e?.message || e}`);
    } finally {
      running = false;
    }
  }
}

main().catch(console.error);
