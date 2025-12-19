import fetch from "node-fetch";
import * as cheerio from "cheerio";
import { createClient } from "@supabase/supabase-js";
import "dotenv/config";

/**
 * =====================================================
 * CONFIG
 * =====================================================
 */

const BASE_URL =
  "https://store.steampowered.com/search/results/?query&count=100&ignore_preferences=1";

const MAX_PAGES = 100;
const PAGE_SIZE = 100;

const PAGE_DELAY_MS = 30;
const CONCURRENCY = 4; // можеш піднімати

const TIMEOUT_MS = 40000;
const MAX_ATTEMPTS = 6;
const CHUNK_SIZE = 1000;

const MIN_VALID_ITEMS_REGION = 500;

/**
 * SECONDARY REGIONS (NO GLOBAL)
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
 * =====================================================
 * SUPABASE
 * =====================================================
 */

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE,
  { db: { schema: "analytics" } }
);

/**
 * =====================================================
 * HELPERS
 * =====================================================
 */

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function log(msg) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

function extractAppId(url) {
  const m = url?.match(/\/app\/(\d+)/);
  return m ? Number(m[1]) : null;
}

/**
 * =====================================================
 * FIXED UTC SCHEDULER (05 15 25 35 45 55)
 * =====================================================
 */

function getNextRunAtUTC(now = new Date()) {
  const minutes = now.getUTCMinutes();
  const hours = now.getUTCHours();
  const slots = [5, 15, 25, 35, 45, 55];

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
      `Waiting until ${new Date(ts).toISOString()} (${Math.round(ms / 1000)}s)`
    );
    await sleep(ms);
  }
}

/**
 * =====================================================
 * SCRAPER
 * =====================================================
 */

async function scrapePage({ cc, page, ts, rankRef }) {
  const url = `${BASE_URL}&filter=topsellers&cc=${cc}&page=${page}`;

  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), TIMEOUT_MS);

      const res = await fetch(url, {
        signal: controller.signal,
        headers: { "User-Agent": "Mozilla/5.0" },
      });

      clearTimeout(timeoutId);

      if (res.status === 429 || res.status === 503) {
        log(`Rate limit ${res.status}, retry ${attempt}`);
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

        rows.push({
          appid,
          cc,
          rank: rankRef.value++,
          ts,
        });
      });

      return rows;
    } catch {
      if (attempt === MAX_ATTEMPTS) {
        log(`Page ${page} failed permanently`);
        return [];
      }
      await sleep(3000);
    }
  }

  return [];
}

/**
 * =====================================================
 * SNAPSHOT FOR ONE REGION
 * =====================================================
 */

async function runSnapshotForRegion({ cc, ts }) {
  log(`Region ${cc}: start`);

  let rows = [];
  let rankRef = { value: 1 };

  for (let page = 1; page <= MAX_PAGES; page++) {
    await sleep(PAGE_DELAY_MS);

    const pageRows = await scrapePage({ cc, page, ts, rankRef });

    if (pageRows.length === 0) {
      log(`Region ${cc}: stop at page ${page}`);
      break;
    }

    rows.push(...pageRows);
  }

  const unique = [...new Map(rows.map((r) => [r.appid, r])).values()];

  if (unique.length < MIN_VALID_ITEMS_REGION) {
    log(`Region ${cc}: skipped (${unique.length} items)`);
    return null;
  }

  log(`Region ${cc}: rows=${rows.length}, unique=${unique.length}`);
  return { rows, unique };
}

/**
 * =====================================================
 * MAIN SNAPSHOT
 * =====================================================
 */

async function runSnapshot() {
  const ts = Math.floor(Date.now() / 1000);
  const startedAt = Date.now();

  log(`SNAPSHOT start ts=${ts}`);

  let historyRows = [];
  let currentRows = [];

  for (let i = 0; i < REGIONS.length; i += CONCURRENCY) {
    const batch = REGIONS.slice(i, i + CONCURRENCY);
    log(`Batch: ${batch.map((b) => b.cc).join(", ")}`);

    const results = await Promise.all(
      batch.map((r) => runSnapshotForRegion({ cc: r.cc, ts }))
    );

    for (let j = 0; j < results.length; j++) {
      const result = results[j];
      if (!result) continue;

      const cc = batch[j].cc;
      const { rows, unique } = result;

      historyRows.push(
        ...rows.map(({ appid, cc, rank, ts }) => ({
          appid,
          cc,
          rank,
          ts,
        }))
      );

      currentRows.push(
        ...unique.map((r) => ({
          appid: r.appid,
          cc,
          rank: r.rank,
          updated_ts: ts,
        }))
      );
    }
  }

  // INSERT HISTORY
  for (let i = 0; i < historyRows.length; i += CHUNK_SIZE) {
    await supabase
      .from("steam_topsellers_history_region")
      .insert(historyRows.slice(i, i + CHUNK_SIZE));
  }

  // UPSERT CURRENT
  await supabase
    .from("steam_topsellers_current_region")
    .upsert(currentRows, { onConflict: "appid,cc" });

  const duration = ((Date.now() - startedAt) / 1000).toFixed(1);
  log(`SNAPSHOT done in ${duration}s`);
}

/**
 * =====================================================
 * LOOP
 * =====================================================
 */

async function main() {
  let running = false;

  while (true) {
    const nextRunAt = getNextRunAtUTC(new Date());
    await sleepUntil(nextRunAt);

    if (running) {
      log("Previous snapshot still running — skipping slot");
      continue;
    }

    running = true;

    try {
      await runSnapshot();
    } catch (err) {
      log(`SNAPSHOT failed: ${err.message}`);
    } finally {
      running = false;
    }
  }
}

main().catch(console.error);
