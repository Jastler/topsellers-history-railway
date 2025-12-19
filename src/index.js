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

const INSERT_CONCURRENCY = 2; // 👈 стартуємо з 2

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

/**
 * ================= FIXED UTC SCHEDULER (05 15 25 35 45 55) =================
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
      `Waiting until ${new Date(ts).toISOString()} (${Math.round(ms / 1000)}s)`
    );
    await sleep(ms);
  }
}

/**
 * ================= INSERT WITH CONCURRENCY =================
 */

async function insertWithConcurrency(table, chunks) {
  for (let i = 0; i < chunks.length; i += INSERT_CONCURRENCY) {
    const batch = chunks.slice(i, i + INSERT_CONCURRENCY);

    const results = await Promise.all(
      batch.map((chunk) => supabase.from(table).insert(chunk))
    );

    // якщо щось впало — покажемо
    for (const r of results) {
      if (r?.error) throw r.error;
    }
  }
}

/**
 * ================= SCRAPER =================
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
      if (attempt === MAX_ATTEMPTS) return [];
      await sleep(3000);
    }
  }
}

/**
 * ================= SNAPSHOT REGION =================
 */

async function runSnapshotForRegion({ cc, ts }) {
  log(`Region ${cc}: start`);

  let rows = [];
  let rankRef = { value: 1 };

  for (let page = 1; page <= MAX_PAGES; page++) {
    await sleep(PAGE_DELAY_MS);

    const pageRows = await scrapePage({ cc, page, ts, rankRef });
    if (pageRows.length === 0) break;

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
  log(`SNAPSHOT start ts=${ts}`);

  let historyRows = [];
  let currentRows = [];

  for (let i = 0; i < REGIONS.length; i += CONCURRENCY) {
    const batch = REGIONS.slice(i, i + CONCURRENCY);
    const results = await Promise.all(
      batch.map((r) => runSnapshotForRegion({ cc: r.cc, ts }))
    );

    for (let j = 0; j < results.length; j++) {
      const res = results[j];
      if (!res) continue;

      const cc = batch[j].cc;

      historyRows.push(...res.rows);

      currentRows.push(
        ...res.unique.map((r) => ({
          appid: r.appid,
          cc,
          rank: r.rank,
          updated_ts: ts,
        }))
      );

      // pages — ОДНА країна (без merge)
      const upd = await supabase
        .from("steam_topsellers_pages_region")
        .update({
          total_pages: res.totalPages,
          updated_ts: ts,
        })
        .eq("cc", cc);

      // якщо рядка ще нема — тоді upsert (бо update нічого не оновить)
      if (!upd.error && Array.isArray(upd.data) && upd.data.length === 0) {
        const ins = await supabase
          .from("steam_topsellers_pages_region")
          .upsert(
            { cc, total_pages: res.totalPages, updated_ts: ts },
            { onConflict: "cc" }
          );
        if (ins.error) throw ins.error;
      } else if (upd.error) {
        throw upd.error;
      }
    }
  }

  // ===== INSERT HISTORY (concurrency=2) =====
  const historyChunks = [];
  for (let i = 0; i < historyRows.length; i += CHUNK_SIZE) {
    historyChunks.push(historyRows.slice(i, i + CHUNK_SIZE));
  }

  await insertWithConcurrency("steam_topsellers_history_region", historyChunks);

  // ===== UPSERT CURRENT =====
  const cur = await supabase
    .from("steam_topsellers_current_region")
    .upsert(currentRows, { onConflict: "appid,cc" });

  if (cur.error) throw cur.error;

  log("SNAPSHOT done");
}

/**
 * ================= LOOP =================
 */

async function main() {
  let running = false;

  while (true) {
    const next = getNextRunAtUTC();
    await sleepUntil(next);

    if (running) continue;
    running = true;

    try {
      await runSnapshot();
    } catch (e) {
      log(`SNAPSHOT failed: ${e.message}`);
    } finally {
      running = false;
    }
  }
}

main().catch(console.error);
