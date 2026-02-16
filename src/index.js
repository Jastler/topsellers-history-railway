import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";
import "dotenv/config";

const STEAM_KEY = process.env.STEAM_KEY;
const API_BASE = "https://api.steampowered.com/IStoreQueryService/Query/v1/";

const BATCH_SIZE = 1000;
const TOTAL_PER_REGION = 10000;
const DELAY_BETWEEN_PAGES_MS = 10000;

const CHUNK_SIZE = 500;
const MIN_VALID_ITEMS_REGION = 500;

const REGION_GROUPS = [
  ["at", "au", "be", "br", "ca", "ch"],
  ["cn", "cz", "de", "dk", "es", "fi"],
  ["fr", "gb", "hk", "it", "jp", "kr"],
  ["nl", "no", "nz", "pl", "ru", "se"],
  ["sg", "th", "tr", "tw", "us"],
];
const ALL_CC = REGION_GROUPS.flat();

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


function buildInput(cc, start) {
  return {
    query: {
      start,
      count: BATCH_SIZE,
      sort: 11,
      filters: { regional_top_n_sellers: TOTAL_PER_REGION },
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

async function processRegion(cc, ts) {
  try {
    const res = await runRegion({ cc, ts });
    if (!res) return null;

    await clearCurrentRegion(cc);
    await insertChunked("steam_topsellers_current_region", res.current);

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

    return {
      history: res.history,
      pages: [{ cc, total_pages: res.totalPages, updated_ts: ts }],
    };
  } catch (e) {
    log(`Region ${cc} failed: ${e?.message ?? e}`);
    return null;
  }
}

function getSnapshotTs() {
  const now = new Date();
  const roundHour = new Date(
    Date.UTC(
      now.getUTCFullYear(),
      now.getUTCMonth(),
      now.getUTCDate(),
      now.getUTCHours(),
      0,
      0,
      0
    )
  );
  return Math.floor(roundHour.getTime() / 1000);
}

async function runSnapshot() {
  const ts = getSnapshotTs();
  const ccs = ALL_CC;

  log(`===== SNAPSHOT START ts=${ts} | ${ccs.length} regions in parallel =====`);

  const results = await Promise.all(ccs.map((cc) => processRegion(cc, ts)));

  let history = [];
  let pages = [];
  for (const r of results) {
    if (r) {
      history.push(...r.history);
      pages.push(...r.pages);
    }
  }

  if (history.length) {
    await insertChunked("steam_topsellers_history_region", history);
    await upsertPages(pages);
  }

  log(`===== SNAPSHOT DONE ts=${ts} =====`);
}

function getNextNiceHourUTC() {
  const d = new Date();
  d.setUTCHours(d.getUTCHours() + 1, 0, 0, 0);
  return d;
}

async function waitUntil(t) {
  const ms = t.getTime() - Date.now();
  log(`Waiting until ${t.toISOString()} (${(ms / 1000).toFixed(1)}s)`);
  if (ms > 0) await sleep(ms);
}

async function main() {
  if (!STEAM_KEY) {
    console.error("Missing STEAM_KEY");
    process.exit(1);
  }

  log("First run immediately");
  try {
    await runSnapshot();
  } catch (e) {
    log("❌ SNAPSHOT FAILED: " + (e?.message ?? e));
  }

  while (true) {
    const nextRun = getNextNiceHourUTC();
    await waitUntil(nextRun);
    try {
      await runSnapshot();
    } catch (e) {
      log("❌ SNAPSHOT FAILED: " + (e?.message ?? e));
    }
  }
}

main().catch(console.error);
