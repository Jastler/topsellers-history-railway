import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

// =====================================================
// CONFIG
// =====================================================
const SUPABASE_URL = "https://psztbppcuwnrbiguicdn.supabase.co";
const SUPABASE_SERVICE_ROLE =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBzenRicHBjdXducmJpZ3VpY2RuIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Mjg1OTA4MiwiZXhwIjoyMDc4NDM1MDgyfQ.dl_mOJeJzvmaip_hr6LlyApMo5kzEXQklCE_ZNmhuWw";

const supabaseAnalytics = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  db: { schema: "analytics" },
});

const supabasePublic = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  db: { schema: "public" },
});

const API_ROOT = "https://games-popularity.com/swagger/api/game/players";

// ❗ ЖОРСТКИЙ СТАРТ
const START_INDEX = 71655;

// ❗ НЕ беремо новіші
const CUTOFF_TS = 1765422000;

// forward-fill максимум 5 годин
const FORWARD_WINDOW = 5 * 3600;

// throttle
const BASE_DELAY_MS = 150;

// =====================================================
// MASTER TIMESTAMPS
// =====================================================
let MASTER_TS = [];

// =====================================================
// UTILS
// =====================================================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// =====================================================
// FORWARD SEARCH (тільки в майбутнє ≤ 5h)
// =====================================================
function findNearestForward(masterTs, rawList) {
  const limit = masterTs + FORWARD_WINDOW;

  // rawList відсортований
  for (const t of rawList) {
    if (t < masterTs) continue;
    if (t > limit) break;
    return t; // перший валідний — найближчий
  }

  return null;
}

// =====================================================
// LOAD MASTER TIMESTAMPS
// =====================================================
async function loadMasterTimestamps() {
  console.log("📥 Loading master timestamps...");

  let all = [];
  let from = 0;
  const size = 1000;

  while (true) {
    const { data, error } = await supabaseAnalytics
      .from("steam_ccu_timestamps")
      .select("ts")
      .order("ts")
      .range(from, from + size - 1);

    if (error) {
      console.error("❌ timestamp load error:", error);
      process.exit(1);
    }

    all.push(...data);
    if (data.length < size) break;
    from += size;
  }

  MASTER_TS = all
    .map((x) => x.ts)
    .filter((ts) => ts < CUTOFF_TS)
    .sort((a, b) => a - b);

  console.log(`✔ Loaded ${MASTER_TS.length} master timestamps`);
}

// =====================================================
// FETCH PAGE
// =====================================================
async function fetchPage(appid, cursor) {
  const url = `${API_ROOT}/${appid}?cursor=${encodeURIComponent(cursor)}`;

  while (true) {
    try {
      const res = await fetch(url, {
        headers: { "User-Agent": "Mozilla/5.0" },
      });

      if (res.status === 404)
        return { skip: true, history: [], nextCursor: null };

      if (res.status === 429) {
        console.log("⚠ 429 → wait 60s");
        await sleep(60000);
        continue;
      }

      if (!res.ok) {
        await sleep(3000);
        continue;
      }

      return await res.json();
    } catch {
      await sleep(5000);
    }
  }
}

// =====================================================
// INSERT CCU (NO ZEROES)
// =====================================================
async function insertCcuRows(appid, items) {
  if (!items?.length) return 0;

  const rawMap = new Map();
  const rawList = [];

  for (const h of items) {
    const ts = Math.floor(new Date(h.added).getTime() / 1000);
    if (ts >= CUTOFF_TS) continue;

    const online = h.players ?? 0;
    if (online <= 0) continue; // ❗ НІКОЛИ не пишемо 0

    rawMap.set(ts, online);
    rawList.push(ts);
  }

  if (rawList.length === 0) return 0;

  rawList.sort((a, b) => a - b);

  const rows = [];

  for (const masterTs of MASTER_TS) {
    // точний збіг
    if (rawMap.has(masterTs)) {
      rows.push({ appid, ts: masterTs, online: rawMap.get(masterTs) });
      continue;
    }

    // forward-fill
    const nearest = findNearestForward(masterTs, rawList);
    if (nearest !== null) {
      rows.push({ appid, ts: masterTs, online: rawMap.get(nearest) });
    }
    // ❗ інакше — нічого
  }

  if (rows.length === 0) return 0;

  const { error } = await supabaseAnalytics
    .from("steam_ccu")
    .upsert(rows, { onConflict: "appid,ts", returning: "minimal" });

  if (error) console.error("❌ UPSERT ERROR", error);

  return rows.length;
}

// =====================================================
// PROCESS ONE APP
// =====================================================
async function processApp(appid, index, total) {
  console.log(`\n=== APP ${appid} (${index}/${total}) ===`);

  let cursor = "0";
  let allItems = [];

  while (true) {
    const data = await fetchPage(appid, cursor);
    if (data.skip) break;

    if (data.history?.length) allItems.push(...data.history);
    if (!data.nextCursor) break;

    cursor = data.nextCursor;
    await sleep(BASE_DELAY_MS);
  }

  const inserted = await insertCcuRows(appid, allItems);
  console.log(`✔ inserted ${inserted}`);
}

// =====================================================
// LOAD APPIDS
// =====================================================
async function loadAllAppIds() {
  let out = [];
  let from = 0;
  const size = 1000;

  while (true) {
    const { data, error } = await supabasePublic
      .from("steam_app_details")
      .select("appid")
      .order("appid")
      .range(from, from + size - 1);

    if (error) process.exit(1);
    out.push(...data);

    if (data.length < size) break;
    from += size;
  }

  return out.map((x) => x.appid);
}

// =====================================================
// MAIN
// =====================================================
async function main() {
  await loadMasterTimestamps();

  const appids = await loadAllAppIds();
  console.log(`🚀 START FROM INDEX ${START_INDEX}`);

  for (let i = START_INDEX; i < appids.length; i++) {
    await processApp(appids[i], i + 1, appids.length);
  }

  console.log("✅ DONE");
}

main().catch(console.error);
