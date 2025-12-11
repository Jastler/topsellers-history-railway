import fetch from "node-fetch";
import fs from "fs";
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

// GP API
const API_ROOT = "https://games-popularity.com/swagger/api/game/players";
const PROGRESS_FILE = "ccu_progress.json";

const CUTOFF_TS = 1765422000; // не беремо новіші
const FORWARD_WINDOW = 2 * 3600; // 2 годин уперед
const BASE_DELAY_MS = 150;

// =====================================================
// MASTER TIMESTAMPS
// =====================================================
let MASTER_TS = [];

// =====================================================
// FS UTILS
// =====================================================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function loadProgress() {
  if (!fs.existsSync(PROGRESS_FILE)) return { index: 0, cursor: "0" };
  return JSON.parse(fs.readFileSync(PROGRESS_FILE, "utf8"));
}

function saveProgress(obj) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(obj, null, 2));
}

// =====================================================
// FORWARD-SEARCH: знайти raw_ts > master_ts у межах 5h
// =====================================================
function findNearestForward(masterTs, rawList) {
  const limit = masterTs + FORWARD_WINDOW;

  // кандидат — кожен raw_ts >= master_ts
  const candidates = rawList.filter((t) => t >= masterTs && t <= limit);

  if (candidates.length === 0) return null;

  // беремо найменший (найближчий уперед)
  candidates.sort((a, b) => a - b);
  return candidates[0];
}

// =====================================================
// LOAD MASTER TIMESTAMPS
// =====================================================
async function loadMasterTimestamps() {
  console.log("📥 Loading master timestamps...");

  let out = [];
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

    out.push(...data);

    if (data.length < size) break;
    from += size;
  }

  MASTER_TS = out.map((x) => x.ts).filter((ts) => ts < CUTOFF_TS);
  MASTER_TS.sort((a, b) => a - b);

  console.log(`✔ Loaded ${MASTER_TS.length} master timestamps`);
}

// =====================================================
// FETCH PAGE FROM GP
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
        console.log(`⚠ HTTP ${res.status} → retry`);
        await sleep(3000);
        continue;
      }

      return await res.json();
    } catch (err) {
      console.log("⚠ NET ERROR:", err.message);
      await sleep(5000);
    }
  }
}

// =====================================================
// INSERT CCU ROWS
// =====================================================
async function insertCcuRows(appid, items) {
  if (!items?.length) return 0;

  // будуємо raw map
  const rawMap = new Map();
  const rawList = [];

  for (const h of items) {
    const t = Math.floor(new Date(h.added).getTime() / 1000);
    if (t >= CUTOFF_TS) continue;
    rawMap.set(t, h.players ?? 0);
    rawList.push(t);
  }

  rawList.sort((a, b) => a - b);

  // тепер створюємо дані для кожного master_ts
  const rows = [];

  for (const masterTs of MASTER_TS) {
    // точний збіг
    if (rawMap.has(masterTs)) {
      rows.push({ appid, ts: masterTs, online: rawMap.get(masterTs) });
      continue;
    }

    // forward nearest (до 5 годин)
    const nearest = findNearestForward(masterTs, rawList);

    if (nearest !== null) {
      rows.push({ appid, ts: masterTs, online: rawMap.get(nearest) });
    } else {
      rows.push({ appid, ts: masterTs, online: 0 });
    }
  }

  console.log(
    `  • filled ${rows.length} rows (raw=${items.length}, raw_filtered=${rawList.length})`
  );

  const { error } = await supabaseAnalytics.from("steam_ccu").upsert(rows, {
    onConflict: "appid,ts",
    returning: "minimal",
  });

  if (error) console.log("⚠ UPSERT ERROR", error);

  return rows.length;
}

// =====================================================
// PROCESS ONE APP
// =====================================================
async function processApp(appid, cursor, index, total) {
  console.log(`\n=== APP ${appid} (${index + 1}/${total}) ===`);

  let allItems = [];

  while (true) {
    const data = await fetchPage(appid, cursor);

    if (data.skip) {
      console.log("  • no history");
      break;
    }

    if (data.history?.length) {
      allItems.push(...data.history);
    }

    if (!data.nextCursor) break;
    cursor = data.nextCursor;
    saveProgress({ index, cursor });

    await sleep(BASE_DELAY_MS);
  }

  const inserted = await insertCcuRows(appid, allItems);
  console.log(`✔ Done appid=${appid}, inserted=${inserted}`);
}

// =====================================================
// LOAD ALL APPIDS
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

    if (error) {
      console.error("❌ Load appids error:", error);
      process.exit(1);
    }

    out.push(...data);
    console.log(`Loaded ${out.length} appids`);

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
  const progress = loadProgress();

  let startIndex = progress.index ?? 0;
  let cursor = progress.cursor ?? "0";

  for (let i = startIndex; i < appids.length; i++) {
    const appid = appids[i];

    try {
      await processApp(appid, cursor, i, appids.length);
      saveProgress({ index: i + 1, cursor: "0" });
      cursor = "0";
    } catch (err) {
      console.error("❌ Crash on app:", appid, err);
      process.exit(1);
    }
  }

  console.log("=== DONE IMPORTING OLD CCU DATA ===");
}

main();
