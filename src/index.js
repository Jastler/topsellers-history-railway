import fetch from "node-fetch";
import fs from "fs";
import { createClient } from "@supabase/supabase-js";

// =====================================================
// CONFIG
// =====================================================
const SUPABASE_URL = "https://psztbppcuwnrbiguicdn.supabase.co";
const SUPABASE_SERVICE_ROLE =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBzenRicHBjdXducmJpZ3VpY2RuIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Mjg1OTA4MiwiZXhwIjoyMDc4NDM1MDgyfQ.dl_mOJeJzvmaip_hr6LlyApMo5kzEXQklCE_ZNmhuWw";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  db: { schema: "analytics" },
});

const API_ROOT = "https://games-popularity.com/swagger/api/game/top-wishlist";

const PROGRESS_FILE = "progress_wishlist.json";

const BASE_DELAY_MS = 150;
const MAX_BACKOFF_MS = 10 * 60 * 1000;

// NEW CUTOFF: Kyiv 2025-11-26 12:00 => UTC 10:00
// unix = 1764151200
const CUTOFF_TS = 1764151200;

// =====================================================
// USER-AGENTS
// =====================================================
const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/123 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123) Gecko/20100101 Firefox/123.0",
  "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122) Gecko/20100101 Firefox/122.0",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2) AppleWebKit/605.1.15 Version/16.2 Safari/605.1.15",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Version/17.0 Mobile Safari/604.1",
  "Mozilla/5.0 (Linux; Android 14; Pixel 7 Pro) AppleWebKit/537.36 Chrome/122 Mobile Safari/537.36",
];

function randomUA() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

// =====================================================
// HELPERS
// =====================================================
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function loadProgress() {
  if (!fs.existsSync(PROGRESS_FILE)) return { appIndex: 0, cursor: "0" };
  try {
    return JSON.parse(fs.readFileSync(PROGRESS_FILE, "utf8"));
  } catch {
    return { appIndex: 0, cursor: "0" };
  }
}

function saveProgress(obj) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(obj, null, 2));
}

function roundTo5Minutes(unixTs) {
  return Math.ceil(unixTs / 300) * 300;
}

// =====================================================
// NETWORK WITH ANTI-BAN + 404 SKIP
// =====================================================
async function fetchPageSafe(appid, cursor) {
  let attempt = 0;

  while (true) {
    attempt++;

    try {
      const url = `${API_ROOT}/${appid}?cursor=${encodeURIComponent(cursor)}`;
      const ua = randomUA();

      const res = await fetch(url, {
        headers: { "User-Agent": ua, accept: "*/*" },
      });

      if (res.ok) return await res.json();

      // NO DATA → skip app
      if (res.status === 404) {
        if (attempt >= 2) {
          console.log(`[404] App ${appid}. Skip.`);
          return { history: [], nextCursor: null, _skipApp: true };
        }
        console.log(`[404] verify ${appid}...`);
        await sleep(2000);
        continue;
      }

      // BAN
      if (res.status === 429) {
        const wait = Math.min(60000 * attempt, MAX_BACKOFF_MS);
        console.log(`[BAN] 429 for ${appid}. Wait ${wait / 1000}s`);
        await sleep(wait);
        continue;
      }

      if (res.status >= 500) {
        const wait = Math.min(5000 * attempt, 120000);
        console.log(`[SERVER] ${res.status}. Wait ${wait / 1000}s`);
        await sleep(wait);
        continue;
      }

      throw new Error(`HTTP ${res.status}`);
    } catch (err) {
      const wait = Math.min(3000 * attempt, 120000);
      console.log(
        `[NET] ${err.message} for ${appid}, cursor=${cursor}. Retry ${
          wait / 1000
        }s`
      );
      await sleep(wait);
    }
  }
}

// =====================================================
// UPSERT ROWS
// =====================================================
async function insertRows(appid, items) {
  if (!items || !items.length) return;

  const rows = [];

  for (const h of items) {
    const tsOrig = Math.floor(new Date(h.added).getTime() / 1000);
    const tsRounded = roundTo5Minutes(tsOrig);

    if (tsRounded >= CUTOFF_TS) continue;

    rows.push({
      appid: Number(appid),
      rank: h.position,
      ts: tsRounded,
    });
  }

  if (!rows.length) return;

  const { error } = await supabase.from("steam_wishlist_history").upsert(rows, {
    onConflict: "appid,ts,rank",
    ignoreDuplicates: true,
    returning: "minimal",
  });

  if (error) console.error("UPSERT error:", error);
}

// =====================================================
// LOAD ALL APP IDS
// =====================================================
async function loadAllAppIds() {
  let all = [];
  let from = 0;
  const pageSize = 1000;

  while (true) {
    const { data, error } = await supabase
      .schema("public")
      .from("steam_app_details")
      .select("appid")
      .range(from, from + pageSize - 1);

    if (error) {
      console.error("Error loading appids:", error);
      process.exit(1);
    }

    all.push(...data);

    if (data.length < pageSize) break;
    from += pageSize;
  }

  return all.map((r) => r.appid);
}

// =====================================================
// PROCESS ONE APP
// =====================================================
async function processApp(appid, startCursor, appIndex, totalApps) {
  console.log(`\n=== APP ${appid} (${appIndex + 1}/${totalApps}) ===`);

  let cursor = startCursor;
  let pages = 0;

  while (true) {
    const data = await fetchPageSafe(appid, cursor);

    if (data._skipApp) {
      console.log(`Skip app ${appid}. No wishlist history.`);
      return;
    }

    if (Array.isArray(data.history) && data.history.length > 0) {
      await insertRows(appid, data.history);
    }

    pages++;
    saveProgress({ appIndex, cursor });

    if (!data.nextCursor) {
      console.log(`Done app ${appid}, pages=${pages}`);
      return;
    }

    cursor = data.nextCursor;
    await sleep(BASE_DELAY_MS);
  }
}

// =====================================================
// MAIN
// =====================================================
async function main() {
  const appids = await loadAllAppIds();
  const progress = loadProgress();

  console.log(`Loaded ${appids.length} appids`);
  console.log("Resume:", progress);

  // ALWAYS START FROM ZERO FOR WISHLIST
  const resumeIndex = progress.appIndex;

  for (let i = resumeIndex; i < appids.length; i++) {
    const appid = appids[i];

    try {
      await processApp(appid, progress.cursor, i, appids.length);

      saveProgress({ appIndex: i + 1, cursor: "0" });
    } catch (err) {
      console.error(`App ${appid} crashed: ${err.message}`);
      console.error("Safe exit. Restart to resume.");
      process.exit(1);
    }
  }

  console.log("\n=== ALL DONE ===");
  saveProgress({ appIndex: appids.length, cursor: "0" });
}

main();
