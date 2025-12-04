import fetch from "node-fetch";
import fs from "fs";
import { createClient } from "@supabase/supabase-js";

// =====================================================
// CONFIG
// =====================================================
const SUPABASE_URL = "https://psztbppcuwnrbiguicdn.supabase.co";
const SUPABASE_SERVICE_ROLE =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBzenRicHBjdXducmJpZ3VpY2RuIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2Mjg1OTA4MiwiZXhwIjoyMDc4NDM1MDgyfQ.dl_mOJeJzvmaip_hr6LlyApMo5kzEXQklCE_ZNmhuWw";

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  db: { schema: "analytics" },
});

// Reviews API
const API_ROOT = "https://games-popularity.com/swagger/api/game/reviews";

// Прогрес (резюм)
const PROGRESS_FILE = "progress_reviews.json";

// Ти поставиш свою межу
const CUTOFF_TS = 1764779400; // не вставляємо новіші TS

const BASE_DELAY_MS = 150;
const MAX_BACKOFF_MS = 10 * 60 * 1000;

// =====================================================
// USER AGENTS
// =====================================================
const UA_POOL = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/123 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123) Gecko/20100101 Firefox/123.0",
  "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122) Gecko/20100101 Firefox/122.0",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2) AppleWebKit/605.1.15 Version/16.2 Safari/605.1.15",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Version/17.0 Mobile Safari/604.1",
  "Mozilla/5.0 (Linux; Android 14; Pixel 7 Pro) AppleWebKit/537.36 Chrome/122 Mobile Safari/537.36",
];

const randomUA = () => UA_POOL[Math.floor(Math.random() * UA_POOL.length)];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// =====================================================
// PROGRESS
// =====================================================
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

// =====================================================
// HELPERS
// =====================================================
function roundTo5Minutes(unix) {
  return Math.floor(unix / 300) * 300;
}

function dedupeRows(rows) {
  const seen = new Set();
  const out = [];

  for (const r of rows) {
    const key = `${r.appid}-${r.ts}`;
    if (!seen.has(key)) {
      seen.add(key);
      out.push(r);
    }
  }

  return out;
}

// =====================================================
// NETWORK (ANTIBAN)
// =====================================================
async function fetchPageSafe(appid, cursor) {
  let attempt = 0;

  while (true) {
    attempt++;
    try {
      const url = `${API_ROOT}/${appid}?cursor=${encodeURIComponent(cursor)}`;

      const res = await fetch(url, {
        headers: {
          "User-Agent": randomUA(),
          Accept: "*/*",
        },
      });

      if (res.ok) {
        return await res.json();
      }

      if (res.status === 404) {
        if (attempt >= 2) {
          console.log(`[404] App ${appid} — no reviews history`);
          return { history: [], nextCursor: null, _skipApp: true };
        }
        console.log(`[404] Retrying ${appid}...`);
        await sleep(2000);
        continue;
      }

      if (res.status === 429) {
        const wait = Math.min(attempt * 60000, MAX_BACKOFF_MS);
        console.log(`[BAN 429] wait ${wait / 1000}s`);
        await sleep(wait);
        continue;
      }

      if (res.status >= 500) {
        const wait = Math.min(attempt * 5000, 120000);
        console.log(`[SERVER ${res.status}] retry in ${wait / 1000}s`);
        await sleep(wait);
        continue;
      }

      throw new Error(`HTTP ${res.status}`);
    } catch (err) {
      const wait = Math.min(attempt * 3000, 120000);
      console.log(`[NET ERR] ${err.message} | retry ${wait / 1000}s`);
      await sleep(wait);
    }
  }
}

// =====================================================
// UPSERT REVIEWS HISTORY
// =====================================================
async function insertReviewRows(appid, items) {
  if (!items?.length) return;

  const rows = [];

  for (const h of items) {
    const tsOrig = Math.floor(new Date(h.added).getTime() / 1000);
    const tsRounded = roundTo5Minutes(tsOrig);

    if (tsRounded >= CUTOFF_TS) continue;

    rows.push({
      appid: Number(appid),
      ts: tsRounded,
      total_reviews: h.reviewsAll,
      total_positive: h.reviewsPositive,
    });
  }

  if (!rows.length) return;

  const clean = dedupeRows(rows);

  const { error } = await supabase.from("steam_reviews_history").upsert(clean, {
    onConflict: "appid,ts",
    ignoreDuplicates: true,
    returning: "minimal",
  });

  if (error) console.error("UPSERT reviews error:", error);
}

// =====================================================
// LOAD ALL APPIDS
// =====================================================
async function loadAllAppIds() {
  let out = [];
  let from = 0;
  const size = 1000;

  while (true) {
    const { data, error } = await supabase
      .schema("public")
      .from("steam_app_details")
      .select("appid")
      .order("appid", { ascending: true })
      .range(from, from + size - 1);

    if (error) {
      console.error("Error loading appids:", error);
      process.exit(1);
    }

    out.push(...data);
    if (data.length < size) break;
    from += size;
  }

  return out.map((x) => x.appid);
}

// =====================================================
// PROCESS 1 APP
// =====================================================
async function processApp(appid, startCursor, index, total) {
  console.log(`\n=== APP ${appid} (${index + 1}/${total}) ===`);

  let cursor = startCursor;
  let pages = 0;

  while (true) {
    const data = await fetchPageSafe(appid, cursor);

    if (data._skipApp) {
      console.log(`Skip ${appid} — no reviews`);
      return;
    }

    if (Array.isArray(data.history) && data.history.length > 0) {
      await insertReviewRows(appid, data.history);
    }

    pages++;
    saveProgress({ appIndex: index, cursor });

    if (!data.nextCursor) {
      console.log(`Done ${appid}, pages=${pages}`);
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
  console.log("Resume from:", progress);

  for (let i = progress.appIndex; i < appids.length; i++) {
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

  console.log("\nALL REVIEWS IMPORTED");
  saveProgress({ appIndex: appids.length, cursor: "0" });
}

main();
