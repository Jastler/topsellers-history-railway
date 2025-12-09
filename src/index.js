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
  console.error("❌ SUPABASE_URL / SUPABASE_SERVICE_ROLE not set");
  process.exit(1);
}

// analytics client (ціни)
const supabaseAnalytics = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  db: { schema: "analytics" },
});

// public client (appid-список)
const supabasePublic = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  db: { schema: "public" },
});

// endpoint історії цін
const API_ROOT = "https://games-popularity.com/swagger/api/game/price";

// файл прогресу
const PROGRESS_FILE = "progress_price.json";

// ✸ ВАЖЛИВО: беремо ТІЛЬКИ ЗАПИСИ СТАРІШІ, НІЖ CUTOFF
//    вставляємо, якщо ts < CUTOFF_TS
const CUTOFF_TS = 1765189200; // наприклад 2025-12-08 20:00:00 UTC

const BASE_DELAY_MS = 150;

// =====================================================
// UTILS
// =====================================================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function roundTo5Minutes(unixSeconds) {
  return Math.floor(unixSeconds / 300) * 300;
}

function dedupeRows(rows) {
  const map = new Map();
  for (const r of rows) {
    map.set(`${r.appid}-${r.ts}`, r);
  }
  return [...map.values()];
}

function loadProgress() {
  if (!fs.existsSync(PROGRESS_FILE)) return { index: 0, cursor: "0" };
  try {
    return JSON.parse(fs.readFileSync(PROGRESS_FILE, "utf8"));
  } catch {
    return { index: 0, cursor: "0" };
  }
}

function saveProgress(obj) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(obj, null, 2));
}

// =====================================================
// FETCH PAGE
// =====================================================
async function fetchPage(appid, cursor) {
  const url = `${API_ROOT}/${appid}?cursor=${encodeURIComponent(cursor)}`;

  while (true) {
    try {
      const res = await fetch(url, {
        headers: {
          "User-Agent": "Mozilla/5.0",
          Accept: "*/*",
        },
      });

      if (res.status === 404) {
        // немає історії
        return { history: [], nextCursor: null, skip: true };
      }

      if (res.status === 429) {
        console.log(`  ⚠ 429 Too Many Requests → wait 60s`);
        await sleep(60000);
        continue;
      }

      if (!res.ok) {
        console.log(`  ⚠ HTTP ${res.status} → retry 3s`);
        await sleep(3000);
        continue;
      }

      return await res.json();
    } catch (err) {
      console.log(`  ⚠ NET ERROR: ${err.message} → retry 5s`);
      await sleep(5000);
    }
  }
}

// =====================================================
// INSERT PRICE HISTORY (only earliest record per price block)
// and ONLY ts < CUTOFF_TS
// =====================================================
async function insertPriceRows(appid, items) {
  if (!items?.length) return 0;

  // історія з API йде "від нових до старих" → розвертаємо
  const sorted = [...items].sort(
    (a, b) => new Date(a.added).getTime() - new Date(b.added).getTime()
  );

  let lastPrice = null;
  const rows = [];

  for (const h of sorted) {
    const ts = Math.floor(new Date(h.added).getTime() / 1000);

    // 🔥 беремо тільки СТАРІШІ за CUTOFF
    if (ts >= CUTOFF_TS) continue;

    const priceCents = Math.round(h.price * 100);

    // якщо ціна така ж як попередня → пропускаємо
    if (priceCents === lastPrice) {
      continue;
    }
    lastPrice = priceCents;

    const tsRounded = roundTo5Minutes(ts);

    rows.push({
      appid,
      ts: tsRounded,
      price_currency: "USD", // у GP все в USD, ти потім можеш розширити
      price_initial: priceCents,
      price_final: priceCents,
      price_discount: 0,
    });
  }

  const unique = dedupeRows(rows);

  console.log(
    `  • history items=${items.length}, written_unique=${unique.length}`
  );

  if (!unique.length) return 0;

  const { error } = await supabaseAnalytics
    .from("steam_price_history")
    .upsert(unique, {
      onConflict: "appid,ts",
      returning: "minimal",
    });

  if (error) {
    console.log(`  ⚠ UPSERT ERROR`, error);
    return 0;
  }

  return unique.length;
}

// =====================================================
// PROCESS ONE APP
// =====================================================
async function processApp(appid, cursor, index, total) {
  const start = Date.now();
  const pct = (((index + 1) / total) * 100).toFixed(2);

  console.log(`\n=== APP ${appid} (${index + 1}/${total}, ${pct}%) ===`);

  let pages = 0;
  let inserted = 0;

  while (true) {
    const data = await fetchPage(appid, cursor);
    pages++;

    if (data.skip) {
      console.log(`  • No price history (404)`);
      break;
    }

    if (data.history?.length) {
      const added = await insertPriceRows(appid, data.history);
      inserted += added;
    }

    if (!data.nextCursor) {
      break;
    }

    cursor = data.nextCursor;
    saveProgress({ index, cursor });
    await sleep(BASE_DELAY_MS);
  }

  const sec = ((Date.now() - start) / 1000).toFixed(1);
  console.log(
    `✔ Done appid=${appid}, pages=${pages}, inserted=${inserted}, time=${sec}s`
  );
}

// =====================================================
// LOAD ALL APPIDS FROM public.steam_app_details
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
      console.error("❌ Error load appids:", error);
      process.exit(1);
    }

    out.push(...data);
    console.log(out.length);
    if (data.length < size) break;

    from += size;
  }

  return out.map((x) => x.appid);
}

// =====================================================
// MAIN
// =====================================================
async function main() {
  const appids = await loadAllAppIds();
  const progress = loadProgress();

  let startIndex = progress.index ?? 0;
  let cursor = progress.cursor ?? "0";

  console.log(`Loaded ${appids.length} appids`);
  console.log(`Resume from index=${startIndex}, cursor=${cursor}`);
  console.log(`CUTOFF_TS=${CUTOFF_TS} (ts >= CUTOFF → SKIP)`);

  for (let i = startIndex; i < appids.length; i++) {
    const appid = appids[i];

    try {
      await processApp(appid, cursor, i, appids.length);
      // після успішного app — переходимо до наступного, курсор скидаємо
      saveProgress({ index: i + 1, cursor: "0" });
      cursor = "0";
    } catch (err) {
      console.error(`❌ App ${appid} crashed:`, err);
      console.error("Safe exit — you can restart later.");
      process.exit(1);
    }
  }

  console.log("\n=== ALL PRICE HISTORY IMPORTED (OLD DATA) ===");
  saveProgress({ index: appids.length, cursor: "0" });
}

main();
