import fetch from "node-fetch";
import fs from "fs";
import path from "path";
import "dotenv/config";

const STEAM_KEY = "0883900C87B2A00C634405E2004B4885";
const API_BASE = "https://api.steampowered.com/IStoreQueryService/Query/v1/";

const REGION_GROUPS = [
  ["at", "au", "be", "br", "ca", "ch"],
  ["cn", "cz", "de", "dk", "es", "fi"],
  ["fr", "gb", "hk", "it", "jp", "kr"],
  ["nl", "no", "nz", "pl", "ru", "se"],
  ["sg", "th", "tr", "tw", "us"],
];

const ALL_CC = REGION_GROUPS.flat();

function buildInput(cc, start = 0, count = 1000) {
  return {
    query: {
      start,
      count,
      sort: 11,
      filters: { regional_top_n_sellers: count },
    },
    context: { language: "en", country_code: cc.toUpperCase() },
    data_request: { include_basic_info: true },
  };
}

async function fetchPage(cc) {
  const input = buildInput(cc);
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
  if (!res.ok)
    throw new Error(`${cc}: HTTP ${res.status} ${text.slice(0, 200)}`);
  const data = JSON.parse(text);
  const items = data?.response?.store_items ?? [];
  return items
    .map((it, i) => ({ appid: it.appid ?? it.id, rank: i + 1 }))
    .filter((r) => r.appid);
}

function compareTwo(ccA, listA, ccB, listB) {
  const setA = new Set(listA.map((r) => r.appid));
  const setB = new Set(listB.map((r) => r.appid));
  const overlap = listA.filter((r) => setB.has(r.appid)).length;
  const onlyA = listA.filter((r) => !setB.has(r.appid)).length;
  const onlyB = listB.filter((r) => !setA.has(r.appid)).length;
  const rankDeltas = [];
  const byAppB = new Map(listB.map((r) => [r.appid, r.rank]));
  for (const r of listA) {
    const rankB = byAppB.get(r.appid);
    if (rankB != null) rankDeltas.push(rankB - r.rank);
  }
  const sameOrder = rankDeltas.every((d) => d === 0);
  const avgDelta = rankDeltas.length
    ? rankDeltas.reduce((a, b) => a + b, 0) / rankDeltas.length
    : 0;
  return {
    overlap,
    onlyA,
    onlyB,
    totalA: listA.length,
    totalB: listB.length,
    sameOrder,
    avgRankDelta: Math.round(avgDelta * 100) / 100,
    overlapPct: listA.length ? Math.round((overlap / listA.length) * 100) : 0,
  };
}

async function main() {
  if (!STEAM_KEY) {
    console.error("Missing STEAM_KEY in .env");
    process.exit(1);
  }

  const outDir = path.join(process.cwd(), "output");
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  console.log(
    `[${new Date().toISOString()}] Fetching first 1000 for ${ALL_CC.length} regions in parallel...`,
  );

  const start = Date.now();
  const results = await Promise.all(
    ALL_CC.map(async (cc) => {
      try {
        const list = await fetchPage(cc);
        return { cc, list, err: null };
      } catch (e) {
        return { cc, list: [], err: e.message };
      }
    }),
  );
  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`[${new Date().toISOString()}] Done in ${elapsed}s.`);

  const byCc = {};
  for (const { cc, list, err } of results) {
    byCc[cc] = err ? { error: err } : { count: list.length, items: list };
  }

  const rawPath = path.join(outDir, `regions-first1000-${ts}.json`);
  fs.writeFileSync(rawPath, JSON.stringify(byCc, null, 2), "utf8");
  console.log(`Saved raw: ${rawPath}`);

  const failed = results.filter((r) => r.err);
  if (failed.length) {
    console.log(
      "Failed regions:",
      failed.map((r) => `${r.cc}: ${r.err}`).join(", "),
    );
  }

  const ok = results.filter((r) => !r.err && r.list.length > 0);
  if (ok.length < 2) {
    console.log("Not enough regions to compare.");
    return;
  }

  const baseline = ok[0];
  const comparisons = [];
  for (let i = 0; i < ok.length; i++) {
    for (let j = i + 1; j < ok.length; j++) {
      const a = ok[i];
      const b = ok[j];
      const cmp = compareTwo(a.cc, a.list, b.cc, b.list);
      comparisons.push({
        A: a.cc,
        B: b.cc,
        ...cmp,
      });
    }
  }

  const summary = {
    fetchedAt: new Date().toISOString(),
    regionsTotal: ALL_CC.length,
    regionsOk: ok.length,
    regionsFailed: failed.length,
    perRegionCount: 1000,
    comparisons: comparisons,
    vsUS: (() => {
      const us = ok.find((x) => x.cc === "us");
      if (!us) return [];
      return ok
        .filter((r) => r.cc !== "us")
        .map((r) => ({
          region: r.cc,
          ...compareTwo("us", us.list, r.cc, r.list),
        }));
    })(),
  };

  const comparePath = path.join(outDir, `regions-compare-${ts}.json`);
  fs.writeFileSync(comparePath, JSON.stringify(summary, null, 2), "utf8");
  console.log(`Saved compare: ${comparePath}`);

  console.log("\n--- Overlap % (vs US) ---");
  for (const v of summary.vsUS) {
    console.log(
      `  ${v.region}: overlap ${v.overlapPct}%, sameOrder=${v.sameOrder}, avgRankDelta=${v.avgRankDelta}`,
    );
  }
  console.log("\n--- Pairwise same order? ---");
  const sameOrderPairs = comparisons.filter((c) => c.sameOrder);
  const diffOrderPairs = comparisons.filter((c) => !c.sameOrder);
  console.log(`  Same order: ${sameOrderPairs.length} pairs`);
  console.log(`  Different order: ${diffOrderPairs.length} pairs`);
  if (diffOrderPairs.length) {
    diffOrderPairs
      .slice(0, 10)
      .forEach((c) =>
        console.log(
          `    ${c.A} vs ${c.B}: overlap ${c.overlapPct}%, avgDelta=${c.avgRankDelta}`,
        ),
      );
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
