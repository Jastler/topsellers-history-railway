import fetchOrig from "node-fetch";
import fetchCookie from "fetch-cookie";
import { CookieJar } from "tough-cookie";
import * as cheerio from "cheerio";
import { createClient } from "@supabase/supabase-js";
import { LoginSession, EAuthSessionGuardType, EAuthTokenPlatformType } from "steam-session";
import { generateAuthCode } from "steam-totp";
import "dotenv/config";

/* ================= COOKIE FETCH (для adult / age-check) ================= */
const jar = new CookieJar();
const fetch = fetchCookie(fetchOrig, jar);

/**
 * ================= CONFIG =================
 */

const BASE_URL =
  "https://store.steampowered.com/search/results/?query&count=100&ignore_preferences=1";

const MAX_PAGES = 100;
const FRONT_PAGE_SIZE = 10;
const PAGE_DELAY_MS = 30;

const TIMEOUT_MS = 40000;
const MAX_ATTEMPTS = 6;
const CHUNK_SIZE = 500;

const MIN_VALID_ITEMS_REGION = 500;

/**
 * 🔁 REGION ROTATION GROUPS (10 хв / група)
 */
const REGION_GROUPS = [
  ["us", "gb", "de", "fr", "pl", "ru"],
  ["uk", "tr", "es", "it", "nl", "th"],
  ["ca", "au", "jp", "kr", "br", "nz"],
  ["se", "dk", "no", "fi", "ch", "tw"],
  ["at", "be", "cz", "hk", "sg"],
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
 * ================= COOKIES (повний набір для adult) =================
 */
const STEAM_LOGIN_SECURE_FALLBACK =
  "76561198052716339%7C%7CeyAidHlwIjogIkpXVCIsICJhbGciOiAiRWREU0EiIH0.eyAiaXNzIjogInI6MDAwRV8yN0FDMjdCMl83Mjg0OCIsICJzdWIiOiAiNzY1NjExOTgwNTI3MTYzMzkiLCAiYXVkIjogWyAid2ViOnN0b3JlIiBdLCAiZXhwIjogMTc3MDk5MTIyNCwgIm5iZiI6IDE3NjIyNjQzMzcsICJpYXQiOiAxNzcwOTA0MzM3LCAianRpIjogIjAwMENfMjdCNkREQ0RfQTM5QzIiLCAib2F0IjogMTc3MDYzNDA1MiwgInJ0X2V4cCI6IDE3ODg5MDM1MzQsICJwZXIiOiAwLCAiaXBfc3ViamVjdCI6ICI3OS4xMTAuMTI5LjY5IiwgImlwX2NvbmZpcm1lciI6ICI3OS4xMTAuMTI5LjY5IiB9.HcW_tvaUsgjW-n9N1zd4xcKOnOokCoEFWioIDY8H1yBv3yfZ12WiGSw3OfIEKH-3_cxJcP0EmRksCJdTU1JCBQ";

let currentSteamLoginSecure = STEAM_LOGIN_SECURE_FALLBACK;
let lastRefreshToken = null;

// TODO: перенести в env vars (Railway Variables)
const STEAM_CREDENTIALS = {
  username: process.env.STEAM_USERNAME || "jastle87",
  password: process.env.STEAM_PASSWORD || "Nfhfcxbhrjd1",
  sharedSecret: process.env.STEAM_SHARED_SECRET || "WUnt7AtHEoU542Q6gd3GarI6Zho=",
};

const STEAM_COOKIES_BASE = [
  { domain: "store.steampowered.com", name: "timezoneOffset", path: "/", secure: false, httpOnly: false, sameSite: "unspecified", value: "7200,0" },
  { domain: "store.steampowered.com", name: "bGameHighlightAudioEnabled", path: "/", secure: false, httpOnly: false, sameSite: "unspecified", value: "true" },
  { domain: "store.steampowered.com", name: "Steam_Language", path: "/", secure: true, httpOnly: false, sameSite: "no_restriction", value: "english" },
  { domain: "store.steampowered.com", name: "timezoneName", path: "/", secure: true, httpOnly: false, sameSite: "no_restriction", value: "Europe/Kiev" },
  { domain: "store.steampowered.com", name: "browserid", path: "/", secure: true, httpOnly: false, sameSite: "no_restriction", value: "409394601280791133" },
  { domain: "store.steampowered.com", name: "lastagecheckage", path: "/", secure: true, httpOnly: false, sameSite: "lax", value: "1-January-1970" },
  { domain: "store.steampowered.com", name: "birthtime", path: "/", secure: true, httpOnly: false, sameSite: "lax", value: "1" },
  { domain: "store.steampowered.com", name: "flGameHighlightPlayerVolume", path: "/", secure: false, httpOnly: false, sameSite: "unspecified", value: "10" },
  { domain: "store.steampowered.com", name: "steamLoginSecure", path: "/", secure: true, httpOnly: true, sameSite: "no_restriction", value: null },
  { domain: "store.steampowered.com", name: "sessionid", path: "/", secure: true, httpOnly: false, sameSite: "no_restriction", value: "fe6dec188564bf91f833f57d" },
  { domain: "store.steampowered.com", name: "steamCountry", path: "/", secure: true, httpOnly: true, sameSite: "no_restriction", value: "UA%7Cdcc52d0e2cd49e8d2b7a84ba6b93b099" },
  { domain: "store.steampowered.com", name: "recentapps", path: "/", secure: true, httpOnly: false, sameSite: "no_restriction", value: "%7B%221997410%22%3A1770916863%2C%223101040%22%3A1770916792%2C%221290000%22%3A1770916788%2C%22552990%22%3A1770916473%2C%222357570%22%3A1770904338%2C%222358720%22%3A1770740184%2C%22686060%22%3A1770740031%2C%22652150%22%3A1770718138%2C%222432860%22%3A1770717774%2C%22730%22%3A1770635331%7D" },
];

function getSteamCookies() {
  return STEAM_COOKIES_BASE.map((c) =>
    c.name === "steamLoginSecure" ? { ...c, value: currentSteamLoginSecure } : c
  );
}

function toCookieString(c) {
  const domain = c.domain || "store.steampowered.com";
  const path = c.path || "/";
  let s = `${c.name}=${c.value}; Domain=${domain}; Path=${path}`;
  if (c.secure) s += "; Secure";
  if (c.httpOnly) s += "; HttpOnly";
  if (c.sameSite && c.sameSite !== "unspecified") s += `; SameSite=${c.sameSite}`;
  return s;
}

async function injectCookies() {
  const storeUrl = "https://store.steampowered.com";
  const cookies = getSteamCookies();
  const names = [];
  for (const c of cookies) {
    const str = toCookieString(c);
    await jar.setCookie(str, storeUrl);
    names.push(c.name);
  }
  log(`Cookies injected: ${names.length} (${names.join(", ")})`);
}

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

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
}

/**
 * ================= STEAM SESSION REFRESH (кожні 12 год) =================
 */
const STEAM_REFRESH_INTERVAL_MS = 12 * 60 * 60 * 1000;

function parseSteamLoginSecureFromCookies(cookieStrings) {
  for (const s of cookieStrings) {
    const m = s.match(/^steamLoginSecure=([^;]+)/);
    if (m) return m[1].trim();
  }
  return null;
}

async function refreshSteamSession() {
  const { username: accountName, password, sharedSecret } = STEAM_CREDENTIALS;

  if (!accountName || !password || !sharedSecret || password === "ADD_YOUR_PASSWORD_HERE") {
    log("Steam auto-refresh skipped: STEAM_USERNAME, STEAM_PASSWORD, STEAM_SHARED_SECRET not set");
    return false;
  }

  const session = new LoginSession(EAuthTokenPlatformType.MobileApp);

  const authPromise = new Promise((resolve, reject) => {
    session.on("authenticated", resolve);
    session.on("timeout", () => reject(new Error("Steam login timed out")));
    session.on("error", reject);
  });

  try {
    if (lastRefreshToken) {
      try {
        session.refreshToken = lastRefreshToken;
        const cookies = await session.getWebCookies();
        const secure = parseSteamLoginSecureFromCookies(cookies);
        if (secure) {
          currentSteamLoginSecure = secure;
          log("Steam session refreshed via refresh token");
          return true;
        }
      } catch (e) {
        log(`Steam refresh token failed: ${e?.message || e}`);
        lastRefreshToken = null;
      }
    }

    const steamGuardCode = generateAuthCode(sharedSecret);
    const startResult = await session.startWithCredentials({
      accountName,
      password,
      steamGuardCode,
    });

    if (startResult.actionRequired && startResult.validActions?.some((a) => a.type === EAuthSessionGuardType.DeviceCode)) {
      try {
        const code = generateAuthCode(sharedSecret);
        await session.submitSteamGuardCode(code);
      } catch (e) {
        log(`Steam Guard submit failed: ${e?.message || e}`);
        return false;
      }
    } else if (startResult.actionRequired) {
      log("Steam login requires action we cannot handle automatically");
      return false;
    }

    await authPromise;
    lastRefreshToken = session.refreshToken;
    const cookies = await session.getWebCookies();
    const secure = parseSteamLoginSecureFromCookies(cookies);
    if (secure) {
      currentSteamLoginSecure = secure;
      log("Steam session refreshed via full login");
      return true;
    }
  } catch (e) {
    log(`Steam session refresh failed: ${e?.message || e}`);
  }
  return false;
}

/**
 * 🕒 GROUP BY UTC MINUTES
 */
function getRegionGroup() {
  const m = new Date().getUTCMinutes();
  const idx = Math.floor(m / 10) % REGION_GROUPS.length;
  return { idx, ccs: REGION_GROUPS[idx] };
}

/**
 * ================= DB HELPERS =================
 */

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

/**
 * ================= SCRAPER =================
 */

async function scrapePage({ cc, page }) {
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
      if (!res.ok) throw new Error();

      const $ = cheerio.load(await res.text());
      const rows = [];

      $(".search_result_row").each((_, el) => {
        const appid = extractAppId($(el).attr("href"));
        if (appid) rows.push({ appid });
      });

      return rows;
    } catch {
      if (attempt === MAX_ATTEMPTS) return [];
      await sleep(3000);
    }
  }
}

/**
 * ================= REGION SCRAPE =================
 */

async function runRegion({ cc, ts }) {
  let rows = [];
  let rank = 1;

  for (let page = 1; page <= MAX_PAGES; page++) {
    const pageRows = await scrapePage({ cc, page });
    if (!pageRows.length) break;

    for (const r of pageRows) {
      rows.push({ appid: r.appid, cc, rank: rank++, ts });
    }

    await sleep(PAGE_DELAY_MS);
  }

  const unique = [...new Map(rows.map((r) => [r.appid, r])).values()];

  if (unique.length < MIN_VALID_ITEMS_REGION) return null;

  return {
    history: rows,
    unique,
    current: unique.map((r, i) => ({
      cc,
      appid: r.appid,
      rank: i + 1,
      updated_ts: ts,
    })),
    totalPages: Math.ceil(unique.length / FRONT_PAGE_SIZE),
  };
}

/**
 * ================= SNAPSHOT =================
 */

async function runSnapshot() {
  const ts = Math.floor(Date.now() / 1000);
  const { idx, ccs } = getRegionGroup();

  log(`===== SNAPSHOT START ts=${ts} | group=${idx} =====`);

  let history = [];
  let pages = [];

  for (const cc of ccs) {
    const res = await runRegion({ cc, ts });
    if (!res) continue;

    history.push(...res.history);

    await clearCurrentRegion(cc);
    await insertChunked("steam_topsellers_current_region", res.current);

    pages.push({
      cc,
      total_pages: res.totalPages,
      updated_ts: ts,
    });

    /**
     * ================= HOURLY (FIXED) =================
     */

    const hourly = res.unique.map((r, i) => ({
      cc,
      appid: r.appid,
      ts,
      rank: i + 1,
    }));

    for (let i = 0; i < hourly.length; i += 1000) {
      await supabase
        .from("steam_app_topsellers_hourly_region") // ✅ FIX
        .upsert(hourly.slice(i, i + 1000), {
          onConflict: "cc,appid,ts",
          ignoreDuplicates: true,
        });
    }

    await supabase
      .from("steam_app_topsellers_hourly_region") // ✅ FIX
      .delete()
      .eq("cc", cc)
      .lt("ts", ts - 48 * 3600);

    /**
     * ================= STATS =================
     */

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
  }

  if (history.length) {
    await insertChunked("steam_topsellers_history_region", history);
    await upsertPages(pages);
  }

  log(`===== SNAPSHOT DONE ts=${ts} =====`);
}

/**
 * ================= LOOP =================
 */

async function main() {
  await refreshSteamSession();
  await injectCookies();

  setInterval(async () => {
    try {
      if (await refreshSteamSession()) {
        await injectCookies();
      }
    } catch (e) {
      log(`Steam session refresh interval error: ${e?.message || e}`);
    }
  }, STEAM_REFRESH_INTERVAL_MS);

  while (true) {
    const now = new Date();
    const next = new Date(now);
    next.setUTCMinutes(Math.ceil(now.getUTCMinutes() / 10) * 10);
    next.setUTCSeconds(0);

    await sleep(next - now);

    try {
      await runSnapshot();
    } catch {
      log("❌ SNAPSHOT FAILED");
    }
  }
}

main().catch(console.error);
