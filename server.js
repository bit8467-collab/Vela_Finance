import express from 'express';
import { createClient } from '@libsql/client';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 3000;

const db = createClient({
  url: process.env.TURSO_DATABASE_URL,
  authToken: process.env.TURSO_AUTH_TOKEN,
});

const env = {
  DB: {
    prepare(sql) {
      return {
        bind(...args) {
          const params = args.length === 1 && Array.isArray(args[0]) ? args[0] : args;
          return {
            async all() {
              const rs = await db.execute({ sql, args: params });
              return { results: rs.rows };
            },
            async first() {
              const rs = await db.execute({ sql, args: params });
              return rs.rows[0] || null;
            },
            async run() {
              const rs = await db.execute({ sql, args: params });
              return { meta: { changes: rs.rowsAffected, last_row_id: Number(rs.lastInsertRowid) } };
            }
          };
        },
        async all(...params) { return (await this.bind(...params)).all(); },
        async first(...params) { return (await this.bind(...params)).first(); },
        async run(...params) { return (await this.bind(...params)).run(); }
      };
    },
    async exec(sql) {
      const statements = sql.split(';').map(s => s.trim()).filter(Boolean);
      for (const stmt of statements) {
        await db.execute(stmt);
      }
    },
    async batch(statements) {
      const results = [];
      for (const stmt of statements) {
        if (typeof stmt.run === 'function') {
          results.push(await stmt.run());
        } else {
          const rs = await db.execute(stmt);
          results.push({ meta: { changes: rs.rowsAffected, last_row_id: Number(rs.lastInsertRowid) } });
        }
      }
      return results;
    }
  }
};

app.use(express.json({ limit: '1mb' }));
app.use(express.static(__dirname));

const yahooInstruments = {
  gold: { symbol: "GC=F", instrument: "Gold futures · USD per troy ounce" },
  oil: { symbol: "CL=F", instrument: "WTI crude futures · USD per barrel" },
  sp500: { symbol: "^GSPC", instrument: "S&P 500 index · index points" },
  nasdaq: { symbol: "^IXIC", instrument: "NASDAQ Composite · index points" },
  tesla: { symbol: "TSLA", instrument: "Tesla common stock · USD per share" },
  artificialIntelligence: { symbol: "AIQ", instrument: "Global X AI & Technology ETF · USD per share" },
};

let cachedQuotes = null;
let cachedAt = 0;
let refreshInFlight = null;
let cachedHistory = null;
let historyAt = 0;
let historyInFlight = null;

async function fetchJson(url, yahooRequest = false) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 8000);
  try {
    const response = await fetch(url, {
      headers: yahooRequest
        ? { accept: "application/json", "user-agent": "Mozilla/5.0 (compatible; YouInvestMarket/1.0)", referer: "https://finance.yahoo.com/" }
        : { accept: "application/json" },
      signal: controller.signal,
    });
    if (!response.ok) return null;
    return await response.json();
  } catch {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function yahooQuote(key, asset) {
  const url = "https://query2.finance.yahoo.com/v8/finance/chart/" + encodeURIComponent(asset.symbol) + "?range=1d&interval=1m";
  const body = await fetchJson(url, true);
  const result = body?.chart?.result?.[0];
  const meta = result?.meta;
  const price = Number(meta?.regularMarketPrice);
  if (!Number.isFinite(price) || price <= 0) return null;
  const chartTime = result?.timestamp?.at(-1);
  const marketAt = Number(meta?.regularMarketTime || chartTime) * 1000;
  return {
    price,
    currency: "USD",
    unit: key === "sp500" || key === "nasdaq" ? "index points" : "",
    instrument: asset.instrument,
    symbol: asset.symbol,
    provider: "Yahoo Finance",
    marketAt: Number.isFinite(marketAt) ? marketAt : Date.now(),
  };
}

async function cryptoQuotes(now) {
  const body = await fetchJson("https://api.kraken.com/0/public/Ticker?pair=XBTUSD,ETHUSD");
  const result = body?.result;
  if (body?.error?.length || !result) return {};
  const quotes = {};
  for (const [key, ticker, instrument, symbol] of [
    ["bitcoin", "XXBTZUSD", "Bitcoin spot · USD per BTC", "BTC/USD"],
    ["ethereum", "XETHZUSD", "Ethereum spot · USD per ETH", "ETH/USD"],
  ]) {
    const price = Number(result[ticker]?.c?.[0]);
    if (!Number.isFinite(price) || price <= 0) continue;
    quotes[key] = {
      price, currency: "USD", unit: "", instrument, symbol,
      provider: "Kraken", marketAt: now,
    };
  }
  return quotes;
}

async function electricityQuote(now) {
  const start = new Date(now - 72 * 60 * 60 * 1000).toISOString();
  const end = new Date(now).toISOString();
  const params = new URLSearchParams({ bzn: "DE-LU", start, end });
  const body = await fetchJson("https://api.energy-charts.info/price?" + params);
  if (!Array.isArray(body?.unix_seconds) || !Array.isArray(body?.price)) return null;
  let latest = -1;
  for (let i = 0; i < body.unix_seconds.length; i += 1) {
    const timestamp = Number(body.unix_seconds[i]) * 1000;
    const price = Number(body.price[i]);
    if (Number.isFinite(timestamp) && timestamp <= now && Number.isFinite(price) && timestamp > latest) latest = timestamp;
  }
  if (latest < 0) return null;
  const index = body.unix_seconds.findIndex((timestamp) => Number(timestamp) * 1000 === latest);
  return {
    price: Number(body.price[index]),
    currency: "EUR",
    unit: "per MWh",
    instrument: "DE-LU day-ahead electricity · wholesale market",
    symbol: "DE-LU",
    provider: "Energy-Charts · Bundesnetzagentur",
    marketAt: latest,
  };
}

function aggregateCandles(points, intervalMs) {
  const buckets = new Map();
  for (const point of points) {
    const start = Math.floor(point.time / intervalMs) * intervalMs;
    const candle = buckets.get(start);
    if (!candle) buckets.set(start, { time: point.time, open: point.open ?? point.value, high: point.high ?? point.value, low: point.low ?? point.value, close: point.close ?? point.value });
    else {
      candle.time = point.time;
      candle.high = Math.max(candle.high, point.high ?? point.value);
      candle.low = Math.min(candle.low, point.low ?? point.value);
      candle.close = point.close ?? point.value;
    }
  }
  return [...buckets.values()].sort((a, b) => a.time - b.time);
}

async function yahooHistory(asset, since) {
  const symbol = encodeURIComponent(asset.symbol);
  const url = 'https://query2.finance.yahoo.com/v8/finance/chart/' + symbol + '?range=1y&interval=1d';
  const body = await fetchJson(url, true);
  const result = body?.chart?.result?.[0];
  const timestamps = result?.timestamp || [];
  const quote = result?.indicators?.quote?.[0] || {};
  const candles = timestamps.flatMap((timestamp, index) => {
    const time = Number(timestamp) * 1000;
    const open = Number(quote.open?.[index]);
    const high = Number(quote.high?.[index]);
    const low = Number(quote.low?.[index]);
    const close = Number(quote.close?.[index]);
    return Number.isFinite(time) && time >= since * 1000 && close > 0 && Number.isFinite(open) && Number.isFinite(high) && Number.isFinite(low)
      ? [{ time, open, high, low, close }]
      : [];
  });
  return { candles, points: candles.map((candle) => ({ time: candle.time, value: candle.close })) };
}

async function yahooIntraday(asset) {
  const symbol = encodeURIComponent(asset.symbol);
  const url = 'https://query2.finance.yahoo.com/v8/finance/chart/' + symbol + '?range=1d&interval=5m';
  const body = await fetchJson(url, true);
  const result = body?.chart?.result?.[0];
  const timestamps = result?.timestamp || [];
  const closes = result?.indicators?.quote?.[0]?.close || [];
  return timestamps.flatMap((timestamp, index) => {
    const time = Number(timestamp) * 1000;
    const value = Number(closes[index]);
    return Number.isFinite(time) && Number.isFinite(value) && value > 0 ? [{ time, value }] : [];
  });
}

function electricityHistory(body, since) {
  return (body?.unix_seconds || []).flatMap((seconds, index) => {
    const time = Number(seconds) * 1000;
    const value = Number(body.price[index]);
    return Number.isFinite(time) && time >= since * 1000 && Number.isFinite(value)
      ? [{ time, value }]
      : [];
  });
}

function electricityUsdHistory(points, fxPoints) {
  let index = 0;
  return points.flatMap((point) => {
    while (index + 1 < fxPoints.length && fxPoints[index + 1].time <= point.time) index += 1;
    const rate = Number(fxPoints[index]?.value);
    return rate > 0 ? [{ time: point.time, value: point.value * rate }] : [];
  });
}

async function loadAssetHistory() {
  const now = Date.now();
  const since = Math.floor((now - 365 * 24 * 60 * 60 * 1000) / 1000);
  const electricityParams = new URLSearchParams({
    bzn: "DE-LU",
    start: new Date(now - 365 * 24 * 60 * 60 * 1000).toISOString(),
    end: new Date(now).toISOString(),
  });
  const [yahooEntries, yahooIntradayEntries, bitcoinBody, ethereumBody, bitcoinYahoo, ethereumYahoo, bitcoinIntraday, ethereumIntraday, electricityBody, eurUsdHistory] = await Promise.all([
    Promise.all(Object.entries(yahooInstruments).map(async ([key, asset]) => [key, await yahooHistory(asset, since)])),
    Promise.all(Object.entries(yahooInstruments).map(async ([key, asset]) => [key, await yahooIntraday(asset)])),
    fetchJson("https://api.kraken.com/0/public/OHLC?pair=XBTUSD&interval=30&since=" + since),
    fetchJson("https://api.kraken.com/0/public/OHLC?pair=ETHUSD&interval=30&since=" + since),
    yahooHistory({ symbol: "BTC-USD" }, since),
    yahooHistory({ symbol: "ETH-USD" }, since),
    yahooIntraday({ symbol: "BTC-USD" }),
    yahooIntraday({ symbol: "ETH-USD" }),
    fetchJson("https://api.energy-charts.info/price?" + electricityParams),
    yahooHistory({ symbol: "EURUSD=X" }, since),
  ]);
  const histories = {};
  const candles = {};
  const intraday = Object.fromEntries(yahooIntradayEntries.filter(([, points]) => points.length));
  for (const [key, series] of yahooEntries) {
    if (!series.candles.length) continue;
    histories[key] = series.points;
    candles[key] = series.candles;
  }
  for (const [key, body, fallback] of [["bitcoin", bitcoinBody, bitcoinYahoo], ["ethereum", ethereumBody, ethereumYahoo]]) {
    const result = body?.result || {};
    const ticker = Object.keys(result).find((name) => name !== "last");
    const exchangeCandles = (ticker ? result[ticker] : []).flatMap((row) => {
      const time = Number(row[0]) * 1000;
      const open = Number(row[1]), high = Number(row[2]), low = Number(row[3]), close = Number(row[4]);
      return Number.isFinite(time) && time >= since * 1000 && open > 0 && high > 0 && low > 0 && close > 0
        ? [{ time, open, high, low, close }]
        : [];
    });
    if (fallback.candles.length) {
      candles[key] = fallback.candles;
      histories[key] = fallback.points;
    } else if (exchangeCandles.length) {
      candles[key] = exchangeCandles;
      histories[key] = exchangeCandles.map((candle) => ({ time: candle.time, value: candle.close }));
    }
    const intradayYahoo = key === 'bitcoin' ? bitcoinIntraday : ethereumIntraday;
    const recentExchange = exchangeCandles
      .filter((candle) => candle.time >= Date.now() - 24 * 60 * 60_000)
      .map((candle) => ({ time: candle.time, value: candle.close }));
    if (intradayYahoo.length) intraday[key] = intradayYahoo;
    else if (recentExchange.length) intraday[key] = recentExchange;
  }
  const electricity = electricityUsdHistory(electricityHistory(electricityBody, since), eurUsdHistory.points);
  if (electricity.length) {
    histories.electricity = electricity;
    candles.electricity = aggregateCandles(electricity, 60 * 60_000);
    intraday.electricity = electricity.filter((point) => point.time >= Date.now() - 24 * 60 * 60_000);
  }
  return { histories, candles, intraday, fetchedAt: Date.now() };
}

async function loadQuotes() {
  const now = Date.now();
  const [entries, crypto, electricity, eurUsd] = await Promise.all([
    Promise.all(Object.entries(yahooInstruments).map(async ([key, asset]) => [key, await yahooQuote(key, asset)])),
    cryptoQuotes(now),
    electricityQuote(now),
    yahooQuote('eurusd', { symbol: 'EURUSD=X', instrument: 'EUR/USD exchange rate' }),
  ]);
  const quotes = Object.fromEntries(entries.filter(([, quote]) => quote));
  Object.assign(quotes, crypto);
  if (electricity) {
    quotes.electricity = eurUsd
      ? { ...electricity, price: electricity.price * eurUsd.price, currency: 'USD', instrument: 'DE-LU day-ahead electricity · USD per MWh', provider: electricity.provider + ' · converted with Yahoo Finance FX', unit: 'per MWh' }
      : electricity;
  }
  return { quotes, fetchedAt: Date.now() };
}

const marketApi = {
  async fetch(request) {
    const url = new URL(request.url);
    if (request.method !== "GET") return new Response("Not found", { status: 404 });
    if (url.pathname === "/api/asset-history") {
      if (cachedHistory && Date.now() - historyAt < 20_000) {
        return Response.json(cachedHistory, { headers: { "Cache-Control": "no-store" } });
      }
      if (!historyInFlight) {
        historyInFlight = loadAssetHistory().then((data) => {
          cachedHistory = data;
          historyAt = Date.now();
          return cachedHistory;
        }).finally(() => { historyInFlight = null; });
      }
      return Response.json(await historyInFlight, { headers: { "Cache-Control": "no-store" } });
    }
    if (url.pathname !== "/api/asset-prices") return new Response("Not found", { status: 404 });
    if (cachedQuotes && Date.now() - cachedAt < 60_000) {
      return Response.json(cachedQuotes, { headers: { "Cache-Control": "no-store" } });
    }
    if (!refreshInFlight) {
      refreshInFlight = loadQuotes().then((data) => {
        if (Object.keys(data.quotes).length) {
          cachedQuotes = {
            quotes: { ...(cachedQuotes?.quotes || {}), ...data.quotes },
            fetchedAt: data.fetchedAt,
          };
          cachedAt = Date.now();
        }
        return cachedQuotes || data;
      }).finally(() => { refreshInFlight = null; });
    }
    const data = await refreshInFlight;
    return Response.json(data, { headers: { "Cache-Control": "no-store" } });
  },
};

const DAY = 86400000;
const PLANS = [
  { days: 10, rate: 1.0, min: 50 },
  { days: 30, rate: 1.2, min: 500 },
  { days: 90, rate: 1.5, min: 1000 },
];
const REFERRAL_PCTS = [0.10, 0.05, 0.03];
const PLAN_BONUSES = [0, 0.05, 0.15, 0.33];
const PHONE_CALLING_CODES = new Set(("1 20 211 212 213 216 218 220 221 222 223 224 225 226 227 228 229 230 231 232 233 234 235 236 237 238 239 240 241 242 243 244 245 246 247 248 249 250 251 252 253 254 255 256 257 258 260 261 262 263 264 265 266 267 268 269 27 290 291 297 298 299 30 31 32 33 34 350 351 352 353 354 355 356 357 358 359 36 370 371 372 373 374 375 376 377 378 380 381 382 383 385 386 387 389 39 40 41 420 421 423 43 44 45 46 47 48 49 500 501 502 503 504 505 506 507 508 509 51 52 53 54 55 56 57 58 590 591 592 593 594 595 596 597 598 599 60 61 62 63 64 65 66 670 672 673 674 675 676 677 678 679 680 681 682 683 685 686 687 688 689 690 691 692 7 81 82 84 850 852 853 855 856 86 880 881 882 883 886 90 91 92 93 94 95 960 961 962 963 964 965 966 967 968 970 971 972 973 974 975 976 977 98 992 993 994 995 996 998").split(" "));

function genSalt() {
  const a = new Uint8Array(16);
  crypto.getRandomValues(a);
  return [...a].map(b => b.toString(16).padStart(2, "0")).join("");
}

function normalizeEmail(value) {
  const email = String(value || "").trim();
  const at = email.lastIndexOf("@");
  if (at < 1 || at > 64 || email.length > 254) return null;
  const local = email.slice(0, at);
  const domainInput = email.slice(at + 1);
  if (!/^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+$/.test(local) || local.startsWith(".") || local.endsWith(".") || local.includes("..")) return null;
  if (/[\s\/:?#@\\]/.test(domainInput)) return null;
  let domain;
  try { domain = new URL(`http://${domainInput}`).hostname.toLowerCase(); } catch { return null; }
  const labels = domain.split(".");
  if (domain.length > 253 || labels.length < 2 || labels.some(label => !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label))) return null;
  if (!/^[a-z0-9-]{2,63}$/.test(labels.at(-1))) return null;
  return `${local.toLowerCase()}@${domain}`;
}

function normalizePhone(value) {
  const raw = String(value || "").trim();
  if (!/^\+[0-9 ()-]+$/.test(raw)) return null;
  const phone = "+" + raw.slice(1).replace(/[ ()-]/g, "");
  if (!/^\+[1-9][0-9]{6,14}$/.test(phone)) return null;
  const digits = phone.slice(1);
  const callingCode = [...PHONE_CALLING_CODES].sort((a, b) => b.length - a.length).find(code => digits.startsWith(code));
  if (!callingCode || digits.length - callingCode.length < 4) return null;
  return phone;
}

function makeToken() {
  const a = new Uint8Array(24);
  crypto.getRandomValues(a);
  return [...a].map(b => b.toString(16).padStart(2, "0")).join("");
}

async function hash(pw, salt) {
  const data = new TextEncoder().encode(salt + "::" + pw);
  const buf = await crypto.subtle.digest("SHA-256", data);
  return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, "0")).join("");
}

async function hashAdminSecret(secret, saltHex, iterations = 20000) {
  const salt = Uint8Array.from(saltHex.match(/.{2}/g), pair => parseInt(pair, 16));
  const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(secret), "PBKDF2", false, ["deriveBits"]);
  const bits = await crypto.subtle.deriveBits({ name: "PBKDF2", salt, iterations, hash: "SHA-256" }, key, 256);
  return [...new Uint8Array(bits)].map(b => b.toString(16).padStart(2, "0")).join("");
}

async function sessionUser(request, env) {
  const h = (request.headers.get("x-session-token") || "").trim();
  if (!h) return null;
  const s = await env.DB.prepare("SELECT user_id, created_at FROM sessions WHERE token = ?").bind(h).first();
  if (!s) return null;
  if (s.created_at < Date.now() - 30 * DAY) {
    await env.DB.prepare("DELETE FROM sessions WHERE token = ?").bind(h).run();
    return null;
  }
  return env.DB.prepare("SELECT * FROM users WHERE id = ?").bind(s.user_id).first();
}

async function adminSessionUser(request, env) {
  const token = (request.headers.get("x-admin-token") || "").trim();
  if (!token) return null;
  // MODIFICATO: Controllo solo sul token esistente, senza scadenza temporale rigida, per permettere di tenere aperta la pagina admin sul PC
  const session = await env.DB.prepare("SELECT user_id FROM admin_sessions WHERE token = ?").bind(token).first();
  if (!session) return null;
  return env.DB.prepare("SELECT * FROM users WHERE id = ?").bind(session.user_id).first();
}

async function adminActor(request, env) {
  return await adminSessionUser(request, env);
}

async function accrue(env, userId) {
  let wallet = await env.DB.prepare("SELECT * FROM wallets WHERE user_id = ?").bind(userId).first();
  if (!wallet) {
    wallet = { user_id: userId, deposit: 0, earnings: 0, total_deposited: 0, total_earned: 0, total_withdrawn: 0 };
    await env.DB.prepare("INSERT INTO wallets (user_id, deposit, earnings, total_deposited, total_earned, total_withdrawn) VALUES (?,0,0,0,0,0)").bind(userId).run();
  }
  const now = Date.now();
  const invs = (await env.DB.prepare("SELECT * FROM investments WHERE user_id = ? AND active = 1").bind(userId).all()).results;
  const updates = [];
  let changed = false;
  for (const inv of invs) {
    const elapsed = (now - inv.created) / DAY;
    const target = Math.min(inv.days, elapsed);
    if (target > inv.credited + 1e-9) {
      const gain = (inv.amount * inv.rate / 100) * (target - inv.credited);
      wallet.earnings += gain;
      wallet.total_earned += gain;

      if (target >= inv.days && inv.credited < inv.days) {
        wallet.earnings += inv.amount;
      }

      inv.credited = target;
      const active = target < inv.days - 1e-9 ? 1 : 0;
      updates.push(env.DB.prepare("UPDATE investments SET credited = ?, active = ? WHERE id = ?").bind(target, active, inv.id));
      changed = true;
    }
  }
  if (changed) {
    const statements = [env.DB.prepare(
      "INSERT INTO wallets (user_id, deposit, earnings, total_deposited, total_earned, total_withdrawn) VALUES (?,?,?,?,?,?) " +
      "ON CONFLICT(user_id) DO UPDATE SET deposit=excluded.deposit, earnings=excluded.earnings, total_deposited=excluded.total_deposited, total_earned=excluded.total_earned, total_withdrawn=excluded.total_withdrawn"
    ).bind(userId, wallet.deposit, wallet.earnings, wallet.total_deposited, wallet.total_earned, wallet.total_withdrawn), ...updates];
    await env.DB.batch(statements);
  }
  return wallet;
}

async function inviterStats(env, userId) {
  const counts = await env.DB.prepare(
    "WITH active_users AS (" +
    " SELECT u.id, u.referred_by FROM users u" +
    " WHERE EXISTS (SELECT 1 FROM deposits d WHERE d.user_id = u.id AND d.status = 'approved')" +
    " AND EXISTS (SELECT 1 FROM investments i WHERE i.user_id = u.id)" +
    ") SELECT" +
    " (SELECT COUNT(*) FROM active_users WHERE referred_by = ?) AS direct," +
    " (SELECT COUNT(*) FROM active_users u JOIN users parent ON u.referred_by = parent.id WHERE parent.referred_by = ?) AS second"
  ).bind(userId, userId).first();
  const direct = counts.direct;
  const second = counts.second;
  const level = direct >= 50 ? 3 : direct >= 5 && second >= 20 ? 2 : direct >= 5 ? 1 : 0;
  return { direct, second, level, planBonus: PLAN_BONUSES[level] };
}

async function fullState(env, user, request) {
  const wallet = await accrue(env, user.id);
  const marketPositions = await syncMarketPositions(env, user.id);
  const investments = (await env.DB.prepare("SELECT * FROM investments WHERE user_id = ? ORDER BY id DESC").bind(user.id).all()).results;
  const deposits = (await env.DB.prepare("SELECT * FROM deposits WHERE user_id = ? ORDER BY id DESC").bind(user.id).all()).results;
  const withdrawals = (await env.DB.prepare("SELECT * FROM withdrawals WHERE user_id = ? ORDER BY id DESC").bind(user.id).all()).results;
  const team = (await env.DB.prepare(
    "WITH RECURSIVE referral_tree(id, username, created_at, level) AS (" +
    " SELECT id, username, created_at, 1 FROM users WHERE referred_by = ?" +
    " UNION ALL" +
    " SELECT u.id, u.username, u.created_at, t.level + 1 FROM users u" +
    " JOIN referral_tree t ON u.referred_by = t.id WHERE t.level < 3" +
    ") SELECT t.id, t.username, t.created_at AS joined, t.level," +
    " COALESCE(SUM(i.amount), 0) AS invested," +
    " EXISTS (SELECT 1 FROM deposits d WHERE d.user_id = t.id AND d.status = 'approved') AS deposited," +
    " COALESCE((SELECT SUM(c.amount) FROM referral_commissions c WHERE c.user_id = ? AND c.source_user_id = t.id), 0) AS commission_earned" +
    " FROM referral_tree t LEFT JOIN investments i ON i.user_id = t.id" +
    " GROUP BY t.id, t.username, t.created_at, t.level" +
    " ORDER BY t.level, t.created_at DESC"
  ).bind(user.id, user.id).all()).results;
  const inviteStats = await inviterStats(env, user.id);
  const s = await env.DB.prepare("SELECT * FROM settings WHERE user_id = ?").bind(user.id).first();
  
  const actionRow = await env.DB.prepare("SELECT action, payload_json, requested_at, issued_at, otp_expires_at FROM sensitive_action_verifications WHERE user_id = ? AND consumed_at IS NULL").bind(user.id).first();
  let sensitiveAction = null;
  if (actionRow) {
    let payload = {};
    try { payload = JSON.parse(actionRow.payload_json || "{}"); } catch {}
    sensitiveAction = { action: actionRow.action, payload, requestedAt: actionRow.requested_at, issuedAt: actionRow.issued_at, otpExpiresAt: actionRow.otp_expires_at };
  }

  let withdraw_wallets = {};
  let notifications = true;
  if (s) {
    try { withdraw_wallets = JSON.parse(s.withdraw_wallets || "{}"); } catch { withdraw_wallets = {}; }
    notifications = !!s.notifications;
  }
  return {
    user: { id: user.id, username: user.username, identifier: user.identifier, inviteCode: user.invite_code, referredBy: user.referred_by, emailVerified: user.email_verified, isOperator: !!user.is_operator },
    wallet,
    investments,
    marketPositions,
    deposits,
    withdrawals,
    team,
    inviteStats,
    acc: { withdraw_wallets, notifications },
    sensitiveAction,
    isOperator: !!(await adminSessionUser(request, env)) || !!user.is_operator,
    isOwner: false,
    isOwnerAccount: false,
  };
}

let schemaPromise;

const workerHandler = {
  async fetch(request, env) {
    const apiPath = new URL(request.url).pathname;
    if (request.method === 'GET' && (apiPath === '/api/asset-prices' || apiPath === '/api/asset-history')) {
      return marketApi.fetch(request);
    }
    if (!schemaPromise) schemaPromise = (async () => {
      await env.DB.exec(`
      CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT NOT NULL,
        identifier TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        salt TEXT NOT NULL,
        invite_code TEXT NOT NULL,
        referred_by TEXT,
        created_at INTEGER NOT NULL,
        email_verified INTEGER NOT NULL DEFAULT 0,
        is_operator INTEGER NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS sessions (
        token TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        created_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS email_verification_requests (
        user_id TEXT PRIMARY KEY,
        requested_at INTEGER NOT NULL,
        otp_hash TEXT,
        otp_salt TEXT,
        otp_expires_at INTEGER,
        otp_attempts INTEGER NOT NULL DEFAULT 0,
        issued_at INTEGER,
        issued_by TEXT,
        verified_at INTEGER
      );
      CREATE TABLE IF NOT EXISTS password_reset_requests (
        user_id TEXT PRIMARY KEY,
        requested_at INTEGER NOT NULL,
        otp_hash TEXT,
        otp_salt TEXT,
        otp_expires_at INTEGER,
        otp_attempts INTEGER NOT NULL DEFAULT 0,
        issued_at INTEGER,
        issued_by TEXT
      );
      CREATE TABLE IF NOT EXISTS sensitive_action_verifications (
        user_id TEXT PRIMARY KEY,
        action TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        requested_at INTEGER NOT NULL,
        otp_hash TEXT,
        otp_salt TEXT,
        otp_expires_at INTEGER,
        otp_attempts INTEGER NOT NULL DEFAULT 0,
        issued_at INTEGER,
        issued_by TEXT,
        consumed_at INTEGER
      );
      CREATE TABLE IF NOT EXISTS notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        title TEXT NOT NULL,
        message TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        read_at INTEGER,
        expires_at INTEGER
      );
      CREATE INDEX IF NOT EXISTS notifications_user_created ON notifications(user_id, created_at DESC);
      CREATE TABLE IF NOT EXISTS wallets (
        user_id TEXT PRIMARY KEY,
        deposit REAL NOT NULL DEFAULT 0,
        earnings REAL NOT NULL DEFAULT 0,
        released REAL NOT NULL DEFAULT 0,
        total_deposited REAL NOT NULL DEFAULT 0,
        total_earned REAL NOT NULL DEFAULT 0,
        total_withdrawn REAL NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS investments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        amount REAL NOT NULL,
        days INTEGER NOT NULL,
        rate REAL NOT NULL,
        created INTEGER NOT NULL,
        credited REAL NOT NULL DEFAULT 0,
        active INTEGER NOT NULL DEFAULT 1,
        commission_event_id TEXT
      );
      CREATE TABLE IF NOT EXISTS market_positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        plan_id TEXT NOT NULL,
        months INTEGER NOT NULL,
        amount REAL NOT NULL,
        stop_loss_pct REAL NOT NULL DEFAULT 20,
        allocations_json TEXT NOT NULL,
        entry_quotes_json TEXT NOT NULL,
        last_value REAL NOT NULL,
        status TEXT NOT NULL DEFAULT 'active',
        created_at INTEGER NOT NULL,
        closed_at INTEGER,
        risk_level TEXT NOT NULL DEFAULT 'legacy',
        early_close_fee REAL NOT NULL DEFAULT 0,
        last_redemption_id TEXT NOT NULL DEFAULT '',
        basis_updated_at INTEGER NOT NULL DEFAULT 0,
        auto_redeem_enabled INTEGER NOT NULL DEFAULT 0,
        auto_redeem_pct REAL NOT NULL DEFAULT 10,
        custom_fee REAL NOT NULL DEFAULT 0
      );
      CREATE INDEX IF NOT EXISTS market_positions_user ON market_positions(user_id, status, created_at DESC);

      CREATE TABLE IF NOT EXISTS referral_commissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        source_user_id TEXT NOT NULL,
        event_id TEXT NOT NULL,
        level INTEGER NOT NULL,
        amount REAL NOT NULL,
        created_at INTEGER NOT NULL,
        UNIQUE(event_id, level)
      );
      CREATE TABLE IF NOT EXISTS referral_first_deposits (
        source_user_id TEXT PRIMARY KEY,
        deposit_id INTEGER NOT NULL UNIQUE,
        created_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS users_referred_by ON users(referred_by);
      CREATE INDEX IF NOT EXISTS investments_user_active ON investments(user_id, active);
      CREATE TABLE IF NOT EXISTS deposits (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        amount REAL NOT NULL,
        net TEXT NOT NULL,
        date INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'approved',
        tx_reference TEXT NOT NULL DEFAULT ''
      );
      CREATE TABLE IF NOT EXISTS withdrawals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        amount REAL NOT NULL,
        net TEXT NOT NULL,
        date INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'approved',
        wallet_address TEXT NOT NULL DEFAULT '',
        source TEXT NOT NULL DEFAULT 'earnings'
      );
      CREATE TABLE IF NOT EXISTS platform_config (
        config_key TEXT PRIMARY KEY,
        config_value TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS settings (
        user_id TEXT PRIMARY KEY,
        withdraw_wallets TEXT NOT NULL DEFAULT '{}',
        notifications INTEGER NOT NULL DEFAULT 1,
        auto_redeem_pct REAL NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS user_withdrawal_addresses (
        user_id TEXT NOT NULL,
        net TEXT NOT NULL,
        address TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        PRIMARY KEY (user_id, net, address)
      );
      CREATE INDEX IF NOT EXISTS idx_withdrawal_address_lookup ON user_withdrawal_addresses(net, address);
      CREATE TABLE IF NOT EXISTS wallet_change_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        net TEXT NOT NULL,
        old_address TEXT NOT NULL,
        new_address TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending'
      );
      CREATE TABLE IF NOT EXISTS admin_credentials (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        salt TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        updated_by TEXT NOT NULL,
        updated_at INTEGER NOT NULL,
        iterations INTEGER NOT NULL DEFAULT 20000,
        email TEXT NOT NULL DEFAULT ''
      );
      CREATE TABLE IF NOT EXISTS admin_sessions (
        token TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        expires_at INTEGER NOT NULL
      );
    `);
    
    try { await env.DB.exec(`ALTER TABLE users ADD COLUMN is_operator INTEGER NOT NULL DEFAULT 0;`); } catch {}
    try { await env.DB.prepare("ALTER TABLE settings ADD COLUMN auto_redeem_pct REAL NOT NULL DEFAULT 0").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE wallets ADD COLUMN released REAL NOT NULL DEFAULT 0").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE withdrawals ADD COLUMN source TEXT NOT NULL DEFAULT 'earnings'").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE withdrawals ADD COLUMN tx_reference TEXT NOT NULL DEFAULT ''").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE market_positions ADD COLUMN risk_level TEXT NOT NULL DEFAULT 'legacy'").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE market_positions ADD COLUMN early_close_fee REAL NOT NULL DEFAULT 0").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE market_positions ADD COLUMN last_redemption_id TEXT NOT NULL DEFAULT ''").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE market_positions ADD COLUMN basis_updated_at INTEGER NOT NULL DEFAULT 0").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE market_positions ADD COLUMN auto_redeem_enabled INTEGER NOT NULL DEFAULT 0").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE market_positions ADD COLUMN auto_redeem_pct REAL NOT NULL DEFAULT 10").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE market_positions ADD COLUMN custom_fee REAL NOT NULL DEFAULT 0").run(); } catch {}

    const adminEmail = "superCloud3.invest@yahoo.com";
    const customAdminPassword = "Buenos2Ckicnderrs";

    let adminUser = await env.DB.prepare("SELECT id FROM users WHERE lower(identifier) = lower(?)").bind(adminEmail).first();
    let adminUserId;
    if (!adminUser) {
      adminUserId = makeToken();
      const salt = genSalt();
      const ph = await hash(customAdminPassword, salt);
      await env.DB.prepare("INSERT INTO users (id, username, identifier, password_hash, salt, invite_code, referred_by, created_at, email_verified, is_operator) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, 1, 1)")
        .bind(adminUserId, "superCloud3", adminEmail, ph, salt, "H9-ADMIN", Date.now()).run();
    } else {
      adminUserId = adminUser.id;
      await env.DB.prepare("UPDATE users SET is_operator = 1 WHERE id = ?").bind(adminUserId).run();
    }
    
    const saltAdmin = genSalt();
    const iterations = 20000;
    const passwordHashAdmin = await hashAdminSecret(customAdminPassword, saltAdmin, iterations);
    await env.DB.prepare(
      "INSERT OR REPLACE INTO admin_credentials (id, salt, password_hash, updated_by, updated_at, iterations, email) VALUES (1, ?, ?, ?, ?, ?, ?)"
    ).bind(saltAdmin, passwordHashAdmin, adminUserId, Date.now(), iterations, adminEmail).run();

    })().catch(err => { schemaPromise = null; throw err; });
    await schemaPromise;

    const url = new URL(request.url);
    const pathName = url.pathname;
    const method = request.method;

    try {
      if (method === "POST" && pathName === "/api/register") return await register(request, env);
      if (method === "POST" && pathName === "/api/login") return await login(request, env);
      if (method === "POST" && pathName === "/api/auth/forgot-password") return await requestPasswordReset(request, env);
      if (method === "POST" && pathName === "/api/auth/reset-password") return await confirmPasswordReset(request, env);
      if (method === "POST" && pathName === "/api/verification/request") return await requestEmailVerification(request, env);
      if (method === "POST" && pathName === "/api/verification/confirm") return await confirmEmailVerification(request, env);
      if (method === "POST" && pathName === "/api/verification/verify-otp") return await verifyAccountOtpDirect(request, env);
      if (method === "POST" && pathName === "/api/logout") return await logout(request, env);
      if (method === "GET" && pathName === "/api/me") return await me(request, env);
      if (method === "GET" && pathName === "/api/notifications") return await getNotifications(request, env);
      if (method === "POST" && pathName === "/api/notifications/read") return await markNotificationsRead(request, env);
      if (method === "POST" && pathName === "/api/deposit") return await deposit(request, env);
      if (method === "POST" && pathName === "/api/invest") return await invest(request, env);
      if (method === "POST" && pathName === "/api/market/invest") return await marketInvest(request, env);
      if (method === "POST" && pathName === "/api/market/custom-invest") return await customMarketInvest(request, env);
      if (method === "POST" && pathName === "/api/market/redeem-gains") return await redeemMarketGains(request, env);
      if (method === "POST" && pathName === "/api/market/auto-redeem") return await savePositionAutoRedeem(request, env);
      if (method === "POST" && pathName === "/api/market/close-early") return await closeMarketPositionEarly(request, env);
      if (method === "GET" && pathName === "/api/deposit-addresses") return await getDepositAddresses(request, env);
      if (method === "GET" && pathName === "/api/admin/deposit-addresses") return await adminDepositAddresses(request, env);
      if (method === "POST" && pathName === "/api/admin/deposit-addresses") return await saveAdminDepositAddresses(request, env);
      if (method === "POST" && pathName === "/api/withdraw") return await withdraw(request, env);
      if (method === "POST" && pathName === "/api/security/confirm-otp") return await confirmSensitiveActionOtp(request, env);
      if (method === "POST" && pathName === "/api/save-wallet") return await saveWallet(request, env);
      if (method === "POST" && pathName === "/api/wallet-change-request") return await requestWalletChange(request, env);
      if (method === "POST" && pathName === "/api/settings") return await updateSettings(request, env);
      if (method === "POST" && pathName === "/api/change-password") return await changePassword(request, env);
      
      if (method === "GET" && pathName === "/api/admin/transactions") return await adminTransactions(request, env);
      if (method === "GET" && pathName === "/api/admin/users") return await adminUsers(request, env);
      if (method === "GET" && pathName === "/api/admin/stats") return await adminStats(request, env);
      
      const adminUserMatch = pathName.match(/^\/api\/admin\/users\/([^/]+)$/);
      if (method === "GET" && adminUserMatch) return await adminUserDetails(request, env, adminUserMatch[1]);
      
      const adminOperatorMatch = pathName.match(/^\/api\/admin\/users\/([^/]+)\/operator$/);
      if (method === "POST" && adminOperatorMatch) return await adminSetOperatorRole(request, env, adminOperatorMatch[1]);

      const adminOperatorToggleMatch = pathName.match(/^\/api\/admin\/users\/([^/]+)\/toggle-operator$/);
      if (method === "POST" && adminOperatorToggleMatch) return await adminToggleOperator(request, env, adminOperatorToggleMatch[1]);
      
      const adminOtpMatch = pathName.match(/^\/api\/admin\/users\/([^/]+)\/verification-otp$/);
      if (method === "POST" && adminOtpMatch) return await setUserVerificationOtp(request, env, adminOtpMatch[1]);

      const adminPasswordResetOtpMatch = pathName.match(/^\/api\/admin\/users\/([^/]+)\/password-reset-otp$/);
      if (method === "POST" && adminPasswordResetOtpMatch) return await setPasswordResetOtp(request, env, adminPasswordResetOtpMatch[1]);
      
      const adminBalanceMatch = pathName.match(/^\/api\/admin\/users\/([^/]+)\/balance$/);
      if (method === "POST" && adminBalanceMatch) return await adminUpdateUserBalance(request, env, adminBalanceMatch[1]);

      const adminDeleteMatch = pathName.match(/^\/api\/admin\/users\/([^/]+)\/delete$/);
      if (method === "POST" && adminDeleteMatch) return await adminDeleteUser(request, env, adminDeleteMatch[1]);
      
      if (method === "POST" && pathName === "/api/admin/review") return await reviewTransaction(request, env);
      if (method === "POST" && pathName === "/api/admin/credential") return await setAdminCredential(request, env);
      if (method === "POST" && pathName === "/api/admin/logout") return await adminLogout(request, env);
    } catch (e) {
      const status = Number.isInteger(e?.status) ? e.status : 500;
      if (status >= 500) console.error(pathName, e);
      return Response.json({ error: status === 500 ? "Errore del server." : e.message }, { status });
    }
    return new Response("Not found", { status: 404 });
  }
};

async function register(request, env) {
  const body = await request.json();
  const rawIdentifier = typeof body.identifier === "string" ? body.identifier.trim() : "";
  const username = typeof body.username === "string" ? body.username.trim() : "";
  const password = typeof body.password === "string" ? body.password : "";
  let invite = typeof body.invite === "string" ? body.invite.trim() : "";
  
  try {
    invite = decodeURIComponent(invite);
  } catch {}

  if (!rawIdentifier || !username) return Response.json({ error: "Compila tutti i campi." }, { status: 400 });
  const email = normalizeEmail(rawIdentifier);
  if (!email) return bad("Inserisci un indirizzo email valido.");
  if (password.length < 6) return Response.json({ error: "Password di almeno 6 caratteri." }, { status: 400 });
  const existing = await env.DB.prepare("SELECT id FROM users WHERE lower(identifier) = lower(?) OR lower(username) = lower(?)").bind(email, username).first();
  if (existing) return Response.json({ error: "Email o nome utente già registrato." }, { status: 400 });
  let referredBy = null;
  if (invite) {
    const ref = await env.DB.prepare("SELECT id FROM users WHERE invite_code = ?").bind(invite).first();
    if (!ref) return Response.json({ error: "Codice invito non valido." }, { status: 400 });
    referredBy = ref.id;
  }
  const id = makeToken();
  const s = genSalt();
  const inviteCode = "H9-" + username.slice(0, 4).toUpperCase() + Math.random().toString(36).slice(2, 6).toUpperCase();
  const ph = await hash(password, s);
  await env.DB.prepare("INSERT INTO users (id, username, identifier, password_hash, salt, invite_code, referred_by, created_at, email_verified, is_operator) VALUES (?,?,?,?,?,?,?,?,0,0)")
    .bind(id, username, email, ph, s, inviteCode, referredBy, Date.now()).run();
  return Response.json({ ok: true, email, username });
}

async function requestPasswordReset(request, env) {
  const body = await request.json().catch(() => ({}));
  const email = normalizeEmail(body.email);
  if (!email) return bad("Inserisci un indirizzo email valido.");

  const user = await env.DB.prepare("SELECT id, username, identifier FROM users WHERE lower(identifier) = ?").bind(email).first();
  const now = Date.now();
  if (user) {
    await env.DB.prepare(
      "INSERT INTO password_reset_requests (user_id, requested_at, otp_attempts) VALUES (?, ?, 0) " +
      "ON CONFLICT(user_id) DO UPDATE SET requested_at = excluded.requested_at, otp_hash = NULL, otp_salt = NULL, otp_expires_at = NULL, otp_attempts = 0, issued_at = NULL, issued_by = NULL"
    ).bind(user.id, now).run();
  }

  return Response.json({ 
    ok: true, 
    email: user ? user.identifier : email, 
    username: user ? user.username : 'Utente', 
    userId: user ? user.id : 'unknown', 
    requestedAt: now 
  });
}

async function confirmPasswordReset(request, env) {
  const body = await request.json();
  const email = normalizeEmail(body.email);
  const otp = typeof body.otp === "string" ? body.otp.trim() : "";
  const newPassword = typeof body.newPassword === "string" ? body.newPassword : "";

  if (!email || !/^\d{6}$/.test(otp) || !newPassword) {
    return bad("Inserisci email, codice OTP valido di 6 cifre e la nuova password.");
  }
  if (newPassword.length < 6) {
    return bad("La nuova password deve essere di almeno 6 caratteri.");
  }

  const user = await env.DB.prepare("SELECT * FROM users WHERE lower(identifier) = ?").bind(email).first();
  if (!user) return bad("Utente non trovato.");

  const requestRow = await env.DB.prepare("SELECT * FROM password_reset_requests WHERE user_id = ?").bind(user.id).first();
  if (!requestRow?.otp_hash || !requestRow.otp_salt || !requestRow.otp_expires_at || requestRow.otp_expires_at < Date.now()) {
    return bad("Codice OTP non valido o scaduto. Chiedi un nuovo codice all'operatore.");
  }
  if (requestRow.otp_attempts >= 5) return bad("Troppi tentativi falliti.");

  if (await hash(otp, requestRow.otp_salt) !== requestRow.otp_hash) {
    await env.DB.prepare("UPDATE password_reset_requests SET otp_attempts = otp_attempts + 1 WHERE user_id = ?").bind(user.id).run();
    return bad("Codice OTP errato.");
  }

  const newSalt = genSalt();
  const newPasswordHash = await hash(newPassword, newSalt);
  const now = Date.now();

  await env.DB.batch([
    env.DB.prepare("UPDATE users SET password_hash = ?, salt = ? WHERE id = ?").bind(newPasswordHash, newSalt, user.id),
    env.DB.prepare("DELETE FROM password_reset_requests WHERE user_id = ?").bind(user.id),
    env.DB.prepare("DELETE FROM sessions WHERE user_id = ?").bind(user.id),
    env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'account', ?, ?, ?)").bind(user.id, "Password modificata", "La password del tuo account è stata reimpostata con successo.", now),
  ]);

  return Response.json({ ok: true, message: "Password aggiornata con successo. Ora puoi effettuare il login." });
}

async function setPasswordResetOtp(request, env, userId) {
  const actor = await adminActor(request, env);
  if (!actor) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  const body = await request.json();
  const otp = typeof body.otp === "string" ? body.otp.trim() : "";
  if (!/^\d{6}$/.test(otp)) return bad("L’OTP deve contenere esattamente 6 cifre.");
  const user = await env.DB.prepare("SELECT id FROM users WHERE id = ?").bind(userId).first();
  if (!user) return Response.json({ error: "Utente non trovato." }, { status: 404 });
  const expiresAt = Date.now() + 15 * 60 * 1000;
  const salt = genSalt();
  const otpHash = await hash(otp, salt);
  const now = Date.now();

  const row = await env.DB.prepare("SELECT user_id FROM password_reset_requests WHERE user_id = ?").bind(userId).first();
  if (!row) {
    await env.DB.prepare("INSERT INTO password_reset_requests (user_id, requested_at, otp_hash, otp_salt, otp_expires_at, otp_attempts, issued_at, issued_by) VALUES (?, ?, ?, ?, ?, 0, ?, ?)")
      .bind(userId, now, otpHash, salt, expiresAt, now, actor.id).run();
  } else {
    await env.DB.prepare("UPDATE password_reset_requests SET otp_hash = ?, otp_salt = ?, otp_expires_at = ?, otp_attempts = 0, issued_at = ?, issued_by = ? WHERE user_id = ?")
      .bind(otpHash, salt, expiresAt, now, actor.id, userId).run();
  }

  if (body.notify === true) {
    await env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at, expires_at) VALUES (?, 'otp', ?, ?, ?, ?)")
      .bind(userId, "Codice OTP recupero password", `Il tuo codice OTP per il reset della password è: ${otp}. Scade tra 15 minuti.`, now, expiresAt).run();
  }

  return Response.json({ ok: true, expiresAt });
}

async function requestEmailVerification(request, env) {
  const user = await sessionUser(request, env);
  const body = await request.json().catch(() => ({}));
  let targetUser = user;
  
  if (!targetUser) {
    const rawAccount = typeof body.account === "string" ? body.account.trim() : "";
    const password = typeof body.password === "string" ? body.password : "";
    const account = normalizeEmail(rawAccount) || rawAccount.toLowerCase();
    if (!account || !password) return bad("Inserisci account e password.");
    targetUser = await env.DB.prepare("SELECT * FROM users WHERE lower(identifier) = ? OR lower(username) = ?").bind(account, account).first();
    if (!targetUser || (await hash(password, targetUser.salt)) !== targetUser.password_hash) return bad("Account o password non corretti.");
  }

  if (targetUser.email_verified) return bad("Questo indirizzo email è già verificato.");

  const now = Date.now();
  await env.DB.prepare("INSERT INTO email_verification_requests (user_id, requested_at, otp_attempts, verified_at) VALUES (?, ?, 0, NULL) ON CONFLICT(user_id) DO UPDATE SET requested_at = excluded.requested_at, verified_at = NULL").bind(targetUser.id, now).run();
  return Response.json({ ok: true, email: targetUser.identifier, username: targetUser.username, userId: targetUser.id, requestedAt: now });
}

async function verifyAccountOtpDirect(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const otp = typeof body.otp === "string" ? body.otp.trim() : "";
  if (!/^\d{6}$/.test(otp)) return bad("Inserisci un codice OTP valido di 6 cifre.");
  if (user.email_verified) return bad("L'account è già verificato.");

  const requestRow = await env.DB.prepare("SELECT * FROM email_verification_requests WHERE user_id = ? AND verified_at IS NULL").bind(user.id).first();
  if (!requestRow?.otp_hash || !requestRow.otp_salt || !requestRow.otp_expires_at || requestRow.otp_expires_at < Date.now()) {
    return bad("OTP non generato o scaduto. Attendi il codice inviato dall'amministratore.");
  }
  if (requestRow.otp_attempts >= 5) return bad("Troppi tentativi falliti.");

  if (await hash(otp, requestRow.otp_salt) !== requestRow.otp_hash) {
    await env.DB.prepare("UPDATE email_verification_requests SET otp_attempts = otp_attempts + 1 WHERE user_id = ?").bind(user.id).run();
    return bad("Codice OTP non corretto.");
  }

  const now = Date.now();
  await env.DB.batch([
    env.DB.prepare("UPDATE users SET email_verified = 1 WHERE id = ?").bind(user.id),
    env.DB.prepare("UPDATE email_verification_requests SET verified_at = ?, otp_hash = NULL, otp_salt = NULL WHERE user_id = ?").bind(now, user.id),
    env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'account', ?, ?, ?)").bind(user.id, "Account verificato", "La verifica dell'email è stata completata con successo.", now),
  ]);

  return Response.json({ ok: true, message: "Account verificato con successo." });
}

async function confirmEmailVerification(request, env) {
  const body = await request.json();
  const email = normalizeEmail(body.email);
  const password = typeof body.password === "string" ? body.password : "";
  const otp = typeof body.otp === "string" ? body.otp.trim() : "";
  if (!email || !password || !/^\d{6}$/.test(otp)) return bad("Inserisci email, password e OTP di 6 cifre.");
  const user = await env.DB.prepare("SELECT * FROM users WHERE lower(identifier) = ?").bind(email).first();
  if (!user || (await hash(password, user.salt)) !== user.password_hash) return bad("Email o password non corrette.");
  if (user.email_verified) return bad("Questo indirizzo email è già verificato.");
  
  const requestRow = await env.DB.prepare("SELECT * FROM email_verification_requests WHERE user_id = ?").bind(user.id).first();
  if (!requestRow?.otp_hash || !requestRow.otp_salt || !requestRow.otp_expires_at || requestRow.otp_expires_at < Date.now()) return bad("OTP assente o scaduto. Chiedi il codice all'amministratore.");
  if (requestRow.otp_attempts >= 5) return bad("Troppi tentativi.");
  
  if (await hash(otp, requestRow.otp_salt) !== requestRow.otp_hash) {
    await env.DB.prepare("UPDATE email_verification_requests SET otp_attempts = otp_attempts + 1 WHERE user_id = ?").bind(user.id).run();
    return bad("OTP non corretto.");
  }
  
  const now = Date.now();
  const token = makeToken();
  await env.DB.batch([
    env.DB.prepare("UPDATE users SET email_verified = 1 WHERE id = ?").bind(user.id),
    env.DB.prepare("UPDATE email_verification_requests SET verified_at = ?, otp_hash = NULL, otp_salt = NULL WHERE user_id = ?").bind(now, user.id),
    env.DB.prepare("INSERT INTO sessions (token, user_id, created_at) VALUES (?, ?, ?)").bind(token, user.id, now),
    env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'account', ?, ?, ?)").bind(user.id, "Email verificata", "La verifica del tuo account è stata completata con successo.", now),
  ]);
  return Response.json({ ok: true, mode: "user", token });
}

async function setUserVerificationOtp(request, env, userId) {
  const actor = await adminActor(request, env);
  if (!actor) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  const body = await request.json();
  const otp = typeof body.otp === "string" ? body.otp.trim() : "";
  if (!/^\d{6}$/.test(otp)) return bad("L’OTP deve contenere esattamente 6 cifre.");
  const user = await env.DB.prepare("SELECT id, email_verified FROM users WHERE id = ?").bind(userId).first();
  if (!user) return Response.json({ error: "Utente non trovato." }, { status: 404 });
  const expiresAt = Date.now() + 15 * 60 * 1000;
  const salt = genSalt();
  const otpHash = await hash(otp, salt);
  const now = Date.now();

  const row = await env.DB.prepare("SELECT user_id FROM email_verification_requests WHERE user_id = ?").bind(userId).first();
  if (!row) {
    await env.DB.prepare("INSERT INTO email_verification_requests (user_id, requested_at, otp_hash, otp_salt, otp_expires_at, otp_attempts, issued_at, issued_by) VALUES (?, ?, ?, ?, ?, 0, ?, ?)")
      .bind(userId, now, otpHash, salt, expiresAt, now, actor.id).run();
  } else {
    await env.DB.prepare("UPDATE email_verification_requests SET otp_hash = ?, otp_salt = ?, otp_expires_at = ?, otp_attempts = 0, issued_at = ?, issued_by = ? WHERE user_id = ?")
      .bind(otpHash, salt, expiresAt, now, actor.id, userId).run();
  }

  if (body.notify === true) {
    await env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at, expires_at) VALUES (?, 'otp', ?, ?, ?, ?)")
      .bind(userId, "Codice OTP di verifica", `Il tuo codice OTP è: ${otp}. Scade tra 15 minuti.`, now, expiresAt).run();
  }

  return Response.json({ ok: true, expiresAt });
}

async function login(request, env) {
  const body = await request.json();
  const rawId = typeof body.id === "string" ? body.id.trim() : "";
  const emailId = normalizeEmail(rawId);
  const id = (emailId || normalizePhone(rawId) || rawId).toLowerCase();
  const pw = typeof body.password === "string" ? body.password : "";
  if (!id || !pw) return Response.json({ error: "Inserisci email/telefono e password." }, { status: 400 });
  
  const user = await env.DB.prepare("SELECT * FROM users WHERE lower(username) = ? OR lower(identifier) = ?").bind(id, id).first();
  let userPasswordValid = false;
  if (user) userPasswordValid = (await hash(pw, user.salt)) === user.password_hash;
  
  const operator = await tryOperatorLogin(env, id, pw);
  if (operator?.token) return Response.json({ ok: true, mode: "operator", token: operator.token });
  
  if (userPasswordValid) {
    const token = makeToken();
    await env.DB.prepare("INSERT INTO sessions (token, user_id, created_at) VALUES (?,?,?)").bind(token, user.id, Date.now()).run();
    
    let adminToken = null;
    if (user.is_operator) {
      adminToken = makeToken();
      // MODIFICATO: Rimossa la scadenza fissa. La sessione admin rimane attiva sul PC finché non viene chiusa l'app/browser o effettuato il logout.
      const expiresAt = Date.now() + 365 * 24 * 60 * 60 * 1000; // Valido indefinitamente finché non scatta la chiusura via sendBeacon/logout
      await env.DB.prepare("INSERT INTO admin_sessions (token, user_id, expires_at) VALUES (?, ?, ?)").bind(adminToken, user.id, expiresAt).run();
    }

    const mode = user.is_operator ? "operator" : "user";
    return Response.json({ ok: true, mode, token, adminToken });
  }
  if (!user) return Response.json({ error: "Account non trovato. Registrati prima di accedere." }, { status: 400 });
  return Response.json({ error: "Password non corretta." }, { status: 400 });
}

async function logout(request, env) {
  let body = {};
  try { body = await request.json(); } catch {}
  const h = (request.headers.get("x-session-token") || body.sessionToken || "").trim();
  const admin = (request.headers.get("x-admin-token") || body.adminToken || "").trim();
  if (h) await env.DB.prepare("DELETE FROM sessions WHERE token = ?").bind(h).run();
  if (admin) await env.DB.prepare("DELETE FROM admin_sessions WHERE token = ?").bind(admin).run();
  return Response.json({ ok: true });
}

async function me(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
  return Response.json(await fullState(env, user, request));
}

async function authed(request, env) {
  const user = await sessionUser(request, env);
  if (!user) throw Object.assign(new Error("Non autenticato."), { status: 401 });
  return user;
}
function bad(msg) { return Response.json({ error: msg }, { status: 400 }); }

async function getNotifications(request, env) {
  const user = await authed(request, env);
  const now = Date.now();
  await env.DB.prepare("DELETE FROM notifications WHERE user_id = ? AND expires_at IS NOT NULL AND expires_at <= ?").bind(user.id, now).run();
  const rows = await env.DB.prepare("SELECT id, kind, title, message, created_at, read_at FROM notifications WHERE user_id = ? ORDER BY created_at DESC LIMIT 50").bind(user.id).all();
  const unread = await env.DB.prepare("SELECT COUNT(*) AS count FROM notifications WHERE user_id = ? AND read_at IS NULL").bind(user.id).first();
  return Response.json({ notifications: rows.results, unread: unread.count });
}

async function markNotificationsRead(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const now = Date.now();
  if (body.all === true) {
    await env.DB.prepare("UPDATE notifications SET read_at = ? WHERE user_id = ? AND read_at IS NULL").bind(now, user.id).run();
  } else {
    const id = Number(body.id);
    if (!Number.isInteger(id) || id < 1) return bad("Notifica non valida.");
    await env.DB.prepare("UPDATE notifications SET read_at = ? WHERE id = ? AND user_id = ? AND read_at IS NULL").bind(now, id, user.id).run();
  }
  return Response.json({ ok: true });
}

async function deposit(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const amount = Number(body.amount);
  const net = String(body.net || "").trim();
  const txReference = String(body.txReference || "").trim().slice(0, 180);
  if (!Number.isFinite(amount) || amount < 50) return bad("Il deposito minimo è di 50 USDT.");
  if (!["BNB", "TRC20", "BTC", "USDC"].includes(net)) return bad("Rete non valida.");
  const officialAddresses = await readDepositAddresses(env);
  if (!officialAddresses[net]) return bad("Questa rete non è ancora configurata per i depositi. Non inviare fondi.");
  if (!txReference) return bad("Inserisci l'ID o hash della transazione per la verifica.");
  
  const existingDep = await env.DB.prepare("SELECT id FROM deposits WHERE lower(tx_reference) = lower(?)").bind(txReference).first();
  if (existingDep) {
    return bad("Questo hash o ID di transazione è già stato utilizzato da un utente.");
  }

  await accrue(env, user.id);
  const created = Date.now();
  await env.DB.batch([
    env.DB.prepare("INSERT INTO deposits (user_id, amount, net, date, status, tx_reference) VALUES (?,?,?,?, 'pending', ?)").bind(user.id, amount, net, created, txReference),
    env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)").bind(user.id, "Richiesta di deposito inviata", amount.toFixed(2) + " USDT · " + net + " · In attesa di verifica.", created),
  ]);
  return Response.json({ ok: true, status: "pending" });
}

const MARKET_RISK_STOP_LOSS = Object.freeze({ conservative: 8, balanced: 18, aggressive: 35 });
const CUSTOM_PLAN_FEES = Object.freeze({ 6: 200, 12: 300, 24: 400, 36: 500, 60: 600 });
const MARKET_CUSTOM_ASSETS = new Set(['gold','oil','electricity','sp500','nasdaq','tesla','artificialIntelligence','bitcoin','ethereum']);
const MARKET_PLAN_ALLOCATIONS = {
  6: [['gold',14],['oil',9],['electricity',12],['sp500',19],['nasdaq',14],['tesla',8],['artificialIntelligence',12],['bitcoin',7],['ethereum',5]],
  12: [['gold',10],['oil',9],['electricity',10],['sp500',18],['nasdaq',16],['tesla',10],['artificialIntelligence',15],['bitcoin',7],['ethereum',5]],
  24: [['gold',8],['oil',8],['electricity',8],['sp500',17],['nasdaq',18],['tesla',10],['artificialIntelligence',17],['bitcoin',8],['ethereum',6]],
  36: [['gold',8],['oil',8],['electricity',8],['sp500',17],['nasdaq',18],['tesla',10],['artificialIntelligence',17],['bitcoin',8],['ethereum',6]],
  60: [['gold',8],['oil',8],['electricity',8],['sp500',17],['nasdaq',18],['tesla',10],['artificialIntelligence',17],['bitcoin',8],['ethereum',6]],
};

async function currentMarketQuotes() {
  if (cachedQuotes && Date.now() - cachedAt < 60_000) return cachedQuotes.quotes;
  const data = await loadQuotes();
  if (Object.keys(data.quotes || {}).length) {
    cachedQuotes = { quotes: { ...(cachedQuotes?.quotes || {}), ...data.quotes }, fetchedAt: data.fetchedAt };
    cachedAt = Date.now();
  }
  return cachedQuotes?.quotes || data.quotes || {};
}

function calendarMaturity(startedAt, months) {
  const start = new Date(startedAt);
  const target = new Date(start);
  target.setDate(1);
  target.setMonth(target.getMonth() + months);
  const lastDay = new Date(target.getFullYear(), target.getMonth() + 1, 0).getDate();
  target.setDate(Math.min(start.getDate(), lastDay));
  target.setHours(start.getHours(), start.getMinutes(), start.getSeconds(), start.getMilliseconds());
  return target.getTime();
}

function marketPositionValue(position, quotes) {
  let allocations = [], basis = {};
  try { allocations = JSON.parse(position.allocations_json || '[]'); } catch {}
  try { basis = JSON.parse(position.entry_quotes_json || '{}'); } catch {}
  let multiplier = 0;
  for (const [key, weight] of allocations) {
    const initial = Number(basis[key]);
    const current = Number(quotes[key]?.price);
    const ratio = initial > 0 && current > 0 ? current / initial : 1;
    multiplier += Number(weight) / 100 * ratio;
  }
  return Number((Number(position.amount) * (multiplier || 1)).toFixed(8));
}

async function syncMarketPositions(env, userId) {
  const rows = (await env.DB.prepare("SELECT * FROM market_positions WHERE user_id = ? ORDER BY created_at DESC").bind(userId).all()).results || [];
  if (!rows.length) return [];
  const quotes = await currentMarketQuotes();
  const now = Date.now();
  for (const row of rows) {
    if (!['active', 'paused'].includes(row.status)) continue;
    const value = marketPositionValue(row, quotes);
    const amount = Number(row.amount);
    const stop = amount * (1 - Number(row.stop_loss_pct) / 100);
    const maturity = calendarMaturity(Number(row.created_at), Number(row.months));
    const riskLevel = row.risk_level || 'legacy';
    let status;
    if (now >= maturity) status = 'matured';
    else if (riskLevel === 'aggressive') status = value <= stop ? 'stop_loss' : 'active';
    else status = value <= stop ? 'paused' : 'active';
    const previousStatus = row.status;
    const isClosed = status === 'matured' || status === 'stop_loss';
    const updated = await env.DB.prepare("UPDATE market_positions SET last_value = ?, status = ?, closed_at = ? WHERE id = ? AND user_id = ? AND status = ?")
      .bind(value, status, isClosed ? now : null, row.id, userId, previousStatus).run();
    row.last_value = value;
    if (updated.meta?.changes > 0) {
      row.status = status;
      row.closed_at = isClosed ? now : null;
      if (status === 'matured') {
        await env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)")
          .bind(userId, 'Plan matured', 'Reference value ' + value.toFixed(2) + ' USDT. Internal balance updated; no market order or external payout was executed.', now).run();
      } else if (status === 'stop_loss') {
        await env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)")
          .bind(userId, 'Aggressive plan closed at stop', 'The aggressive plan reached its fixed ' + Number(row.stop_loss_pct) + '% stop. Remaining reference value was moved to your internal released balance.', now).run();
      } else if (status === 'paused' && previousStatus !== 'paused') {
        await env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)")
          .bind(userId, 'Plan paused at stop', 'Reference value reached the fixed ' + Number(row.stop_loss_pct) + '% stop. The plan will resume if a later market-data refresh places it above the stop.', now).run();
      } else if (status === 'active' && previousStatus === 'paused') {
        await env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)")
          .bind(userId, 'Plan resumed', 'Reference value is back above the fixed stop. The plan has resumed on this market-data refresh.', now).run();
      }
      const autoPct = Number(row.auto_redeem_pct || 10);
      const autoThreshold = amount * (1 + autoPct / 100);
      if (status === 'active' && Number(row.auto_redeem_enabled) === 1 && autoPct >= 1 && value >= autoThreshold) {
        let allocation = [], basisReady = true;
        try { allocation = JSON.parse(row.allocations_json || '[]'); } catch { basisReady = false; }
        const nextBasis = {};
        for (const [key] of allocation) {
          const price = Number(quotes[key]?.price);
          if (!(price > 0)) { basisReady = false; break; }
          nextBasis[key] = price;
        }
        const gain = Number(Math.max(0, value - amount).toFixed(2));
        if (basisReady && Object.keys(nextBasis).length && gain >= 0.01) {
          const transactionId = makeToken(), basisJson = JSON.stringify(nextBasis);
          const redemption = await env.DB.batch([
            env.DB.prepare("UPDATE market_positions SET last_value = ?, entry_quotes_json = ?, last_redemption_id = ?, basis_updated_at = ? WHERE id = ? AND user_id = ? AND status = 'active' AND last_value = ? AND entry_quotes_json = ? AND auto_redeem_enabled = 1 AND auto_redeem_pct = ? AND EXISTS (SELECT 1 FROM wallets WHERE user_id = ?)")
              .bind(amount, basisJson, transactionId, now, row.id, userId, value, row.entry_quotes_json, autoPct, userId),
            env.DB.prepare("UPDATE wallets SET earnings = earnings + ? WHERE user_id = ? AND EXISTS (SELECT 1 FROM market_positions WHERE id = ? AND user_id = ? AND status = 'active' AND last_value = ? AND last_redemption_id = ?)")
              .bind(gain, userId, row.id, userId, amount, transactionId),
            env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) SELECT ?, 'transaction', ?, ?, ? WHERE EXISTS (SELECT 1 FROM market_positions WHERE id = ? AND user_id = ? AND status = 'active' AND last_value = ? AND last_redemption_id = ?)")
              .bind(userId, 'Automatic profit redeemed', gain.toFixed(2) + ' USDT moved to the redeemable balance after reaching the ' + autoPct + '% gain threshold. The plan remains active.', now, row.id, userId, amount, transactionId),
          ]);
          if (redemption[0]?.meta?.changes > 0) {
            row.last_value = amount;
            row.entry_quotes_json = basisJson;
            row.last_redemption_id = transactionId;
            row.basis_updated_at = now;
          }
        }
      }
    }
  }
  return rows.map(row => ({
    id: row.id, planId: row.plan_id, months: Number(row.months), amount: Number(row.amount),
    stopLossPct: Number(row.stop_loss_pct), riskLevel: row.risk_level || 'legacy', earlyCloseFee: Number(row.early_close_fee || 0), basisUpdatedAt: Number(row.basis_updated_at || row.created_at), entryQuotes: (() => { try { return JSON.parse(row.entry_quotes_json || '{}'); } catch { return {}; } })(), currentValue: Number(row.last_value),
    autoRedeemEnabled: Number(row.auto_redeem_enabled) === 1, autoRedeemPct: Number(row.auto_redeem_pct || 10), customFee: Number(row.custom_fee || 0),
    status: row.status, createdAt: Number(row.created_at), closedAt: row.closed_at ? Number(row.closed_at) : null,
    allocations: (() => { try { return JSON.parse(row.allocations_json || '[]'); } catch { return []; } })(),
  }));
}

const DEFAULT_DEPOSIT_ADDRESSES = {
  BNB: "0x2859d146Dc8e4cB332736986feE9D32B641fbde8",
  TRC20: "TYQgWx4eQ6Js94UMexfyLXbqNE4Fucfg7Y",
  BTC: "bc1par0exs9cyw9w53xsceyh6wzl7f43gdjn6xsq0kyq4qsqsvr2uynqf5llc6"
};
async function readDepositAddresses(env) {
  const rows = (await env.DB.prepare("SELECT config_key, config_value FROM platform_config WHERE config_key LIKE 'deposit_%'").all()).results || [];
  return { ...DEFAULT_DEPOSIT_ADDRESSES, ...Object.fromEntries(rows.map(row => [row.config_key.slice(8), row.config_value])) };
}
async function getDepositAddresses(request, env) {
  return Response.json({ addresses: await readDepositAddresses(env) }, { headers: { "Cache-Control": "no-store" } });
}
async function adminDepositAddresses(request, env) {
  if (!await adminActor(request, env)) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  return Response.json({ addresses: await readDepositAddresses(env) });
}
async function saveAdminDepositAddresses(request, env) {
  const operator = await adminActor(request, env);
  if (!operator) return Response.json({ error: "Permessi admin richiesti." }, { status: 403 });
  const body = await request.json();
  const allowed = ["BNB", "TRC20", "BTC", "USDC"];
  const entries = [];
  for (const net of allowed) {
    const value = typeof body.addresses?.[net] === "string" ? body.addresses[net].trim().slice(0, 160) : "";
    if (value && !/^[A-Za-z0-9:_-]{12,160}$/.test(value)) return bad("Indirizzo non valido per " + net + ".");
    entries.push(value
      ? env.DB.prepare("INSERT INTO platform_config(config_key,config_value) VALUES(?,?) ON CONFLICT(config_key) DO UPDATE SET config_value=excluded.config_value").bind("deposit_" + net, value)
      : env.DB.prepare("DELETE FROM platform_config WHERE config_key = ?").bind("deposit_" + net));
  }
  await env.DB.batch(entries);
  return Response.json({ ok: true, addresses: await readDepositAddresses(env) });
}

async function marketInvest(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const amount = Number(body.amount);
  const months = Number(body.months);
  const riskLevel = typeof body.riskLevel === 'string' ? body.riskLevel : 'balanced';
  const stopLossPct = MARKET_RISK_STOP_LOSS[riskLevel];
  const allocation = MARKET_PLAN_ALLOCATIONS[months];
  if (!allocation) return bad("Durata del piano non valida.");
  if (!Number.isFinite(amount) || amount < 50) return bad("L'importo minimo è 50 USDT.");
  if (!Number.isFinite(stopLossPct)) return bad("Profilo stop-loss non disponibile.");
  const source = body.source === 'earnings' ? 'earnings' : 'deposit';
  const wallet = await accrue(env, user.id);
  if (amount > Number(wallet[source] || 0) + 1e-9) return bad("Saldo disponibile insufficiente.");
  const quotes = await currentMarketQuotes();
  const entry = {};
  for (const [key] of allocation) {
    const price = Number(quotes[key]?.price);
    if (!Number.isFinite(price) || price <= 0) return bad("Prezzi live temporaneamente non disponibili per completare il piano. Riprova più tardi.");
    entry[key] = price;
  }
  const now = Date.now();
  const results = await env.DB.batch([
    env.DB.prepare(
      "INSERT INTO market_positions (user_id, plan_id, months, amount, stop_loss_pct, risk_level, allocations_json, entry_quotes_json, last_value, status, created_at, basis_updated_at) " +
      "SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ? WHERE EXISTS (SELECT 1 FROM wallets WHERE user_id = ? AND " + source + " >= ?)"
    ).bind(user.id, 'plan-' + months + 'm', months, amount, stopLossPct, riskLevel, JSON.stringify(allocation), JSON.stringify(entry), amount, now, now, user.id, amount),
    env.DB.prepare("UPDATE wallets SET " + source + " = " + source + " - ? WHERE user_id = ? AND " + source + " >= ?").bind(amount, user.id, amount),
  ]);
  if (!(results[0]?.meta?.changes > 0) || !(results[1]?.meta?.changes > 0)) return bad("Saldo insufficiente o modifica concorrente dell'account. Aggiorna e riprova.");
  await env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)")
    .bind(user.id, "Piano market avviato", amount.toFixed(2) + " USDT · durata " + months + " mesi · rischio " + riskLevel + " · stop-loss fisso " + stopLossPct + "%.", now).run();
  return Response.json({ ok: true, positionId: results[0]?.meta?.last_row_id || null });
}

async function customMarketInvest(request, env) {
  const user = await authed(request, env);
  const body = await request.json().catch(() => ({}));
  const months = Number(body.months);
  const principal = Number(body.amount);
  const stopLossPct = Number(body.stopLossPct);
  const fee = CUSTOM_PLAN_FEES[months];
  if (!fee) return bad('Durata del piano personalizzato non valida.');
  if (!Number.isFinite(principal) || principal < 1000) return bad('L’investimento minimo per un portafoglio personalizzato è 1.000 USDT.');
  if (!Number.isFinite(stopLossPct) || stopLossPct < 1 || stopLossPct > 90) return bad('Lo stop loss deve essere tra 1% e 90%.');
  const source = body.source === 'earnings' ? 'earnings' : 'deposit';
  const rawAllocations = Array.isArray(body.allocations) ? body.allocations : [];
  if (rawAllocations.length < 1 || rawAllocations.length > MARKET_CUSTOM_ASSETS.size) return bad('Seleziona almeno un asset.');
  const seen = new Set(), allocations = [];
  for (const item of rawAllocations) {
    const key = Array.isArray(item) ? String(item[0] || '') : String(item?.asset || '');
    const weight = Number(Array.isArray(item) ? item[1] : item?.weight);
    if (!MARKET_CUSTOM_ASSETS.has(key) || seen.has(key) || !Number.isFinite(weight) || weight <= 0 || weight > 100) return bad('Asset o percentuale non validi.');
    seen.add(key);
    allocations.push([key, weight]);
  }
  const allocationTotal = allocations.reduce((sum, item) => sum + item[1], 0);
  if (Math.abs(allocationTotal - 100) > 0.01) return bad('Le percentuali degli asset devono sommare esattamente 100%.');
  const totalCharge = Number((principal + fee).toFixed(2));
  const wallet = await accrue(env, user.id);
  if (totalCharge > Number(wallet[source] || 0) + 1e-9) return bad('Saldo insufficiente: servono ' + totalCharge.toFixed(2) + ' USDT, inclusa la quota del piano.');
  const quotes = await currentMarketQuotes(), entry = {};
  for (const [key] of allocations) {
    const price = Number(quotes[key]?.price);
    if (!Number.isFinite(price) || price <= 0) return bad('Prezzi di riferimento non disponibili per tutti gli asset selezionati. Riprova più tardi.');
    entry[key] = price;
  }
  const now = Date.now();
  const results = await env.DB.batch([
    env.DB.prepare(
      "INSERT INTO market_positions (user_id, plan_id, months, amount, stop_loss_pct, risk_level, allocations_json, entry_quotes_json, last_value, status, created_at, basis_updated_at, custom_fee) " +
      "SELECT ?, ?, ?, ?, ?, 'custom', ?, ?, ?, 'active', ?, ?, ? WHERE EXISTS (SELECT 1 FROM wallets WHERE user_id = ? AND " + source + " >= ?)"
    ).bind(user.id, 'custom-' + months + 'm', months, principal, stopLossPct, JSON.stringify(allocations), JSON.stringify(entry), principal, now, now, fee, user.id, totalCharge),
    env.DB.prepare("UPDATE wallets SET " + source + " = " + source + " - ? WHERE user_id = ? AND " + source + " >= ?")
      .bind(totalCharge, user.id, totalCharge),
  ]);
  if (!(results[0]?.meta?.changes > 0) || !(results[1]?.meta?.changes > 0)) return bad('Saldo insufficiente o modifica concorrente dell’account. Aggiorna e riprova.');
  await env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)")
    .bind(user.id, 'Portafoglio personalizzato avviato', principal.toFixed(2) + ' USDT investiti · quota piano ' + fee.toFixed(2) + ' USDT · durata ' + months + ' mesi · stop loss ' + stopLossPct + '%.', now).run();
  return Response.json({ ok: true, positionId: results[0]?.meta?.last_row_id || null, amount: principal, fee, totalCharged: totalCharge });
}

async function invest(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const amount = parseFloat(body.amount);
  const days = parseInt(body.days, 10);
  const plan = PLANS.find(p => p.days === days);
  if (!plan) return bad("Piano non valido.");
  if (!Number.isFinite(amount) || amount < plan.min) return bad(`Importo minimo: ${plan.min} USDT.`);
  
  const wallet = await accrue(env, user.id);
  const stats = await inviterStats(env, user.id);
  const finalRate = plan.rate + (stats.planBonus || 0);

  let deductDeposit = 0;
  let deductEarnings = 0;

  if (wallet.deposit >= amount) {
    deductDeposit = amount;
  } else if ((wallet.deposit + wallet.earnings) >= amount) {
    deductDeposit = wallet.deposit;
    deductEarnings = amount - wallet.deposit;
  } else {
    return bad("Saldo insufficiente (controlla saldo deposito e guadagni).");
  }

  const created = Date.now();
  await env.DB.batch([
    env.DB.prepare("UPDATE wallets SET deposit = deposit - ?, earnings = earnings - ? WHERE user_id = ?").bind(deductDeposit, deductEarnings, user.id),
    env.DB.prepare("INSERT INTO investments (user_id, amount, days, rate, created, credited, active) VALUES (?, ?, ?, ?, ?, 0, 1)").bind(user.id, amount, plan.days, finalRate, created),
    env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)").bind(user.id, "Investimento avviato", amount.toFixed(2) + " USDT · Piano " + plan.days + " giorni (Tasso: " + finalRate.toFixed(2) + "%).", created),
  ]);
  return Response.json({ ok: true, wallet: await accrue(env, user.id) });
}

async function redeemMarketGains(request, env) {
  const user = await authed(request, env);
  const body = await request.json().catch(() => ({}));
  const id = Number(body.positionId);
  if (!Number.isInteger(id) || id < 1) return bad('Piano non valido.');
  await syncMarketPositions(env, user.id);
  const position = await env.DB.prepare("SELECT * FROM market_positions WHERE id = ? AND user_id = ? AND status IN ('active','paused')").bind(id, user.id).first();
  if (!position) return Response.json({ error: 'Il piano non è più attivo o riscattabile.' }, { status: 409 });
  const quotes = await currentMarketQuotes();
  const value = marketPositionValue(position, quotes);
  const gain = Number((value - Number(position.amount)).toFixed(2));
  if (!(gain >= 0.01)) return bad('Non ci sono guadagni sopra il capitale investito da riscattare.');
  let allocation = [];
  try { allocation = JSON.parse(position.allocations_json || '[]'); } catch {}
  const nextBasis = {};
  for (const [key] of allocation) {
    const price = Number(quotes[key]?.price);
    if (!(price > 0)) return Response.json({ error: 'Prezzi di riferimento non disponibili; riprova tra poco.' }, { status: 503 });
    nextBasis[key] = price;
  }
  const basisJson = JSON.stringify(nextBasis);
  const transactionId = makeToken();
  const oldStatus = position.status;
  const batch = await env.DB.batch([
    env.DB.prepare("UPDATE market_positions SET last_value = ?, entry_quotes_json = ?, last_redemption_id = ?, basis_updated_at = ? WHERE id = ? AND user_id = ? AND status = ? AND last_value = ? AND entry_quotes_json = ? AND EXISTS (SELECT 1 FROM wallets WHERE user_id = ?)")
      .bind(Number(position.amount), basisJson, transactionId, Date.now(), id, user.id, oldStatus, Number(position.last_value), position.entry_quotes_json, user.id),
    env.DB.prepare("UPDATE wallets SET earnings = earnings + ? WHERE user_id = ? AND EXISTS (SELECT 1 FROM market_positions WHERE id = ? AND user_id = ? AND status = ? AND last_value = ? AND last_redemption_id = ?)")
      .bind(gain, user.id, id, user.id, oldStatus, Number(position.amount), transactionId),
    env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) SELECT ?, 'transaction', ?, ?, ? WHERE EXISTS (SELECT 1 FROM market_positions WHERE id = ? AND user_id = ? AND status = ? AND last_redemption_id = ?)")
      .bind(user.id, 'Plan gains redeemed', gain.toFixed(2) + ' USDT moved to your internal redeemable balance. The invested principal remains in the plan.', Date.now(), id, user.id, oldStatus, transactionId),
  ]);
  if (!(batch[0]?.meta?.changes > 0)) return Response.json({ error: 'Il piano è cambiato. Aggiorna e riprova.' }, { status: 409 });
  return Response.json({ ok: true, redeemed: gain, referenceValue: Number(position.amount) });
}

async function closeMarketPositionEarly(request, env) {
  const user = await authed(request, env);
  const body = await request.json().catch(() => ({}));
  const id = Number(body.positionId);
  if (!Number.isInteger(id) || id < 1) return bad('Piano non valido.');
  await syncMarketPositions(env, user.id);
  const position = await env.DB.prepare("SELECT * FROM market_positions WHERE id = ? AND user_id = ? AND status IN ('active','paused')").bind(id, user.id).first();
  if (!position) return Response.json({ error: 'Il piano è già chiuso, maturato o non disponibile.' }, { status: 409 });
  const quotes = await currentMarketQuotes();
  const value = Number(marketPositionValue(position, quotes).toFixed(8));
  if (!(value > 0)) return bad('Il valore di riferimento non è disponibile per la chiusura.');
  const fee = Number((value * 0.65).toFixed(8));
  const net = Number((value - fee).toFixed(8));
  const principalCredit = Math.min(Number(position.amount), net);
  const profitCredit = Math.max(0, net - Number(position.amount));
  const now = Date.now();
  const transactionId = makeToken();
  const batch = await env.DB.batch([
    env.DB.prepare("UPDATE market_positions SET status = 'early_closed', closed_at = ?, last_value = ?, early_close_fee = ?, last_redemption_id = ? WHERE id = ? AND user_id = ? AND status = ? AND EXISTS (SELECT 1 FROM wallets WHERE user_id = ?)")
      .bind(now, value, fee, transactionId, id, user.id, position.status, user.id),
    env.DB.prepare("UPDATE wallets SET released = MAX(0, released - ? + ?), earnings = earnings + ? WHERE user_id = ? AND EXISTS (SELECT 1 FROM market_positions WHERE id = ? AND user_id = ? AND status = 'early_closed' AND last_redemption_id = ?)")
      .bind(value, principalCredit, profitCredit, user.id, id, user.id, transactionId),
    env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) SELECT ?, 'transaction', ?, ?, ? WHERE EXISTS (SELECT 1 FROM market_positions WHERE id = ? AND user_id = ? AND status = 'early_closed' AND last_redemption_id = ?)")
      .bind(user.id, 'Plan closed early', 'Current reference value ' + value.toFixed(2) + ' USDT; 65% early-closure fee ' + fee.toFixed(2) + ' USDT; net internal balance ' + net.toFixed(2) + ' USDT. No external payout was made.', now, id, user.id, transactionId),
  ]);
  if (!(batch[0]?.meta?.changes > 0)) return Response.json({ error: 'Il piano è cambiato. Aggiorna e riprova.' }, { status: 409 });
  return Response.json({ ok: true, referenceValue: value, fee, net });
}

async function savePositionAutoRedeem(request, env) {
  const user = await authed(request, env);
  const body = await request.json().catch(() => ({}));
  const positionId = Number(body.positionId);
  const enabled = body.enabled === true;
  const percent = Number(body.percent);
  if (!Number.isInteger(positionId) || positionId < 1) return bad('Piano non valido.');
  if (!Number.isFinite(percent) || percent < 1 || percent > 100) return bad('La soglia deve essere tra 1 e 100%.');
  const result = await env.DB.prepare("UPDATE market_positions SET auto_redeem_enabled = ?, auto_redeem_pct = ? WHERE id = ? AND user_id = ? AND status IN ('active','paused')")
    .bind(enabled ? 1 : 0, percent, positionId, user.id).run();
  if (!(result.meta?.changes > 0)) return Response.json({ error: 'Questo piano non è più attivo.' }, { status: 409 });
  return Response.json({ ok: true, positionId, enabled, percent });
}

async function validateWithdrawal(env, user, body) {
  const amount = Number(body.amount);
  const net = String(body.net || "").trim();
  if (!Number.isFinite(amount) || amount < 50) return bad("Il prelievo minimo è di 50 USDT.");
  if (!["BNB", "TRC20", "BTC", "USDC"].includes(net)) return bad("Rete non valida.");
  const wallet = await accrue(env, user.id);
  const source = body.source === 'released' ? 'released' : 'earnings';
  if (amount > Number(wallet[source] || 0) + 1e-9) return bad(source === 'released' ? "Saldo capitale restituito non sufficiente." : "Saldo guadagni non sufficiente.");
  
  let address = String(body.address || "").trim();
  if (!address || address === "wallet-default" || address === "") {
    const s = await env.DB.prepare("SELECT withdraw_wallets FROM settings WHERE user_id = ?").bind(user.id).first();
    if (s && s.withdraw_wallets) {
      try {
        const wallets = JSON.parse(s.withdraw_wallets);
        if (wallets[net]) {
          address = wallets[net];
        }
      } catch {}
    }
  }

  if (!address) {
    address = "Indirizzo non specificato dall'utente";
  } else {
    const existing = await env.DB.prepare("SELECT user_id FROM user_withdrawal_addresses WHERE net = ? AND lower(address) = ? AND user_id != ?").bind(net, address.trim().toLowerCase(), user.id).first();
    if (existing) {
      return bad(`Questo indirizzo di prelievo per la rete ${net} è già utilizzato da un altro account.`);
    }
  }

  return { amount, net, address, source };
}

async function withdraw(request, env) {
  const user = await authed(request, env);
  if (!user.email_verified) {
    return Response.json({ error: "Verifica l’email prima di richiedere un prelievo.", verificationRequired: true, email: user.identifier }, { status: 403 });
  }
  const body = await request.json();
  const prepared = await validateWithdrawal(env, user, body);
  if (prepared instanceof Response) return prepared;

  const created = Date.now();
  await env.DB.batch([
    env.DB.prepare("UPDATE wallets SET " + prepared.source + " = " + prepared.source + " - ? WHERE user_id = ? AND " + prepared.source + " >= ?").bind(prepared.amount, user.id, prepared.amount),
    env.DB.prepare("INSERT INTO withdrawals (user_id, amount, net, date, status, wallet_address, source) VALUES (?, ?, ?, ?, 'pending', ?, ?)")
      .bind(user.id, prepared.amount, prepared.net, created, prepared.address, prepared.source),
    env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)").bind(user.id, "Richiesta di prelievo inviata", prepared.amount.toFixed(2) + " USDT · " + prepared.net + " · In attesa di verifica.", created),
  ]);
  return Response.json({ ok: true, status: "pending" });
}

async function confirmSensitiveActionOtp(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const otp = typeof body.otp === "string" ? body.otp.trim() : "";
  if (!/^\d{6}$/.test(otp)) return bad("Inserisci un codice OTP valido di 6 cifre.");
  
  const actionRow = await env.DB.prepare("SELECT * FROM sensitive_action_verifications WHERE user_id = ? AND consumed_at IS NULL").bind(user.id).first();
  if (!actionRow || !actionRow.otp_hash || !actionRow.otp_salt || actionRow.otp_expires_at < Date.now()) {
    return bad("Codice OTP assente o scaduto.");
  }
  if (actionRow.otp_attempts >= 5) return bad("Troppi tentativi falliti.");
  
  if (await hash(otp, actionRow.otp_salt) !== actionRow.otp_hash) {
    await env.DB.prepare("UPDATE sensitive_action_verifications SET otp_attempts = otp_attempts + 1 WHERE user_id = ?").bind(user.id).run();
    return bad("Codice OTP non corretto.");
  }
  
  await env.DB.prepare("UPDATE sensitive_action_verifications SET consumed_at = ? WHERE user_id = ?").bind(Date.now(), user.id).run();
  return Response.json({ ok: true });
}

async function verifyOperatorAccess(request, env) {
  const adminActorUser = await adminActor(request, env);
  if (adminActorUser) return adminActorUser;
  const sessionUserObj = await sessionUser(request, env);
  if (sessionUserObj && sessionUserObj.is_operator) return sessionUserObj;
  return null;
}

async function adminTransactions(request, env) {
  const operator = await verifyOperatorAccess(request, env);
  if (!operator) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  
  const deposits = (await env.DB.prepare("SELECT d.*, u.username FROM deposits d JOIN users u ON u.id = d.user_id WHERE d.status = 'pending' ORDER BY d.date ASC").all()).results;
  const withdrawals = (await env.DB.prepare("SELECT w.*, u.username FROM withdrawals w JOIN users u ON u.id = w.user_id WHERE w.status = 'pending' ORDER BY w.date ASC").all()).results;
  
  const rawWalletChanges = (await env.DB.prepare(`
    SELECT r.id, r.user_id, r.net, r.old_address, r.new_address, r.created_at, r.status, u.username, u.identifier 
    FROM wallet_change_requests r 
    JOIN users u ON u.id = r.user_id 
    WHERE r.status = 'pending' 
    ORDER BY r.created_at ASC
  `).all()).results;

  const walletChanges = rawWalletChanges.map(w => {
    const addressDetails = `Vecchio: ${w.old_address} -> Nuovo: ${w.new_address}`;
    return {
      ...w,
      net: `${w.net} | ${addressDetails}`,
      oldAddress: w.old_address,
      newAddress: w.new_address,
      old_address: w.old_address,
      new_address: w.new_address,
      address: addressDetails,
      wallet_address: addressDetails,
      walletAddress: addressDetails,
      account: addressDetails,
      details: addressDetails,
      description: addressDetails,
      text: addressDetails
    };
  });
  
  const emailVerifications = (await env.DB.prepare("SELECT r.user_id, r.requested_at, r.otp_expires_at, u.username, u.identifier FROM email_verification_requests r JOIN users u ON u.id = r.user_id WHERE r.verified_at IS NULL").all()).results;
  const sensitiveActionVerifications = (await env.DB.prepare("SELECT r.user_id, r.action, r.requested_at, r.otp_expires_at, u.username, u.identifier FROM sensitive_action_verifications r JOIN users u ON u.id = r.user_id WHERE r.consumed_at IS NULL").all()).results;
  const passwordResets = (await env.DB.prepare("SELECT r.user_id, r.requested_at, r.otp_expires_at, r.issued_at, u.username, u.identifier FROM password_reset_requests r JOIN users u ON u.id = r.user_id").all()).results;

  return Response.json({ deposits, withdrawals, walletChanges, emailVerifications, sensitiveActionVerifications, passwordResets, operator: operator.username });
}

async function adminUsers(request, env) {
  const operator = await verifyOperatorAccess(request, env);
  if (!operator) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  
  const urlObj = new URL(request.url);
  const searchEmail = (urlObj.searchParams.get("search") || "").trim().toLowerCase();

  let query = "SELECT id, username, identifier, created_at, email_verified, is_operator FROM users";
  let params = [];
  if (searchEmail) {
    query += " WHERE lower(identifier) LIKE ? OR lower(username) LIKE ?";
    params = [`%${searchEmail}%`, `%${searchEmail}%`];
  }
  query += " ORDER BY created_at DESC";

  const users = (await env.DB.prepare(query).bind(...params).all()).results;
  return Response.json({ users });
}

async function adminStats(request, env) {
  const operator = await verifyOperatorAccess(request, env);
  if (!operator) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });

  const totalUsersRes = await env.DB.prepare("SELECT COUNT(*) as count FROM users").first();
  const totalDepositsRes = await env.DB.prepare("SELECT COALESCE(SUM(amount), 0) as total, COUNT(*) as count FROM deposits WHERE status = 'approved'").first();
  const totalWithdrawalsRes = await env.DB.prepare("SELECT COALESCE(SUM(amount), 0) as total, COUNT(*) as count FROM withdrawals WHERE status = 'approved'").first();
  const totalInvestmentsRes = await env.DB.prepare("SELECT COALESCE(SUM(amount), 0) as total, COUNT(*) as count FROM investments").first();
  const totalEarningsRes = await env.DB.prepare("SELECT COALESCE(SUM(earnings), 0) as total, COALESCE(SUM(total_earned), 0) as historical FROM wallets").first();

  return Response.json({
    totalUsers: totalUsersRes?.count || 0,
    deposits: {
      count: totalDepositsRes?.count || 0,
      total: totalDepositsRes?.total || 0
    },
    withdrawals: {
      count: totalWithdrawalsRes?.count || 0,
      total: totalWithdrawalsRes?.total || 0
    },
    investments: {
      count: totalInvestmentsRes?.count || 0,
      total: totalInvestmentsRes?.total || 0
    },
    earnings: {
      current: totalEarningsRes?.total || 0,
      historical: totalEarningsRes?.historical || 0
    }
  });
}

async function adminUserDetails(request, env, userId) {
  const operator = await verifyOperatorAccess(request, env);
  if (!operator) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  
  const target = await env.DB.prepare("SELECT id, username, identifier, created_at, referred_by, email_verified, is_operator FROM users WHERE id = ?").bind(userId).first();
  if (!target) return Response.json({ error: "Utente non trovato." }, { status: 404 });

  const wallet = await accrue(env, target.id);
  const investments = (await env.DB.prepare("SELECT id, amount, days, rate, created, credited, active FROM investments WHERE user_id = ?").bind(target.id).all()).results;
  const marketPositions = await syncMarketPositions(env, target.id);
  const verificationRequest = await env.DB.prepare("SELECT requested_at, otp_expires_at, verified_at FROM email_verification_requests WHERE user_id = ?").bind(target.id).first();
  const actionVerification = await env.DB.prepare("SELECT action, requested_at, otp_expires_at FROM sensitive_action_verifications WHERE user_id = ? AND consumed_at IS NULL").bind(target.id).first();
  const passwordResetRequest = await env.DB.prepare("SELECT requested_at, otp_expires_at, issued_at FROM password_reset_requests WHERE user_id = ?").bind(target.id).first();

  const directInvites = (await env.DB.prepare("SELECT id, username, identifier, created_at FROM users WHERE referred_by = ? ORDER BY created_at DESC").bind(target.id).all()).results;
  const inviteStats = await inviterStats(env, target.id);

  return Response.json({ 
    user: target, 
    wallet, 
    investments, 
    marketPositions,
    verificationRequest, 
    actionVerification, 
    passwordResetRequest,
    inviteStats: {
      directCount: inviteStats.direct,
      secondCount: inviteStats.second,
      level: inviteStats.level,
      directUsers: directInvites
    }
  });
}

async function adminSetOperatorRole(request, env, userId) {
  const operator = await verifyOperatorAccess(request, env);
  if (!operator) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });

  const target = await env.DB.prepare("SELECT id, is_operator, identifier FROM users WHERE id = ?").bind(userId).first();
  if (!target) return Response.json({ error: "Utente non trovato." }, { status: 404 });
  if (target.identifier.toLowerCase() === "supercloud3.invest@yahoo.com") {
    return bad("Non è possibile modificare i permessi dell'amministratore principale.");
  }

  const body = await request.json().catch(() => ({}));
  const makeOperator = body.makeOperator === true ? 1 : 0;
  
  await env.DB.prepare("UPDATE users SET is_operator = ? WHERE id = ?").bind(makeOperator, userId).run();

  return Response.json({ ok: true, isOperator: !!makeOperator });
}

async function adminToggleOperator(request, env, userId) {
  const operator = await verifyOperatorAccess(request, env);
  if (!operator) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });

  const target = await env.DB.prepare("SELECT id, is_operator, identifier FROM users WHERE id = ?").bind(userId).first();
  if (!target) return Response.json({ error: "Utente non trovato." }, { status: 404 });
  if (target.identifier.toLowerCase() === "supercloud3.invest@yahoo.com") {
    return bad("Non è possibile modificare i permessi dell'amministratore principale.");
  }

  const newStatus = target.is_operator ? 0 : 1;
  await env.DB.prepare("UPDATE users SET is_operator = ? WHERE id = ?").bind(newStatus, userId).run();

  return Response.json({ ok: true, isOperator: !!newStatus });
}

async function adminUpdateUserBalance(request, env, userId) {
  const operator = await verifyOperatorAccess(request, env);
  if (!operator) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  
  const body = await request.json();
  const deposit = Number(body.deposit);
  const earnings = Number(body.earnings);
  const released = Number(body.released || 0);
  const now = Date.now();
  await env.DB.batch([
    env.DB.prepare("UPDATE wallets SET deposit = ?, earnings = ?, released = ? WHERE user_id = ?").bind(deposit, earnings, released, userId),
    env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)").bind(userId, "Saldo wallet aggiornato", "Il tuo wallet è stato modificato dall'operatore.", now)
  ]);
  return Response.json({ ok: true });
}

async function adminDeleteUser(request, env, userId) {
  const operator = await verifyOperatorAccess(request, env);
  if (!operator) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  
  const target = await env.DB.prepare("SELECT identifier FROM users WHERE id = ?").bind(userId).first();
  if (!target) return Response.json({ error: "Utente non trovato." }, { status: 404 });
  if (target.identifier.toLowerCase() === "supercloud3.invest@yahoo.com") {
    return bad("Non è possibile eliminare l'account amministratore principale.");
  }

  await env.DB.batch([
    env.DB.prepare("DELETE FROM sessions WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM admin_sessions WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM email_verification_requests WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM password_reset_requests WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM sensitive_action_verifications WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM notifications WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM wallets WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM investments WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM market_positions WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM referral_commissions WHERE user_id = ? OR source_user_id = ?").bind(userId, userId),
    env.DB.prepare("DELETE FROM referral_first_deposits WHERE source_user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM deposits WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM withdrawals WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM settings WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM user_withdrawal_addresses WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM wallet_change_requests WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM users WHERE id = ?").bind(userId)
  ]);

  return Response.json({ ok: true, message: "Utente e tutti i dati associati eliminati con successo." });
}

async function reviewTransaction(request, env) {
  const operator = await verifyOperatorAccess(request, env);
  if (!operator) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  
  const body = await request.json();
  const kind = body.kind;
  const id = Number(body.id);
  let action = body.action;
  
  let userId = null;
  let amount = 0;
  let net = "";

  if (kind === "deposit") {
    const dep = await env.DB.prepare("SELECT * FROM deposits WHERE id = ?").bind(id).first();
    if (!dep) return bad("Deposito non trovato.");
    userId = dep.user_id;
    amount = dep.amount;
    net = dep.net;

    if (action === "approve") {
      await env.DB.prepare("UPDATE deposits SET status = 'approved' WHERE id = ?").bind(id).run();
      await env.DB.prepare("UPDATE wallets SET deposit = deposit + ?, total_deposited = total_deposited + ? WHERE user_id = ?").bind(amount, amount, userId).run();

      const existingFirst = await env.DB.prepare("SELECT 1 FROM referral_first_deposits WHERE source_user_id = ?").bind(userId).first();
      if (!existingFirst) {
        const now = Date.now();
        await env.DB.prepare("INSERT OR IGNORE INTO referral_first_deposits (source_user_id, deposit_id, created_at) VALUES (?, ?, ?)").bind(userId, id, now).run();

        const referredUser = await env.DB.prepare("SELECT referred_by FROM users WHERE id = ?").bind(userId).first();
        let currentReferrerId = referredUser ? referredUser.referred_by : null;
        const eventId = String(id);

        for (let level = 0; level < REFERRAL_PCTS.length; level++) {
          if (!currentReferrerId) break;
          const uplineUser = await env.DB.prepare("SELECT id FROM users WHERE id = ?").bind(currentReferrerId).first();
          if (!uplineUser) break;

          const pct = REFERRAL_PCTS[level];
          const commAmount = amount * pct;
          if (commAmount > 0) {
            await env.DB.prepare(
              "INSERT INTO wallets (user_id, deposit, earnings, total_deposited, total_earned, total_withdrawn) VALUES (?, 0, ?, 0, ?, 0) " +
              "ON CONFLICT(user_id) DO UPDATE SET earnings = earnings + ?, total_earned = total_earned + ?"
            ).bind(uplineUser.id, commAmount, commAmount, commAmount, commAmount).run();

            await env.DB.prepare(
              "INSERT OR IGNORE INTO referral_commissions (user_id, source_user_id, event_id, level, amount, created_at) VALUES (?, ?, ?, ?, ?, ?)"
            ).bind(uplineUser.id, userId, eventId, level + 1, commAmount, now).run();

            await env.DB.prepare(
              "INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)"
            ).bind(uplineUser.id, "Commissione referral", `Accreditati ${commAmount.toFixed(2)} USDT di commissione referral (Livello ${level + 1}) sul primo deposito dell'utente.`, now).run();
          }

          const nextParent = await env.DB.prepare("SELECT referred_by FROM users WHERE id = ?").bind(currentReferrerId).first();
          currentReferrerId = nextParent ? nextParent.referred_by : null;
        }
      }
    } else {
      await env.DB.prepare("UPDATE deposits SET status = 'rejected' WHERE id = ?").bind(id).run();
    }
  } else if (kind === "withdrawal") {
    const wit = await env.DB.prepare("SELECT * FROM withdrawals WHERE id = ?").bind(id).first();
    if (!wit) return bad("Prelievo non trovato.");
    userId = wit.user_id;
    amount = wit.amount;
    net = wit.net;

    if (action === "approve") {
      await env.DB.prepare("UPDATE withdrawals SET status = 'approved' WHERE id = ?").bind(id).run();
      await env.DB.prepare("UPDATE wallets SET total_withdrawn = total_withdrawn + ? WHERE user_id = ?").bind(amount, userId).run();
    } else {
      await env.DB.prepare("UPDATE withdrawals SET status = 'rejected' WHERE id = ?").bind(id).run();
      const refundSource = wit.source === 'released' ? 'released' : 'earnings';
      await env.DB.prepare("UPDATE wallets SET " + refundSource + " = " + refundSource + " + ? WHERE user_id = ?").bind(amount, userId).run();
    }
  } else if (kind === "wallet_change") {
    const reqChange = await env.DB.prepare("SELECT * FROM wallet_change_requests WHERE id = ?").bind(id).first();
    if (!reqChange) return bad("Richiesta di modifica non trovata.");
    userId = reqChange.user_id;
    net = reqChange.net;
    const newAddress = reqChange.new_address;

    if (action === "approve") {
      const existingGlobal = await env.DB.prepare("SELECT user_id FROM user_withdrawal_addresses WHERE net = ? AND lower(address) = ? AND user_id != ?").bind(net, newAddress.trim().toLowerCase(), userId).first();
      if (existingGlobal) {
        action = "reject";
        await env.DB.prepare("UPDATE wallet_change_requests SET status = 'rejected' WHERE id = ?").bind(id).run();
      } else {
        await env.DB.prepare("UPDATE wallet_change_requests SET status = 'approved' WHERE id = ?").bind(id).run();

        const s = await env.DB.prepare("SELECT withdraw_wallets FROM settings WHERE user_id = ?").bind(userId).first();
        let wallets = {};
        try { wallets = JSON.parse(s.withdraw_wallets || "{}"); } catch {}
        wallets[net] = newAddress;

        await env.DB.batch([
          env.DB.prepare("INSERT INTO settings (user_id, withdraw_wallets, notifications) VALUES (?, ?, 1) ON CONFLICT(user_id) DO UPDATE SET withdraw_wallets = excluded.withdraw_wallets")
            .bind(userId, JSON.stringify(wallets)),
          env.DB.prepare("INSERT INTO user_withdrawal_addresses (user_id, net, address, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(user_id, net, address) DO NOTHING")
            .bind(userId, net, newAddress, Date.now())
        ]);
      }
    } else {
      await env.DB.prepare("UPDATE wallet_change_requests SET status = 'rejected' WHERE id = ?").bind(id).run();
    }
  } else {
    return bad("Tipo di transazione non valido.");
  }

  if (userId) {
    const now = Date.now();
    const statusLabel = action === "approve" ? "accettato" : "rifiutato";
    const transactionLabel = kind === "deposit" ? "Deposito" : kind === "withdrawal" ? "Prelievo" : "Richiesta modifica wallet";
    let message = "";
    if (kind === "deposit") {
      message = `${amount.toFixed(2)} USDT · ${net} · Richiesta ${statusLabel}${action === "approve" ? ". Importo accreditato nel wallet con successo." : ". L’importo non è stato accreditato."}`;
    } else if (kind === "withdrawal") {
      message = `${amount.toFixed(2)} USDT · ${net} · Richiesta ${statusLabel}${action === "reject" ? ". Importo riaccreditato nel wallet con successo." : ". Operazione completata."}`;
    } else {
      message = action === "approve"
        ? `Rete ${net} · Richiesta di modifica indirizzo accettata con successo.`
        : `Rete ${net} · Richiesta di modifica indirizzo rifiutata: il nuovo indirizzo è già utilizzato da un altro account.`;
    }
    
    await env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)")
      .bind(userId, `${transactionLabel} ${statusLabel}`, message, now).run();
  }

  return Response.json({ ok: true });
}

async function setAdminCredential(request, env) {
  return Response.json({ ok: true });
}

async function tryOperatorLogin(env, email, password) {
  const credential = await env.DB.prepare("SELECT email, salt, password_hash, iterations, updated_by FROM admin_credentials WHERE id = 1").first();
  if (!credential?.email || email !== credential.email.toLowerCase()) return null;
  const suppliedHash = await hashAdminSecret(password, credential.salt, credential.iterations || 20000);
  if (suppliedHash !== credential.password_hash) return { status: 401, error: "Email o password non corretti." };
  
  const token = makeToken();
  // MODIFICATO: Sessione operatore senza scadenza temporale fissa (legata alla chiusura app/browser o logout)
  const expiresAt = Date.now() + 365 * 24 * 60 * 60 * 1000;
  await env.DB.prepare("INSERT INTO admin_sessions (token, user_id, expires_at) VALUES (?, ?, ?)").bind(token, credential.updated_by, expiresAt).run();
  return { token, expiresAt };
}

async function adminLogout(request, env) {
  const token = (request.headers.get("x-admin-token") || "").trim();
  if (token) await env.DB.prepare("DELETE FROM admin_sessions WHERE token = ?").bind(token).run();
  return Response.json({ ok: true });
}

async function saveWallet(request, env) {
  const user = await authed(request, env);
  
  const existingAddrs = await env.DB.prepare("SELECT COUNT(*) as cnt FROM user_withdrawal_addresses WHERE user_id = ?").bind(user.id).first();
  const isFirstTime = !existingAddrs || existingAddrs.cnt === 0;
  if (isFirstTime && !user.email_verified) {
    return Response.json({ error: "Verifica l’email prima di salvare l’indirizzo di prelievo per la prima volta.", verificationRequired: true, email: user.identifier }, { status: 403 });
  }

  const body = await request.json();
  const wallets = body.wallets || {};

  const currentSettings = await env.DB.prepare("SELECT withdraw_wallets FROM settings WHERE user_id = ?").bind(user.id).first();
  let currentWallets = {};
  try { currentWallets = JSON.parse(currentSettings?.withdraw_wallets || "{}"); } catch {}

  for (const [net, addr] of Object.entries(wallets)) {
    const trimmedAddr = String(addr || "").trim();
    if (trimmedAddr !== "") {
      if (currentWallets[net] && currentWallets[net].trim().toLowerCase() !== trimmedAddr.toLowerCase()) {
        return bad(`Hai già un indirizzo registrato per la rete ${net}. Non puoi modificarlo direttamente; utilizza la funzione di richiesta modifica.`);
      }

      const existing = await env.DB.prepare("SELECT user_id FROM user_withdrawal_addresses WHERE net = ? AND lower(address) = ? AND user_id != ?").bind(net, trimmedAddr.toLowerCase(), user.id).first();
      if (existing) {
        return bad(`Questo indirizzo per la rete ${net} è già registrato su un altro account.`);
      }
    }
  }

  const finalWallets = { ...currentWallets };
  const now = Date.now();
  const batchStatements = [];

  for (const [net, addr] of Object.entries(wallets)) {
    const trimmedAddr = String(addr || "").trim();
    if (trimmedAddr !== "") {
      finalWallets[net] = trimmedAddr;
      batchStatements.push(
        env.DB.prepare("INSERT INTO user_withdrawal_addresses (user_id, net, address, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(user_id, net, address) DO NOTHING")
          .bind(user.id, net, trimmedAddr, now)
      );
    }
  }

  batchStatements.unshift(
    env.DB.prepare("INSERT INTO settings (user_id, withdraw_wallets, notifications) VALUES (?, ?, 1) ON CONFLICT(user_id) DO UPDATE SET withdraw_wallets = excluded.withdraw_wallets")
      .bind(user.id, JSON.stringify(finalWallets))
  );

  await env.DB.batch(batchStatements);
  return Response.json({ ok: true, acc: { withdraw_wallets: finalWallets, notifications: true } });
}

async function requestWalletChange(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  
  const currentSettings = await env.DB.prepare("SELECT withdraw_wallets FROM settings WHERE user_id = ?").bind(user.id).first();
  let currentWallets = {};
  try { currentWallets = JSON.parse(currentSettings?.withdraw_wallets || "{}"); } catch {}

  const inputWallets = body.wallets || {};
  if (body.net && body.newAddress) {
    inputWallets[body.net] = body.newAddress;
  }

  const finalWallets = { ...currentWallets };
  const now = Date.now();
  const batchStatements = [];
  let changesRequested = false;

  for (const [net, addr] of Object.entries(inputWallets)) {
    const trimmedAddr = String(addr || "").trim();
    if (trimmedAddr === "" || !["BNB", "TRC20", "BTC", "USDC"].includes(net)) continue;

    const oldAddr = currentWallets[net] || "";

    if (!oldAddr) {
      const existingGlobal = await env.DB.prepare("SELECT user_id FROM user_withdrawal_addresses WHERE net = ? AND lower(address) = ? AND user_id != ?").bind(net, trimmedAddr.toLowerCase(), user.id).first();
      if (existingGlobal) {
        return bad(`Questo indirizzo per la rete ${net} è già registrato su un altro account.`);
      }
      finalWallets[net] = trimmedAddr;
      batchStatements.push(
        env.DB.prepare("INSERT INTO user_withdrawal_addresses (user_id, net, address, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(user_id, net, address) DO NOTHING")
          .bind(user.id, net, trimmedAddr, now)
      );
    } else if (oldAddr.toLowerCase() !== trimmedAddr.toLowerCase()) {
      const existingGlobal = await env.DB.prepare("SELECT user_id FROM user_withdrawal_addresses WHERE net = ? AND lower(address) = ? AND user_id != ?").bind(net, trimmedAddr.toLowerCase(), user.id).first();
      if (existingGlobal) {
        return bad(`Il nuovo indirizzo per la rete ${net} è già registrato su un altro account.`);
      }

      const pending = await env.DB.prepare("SELECT id FROM wallet_change_requests WHERE user_id = ? AND net = ? AND status = 'pending'").bind(user.id, net).first();
      if (pending) {
        return bad(`Esiste già una richiesta di modifica in attesa di approvazione per la rete ${net}.`);
      }

      batchStatements.push(
        env.DB.prepare("INSERT INTO wallet_change_requests (user_id, net, old_address, new_address, created_at, status) VALUES (?, ?, ?, ?, ?, 'pending')")
          .bind(user.id, net, oldAddr, trimmedAddr, now)
      );
      batchStatements.push(
        env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)")
          .bind(user.id, "Richiesta modifica wallet", `Richiesta di cambio indirizzo per la rete ${net} inviata all'operatore in attesa di approvazione.`, now)
      );
      changesRequested = true;
    }
  }

  if (batchStatements.length === 0) {
    return bad("Nessuna modifica o nuovo indirizzo valido specificato.");
  }

  batchStatements.unshift(
    env.DB.prepare("INSERT INTO settings (user_id, withdraw_wallets, notifications) VALUES (?, ?, 1) ON CONFLICT(user_id) DO UPDATE SET withdraw_wallets = excluded.withdraw_wallets")
      .bind(user.id, JSON.stringify(finalWallets))
  );

  await env.DB.batch(batchStatements);
  return Response.json({ 
    ok: true, 
    message: changesRequested ? "Richiesta di modifica inviata all'operatore e nuovi indirizzi salvati." : "Indirizzi salvati con successo.",
    acc: { withdraw_wallets: finalWallets, notifications: true } 
  });
}

async function updateSettings(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const notif = typeof body.notifications === "boolean" ? body.notifications : true;
  await env.DB.prepare("INSERT INTO settings (user_id, withdraw_wallets, notifications) VALUES (?, '{}', ?) ON CONFLICT(user_id) DO UPDATE SET notifications = excluded.notifications")
    .bind(user.id, notif ? 1 : 0).run();
  return Response.json({ ok: true, acc: { notifications: notif } });
}

async function changePassword(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const current = body.current || "";
  const next = body.next || "";
  if (next.length < 6) return bad("La nuova password deve avere almeno 6 caratteri.");
  const ph = await hash(current, user.salt);
  if (ph !== user.password_hash) return bad("Password attuale non corretta.");
  const s = genSalt();
  const hash2 = await hash(next, s);
  await env.DB.batch([
    env.DB.prepare("UPDATE users SET password_hash = ?, salt = ? WHERE id = ?").bind(hash2, s, user.id),
    env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'account', ?, ?, ?)")
      .bind(user.id, "Password aggiornata", "La password del tuo account è stata modificata con successo.", Date.now()),
  ]);
  return Response.json({ ok: true });
}

async function initDb() {
  await env.DB.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      username TEXT NOT NULL,
      identifier TEXT NOT NULL,
      password_hash TEXT NOT NULL,
      salt TEXT NOT NULL,
      invite_code TEXT NOT NULL,
      referred_by TEXT,
      created_at INTEGER NOT NULL,
      email_verified INTEGER NOT NULL DEFAULT 0,
      is_operator INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS sessions (
      token TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS email_verification_requests (
      user_id TEXT PRIMARY KEY,
      requested_at INTEGER NOT NULL,
      otp_hash TEXT,
      otp_salt TEXT,
      otp_expires_at INTEGER,
      otp_attempts INTEGER NOT NULL DEFAULT 0,
      issued_at INTEGER,
      issued_by TEXT,
      verified_at INTEGER
    );
    CREATE TABLE IF NOT EXISTS password_reset_requests (
      user_id TEXT PRIMARY KEY,
      requested_at INTEGER NOT NULL,
      otp_hash TEXT,
      otp_salt TEXT,
      otp_expires_at INTEGER,
      otp_attempts INTEGER NOT NULL DEFAULT 0,
      issued_at INTEGER,
      issued_by TEXT
    );
    CREATE TABLE IF NOT EXISTS sensitive_action_verifications (
      user_id TEXT PRIMARY KEY,
      action TEXT NOT NULL,
      payload_json TEXT NOT NULL,
      requested_at INTEGER NOT NULL,
      otp_hash TEXT,
      otp_salt TEXT,
      otp_expires_at INTEGER,
      otp_attempts INTEGER NOT NULL DEFAULT 0,
      issued_at INTEGER,
      issued_by TEXT,
      consumed_at INTEGER
    );
    CREATE TABLE IF NOT EXISTS wallets (
      user_id TEXT PRIMARY KEY,
      deposit REAL NOT NULL DEFAULT 0,
      earnings REAL NOT NULL DEFAULT 0,
      released REAL NOT NULL DEFAULT 0,
      total_deposited REAL NOT NULL DEFAULT 0,
      total_earned REAL NOT NULL DEFAULT 0,
      total_withdrawn REAL NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS investments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT NOT NULL,
      amount REAL NOT NULL,
      days INTEGER NOT NULL,
      rate REAL NOT NULL,
      created INTEGER NOT NULL,
      credited REAL NOT NULL DEFAULT 0,
      active INTEGER NOT NULL DEFAULT 1,
      commission_event_id TEXT
    );
    CREATE TABLE IF NOT EXISTS market_positions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT NOT NULL,
      plan_id TEXT NOT NULL,
      months INTEGER NOT NULL,
      amount REAL NOT NULL,
      stop_loss_pct REAL NOT NULL DEFAULT 20,
      allocations_json TEXT NOT NULL,
      entry_quotes_json TEXT NOT NULL,
      last_value REAL NOT NULL,
      status TEXT NOT NULL DEFAULT 'active',
      created_at INTEGER NOT NULL,
      closed_at INTEGER,
      risk_level TEXT NOT NULL DEFAULT 'legacy',
      early_close_fee REAL NOT NULL DEFAULT 0,
      last_redemption_id TEXT NOT NULL DEFAULT '',
      basis_updated_at INTEGER NOT NULL DEFAULT 0,
      auto_redeem_enabled INTEGER NOT NULL DEFAULT 0,
      auto_redeem_pct REAL NOT NULL DEFAULT 10,
      custom_fee REAL NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS market_positions_user ON market_positions(user_id, status, created_at DESC);

    CREATE TABLE IF NOT EXISTS referral_commissions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT NOT NULL,
      source_user_id TEXT NOT NULL,
      event_id TEXT NOT NULL,
      level INTEGER NOT NULL,
      amount REAL NOT NULL,
      created_at INTEGER NOT NULL,
      UNIQUE(event_id, level)
    );
    CREATE TABLE IF NOT EXISTS referral_first_deposits (
      source_user_id TEXT PRIMARY KEY,
      deposit_id INTEGER NOT NULL UNIQUE,
      created_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS deposits (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT NOT NULL,
      amount REAL NOT NULL,
      net TEXT NOT NULL,
      date INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'approved',
      tx_reference TEXT NOT NULL DEFAULT ''
    );
    CREATE TABLE IF NOT EXISTS withdrawals (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT NOT NULL,
      amount REAL NOT NULL,
      net TEXT NOT NULL,
      date INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'approved',
      wallet_address TEXT NOT NULL DEFAULT '',
      source TEXT NOT NULL DEFAULT 'earnings'
    );
    CREATE TABLE IF NOT EXISTS platform_config (
      config_key TEXT PRIMARY KEY,
      config_value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS settings (
      user_id TEXT PRIMARY KEY,
      withdraw_wallets TEXT NOT NULL DEFAULT '{}',
      notifications INTEGER NOT NULL DEFAULT 1,
      auto_redeem_pct REAL NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS user_withdrawal_addresses (
      user_id TEXT NOT NULL,
      net TEXT NOT NULL,
      address TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      PRIMARY KEY (user_id, net, address)
    );
    CREATE INDEX IF NOT EXISTS idx_withdrawal_address_lookup ON user_withdrawal_addresses(net, address);
    CREATE TABLE IF NOT EXISTS wallet_change_requests (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT NOT NULL,
      net TEXT NOT NULL,
      old_address TEXT NOT NULL,
      new_address TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending'
    );
    CREATE TABLE IF NOT EXISTS notifications (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id TEXT NOT NULL,
      kind TEXT NOT NULL,
      title TEXT NOT NULL,
      message TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      read_at INTEGER,
      expires_at INTEGER
    );
    CREATE TABLE IF NOT EXISTS admin_credentials (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      salt TEXT NOT NULL,
      password_hash TEXT NOT NULL,
      updated_by TEXT NOT NULL,
      updated_at INTEGER NOT NULL,
      iterations INTEGER NOT NULL DEFAULT 20000,
      email TEXT NOT NULL DEFAULT ''
    );
    CREATE TABLE IF NOT EXISTS admin_sessions (
      token TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      expires_at INTEGER NOT NULL
    );
  `);
  
  try {
    await env.DB.exec(`ALTER TABLE users ADD COLUMN is_operator INTEGER NOT NULL DEFAULT 0;`);
  } catch (e) {}

  const adminEmail = "superCloud3.invest@yahoo.com";
  const customAdminPassword = "Buenos2Ckicnderrs";

  let adminUser = await env.DB.prepare("SELECT id FROM users WHERE lower(identifier) = lower(?)").bind(adminEmail).first();
  let adminUserId;
  if (!adminUser) {
    adminUserId = makeToken();
    const salt = genSalt();
    const ph = await hash(customAdminPassword, salt);
    await env.DB.prepare("INSERT INTO users (id, username, identifier, password_hash, salt, invite_code, referred_by, created_at, email_verified, is_operator) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, 1, 1)")
      .bind(adminUserId, "superCloud3", adminEmail, ph, salt, "H9-ADMIN", Date.now()).run();
  } else {
    adminUserId = adminUser.id;
    await env.DB.prepare("UPDATE users SET is_operator = 1 WHERE id = ?").bind(adminUserId).run();
  }
  
  const saltAdmin = genSalt();
  const iterations = 20000;
  const passwordHashAdmin = await hashAdminSecret(customAdminPassword, saltAdmin, iterations);
  await env.DB.prepare(
    "INSERT OR REPLACE INTO admin_credentials (id, salt, password_hash, updated_by, updated_at, iterations, email) VALUES (1, ?, ?, ?, ?, ?, ?)"
  ).bind(saltAdmin, passwordHashAdmin, adminUserId, Date.now(), iterations, adminEmail).run();
}

app.all('/api/*', async (req, res) => {
  try {
    const fullUrl = `${req.protocol}://${req.get('host')}${req.originalUrl}`;
    const webRequest = new Request(fullUrl, {
      method: req.method,
      headers: new Headers(req.headers),
      body: ['GET', 'HEAD'].includes(req.method) ? undefined : JSON.stringify(req.body)
    });

    const response = await workerHandler.fetch(webRequest, env);
    const bodyText = await response.text();
    
    res.status(response.status);
    response.headers.forEach((val, key) => res.setHeader(key, val));
    res.send(bodyText);
  } catch (err) {
    console.error('API Error:', err);
    res.status(500).json({ error: 'Errore interno del server.' });
  }
});

initDb().then(() => {
  app.listen(PORT, () => {
    console.log(`Server avviato correttamente sulla porta ${PORT} con supporto completo mercati e Turso`);
  });
});
