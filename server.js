import express from 'express';
import { createClient } from '@libsql/client';
import path from 'path';
import { fileURLToPath } from 'url';
import { readFileSync } from 'fs';
import { deflateRawSync } from 'zlib';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.set('trust proxy', 1);
const PORT = process.env.PORT || 3000;
const notificationStreams = new Map();
const assetPriceStreams = new Set();
let assetPriceStreamTimer = null;
let assetPriceStreamPublishPromise = null;
let lastAssetPriceBroadcastSignature = "";

function publishNotification(userId, notificationId) {
  const clients = notificationStreams.get(String(userId));
  if (!clients?.size) return;
  const event = `event: notification\ndata: ${JSON.stringify({ id: Number(notificationId) })}\n\n`;
  for (const response of clients) {
    if (response.writableEnded || response.destroyed) clients.delete(response);
    else response.write(event);
  }
  if (!clients.size) notificationStreams.delete(String(userId));
}

function notificationRecipient(sql, params) {
  if (!/INSERT\s+INTO\s+notifications\b/i.test(sql)) return null;
  const columns = String(sql).match(/INSERT\s+INTO\s+notifications\s*\(([^)]+)\)/i)?.[1];
  if (!columns) return null;
  const index = columns.split(',').findIndex((column) => column.trim().replace(/["']/g, '').toLowerCase() === 'user_id');
  return index >= 0 ? params[index] ?? null : null;
}

const db = createClient({
  url: process.env.TURSO_DATABASE_URL || "libsql://velafin-velazzo.aws-us-east-1.turso.io",
  authToken: process.env.TURSO_AUTH_TOKEN,
});

const env = {
  DB: {
    prepare(sql) {
      return {
        bind(...args) {
          const params = args.length === 1 && Array.isArray(args[0]) ? args[0] : args;
          return {
            sql,
            params,
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
              const recipient = notificationRecipient(sql, params);
              if (rs.rowsAffected > 0 && recipient != null && rs.lastInsertRowid != null) publishNotification(recipient, rs.lastInsertRowid);
              return { meta: { changes: rs.rowsAffected, last_row_id: Number(rs.lastInsertRowid) } };
            }
          };
        },
        async all(...params) { return (await this.bind(...params)).all(); },
        async first(...params) { return (await this.bind(...params)).first(); },
        async run(...params) { return (await this.bind(...params)).run(); }
      };
    },
    async exec(sqlStatements) {
      const statements = Array.isArray(sqlStatements) 
        ? sqlStatements 
        : sqlStatements.split(';').map(s => s.trim()).filter(Boolean);
      for (const stmt of statements) {
        if (stmt) {
          await db.execute(stmt);
        }
      }
    },
    async batch(statements) {
      const results = await db.batch(statements.map(stmt => ({ sql: stmt.sql, args: stmt.params })), 'write');
      const output = [];
      for (let i = 0; i < results.length; i++) {
        const result = results[i], stmt = statements[i];
        const recipient = notificationRecipient(stmt.sql, stmt.params);
        if (result.rowsAffected > 0 && recipient != null && result.lastInsertRowid != null) publishNotification(recipient, result.lastInsertRowid);
        output.push({ meta: { changes: result.rowsAffected, last_row_id: Number(result.lastInsertRowid) } });
      }
      return output;
    }
  }
};

app.use(express.json({ limit: '1mb' }));
app.get(['/FULL_VELA.zip', '/download-app'], (req, res) => res.sendStatus(404));
app.use(express.static(__dirname));

function crc32(data) {
  let crc = 0xffffffff;
  for (const byte of data) { crc ^= byte; for (let bit = 0; bit < 8; bit++) crc = (crc >>> 1) ^ ((crc & 1) ? 0xedb88320 : 0); }
  return (crc ^ 0xffffffff) >>> 0;
}
function buildApplicationZip() {
  const files = ['index.html', 'styles.css', 'script.js', 'server.js', 'package.json'];
  const localParts = [], centralParts = []; let offset = 0;
  const now = new Date();
  const dosTime = (now.getHours() << 11) | (now.getMinutes() << 5) | Math.floor(now.getSeconds() / 2);
  const dosDate = ((Math.max(1980, now.getFullYear()) - 1980) << 9) | ((now.getMonth() + 1) << 5) | now.getDate();
  for (const filename of files) {
    const name = Buffer.from(filename, 'utf8'), raw = readFileSync(path.join(__dirname, filename));
    const compressed = deflateRawSync(raw), checksum = crc32(raw), local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0); local.writeUInt16LE(20, 4); local.writeUInt16LE(0x0800, 6); local.writeUInt16LE(8, 8);
    local.writeUInt16LE(dosTime, 10); local.writeUInt16LE(dosDate, 12); local.writeUInt32LE(checksum, 14);
    local.writeUInt32LE(compressed.length, 18); local.writeUInt32LE(raw.length, 22); local.writeUInt16LE(name.length, 26); local.writeUInt16LE(0, 28);
    localParts.push(local, name, compressed);
    const central = Buffer.alloc(46);
    central.writeUInt32LE(0x02014b50, 0); central.writeUInt16LE(20, 4); central.writeUInt16LE(20, 6); central.writeUInt16LE(0x0800, 8);
    central.writeUInt16LE(8, 10); central.writeUInt16LE(dosTime, 12); central.writeUInt16LE(dosDate, 14); central.writeUInt32LE(checksum, 16);
    central.writeUInt32LE(compressed.length, 20); central.writeUInt32LE(raw.length, 24); central.writeUInt16LE(name.length, 28);
    central.writeUInt16LE(0, 30); central.writeUInt16LE(0, 32); central.writeUInt16LE(0, 34); central.writeUInt16LE(0, 36);
    central.writeUInt32LE(0, 38); central.writeUInt32LE(offset, 42); centralParts.push(central, name);
    offset += local.length + name.length + compressed.length;
  }
  const centralDirectory = Buffer.concat(centralParts), end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0); end.writeUInt16LE(0, 4); end.writeUInt16LE(0, 6);
  end.writeUInt16LE(files.length, 8); end.writeUInt16LE(files.length, 10); end.writeUInt32LE(centralDirectory.length, 12);
  end.writeUInt32LE(offset, 16); end.writeUInt16LE(0, 20);
  return Buffer.concat([...localParts, centralDirectory, end]);
}

const yahooInstruments = {
  gold: { symbol: "GC=F", instrument: "Gold futures · USD per troy ounce" },
  oil: { symbol: "CL=F", instrument: "WTI crude futures · USD per barrel" },
  sp500: { symbol: "^GSPC", instrument: "S&P 500 index · index points" },
  nasdaq: { symbol: "^IXIC", instrument: "NASDAQ Composite · index points" },
  tesla: { symbol: "TSLA", instrument: "Tesla common stock · USD per share" },
  artificialIntelligence: { symbol: "AIQ", instrument: "Global X AI & Technology ETF · USD per share" },
  dowJones: { symbol: "^DJI", instrument: "Dow Jones Industrial Average · index points" },
  russell2000: { symbol: "^RUT", instrument: "Russell 2000 · index points" },
  silver: { symbol: "SI=F", instrument: "Silver futures · USD per troy ounce" },
  naturalGas: { symbol: "NG=F", instrument: "Henry Hub natural gas futures · USD per MMBtu" },
  copper: { symbol: "HG=F", instrument: "Copper futures · USD per pound" },
  euroUsd: { symbol: "EURUSD=X", instrument: "EUR/USD spot exchange rate" },
};
const ELECTRICITY_MARKETS = Object.freeze({ "DE-LU": "Germany–Luxembourg", FR: "France", NL: "Netherlands", BE: "Belgium", AT: "Austria", CH: "Switzerland", DK1: "Denmark West", DK2: "Denmark East" });
function normalizeElectricityMarket(value) { return Object.prototype.hasOwnProperty.call(ELECTRICITY_MARKETS, value) ? value : "DE-LU"; }

let cachedQuotes = null;
let cachedAt = 0;
let refreshInFlight = new Map();
let cachedQuotesMarket = "DE-LU";
let cachedHistory = null;
let historyAt = 0;
let historyInFlight = new Map();
let cachedHistoryMarket = "DE-LU";
const positionQuoteCache = new Map();

async function fetchJson(url, yahooRequest = false) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 3000);
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
    unit: ["sp500", "nasdaq", "dowJones", "russell2000"].includes(key) ? "index points" : "",
    instrument: asset.instrument,
    symbol: asset.symbol,
    provider: "Yahoo Finance · delay may vary",
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

async function electricityQuote(now, selectedMarket = "DE-LU") {
  const market = normalizeElectricityMarket(selectedMarket);
  const start = new Date(now - 72 * 60 * 60 * 1000).toISOString();
  const end = new Date(now).toISOString();
  const params = new URLSearchParams({ bzn: market, start, end });
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
  const marketName = ELECTRICITY_MARKETS[market];
  return { price: Number(body.price[index]), currency: "EUR", unit: "per MWh", instrument: marketName + " day-ahead electricity · regional wholesale price", symbol: market, marketName, provider: "Energy-Charts · Fraunhofer ISE", marketAt: latest };
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

async function loadAssetHistory(selectedMarket = "DE-LU") {
  const electricityMarket = normalizeElectricityMarket(selectedMarket);
  const now = Date.now();
  const since = Math.floor((now - 365 * 24 * 60 * 60 * 1000) / 1000);
  const electricityParams = new URLSearchParams({
    bzn: electricityMarket,
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

async function loadQuotes(selectedMarket = "DE-LU") {
  const now = Date.now(), electricityMarket = normalizeElectricityMarket(selectedMarket);
  const [entries, crypto, electricity] = await Promise.all([
    Promise.all(Object.entries(yahooInstruments).map(async ([key, asset]) => [key, await yahooQuote(key, asset)])),
    cryptoQuotes(now), electricityQuote(now, electricityMarket),
  ]);
  const quotes = Object.fromEntries(entries.filter(([, quote]) => quote));
  Object.assign(quotes, crypto);
  if (electricity) {
    const eurUsd = quotes.euroUsd;
    quotes.electricity = eurUsd
      ? { ...electricity, price: Number((electricity.price * eurUsd.price).toFixed(6)), currency: 'USD', instrument: electricity.marketName + ' day-ahead electricity · USD per MWh', provider: electricity.provider + ' · EUR/USD reference conversion', unit: 'per MWh' }
      : electricity;
  }
  for (const quote of Object.values(quotes)) { const price = Number(quote?.price); if (Number.isFinite(price)) quote.price = Number(price.toFixed(3)); }
  return { quotes, electricityMarket, fetchedAt: Date.now() };
}

const marketApi = {
  async fetch(request) {
    const url = new URL(request.url);
    if (request.method !== "GET") return new Response("Not found", { status: 404 });
    const selectedMarket = normalizeElectricityMarket(url.searchParams.get("electricity"));
    if (url.pathname === "/api/asset-history") {
      if (cachedHistory && cachedHistoryMarket === selectedMarket && Date.now() - historyAt < 300_000) return Response.json(cachedHistory, { headers: { "Cache-Control": "no-store" } });
      if (!historyInFlight.has(selectedMarket)) {
        const task = loadAssetHistory(selectedMarket).then((data) => { cachedHistory = data; cachedHistoryMarket = selectedMarket; historyAt = Date.now(); return data; }).finally(() => historyInFlight.delete(selectedMarket));
        historyInFlight.set(selectedMarket, task);
      }
      return Response.json(await historyInFlight.get(selectedMarket), { headers: { "Cache-Control": "no-store" } });
    }
    if (url.pathname !== "/api/asset-prices") return new Response("Not found", { status: 404 });
    if (cachedQuotes && cachedQuotesMarket === selectedMarket && Date.now() - cachedAt < 2_500) return Response.json({ ...cachedQuotes, electricityMarket: selectedMarket }, { headers: { "Cache-Control": "no-store" } });
    if (!refreshInFlight.has(selectedMarket)) {
      const task = loadQuotes(selectedMarket).then((data) => {
        if (Object.keys(data.quotes).length) { cachedQuotes = { quotes: { ...(cachedQuotes?.quotes || {}), ...data.quotes }, fetchedAt: data.fetchedAt }; cachedAt = Date.now(); cachedQuotesMarket = selectedMarket; }
        return { ...(cachedQuotes || data), electricityMarket: selectedMarket };
      }).finally(() => refreshInFlight.delete(selectedMarket));
      refreshInFlight.set(selectedMarket, task);
    }
    return Response.json(await refreshInFlight.get(selectedMarket), { headers: { "Cache-Control": "no-store" } });
  },
};

async function publishAssetPriceUpdate() {
  if (!assetPriceStreams.size || assetPriceStreamPublishPromise) return assetPriceStreamPublishPromise;
  assetPriceStreamPublishPromise = (async () => {
    const response = await marketApi.fetch(new Request('http://localhost/api/asset-prices'));
    if (!response.ok) return;
    const data = await response.json();
    const fetchedAt = Number(data?.fetchedAt) || 0;
    const signature = JSON.stringify(Object.entries(data?.quotes || {}).map(([key, quote]) => [key, Number(quote?.price)]).sort((a, b) => a[0].localeCompare(b[0])));
    if (!fetchedAt || !signature || signature === lastAssetPriceBroadcastSignature) return;
    lastAssetPriceBroadcastSignature = signature;
    const event = ['event: quotes', 'data: ' + JSON.stringify(data), '', ''].join(String.fromCharCode(10));
    for (const client of assetPriceStreams) {
      if (client.writableEnded || client.destroyed) assetPriceStreams.delete(client);
      else client.write(event);
    }
  })().catch((error) => console.warn('Asset price broadcast failed:', error)).finally(() => { assetPriceStreamPublishPromise = null; });
  return assetPriceStreamPublishPromise;
}

// Persistent market history survives Render sleep/wake and process restarts.
// API values are merged by timestamp; new API values replace matching points.
let marketHistoryTablePromise = null;
let storedMarketHistory = null;
let marketHistoryQueue = Promise.resolve();
let lastMarketHistoryPruneAt = 0;

function emptyStoredMarketHistory() { return { histories: {}, candles: {}, intraday: {} }; }
function addStoredMarketPoint(target, type, key, row) {
  if (!target[type][key]) target[type][key] = [];
  target[type][key].push(row);
}

async function ensureStoredMarketHistory(env) {
  if (!marketHistoryTablePromise) {
    marketHistoryTablePromise = (async () => {
      await env.DB.exec('CREATE TABLE IF NOT EXISTS market_history_points (asset_key TEXT NOT NULL, series_type TEXT NOT NULL, point_time INTEGER NOT NULL, value REAL, open REAL, high REAL, low REAL, close REAL, PRIMARY KEY (asset_key, series_type, point_time))');
      await env.DB.exec(['CREATE INDEX IF NOT EXISTS market_history_point_time ON market_history_points(point_time)', 'CREATE INDEX IF NOT EXISTS market_history_series_time ON market_history_points(series_type, point_time)']);
      const historyCutoff = Date.now() - 366 * 86400000;
      const result = await env.DB.prepare('SELECT asset_key, series_type, point_time, value, open, high, low, close FROM market_history_points WHERE point_time >= ? ORDER BY point_time ASC').bind(historyCutoff).all();
      const loaded = emptyStoredMarketHistory();
      const cutoff = Date.now() - 2 * 86400000;
      for (const row of result.results || []) {
        const time = Number(row.point_time);
        if (!Number.isFinite(time)) continue;
        const key = String(row.asset_key);
        if (row.series_type === 'candles') {
          const candle = { time, open: Number(row.open), high: Number(row.high), low: Number(row.low), close: Number(row.close) };
          if ([candle.open, candle.high, candle.low, candle.close].every(Number.isFinite)) addStoredMarketPoint(loaded, 'candles', key, candle);
        } else if (row.series_type === 'histories' || row.series_type === 'intraday') {
          if (row.series_type === 'intraday' && time < cutoff) continue;
          const point = { time, value: Number(row.value) };
          if (Number.isFinite(point.value)) addStoredMarketPoint(loaded, row.series_type, key, point);
        }
      }
      storedMarketHistory = loaded;
      return loaded;
    })().catch((error) => { marketHistoryTablePromise = null; throw error; });
  }
  await marketHistoryTablePromise;
  return storedMarketHistory;
}

function normalizeMarketHistory(data) {
  const normalized = emptyStoredMarketHistory();
  for (const type of ['histories', 'intraday']) {
    for (const [key, points] of Object.entries(data?.[type] || {})) {
      if (!Array.isArray(points)) continue;
      const valid = points.map((point) => ({ time: Number(point?.time), value: Number(point?.value) }))
        .filter((point) => Number.isFinite(point.time) && Number.isFinite(point.value));
      if (valid.length) normalized[type][key] = valid;
    }
  }
  for (const [key, candles] of Object.entries(data?.candles || {})) {
    if (!Array.isArray(candles)) continue;
    const valid = candles.map((candle) => ({ time: Number(candle?.time), open: Number(candle?.open), high: Number(candle?.high), low: Number(candle?.low), close: Number(candle?.close) }))
      .filter((candle) => Number.isFinite(candle.time) && [candle.open, candle.high, candle.low, candle.close].every(Number.isFinite));
    if (valid.length) normalized.candles[key] = valid;
  }
  return normalized;
}

function sameMarketPoint(a, b, type) {
  if (!a) return false;
  return type === 'candles' ? a.open === b.open && a.high === b.high && a.low === b.low && a.close === b.close : a.value === b.value;
}

async function mergeAndStoreMarketHistory(env, apiData) {
  await ensureStoredMarketHistory(env);
  const incoming = normalizeMarketHistory(apiData);
  const merged = emptyStoredMarketHistory();
  const changedRows = [];
  const now = Date.now();
  const cutoff = now - 2 * 86400000;
  for (const type of ['histories', 'candles', 'intraday']) {
    const keys = new Set([...Object.keys(storedMarketHistory[type]), ...Object.keys(incoming[type])]);
    for (const key of keys) {
      const oldPoints = (storedMarketHistory[type][key] || []).filter((point) => point.time >= now - 366 * 86400000 && (type !== 'intraday' || point.time >= cutoff));
      const byTime = new Map(oldPoints.map((point) => [point.time, point]));
      for (const point of incoming[type][key] || []) {
        if (type === 'intraday' && point.time < cutoff) continue;
        const previous = byTime.get(point.time);
        byTime.set(point.time, point);
        if (!sameMarketPoint(previous, point, type)) changedRows.push({ key, type, point });
      }
      const points = [...byTime.values()].sort((a, b) => a.time - b.time);
      if (points.length) merged[type][key] = points;
    }
  }
  const writeStatements = [];
  for (let start = 0; start < changedRows.length; start += 80) {
    const batch = changedRows.slice(start, start + 80);
    const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?, ?, ?)').join(',');
    const args = [];
    for (const { key, type, point } of batch) args.push(key, type, point.time,
      type === 'candles' ? null : point.value, type === 'candles' ? point.open : null,
      type === 'candles' ? point.high : null, type === 'candles' ? point.low : null,
      type === 'candles' ? point.close : null);
    const sql = 'INSERT INTO market_history_points (asset_key, series_type, point_time, value, open, high, low, close) VALUES ' + placeholders +
      ' ON CONFLICT(asset_key, series_type, point_time) DO UPDATE SET value=excluded.value, open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close';
    writeStatements.push(env.DB.prepare(sql).bind(...args));
  }
  if (writeStatements.length) await env.DB.batch(writeStatements);
  if (now - lastMarketHistoryPruneAt >= 86400000) {
    await env.DB.batch([
      env.DB.prepare("DELETE FROM market_history_points WHERE series_type = 'intraday' AND point_time < ?").bind(cutoff),
      env.DB.prepare('DELETE FROM market_history_points WHERE point_time < ?').bind(now - 366 * 86400000),
    ]);
    lastMarketHistoryPruneAt = now;
  }
  storedMarketHistory = merged;
  return { ...merged, fetchedAt: Number(apiData?.fetchedAt) || now };
}

function persistMarketHistory(env, data) {
  const task = marketHistoryQueue.then(() => mergeAndStoreMarketHistory(env, data));
  marketHistoryQueue = task.catch(() => {});
  return task;
}

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
  const session = await env.DB.prepare("SELECT user_id FROM admin_sessions WHERE token = ? AND expires_at > ?").bind(token, Date.now()).first();
  if (!session) return null;
  if (session.user_id === PRIMARY_ADMIN_ID) return { id: PRIMARY_ADMIN_ID, username: "Admin", identifier: PRIMARY_ADMIN_EMAIL, email_verified: 1, created_at: 0 };
  return env.DB.prepare("SELECT * FROM users WHERE id = ?").bind(session.user_id).first();
}

function isPrimaryAdmin(actor) {
  return String(actor?.identifier || "").trim().toLowerCase() === PRIMARY_ADMIN_EMAIL.toLowerCase();
}

async function adminActor(request, env) {
  const actor = await adminSessionUser(request, env);
  if (!actor) return null;
  const authenticator = await env.DB.prepare("SELECT enabled FROM admin_authenticator WHERE user_id = ?").bind(actor.id).first();
  if (Number(authenticator?.enabled) !== 1) return null;
  const role = await env.DB.prepare("SELECT role FROM admin_roles WHERE user_id = ?").bind(actor.id).first();
  return role || isPrimaryAdmin(actor) ? actor : null;
}

async function privilegedAdminActor(request, env) {
  const actor = await adminActor(request, env);
  if (!actor) return null;
  if (isPrimaryAdmin(actor)) return actor;
  const permissions = await readAdminPermissions(actor, env);
  return permissions.allAdmin ? actor : null;
}

const ADMIN_PERMISSION_KEYS = ["fullPanel", "requests", "walletChanges", "otp", "support", "users", "antifraud", "allAdmin"];
async function readAdminPermissions(actor, env) {
  if (isPrimaryAdmin(actor)) return { fullPanel: true, requests: true, walletChanges: true, otp: true, support: true, users: true, allAdmin: true };
  const role = await env.DB.prepare("SELECT role, permissions_json FROM admin_roles WHERE user_id = ?").bind(actor.id).first();
  let stored = {};
  try { stored = JSON.parse(role?.permissions_json || "{}"); } catch {}
  const permissions = Object.fromEntries(ADMIN_PERMISSION_KEYS.map(key => [key, stored[key] === true]));
  if (permissions.fullPanel) for (const key of ADMIN_PERMISSION_KEYS.filter(key => key !== "allAdmin")) permissions[key] = true;
  if (permissions.allAdmin) for (const key of ADMIN_PERMISSION_KEYS) permissions[key] = true;
  return permissions;
}
async function adminAccess(request, env, permission) {
  const actor = await adminActor(request, env);
  if (!actor) return null;
  const permissions = await readAdminPermissions(actor, env);
  if (permission === "antifraud") {
    const assignedRole = await env.DB.prepare("SELECT role FROM admin_roles WHERE user_id = ?").bind(actor.id).first();
    if (isPrimaryAdmin(actor) || assignedRole?.role === "admin" || permissions.allAdmin === true || permissions.fullPanel === true || permissions.antifraud === true) return actor;
    return null;
  }
  return permissions[permission] ? actor : null;
}

async function accrue(env, userId) {
  let wallet = await env.DB.prepare("SELECT * FROM wallets WHERE user_id = ?").bind(userId).first();
  if (!wallet) {
    wallet = { user_id: userId, deposit: 0, earnings: 0, released: 0, total_deposited: 0, total_earned: 0, total_withdrawn: 0 };
    await env.DB.prepare("INSERT INTO wallets (user_id, deposit, earnings, released, total_deposited, total_earned, total_withdrawn) VALUES (?,0,0,0,0,0,0)").bind(userId).run();
  }
  const now = Date.now();
  const invs = (await env.DB.prepare("SELECT * FROM investments WHERE user_id = ? AND active = 1").bind(userId).all()).results;
  const updates = [];
  let changed = false;
  for (const inv of invs) {
    const elapsed = (now - inv.created) / DAY;
    const target = Math.min(inv.days, elapsed);
    if (target > inv.credited + 1e-9) {
      const gain = Number(((inv.amount * inv.rate / 100) * (target - inv.credited)).toFixed(3));
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
  let wallet = await accrue(env, user.id);
  const marketPositions = await syncMarketPositions(env, user.id);
  const [simpleEarnResult, redemptionResult, walletResult, investmentsResult, depositsResult, withdrawalsResult, teamResult, inviteStats, s, actionRow] = await Promise.all([
    env.DB.prepare("SELECT * FROM simple_earn_positions WHERE user_id = ? ORDER BY created_at DESC").bind(user.id).all(),
    env.DB.prepare("SELECT position_id, amount, reference_value, created_at, segment_start_at, basis_quotes_json FROM market_position_redemptions WHERE user_id = ? ORDER BY created_at DESC").bind(user.id).all(),
    env.DB.prepare("SELECT * FROM wallets WHERE user_id = ?").bind(user.id).first(),
    env.DB.prepare("SELECT * FROM investments WHERE user_id = ? ORDER BY id DESC").bind(user.id).all(),
    env.DB.prepare("SELECT * FROM deposits WHERE user_id = ? ORDER BY id DESC").bind(user.id).all(),
    env.DB.prepare("SELECT * FROM withdrawals WHERE user_id = ? ORDER BY id DESC").bind(user.id).all(),
    env.DB.prepare(
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
    ).bind(user.id, user.id).all(),
    inviterStats(env, user.id),
    env.DB.prepare("SELECT * FROM settings WHERE user_id = ?").bind(user.id).first(),
    env.DB.prepare("SELECT action, payload_json, requested_at, issued_at, otp_expires_at FROM sensitive_action_verifications WHERE user_id = ? AND consumed_at IS NULL").bind(user.id).first(),
  ]);
  const simpleEarnRows = simpleEarnResult.results || [];
  const simpleEarnPositions = simpleEarnRows.map(position => { const elapsed = Math.max(0, Date.now() - Number(position.created_at)); const accruedInterest = Number((Number(position.principal) * Number(position.daily_rate) * elapsed / 86400000).toFixed(3)); return { ...position, accruedInterest, currentValue: Number((Number(position.principal) + accruedInterest).toFixed(3)) }; });
  const redemptionRows = redemptionResult.results || [];
  const redemptionsByPosition = new Map();
  for (const row of redemptionRows) {
    let bucket = redemptionsByPosition.get(Number(row.position_id));
    if (!bucket) redemptionsByPosition.set(Number(row.position_id), bucket = []);
    let basisQuotes = {}; try { basisQuotes = JSON.parse(row.basis_quotes_json || '{}'); } catch {}
    bucket.push({ amount: Number(row.amount), referenceValue: Number(row.reference_value), createdAt: Number(row.created_at), segmentStartAt: Number(row.segment_start_at), basisQuotes });
  }
  for (const position of marketPositions) position.redemptions = redemptionsByPosition.get(Number(position.id)) || [];
  wallet = walletResult || wallet;
  const investments = investmentsResult.results;
  const deposits = depositsResult.results;
  const withdrawals = withdrawalsResult.results;
  const team = teamResult.results;
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
    user: { id: user.id, username: user.username, identifier: user.identifier, inviteCode: user.invite_code, referredBy: user.referred_by, emailVerified: user.email_verified },
    wallet,
    investments,
    marketPositions,
    simpleEarnPositions,
    deposits,
    withdrawals,
    team,
    inviteStats,
    acc: { withdraw_wallets, notifications },
    sensitiveAction,
    isOperator: !!(await adminSessionUser(request, env)),
    isOwner: false,
    isOwnerAccount: false,
  };
}

let persistedMarketHistoryAt = 0;
let persistedMarketHistoryData = null;
let persistedMarketHistoryPromise = null;
let schemaPromise;

const workerHandler = {
  async fetch(request, env) {
    const apiPath = new URL(request.url).pathname;
    if (request.method === 'GET' && apiPath === '/api/asset-history') {
      const response = await marketApi.fetch(request);
      if (!response.ok) return response;
      const apiData = await response.json();
      try {
        if (persistedMarketHistoryData && Number(apiData.fetchedAt) <= persistedMarketHistoryAt) {
          return Response.json(persistedMarketHistoryData, { headers: { 'Cache-Control': 'no-store' } });
        }
        if (!persistedMarketHistoryPromise) {
          persistedMarketHistoryPromise = persistMarketHistory(env, apiData).then((completeHistory) => {
            persistedMarketHistoryData = completeHistory;
            persistedMarketHistoryAt = Number(apiData.fetchedAt) || Date.now();
            return completeHistory;
          }).finally(() => { persistedMarketHistoryPromise = null; });
        }
        return Response.json(await persistedMarketHistoryPromise, { headers: { 'Cache-Control': 'no-store' } });
      } catch (error) {
        console.error('Market history persistence failed:', error);
        return Response.json(apiData, { headers: { 'Cache-Control': 'no-store' } });
      }
    }
    if (request.method === 'GET' && apiPath === '/api/asset-prices') {
      return marketApi.fetch(request);
    }
    if (!schemaPromise) schemaPromise = (async () => {
      await env.DB.exec([
        `CREATE TABLE IF NOT EXISTS users (
          id TEXT PRIMARY KEY,
          username TEXT NOT NULL,
          identifier TEXT NOT NULL,
          password_hash TEXT NOT NULL,
          salt TEXT NOT NULL,
          invite_code TEXT NOT NULL,
          referred_by TEXT,
          created_at INTEGER NOT NULL,
          email_verified INTEGER NOT NULL DEFAULT 0
        );`,
        `CREATE TABLE IF NOT EXISTS sessions (
          token TEXT PRIMARY KEY,
          user_id TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );`,
        `CREATE TABLE IF NOT EXISTS email_verification_requests (
          user_id TEXT PRIMARY KEY,
          requested_at INTEGER NOT NULL,
          otp_hash TEXT,
          otp_salt TEXT,
          otp_expires_at INTEGER,
          otp_attempts INTEGER NOT NULL DEFAULT 0,
          issued_at INTEGER,
          issued_by TEXT,
          verified_at INTEGER
        );`,
        `CREATE TABLE IF NOT EXISTS password_reset_requests (
          user_id TEXT PRIMARY KEY,
          requested_at INTEGER NOT NULL,
          otp_hash TEXT,
          otp_salt TEXT,
          otp_expires_at INTEGER,
          otp_attempts INTEGER NOT NULL DEFAULT 0,
          issued_at INTEGER,
          issued_by TEXT
        );`,
        `CREATE TABLE IF NOT EXISTS sensitive_action_verifications (
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
        );`,
        `CREATE TABLE IF NOT EXISTS notifications (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id TEXT NOT NULL,
          kind TEXT NOT NULL,
          title TEXT NOT NULL,
          message TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          read_at INTEGER,
          expires_at INTEGER
        );`,
        `CREATE INDEX IF NOT EXISTS notifications_user_created ON notifications(user_id, created_at DESC);`,
        `CREATE TABLE IF NOT EXISTS antifraud_events (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL, username TEXT NOT NULL, event_type TEXT NOT NULL CHECK (event_type IN ('registration','deposit','withdrawal')), ip_address TEXT, event_ref TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, shared_ip INTEGER NOT NULL DEFAULT 0);`,
        `CREATE INDEX IF NOT EXISTS antifraud_events_ip_time ON antifraud_events(ip_address, created_at DESC);`,
        `CREATE INDEX IF NOT EXISTS antifraud_events_created ON antifraud_events(created_at DESC);`,
        `CREATE TABLE IF NOT EXISTS antifraud_notice_log (id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER NOT NULL, user_id TEXT NOT NULL, stage INTEGER NOT NULL CHECK (stage BETWEEN 1 AND 3), admin_id TEXT NOT NULL, created_at INTEGER NOT NULL, UNIQUE(user_id, stage));`,
        `CREATE TABLE IF NOT EXISTS support_threads (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'bot' CHECK (status IN ('bot','waiting','live','closed')),
          question_count INTEGER NOT NULL DEFAULT 0,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );`,
        `CREATE INDEX IF NOT EXISTS support_threads_user_updated ON support_threads(user_id, updated_at DESC);`,
        `CREATE TABLE IF NOT EXISTS support_messages (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          thread_id INTEGER NOT NULL,
          sender_role TEXT NOT NULL CHECK (sender_role IN ('user','assistant','operator')),
          body TEXT NOT NULL,
          created_at INTEGER NOT NULL
        );`,
        `CREATE INDEX IF NOT EXISTS support_messages_thread_id ON support_messages(thread_id, id);`,
        `CREATE TABLE IF NOT EXISTS operator_admin_requests (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          operator_id TEXT NOT NULL,
          operator_username TEXT NOT NULL,
          thread_id INTEGER NOT NULL,
          user_id TEXT NOT NULL,
          username TEXT NOT NULL,
          user_identifier TEXT NOT NULL,
          details TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','resolved')),
          created_at INTEGER NOT NULL,
          resolved_at INTEGER,
          admin_reply TEXT NOT NULL DEFAULT '',
          replied_at INTEGER
        );`,
        `CREATE INDEX IF NOT EXISTS operator_admin_requests_status_created ON operator_admin_requests(status, created_at);`,
        `CREATE TABLE IF NOT EXISTS operator_review_audit (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          operator_id TEXT NOT NULL,
          operator_username TEXT NOT NULL,
          request_kind TEXT NOT NULL,
          request_id INTEGER NOT NULL,
          user_id TEXT NOT NULL,
          username TEXT NOT NULL DEFAULT '',
          action TEXT NOT NULL CHECK (action IN ('approve','reject')),
          request_details TEXT NOT NULL DEFAULT '{}',
          created_at INTEGER NOT NULL
        );`,
        `CREATE INDEX IF NOT EXISTS operator_review_audit_operator_created ON operator_review_audit(operator_id, created_at DESC);`,
        `CREATE TABLE IF NOT EXISTS wallets (
          user_id TEXT PRIMARY KEY,
          deposit REAL NOT NULL DEFAULT 0,
          earnings REAL NOT NULL DEFAULT 0,
          released REAL NOT NULL DEFAULT 0,
          total_deposited REAL NOT NULL DEFAULT 0,
          total_earned REAL NOT NULL DEFAULT 0,
          total_withdrawn REAL NOT NULL DEFAULT 0
        );`,
        `CREATE TABLE IF NOT EXISTS investments (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id TEXT NOT NULL,
          amount REAL NOT NULL,
          days INTEGER NOT NULL,
          rate REAL NOT NULL,
          created INTEGER NOT NULL,
          credited REAL NOT NULL DEFAULT 0,
          active INTEGER NOT NULL DEFAULT 1,
          commission_event_id TEXT
        );`,
        `CREATE TABLE IF NOT EXISTS simple_earn_positions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id TEXT NOT NULL,
          principal REAL NOT NULL,
          source TEXT NOT NULL CHECK(source IN ('deposit','earnings')),
          daily_rate REAL NOT NULL DEFAULT 0.0027,
          status TEXT NOT NULL DEFAULT 'active',
          created_at INTEGER NOT NULL,
          closed_at INTEGER,
          interest_paid REAL NOT NULL DEFAULT 0
        );`,
        `CREATE INDEX IF NOT EXISTS simple_earn_user_status ON simple_earn_positions(user_id, status, created_at DESC);`,
        `CREATE TABLE IF NOT EXISTS market_positions (
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
          custom_fee REAL NOT NULL DEFAULT 0,
          auto_add_enabled INTEGER NOT NULL DEFAULT 0,
          auto_add_drawdown_pct REAL NOT NULL DEFAULT 10,
          auto_add_mode TEXT NOT NULL DEFAULT 'fixed',
          auto_add_value REAL NOT NULL DEFAULT 10,
          auto_add_anchor_value REAL NOT NULL DEFAULT 0
        );`,
        `CREATE INDEX IF NOT EXISTS market_positions_user ON market_positions(user_id, status, created_at DESC);`,
        `CREATE TABLE IF NOT EXISTS market_position_redemptions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          position_id INTEGER NOT NULL,
          user_id TEXT NOT NULL,
          event_id TEXT NOT NULL UNIQUE,
          amount REAL NOT NULL,
          reference_value REAL NOT NULL,
          created_at INTEGER NOT NULL,
          segment_start_at INTEGER NOT NULL,
          basis_quotes_json TEXT NOT NULL
        );`,
        `CREATE INDEX IF NOT EXISTS market_position_redemptions_user_position ON market_position_redemptions(user_id, position_id, created_at DESC);`,
        `CREATE TABLE IF NOT EXISTS referral_commissions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id TEXT NOT NULL,
          source_user_id TEXT NOT NULL,
          event_id TEXT NOT NULL,
          level INTEGER NOT NULL,
          amount REAL NOT NULL,
          created_at INTEGER NOT NULL,
          UNIQUE(event_id, level)
        );`,
        `CREATE TABLE IF NOT EXISTS referral_first_deposits (
          source_user_id TEXT PRIMARY KEY,
          deposit_id INTEGER NOT NULL UNIQUE,
          created_at INTEGER NOT NULL
        );`,
        `CREATE INDEX IF NOT EXISTS users_referred_by ON users(referred_by);`,
        `CREATE INDEX IF NOT EXISTS investments_user_active ON investments(user_id, active);`,
        `CREATE TABLE IF NOT EXISTS deposits (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id TEXT NOT NULL,
          amount REAL NOT NULL,
          net TEXT NOT NULL,
          date INTEGER NOT NULL,
          status TEXT NOT NULL DEFAULT 'approved',
          tx_reference TEXT NOT NULL DEFAULT ''
        );`,
        `CREATE TABLE IF NOT EXISTS withdrawals (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id TEXT NOT NULL,
          amount REAL NOT NULL,
          net TEXT NOT NULL,
          date INTEGER NOT NULL,
          status TEXT NOT NULL DEFAULT 'approved',
          wallet_address TEXT NOT NULL DEFAULT '',
          source TEXT NOT NULL DEFAULT 'earnings',
          tx_reference TEXT NOT NULL DEFAULT '',
          fee_amount REAL NOT NULL DEFAULT 0,
          charged_amount REAL NOT NULL DEFAULT 0
        );`,
        `CREATE TABLE IF NOT EXISTS balance_transfers (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id TEXT NOT NULL,
          amount REAL NOT NULL,
          fee_amount REAL NOT NULL,
          credited_amount REAL NOT NULL,
          created_at INTEGER NOT NULL
);`,
        `CREATE TABLE IF NOT EXISTS platform_config (
          config_key TEXT PRIMARY KEY,
          config_value TEXT NOT NULL
        );`,
        `CREATE TABLE IF NOT EXISTS settings (
          user_id TEXT PRIMARY KEY,
          withdraw_wallets TEXT NOT NULL DEFAULT '{}',
          notifications INTEGER NOT NULL DEFAULT 1,
          auto_redeem_pct REAL NOT NULL DEFAULT 0
        );`,
        `CREATE TABLE IF NOT EXISTS user_withdrawal_addresses (
          user_id TEXT NOT NULL,
          net TEXT NOT NULL,
          address TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          PRIMARY KEY (user_id, net, address)
        );`,
        `CREATE INDEX IF NOT EXISTS idx_withdrawal_address_lookup ON user_withdrawal_addresses(net, address);`,
        `CREATE TABLE IF NOT EXISTS wallet_change_requests (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id TEXT NOT NULL,
          net TEXT NOT NULL,
          old_address TEXT NOT NULL,
          new_address TEXT NOT NULL,
          created_at INTEGER NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending'
        );`,
        `CREATE TABLE IF NOT EXISTS admin_credentials (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          salt TEXT NOT NULL,
          password_hash TEXT NOT NULL,
          updated_by TEXT NOT NULL,
          updated_at INTEGER NOT NULL,
          iterations INTEGER NOT NULL DEFAULT 20000,
          email TEXT NOT NULL DEFAULT ''
        );`,
        `CREATE TABLE IF NOT EXISTS admin_sessions (
          token TEXT PRIMARY KEY,
          user_id TEXT NOT NULL,
          expires_at INTEGER NOT NULL
        );`,
        `CREATE TABLE IF NOT EXISTS admin_roles (
          user_id TEXT PRIMARY KEY,
          role TEXT NOT NULL CHECK (role IN ('admin','operator')),
          permissions_json TEXT NOT NULL DEFAULT '{}',
          granted_by TEXT NOT NULL,
          granted_at INTEGER NOT NULL
        );`,
        `CREATE TABLE IF NOT EXISTS admin_authenticator (
          user_id TEXT PRIMARY KEY,
          secret TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 0,
          updated_at INTEGER NOT NULL
        );`,
        `CREATE TABLE IF NOT EXISTS admin_mfa_challenges (
          challenge TEXT PRIMARY KEY,
          user_id TEXT NOT NULL,
          expires_at INTEGER NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0
        );`
      ].join("\n"));
    
    await env.DB.exec([
      'CREATE INDEX IF NOT EXISTS users_identifier_lower ON users(lower(identifier))',
      'CREATE INDEX IF NOT EXISTS users_username_lower ON users(lower(username))',
      'CREATE INDEX IF NOT EXISTS users_invite_code ON users(invite_code)',
      'CREATE INDEX IF NOT EXISTS sessions_user_id ON sessions(user_id)',
      'CREATE INDEX IF NOT EXISTS deposits_user_status_id ON deposits(user_id, status, id DESC)',
      'CREATE INDEX IF NOT EXISTS deposits_status_date ON deposits(status, date)',
      'CREATE INDEX IF NOT EXISTS deposits_tx_reference_lower ON deposits(lower(tx_reference))',
      'CREATE INDEX IF NOT EXISTS withdrawals_user_date ON withdrawals(user_id, date DESC)',
      'CREATE INDEX IF NOT EXISTS withdrawals_status_date ON withdrawals(status, date)',
      'CREATE INDEX IF NOT EXISTS referral_commissions_user_source ON referral_commissions(user_id, source_user_id)',
    ]);
    try { await env.DB.prepare("ALTER TABLE admin_roles ADD COLUMN permissions_json TEXT NOT NULL DEFAULT '{}'").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE settings ADD COLUMN auto_redeem_pct REAL NOT NULL DEFAULT 0").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE withdrawals ADD COLUMN fee_amount REAL NOT NULL DEFAULT 0").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE withdrawals ADD COLUMN charged_amount REAL NOT NULL DEFAULT 0").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE support_threads ADD COLUMN final_message_id INTEGER").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE support_threads ADD COLUMN support_operator_id TEXT").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE support_threads ADD COLUMN support_operator_username TEXT").run(); } catch {}
    await env.DB.prepare("CREATE TABLE IF NOT EXISTS support_feedback (thread_id INTEGER PRIMARY KEY, user_id TEXT NOT NULL, username TEXT NOT NULL, operator_id TEXT NOT NULL, operator_username TEXT NOT NULL, rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5), comment TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL)").run();
    await env.DB.prepare("CREATE INDEX IF NOT EXISTS support_feedback_operator_created ON support_feedback(operator_id, created_at DESC)").run();
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
    try { await env.DB.prepare("ALTER TABLE market_positions ADD COLUMN auto_add_enabled INTEGER NOT NULL DEFAULT 0").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE market_positions ADD COLUMN auto_add_drawdown_pct REAL NOT NULL DEFAULT 10").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE market_positions ADD COLUMN auto_add_mode TEXT NOT NULL DEFAULT 'fixed'").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE market_positions ADD COLUMN auto_add_value REAL NOT NULL DEFAULT 10").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE market_positions ADD COLUMN auto_add_anchor_value REAL NOT NULL DEFAULT 0").run(); } catch {}
    await env.DB.prepare("UPDATE market_positions SET basis_updated_at = created_at WHERE basis_updated_at = 0").run();
    await env.DB.prepare("UPDATE market_positions SET risk_level = CASE WHEN stop_loss_pct <= 8 THEN 'conservative' WHEN stop_loss_pct >= 30 THEN 'aggressive' ELSE 'balanced' END WHERE risk_level = 'legacy'").run();
    
    try { await env.DB.exec("DROP TRIGGER IF EXISTS market_position_release;"); } catch {}
    try {
      await env.DB.exec(`
        CREATE TRIGGER market_position_release AFTER UPDATE OF status ON market_positions
        WHEN OLD.status IN ('active','paused') AND NEW.status IN ('matured','stop_loss','early_closed')
        BEGIN 
          UPDATE wallets SET released = released + NEW.last_value WHERE user_id = NEW.user_id; 
        END;
      `);
    } catch {}

    try { await env.DB.prepare("ALTER TABLE operator_admin_requests ADD COLUMN admin_reply TEXT NOT NULL DEFAULT ''").run(); } catch {}
    try { await env.DB.prepare("ALTER TABLE operator_admin_requests ADD COLUMN replied_at INTEGER").run(); } catch {}

    // Keep normal user authentication available when Render's admin secrets are not configured.
    // Never rewrite or clear existing administrator credentials in that case.
    if (PRIMARY_ADMIN_EMAIL && PRIMARY_ADMIN_PASSWORD) {
      const adminEmail = PRIMARY_ADMIN_EMAIL.toLowerCase();
      const legacyAdmin = await env.DB.prepare("SELECT id FROM users WHERE lower(identifier) = ?").bind(adminEmail).first();
      const existingAdminCreds = await env.DB.prepare("SELECT salt, password_hash, iterations, updated_by FROM admin_credentials WHERE id = 1").first();
      const oldAdminIds = [...new Set([legacyAdmin?.id, existingAdminCreds?.updated_by].filter(id => id && id !== PRIMARY_ADMIN_ID))];
      for (const oldId of oldAdminIds) {
        await env.DB.batch([
          env.DB.prepare("DELETE FROM sessions WHERE user_id = ?").bind(oldId),
          env.DB.prepare("DELETE FROM admin_sessions WHERE user_id = ?").bind(oldId),
          env.DB.prepare("DELETE FROM admin_mfa_challenges WHERE user_id = ?").bind(oldId),
          env.DB.prepare("DELETE FROM admin_authenticator WHERE user_id = ?").bind(oldId),
          env.DB.prepare("DELETE FROM admin_roles WHERE user_id = ?").bind(oldId)
        ]);
      }
      if (legacyAdmin) await env.DB.prepare("DELETE FROM users WHERE id = ?").bind(legacyAdmin.id).run();
      await env.DB.prepare("DELETE FROM admin_roles WHERE role = 'admin' AND user_id != ?").bind(PRIMARY_ADMIN_ID).run();

      let iterations = 20000;
      if (!existingAdminCreds) {
        const saltAdmin = genSalt();
        const passwordHashAdmin = await hashAdminSecret(PRIMARY_ADMIN_PASSWORD, saltAdmin, iterations);
        await env.DB.prepare("INSERT INTO admin_credentials (id, salt, password_hash, updated_by, updated_at, iterations, email) VALUES (1, ?, ?, ?, ?, ?, ?)")
          .bind(saltAdmin, passwordHashAdmin, PRIMARY_ADMIN_ID, Date.now(), iterations, adminEmail).run();
      } else {
        iterations = existingAdminCreds.iterations || iterations;
        const saltAdmin = genSalt();
        const passwordHashAdmin = await hashAdminSecret(PRIMARY_ADMIN_PASSWORD, saltAdmin, iterations);
        await env.DB.prepare("UPDATE admin_credentials SET salt = ?, password_hash = ?, updated_by = ?, updated_at = ?, iterations = ?, email = ? WHERE id = 1")
          .bind(saltAdmin, passwordHashAdmin, PRIMARY_ADMIN_ID, Date.now(), iterations, adminEmail).run();
      }
    } else {
      console.warn("Admin authentication is disabled: ADMIN_EMAIL and ADMIN_PASSWORD must both be configured.");
    }

    })().catch(err => { schemaPromise = null; throw err; });
    await schemaPromise;

    const url = new URL(request.url);
    const pathName = url.pathname;
    const method = request.method;

    try {
      if (method === "POST" && pathName === "/api/register") return await register(request, env);
      if (method === "POST" && pathName === "/api/login") return await login(request, env);
      if (method === "POST" && pathName === "/api/admin/mfa/verify") return await verifyAdminAuthenticator(request, env);
      if (method === "POST" && pathName === "/api/auth/forgot-password") return await requestPasswordReset(request, env);
      if (method === "POST" && pathName === "/api/auth/reset-password") return await confirmPasswordReset(request, env);
      if (method === "POST" && pathName === "/api/verification/request") return await requestEmailVerification(request, env);
      if (method === "POST" && pathName === "/api/verification/confirm") return await confirmEmailVerification(request, env);
      if (method === "POST" && pathName === "/api/verification/verify-otp") return await verifyAccountOtpDirect(request, env);
      if (method === "POST" && pathName === "/api/logout") return await logout(request, env);
      if (method === "GET" && pathName === "/api/me") return await me(request, env);
      if (method === "GET" && pathName === "/api/notifications") return await getNotifications(request, env);
      if (method === "POST" && pathName === "/api/notifications/read") return await markNotificationsRead(request, env);
      if (method === "GET" && pathName === "/api/support/current") return await supportCurrent(request, env);
      if (method === "POST" && pathName === "/api/support/message") return await supportMessage(request, env);
      if (method === "POST" && pathName === "/api/support/escalate") return await supportEscalate(request, env);
      if (method === "POST" && pathName === "/api/support/choice") return await supportChoice(request, env);
      if (method === "POST" && pathName === "/api/support/feedback") return await submitSupportFeedback(request, env);
      if (method === "GET" && pathName === "/api/admin/support") return await adminSupportList(request, env);
      if (method === "GET" && pathName === "/api/admin/support-requests") return await listOperatorAdminRequests(request, env);
      if (method === "POST" && pathName === "/api/admin/support-requests") return await createOperatorAdminRequest(request, env);
      const adminSupportRequestResolveMatch = pathName.match(/^\/api\/admin\/support-requests\/(\d+)\/resolve$/);
      if (method === "POST" && adminSupportRequestResolveMatch) return await resolveOperatorAdminRequest(request, env, Number(adminSupportRequestResolveMatch[1]));
      const adminSupportRequestReplyMatch = pathName.match(/^\/api\/admin\/support-requests\/(\d+)\/reply$/);
      if (method === "POST" && adminSupportRequestReplyMatch) return await replyToOperatorAdminRequest(request, env, Number(adminSupportRequestReplyMatch[1]));
      const adminSupportMessagesMatch = pathName.match(/^\/api\/admin\/support\/(\d+)\/messages$/);
      if (method === "POST" && adminSupportMessagesMatch) return await adminSupportReply(request, env, Number(adminSupportMessagesMatch[1]));
      const adminSupportThreadMatch = pathName.match(/^\/api\/admin\/support\/(\d+)$/);
      if (method === "GET" && adminSupportThreadMatch) return await adminSupportThread(request, env, Number(adminSupportThreadMatch[1]));
      if (method === "POST" && pathName === "/api/deposit") return await deposit(request, env);
      if (method === "POST" && pathName === "/api/invest") return Response.json({ error: "I vecchi piani a rendimento fisso non accettano nuovi investimenti. Usa i portafogli market con valore variabile." }, { status: 410 });
      if (method === "POST" && pathName === "/api/market/invest") return await marketInvest(request, env);
      if (method === "POST" && pathName === "/api/market/simple-earn") return await simpleEarnInvest(request, env);
      if (method === "POST" && pathName === "/api/market/simple-earn/redeem") return await simpleEarnRedeem(request, env);
      if (method === "POST" && pathName === "/api/market/custom-invest") return await customMarketInvest(request, env);
      if (method === "POST" && pathName === "/api/market/redeem-gains") return await redeemMarketGains(request, env);
      if (method === "POST" && pathName === "/api/market/auto-redeem") return await savePositionAutoRedeem(request, env);
      if (method === "POST" && pathName === "/api/market/auto-add") return await savePositionAutoAdd(request, env);
      if (method === "POST" && pathName === "/api/market/close-early") return await closeMarketPositionEarly(request, env);
      if (method === "GET" && pathName === "/api/deposit-addresses") return await getDepositAddresses(request, env);
      if (method === "GET" && pathName === "/api/admin/deposit-addresses") return await adminDepositAddresses(request, env);
      if (method === "POST" && pathName === "/api/admin/deposit-addresses") return await saveAdminDepositAddresses(request, env);
      if (method === "POST" && pathName === "/api/withdraw") return await withdraw(request, env);
      if (method === "POST" && pathName === "/api/transfer-deposit-earnings") return await transferDepositToEarnings(request, env);
      if (method === "POST" && pathName === "/api/security/confirm-otp") return await confirmSensitiveActionOtp(request, env);
      if (method === "POST" && pathName === "/api/save-wallet") return await saveWallet(request, env);
      if (method === "POST" && pathName === "/api/wallet-change-request") return await requestWalletChange(request, env);
      if (method === "POST" && pathName === "/api/settings") return await updateSettings(request, env);
      if (method === "POST" && pathName === "/api/change-password") return await changePassword(request, env);
      if (method === "GET" && pathName === "/api/admin/permissions") return await adminPermissions(request, env);
      if (method === "GET" && pathName === "/api/admin/antifraud") return await adminAntifraudEvents(request, env);
      if (method === "POST" && pathName === "/api/admin/antifraud/notify") return await sendAntifraudNotice(request, env);
      if (method === "GET" && pathName === "/api/admin/backup/export") return await adminDatabaseBackup(request, env);
      if (method === "GET" && pathName === "/api/admin/transactions") return await adminTransactions(request, env);
      if (method === "GET" && pathName === "/api/admin/ledger") return await adminGlobalLedger(request, env);
       if (method === "GET" && pathName === "/api/admin/password-reset-requests") return await listPasswordResetRequests(request, env);
      if (method === "POST" && pathName === "/api/admin/payments/mark-sent") return await markWithdrawalSent(request, env);
      if (method === "GET" && pathName === "/api/admin/statistics") return await adminStatistics(request, env);
      if (method === "GET" && pathName === "/api/admin/users") return await adminUsers(request, env);
      if (method === "GET" && pathName === "/api/admin/operator-audit") return await adminOperatorAudit(request, env);
      if (method === "POST" && pathName === "/api/market/add-funds") return await addMarketFunds(request, env);
      if (method === "POST" && pathName === "/api/admin/users/role") return await adminSetUserRole(request, env);
      
      const adminProfileMatch = pathName.match(/^\/api\/admin\/users\/([^/]+)\/profile$/);
      if (method === "POST" && adminProfileMatch) return await adminUpdateUserProfile(request, env, adminProfileMatch[1]);
      
      const adminUserMatch = pathName.match(/^\/api\/admin\/users\/([^/]+)$/);
      if (method === "GET" && adminUserMatch) return await adminUserDetails(request, env, adminUserMatch[1]);
      
      const adminOtpMatch = pathName.match(/^\/api\/admin\/users\/([^/]+)\/verification-otp$/);
      if (method === "POST" && adminOtpMatch) return await setUserVerificationOtp(request, env, adminOtpMatch[1]);

      const adminWalletOtpMatch = pathName.match(new RegExp("^/api/admin/users/([^/]+)/wallet-change-otp$"));
      if (method === "POST" && adminWalletOtpMatch) return await setWalletChangeOtp(request, env, adminWalletOtpMatch[1]);

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

async function supportCurrent(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Accedi per usare l’assistenza." }, { status: 401 });
  const thread = await env.DB.prepare("SELECT t.id, t.user_id, t.status, t.question_count, t.final_message_id, t.created_at, t.updated_at, t.support_operator_id, t.support_operator_username FROM support_threads t WHERE t.user_id = ? AND (t.status != 'closed' OR (t.support_operator_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM support_feedback f WHERE f.thread_id = t.id))) ORDER BY CASE WHEN t.status = 'closed' THEN 1 ELSE 0 END, t.id DESC LIMIT 1").bind(user.id).first();
  if (!thread) return Response.json({ thread: null, messages: [] });
  const messages = (await env.DB.prepare("SELECT id, sender_role, body, created_at FROM support_messages WHERE thread_id = ? ORDER BY id ASC LIMIT 200").bind(thread.id).all()).results;
  return Response.json({ thread, messages });
}

async function supportMessage(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Accedi per usare l’assistenza." }, { status: 401 });
  const body = await request.json();
  const message = typeof body.message === "string" ? body.message.trim() : "";
  if (!message || message.length > 2000) return Response.json({ error: "Il messaggio deve contenere da 1 a 2000 caratteri." }, { status: 400 });
  const now = Date.now();
  let thread = await env.DB.prepare("SELECT id, status, question_count, final_message_id FROM support_threads WHERE user_id = ? AND status != 'closed' ORDER BY id DESC LIMIT 1").bind(user.id).first();
  if (!thread) {
    const created = await env.DB.prepare("INSERT INTO support_threads (user_id, status, question_count, created_at, updated_at) VALUES (?, 'bot', 0, ?, ?)").bind(user.id, now, now).run();
    thread = { id: Number(created.meta?.last_row_id), status: 'bot' };
  }
  if (thread.final_message_id) return Response.json({ error: "Rispondi prima alla domanda di chiusura." }, { status: 400 });
  await env.DB.prepare("INSERT INTO support_messages (thread_id, sender_role, body, created_at) VALUES (?, 'user', ?, ?)").bind(thread.id, message, now).run();
  let reply = "";
  if (thread.status === 'bot') {
    reply = typeof body.botReply === "string" ? body.botReply.trim().slice(0, 4000) : "";
    if (!reply) reply = "Mi dispiace, non ho trovato una risposta precisa. Puoi riformulare la domanda o chiedermi informazioni su account, depositi, prelievi e portafogli.";
    if (Number(thread.question_count || 0) >= 4 && (Number(thread.question_count || 0) - 4) % 3 === 0) reply += String.fromCharCode(10) + String.fromCharCode(10) + "Vuoi parlare con un operatore?";
    await env.DB.prepare("INSERT INTO support_messages (thread_id, sender_role, body, created_at) VALUES (?, 'assistant', ?, ?)").bind(thread.id, reply, now + 1).run();
  }
  await env.DB.prepare("UPDATE support_threads SET question_count = question_count + 1, updated_at = ? WHERE id = ?").bind(now, thread.id).run();
  const updated = await env.DB.prepare("SELECT id, user_id, status, question_count, final_message_id, created_at, updated_at FROM support_threads WHERE id = ?").bind(thread.id).first();
  const messages = (await env.DB.prepare("SELECT id, sender_role, body, created_at FROM support_messages WHERE thread_id = ? ORDER BY id ASC LIMIT 200").bind(thread.id).all()).results;
  return Response.json({ thread: updated, messages });
}

async function supportEscalate(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Accedi per usare l’assistenza." }, { status: 401 });
  const thread = await env.DB.prepare("SELECT id, status, question_count, final_message_id FROM support_threads WHERE user_id = ? AND status != 'closed' ORDER BY id DESC LIMIT 1").bind(user.id).first();
  if (!thread) return Response.json({ error: "Invia prima cinque domande al bot." }, { status: 400 });
  if (Number(thread.question_count) < 5) return Response.json({ error: "Puoi richiedere un operatore dopo cinque domande." }, { status: 400 });
  if (thread.status === 'bot') await env.DB.prepare("UPDATE support_threads SET status = 'waiting', updated_at = ? WHERE id = ?").bind(Date.now(), thread.id).run();
  const updated = await env.DB.prepare("SELECT id, user_id, status, question_count, final_message_id, created_at, updated_at FROM support_threads WHERE id = ?").bind(thread.id).first();
  return Response.json({ thread: updated });
}

async function submitSupportFeedback(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Accedi per inviare un feedback." }, { status: 401 });
  const body = await request.json();
  const threadId = Number(body.threadId);
  const rating = Number(body.rating);
  const comment = typeof body.comment === "string" ? body.comment.trim() : "";
  if (!Number.isInteger(threadId) || threadId < 1) return bad("Conversazione non valida.");
  if (!Number.isInteger(rating) || rating < 1 || rating > 5) return bad("Seleziona una valutazione da 1 a 5 stelle.");
  if (comment.length > 1200) return bad("Il commento non può superare 1200 caratteri.");
  const thread = await env.DB.prepare("SELECT id, status, support_operator_id, support_operator_username FROM support_threads WHERE id = ? AND user_id = ?").bind(threadId, user.id).first();
  if (!thread || thread.status !== "closed" || !thread.support_operator_id) return Response.json({ error: "Il feedback è disponibile solo dopo una conversazione conclusa con un operatore." }, { status: 400 });
  const result = await env.DB.prepare("INSERT OR IGNORE INTO support_feedback (thread_id, user_id, username, operator_id, operator_username, rating, comment, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
    .bind(threadId, user.id, String(user.username || "user"), thread.support_operator_id, String(thread.support_operator_username || "Operator"), rating, comment, Date.now()).run();
  if (!result.meta?.changes) return Response.json({ error: "Hai già inviato il feedback per questa conversazione." }, { status: 409 });
  return Response.json({ ok: true });
}

async function supportChoice(request, env) {
  const user = await sessionUser(request, env);
  if (!user) return Response.json({ error: "Accedi per usare l’assistenza." }, { status: 401 });
  const body = await request.json();
  const choice = typeof body.choice === "string" ? body.choice : "";
  const thread = await env.DB.prepare("SELECT id, user_id, status, question_count, final_message_id, created_at, updated_at, support_operator_id, support_operator_username FROM support_threads WHERE user_id = ? AND status != 'closed' ORDER BY id DESC LIMIT 1").bind(user.id).first();
  if (!thread || (thread.status !== "bot" && !(thread.status === "live" && thread.final_message_id && (choice === "close" || choice === "continue"))) || (thread.status === "bot" && Number(thread.question_count) < 5)) return Response.json({ error: "La scelta non è più disponibile." }, { status: 400 });
  const last = await env.DB.prepare("SELECT id, sender_role, body FROM support_messages WHERE thread_id = ? ORDER BY id DESC LIMIT 1").bind(thread.id).first();
  const now = Date.now();
  if ((choice === "close" || choice === "continue") && thread.status === "live" && thread.final_message_id && Number(thread.final_message_id) === Number(last?.id) && last?.sender_role === "operator") {
    const answer = choice === "close" ? "Sì, chiudi la chat." : "No, desidero continuare.";
    const reply = choice === "close" ? "Chat chiusa. Puoi riaprire l’assistenza quando vuoi." : "Va bene, continuiamo con l’assistenza virtuale. Scrivi pure la tua domanda.";
    await env.DB.prepare("INSERT INTO support_messages (thread_id, sender_role, body, created_at) VALUES (?, 'user', ?, ?)").bind(thread.id, answer, now).run();
    if (choice === "close") await env.DB.prepare("INSERT INTO support_messages (thread_id, sender_role, body, created_at) VALUES (?, 'assistant', ?, ?)").bind(thread.id, reply, now + 1).run();
    await env.DB.prepare("UPDATE support_threads SET status = ?, final_message_id = NULL, updated_at = ? WHERE id = ?").bind(choice === "close" ? "closed" : "live", now, thread.id).run();
  } else if (choice === "operator" || choice === "decline") {
    if (last?.sender_role !== "assistant" || !String(last.body).includes("Vuoi parlare con un operatore?")) return Response.json({ error: "La scelta non è più disponibile." }, { status: 400 });
    const answer = choice === "operator" ? "Sì, desidero parlare con un operatore." : "No, grazie.";
    await env.DB.prepare("INSERT INTO support_messages (thread_id, sender_role, body, created_at) VALUES (?, 'user', ?, ?)").bind(thread.id, answer, now).run();
    if (choice === "operator") {
      await env.DB.prepare("UPDATE support_threads SET status = 'waiting', updated_at = ? WHERE id = ?").bind(now, thread.id).run();
    } else {
      await env.DB.prepare("INSERT INTO support_messages (thread_id, sender_role, body, created_at) VALUES (?, 'assistant', ?, ?)").bind(thread.id, "Vuoi chiudere la chat?", now + 1).run();
      await env.DB.prepare("UPDATE support_threads SET updated_at = ? WHERE id = ?").bind(now, thread.id).run();
    }
  } else if (choice === "close" || choice === "continue") {
    if (thread.status !== "bot" || last?.sender_role !== "assistant" || !String(last.body).includes("Vuoi chiudere la chat?")) return Response.json({ error: "La scelta non è più disponibile." }, { status: 400 });
    const answer = choice === "close" ? "Sì, chiudi la chat." : "No, desidero continuare.";
    const reply = choice === "close" ? "Chat chiusa. Puoi riaprire l’assistenza quando vuoi." : "Va bene, continuiamo con l’assistenza virtuale. Scrivi pure la tua domanda.";
    await env.DB.prepare("INSERT INTO support_messages (thread_id, sender_role, body, created_at) VALUES (?, 'user', ?, ?)").bind(thread.id, answer, now).run();
    await env.DB.prepare("INSERT INTO support_messages (thread_id, sender_role, body, created_at) VALUES (?, 'assistant', ?, ?)").bind(thread.id, reply, now + 1).run();
    await env.DB.prepare("UPDATE support_threads SET status = ?, final_message_id = NULL, updated_at = ? WHERE id = ?").bind(choice === "close" ? "closed" : "bot", now, thread.id).run();
  } else {
    return Response.json({ error: "Scelta non valida." }, { status: 400 });
  }
  const updated = await env.DB.prepare("SELECT id, user_id, status, question_count, final_message_id, created_at, updated_at, support_operator_id, support_operator_username FROM support_threads WHERE id = ?").bind(thread.id).first();
  const messages = (await env.DB.prepare("SELECT id, sender_role, body, created_at FROM support_messages WHERE thread_id = ? ORDER BY id ASC LIMIT 200").bind(thread.id).all()).results;
  return Response.json({ thread: updated, messages });
}

async function adminSupportList(request, env) {
  const actor = await adminAccess(request, env, "support");
  if (!actor) return Response.json({ error: "Permessi operatore richiesti." }, { status: 403 });
  const conversations = (await env.DB.prepare("SELECT t.id, t.status, t.question_count, t.updated_at, u.username, (SELECT m.body FROM support_messages m WHERE m.thread_id = t.id ORDER BY m.id DESC LIMIT 1) AS last_message FROM support_threads t JOIN users u ON u.id = t.user_id WHERE t.status IN ('waiting','live') ORDER BY t.updated_at DESC LIMIT 100").all()).results;
  return Response.json({ conversations });
}

async function adminSupportThread(request, env, id) {
  const actor = await adminAccess(request, env, "support");
  if (!actor) return Response.json({ error: "Permessi operatore richiesti." }, { status: 403 });
  const thread = await env.DB.prepare("SELECT t.id, t.user_id, t.status, t.question_count, t.final_message_id, t.created_at, t.updated_at, u.username, u.identifier FROM support_threads t JOIN users u ON u.id = t.user_id WHERE t.id = ?").bind(id).first();
  if (!thread) return Response.json({ error: "Conversazione non trovata." }, { status: 404 });
  const messages = (await env.DB.prepare("SELECT id, sender_role, body, created_at FROM support_messages WHERE thread_id = ? ORDER BY id ASC LIMIT 200").bind(id).all()).results;
  return Response.json({ thread, messages });
}

async function adminSupportReply(request, env, id) {
  const actor = await adminAccess(request, env, "support");
  if (!actor) return Response.json({ error: "Permessi operatore richiesti." }, { status: 403 });
  const body = await request.json();
  const message = typeof body.message === "string" ? body.message.trim() : "";
  if ((!message && body.final !== true) || message.length > 2000) return Response.json({ error: "Il messaggio deve contenere da 1 a 2000 caratteri." }, { status: 400 });
  const thread = await env.DB.prepare("SELECT id, final_message_id FROM support_threads WHERE id = ? AND status IN ('waiting','live')").bind(id).first();
  if (!thread) return Response.json({ error: "Conversazione non trovata o non più attiva." }, { status: 404 });
  if (thread.final_message_id) return Response.json({ error: "La conversazione attende la risposta dell’utente." }, { status: 400 });
  await env.DB.prepare("UPDATE support_threads SET support_operator_id = ?, support_operator_username = ? WHERE id = ?").bind(actor.id, String(actor.username || "Operator"), id).run();
  const now = Date.now();
  if (body.final === true) {
    await env.DB.prepare("INSERT INTO support_messages (thread_id, sender_role, body, created_at) VALUES (?, 'operator', ?, ?)").bind(id, "Vuoi chiudere la chat?", now).run();
    const finalMessage = await env.DB.prepare("SELECT id FROM support_messages WHERE thread_id = ? ORDER BY id DESC LIMIT 1").bind(id).first();
    await env.DB.prepare("UPDATE support_threads SET status = 'live', final_message_id = ?, updated_at = ? WHERE id = ?").bind(finalMessage.id, now, id).run();
    return Response.json({ ok: true, finalMessageId: finalMessage.id });
  }
  await env.DB.prepare("INSERT INTO support_messages (thread_id, sender_role, body, created_at) VALUES (?, 'operator', ?, ?)").bind(id, message, now).run();
  await env.DB.prepare("UPDATE support_threads SET status = 'live', updated_at = ? WHERE id = ?").bind(now, id).run();
  return Response.json({ ok: true });
}

async function createOperatorAdminRequest(request, env) {
  const operator = await adminAccess(request, env, "support");
  if (!operator) return Response.json({ error: "Permessi supporto richiesti." }, { status: 403 });
  const body = await request.json();
  const threadId = Number(body.threadId);
  const details = typeof body.details === "string" ? body.details.trim() : "";
  if (!Number.isInteger(threadId) || threadId < 1) return bad("Conversazione non valida.");
  if (!details || details.length > 2000) return bad("Descrivi il problema in massimo 2000 caratteri.");
  const thread = await env.DB.prepare("SELECT t.id, t.user_id, u.username, u.identifier FROM support_threads t JOIN users u ON u.id = t.user_id WHERE t.id = ? AND t.status IN ('waiting','live')").bind(threadId).first();
  if (!thread) return bad("La conversazione non è più attiva.");
  const now = Date.now();
  const result = await env.DB.prepare("INSERT INTO operator_admin_requests (operator_id, operator_username, thread_id, user_id, username, user_identifier, details, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?)")
    .bind(operator.id, operator.username || "operator", thread.id, thread.user_id, thread.username, thread.identifier, details, now).run();
  return Response.json({ ok: true, id: Number(result.meta?.last_row_id), createdAt: now });
}

async function listOperatorAdminRequests(request, env) {
  const actor = await adminAccess(request, env, "support");
  if (!actor) return Response.json({ error: "Permessi supporto richiesti." }, { status: 403 });
  const url = new URL(request.url);
  const threadId = Number(url.searchParams.get("threadId")) || 0;
  if (isPrimaryAdmin(actor) || (await readAdminPermissions(actor, env)).allAdmin) {
    const rows = (await env.DB.prepare("SELECT id, operator_username, thread_id, user_id, username, user_identifier, details, admin_reply, replied_at, created_at FROM operator_admin_requests WHERE status = 'open' ORDER BY created_at ASC LIMIT 100").all()).results;
    return Response.json({ requests: rows });
  }
  let sql = "SELECT id, thread_id, details, admin_reply, replied_at, status, created_at FROM operator_admin_requests WHERE operator_id = ?";
  const params = [actor.id];
  if (Number.isInteger(threadId) && threadId > 0) { sql += " AND thread_id = ?"; params.push(threadId); }
  sql += " ORDER BY created_at DESC LIMIT 100";
  const rows = (await env.DB.prepare(sql).bind(...params).all()).results;
  return Response.json({ requests: rows });
}

async function replyToOperatorAdminRequest(request, env, id) {
  const admin = await privilegedAdminActor(request, env);
  if (!admin) return Response.json({ error: "Permessi admin richiesti." }, { status: 403 });
  const body = await request.json();
  const message = typeof body.message === "string" ? body.message.trim() : "";
  if (!message || message.length > 2000) return bad("Le istruzioni devono contenere da 1 a 2000 caratteri.");
  const result = await env.DB.prepare("UPDATE operator_admin_requests SET admin_reply = ?, replied_at = ? WHERE id = ? AND status = 'open'").bind(message, Date.now(), id).run();
  if (!result.meta?.changes) return bad("Richiesta non trovata o già risolta.");
  return Response.json({ ok: true });
}

async function resolveOperatorAdminRequest(request, env, id) {
  const admin = await privilegedAdminActor(request, env);
  if (!admin) return Response.json({ error: "Permessi admin richiesti." }, { status: 403 });
  const result = await env.DB.prepare("UPDATE operator_admin_requests SET status = 'resolved', resolved_at = ? WHERE id = ? AND status = 'open'").bind(Date.now(), id).run();
  if (!result.meta?.changes) return bad("Richiesta non trovata o già risolta.");
  return Response.json({ ok: true });
}

async function register(request, env) {
  const body = await request.json();
  const rawIdentifier = typeof body.identifier === "string" ? body.identifier.trim() : "";
  const username = typeof body.username === "string" ? body.username.trim() : "";
  const password = typeof body.password === "string" ? body.password : "";
  const invite = typeof body.invite === "string" ? body.invite.trim() : "";
  if (!rawIdentifier || !username) return Response.json({ error: "Compila tutti i campi." }, { status: 400 });
  const email = normalizeEmail(rawIdentifier);
  if (!email) return bad("Inserisci un indirizzo email valido.");
  if (email.toLowerCase() === PRIMARY_ADMIN_EMAIL.toLowerCase()) return bad("Indirizzo riservato all’accesso amministratore.");
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
  await env.DB.prepare("INSERT INTO users (id, username, identifier, password_hash, salt, invite_code, referred_by, created_at, email_verified) VALUES (?,?,?,?,?,?,?,?,0)")
    .bind(id, username, email, ph, s, inviteCode, referredBy, Date.now()).run();
  await recordAntifraudEvent(request, env, { id, username }, 'registration');
  return Response.json({ ok: true, email, username });
}

async function requestPasswordReset(request, env) {
  const body = await request.json().catch(() => ({}));
  const email = normalizeEmail(body.email);
  if (!email) return bad("Inserisci un indirizzo email valido.");

  const user = await env.DB.prepare("SELECT id, username, identifier FROM users WHERE lower(identifier) = ?").bind(email).first();
  const now = Date.now();
  if (user) {
    const created = await env.DB.prepare(
      "INSERT INTO password_reset_requests (user_id, requested_at, otp_attempts) VALUES (?, ?, 0) ON CONFLICT(user_id) DO NOTHING"
    ).bind(user.id, now).run();
    if (!(created.meta?.changes > 0)) return Response.json({ error: "È già presente una richiesta di reimpostazione della password. Completa quella in corso prima di inviarne un'altra." }, { status: 409 });
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
  await env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'account', ?, ?, ?)").bind(targetUser.id, "Email verification request", "Verification request sent. Wait for the code sent by email, then enter it here.", now).run();
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

async function setWalletChangeOtp(request, env, userId) {
  const actor = await adminAccess(request, env, "otp");
  if (!actor) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  const body = await request.json();
  const otp = typeof body.otp === "string" ? body.otp.trim() : "";
  if (otp.length !== 6 || !/^[0-9]{6}$/.test(otp)) return bad("L’OTP deve contenere esattamente 6 cifre.");
  const row = await env.DB.prepare("SELECT action, consumed_at FROM sensitive_action_verifications WHERE user_id = ?").bind(userId).first();
  if (!row || row.action !== "wallet-change" || row.consumed_at) return bad("Richiesta di verifica wallet non trovata o già completata.");
  const salt = genSalt();
  const otpHash = await hash(otp, salt);
  const now = Date.now();
  const expiresAt = now + 15 * 60 * 1000;
  await env.DB.prepare("UPDATE sensitive_action_verifications SET otp_hash = ?, otp_salt = ?, otp_expires_at = ?, otp_attempts = 0, issued_at = ?, issued_by = ? WHERE user_id = ? AND action = 'wallet-change' AND consumed_at IS NULL")
    .bind(otpHash, salt, expiresAt, now, actor.id, userId).run();
  return Response.json({ ok: true, expiresAt });
}

async function setUserVerificationOtp(request, env, userId) {
  const actor = await adminAccess(request, env, "otp");
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

  return Response.json({ ok: true, expiresAt });
}

async function login(request, env) {
  const body = await request.json();
  const rawId = typeof body.id === "string" ? body.id.trim() : "";
  const emailId = normalizeEmail(rawId);
  const id = (emailId || normalizePhone(rawId) || rawId).toLowerCase();
  const pw = typeof body.password === "string" ? body.password : "";
  if (!id || !pw) return Response.json({ error: "Inserisci email/telefono e password." }, { status: 400 });
  
  if (PRIMARY_ADMIN_EMAIL && id === PRIMARY_ADMIN_EMAIL.toLowerCase()) {
    if (!PRIMARY_ADMIN_PASSWORD) return Response.json({ error: "Accesso amministratore non configurato sul server." }, { status: 503 });
    const admin = await tryOperatorLogin(env, id, pw);
    if (!admin) return Response.json({ error: "Email o password non corretti." }, { status: 401 });
    return admin;
  }

  const user = await env.DB.prepare("SELECT * FROM users WHERE lower(username) = ? OR lower(identifier) = ?").bind(id, id).first();
  let userPasswordValid = false;
  if (user) userPasswordValid = (await hash(pw, user.salt)) === user.password_hash;
  
  if (userPasswordValid) {
    const role = await env.DB.prepare("SELECT role FROM admin_roles WHERE user_id = ?").bind(user.id).first();
    if (role) return await beginAdminAuthenticator(env, user.id);
    const token = makeToken();
    await env.DB.prepare("INSERT INTO sessions (token, user_id, created_at) VALUES (?,?,?)").bind(token, user.id, Date.now()).run();
    return Response.json({ ok: true, mode: "user", token });
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
    env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)").bind(user.id, "Deposit request submitted", amount.toFixed(3) + " USDT · " + net + " · Pending review.", created),
  ]);
  await recordAntifraudEvent(request, env, user, 'deposit', txReference);
  return Response.json({ ok: true, status: "pending" });
}

const MARKET_RISK_STOP_LOSS = Object.freeze({ conservative: 8, balanced: 18, aggressive: 35 });
const CUSTOM_PLAN_FEES = Object.freeze({ 6: 200, 12: 300, 24: 400, 36: 500, 60: 600 });
const MARKET_CUSTOM_ASSETS = new Set(['gold','oil','electricity','sp500','nasdaq','tesla','artificialIntelligence','bitcoin','ethereum','dowJones','russell2000','silver','naturalGas','copper','euroUsd']);
const MARKET_PLAN_ALLOCATIONS = {
  6: [['gold',10],['oil',7],['electricity',8],['sp500',12],['nasdaq',10],['tesla',4],['artificialIntelligence',8],['bitcoin',4],['ethereum',3],['dowJones',8],['russell2000',6],['silver',5],['naturalGas',5],['copper',4],['euroUsd',6]],
  12: [['gold',8],['oil',7],['electricity',8],['sp500',12],['nasdaq',12],['tesla',6],['artificialIntelligence',10],['bitcoin',5],['ethereum',4],['dowJones',7],['russell2000',6],['silver',4],['naturalGas',4],['copper',3],['euroUsd',4]],
  24: [['gold',8],['oil',6],['electricity',6],['sp500',14],['nasdaq',14],['tesla',7],['artificialIntelligence',12],['bitcoin',8],['ethereum',7],['dowJones',6],['russell2000',4],['silver',3],['naturalGas',2],['copper',2],['euroUsd',1]],
  36: [['gold',8],['oil',6],['electricity',6],['sp500',14],['nasdaq',14],['tesla',7],['artificialIntelligence',12],['bitcoin',8],['ethereum',7],['dowJones',6],['russell2000',4],['silver',3],['naturalGas',2],['copper',2],['euroUsd',1]],
  60: [['gold',8],['oil',6],['electricity',6],['sp500',14],['nasdaq',14],['tesla',7],['artificialIntelligence',12],['bitcoin',8],['ethereum',7],['dowJones',6],['russell2000',4],['silver',3],['naturalGas',2],['copper',2],['euroUsd',1]],
};

async function currentMarketQuotes(selectedMarket = "DE-LU") {
  const market = normalizeElectricityMarket(selectedMarket), cached = positionQuoteCache.get(market);
  let freshQuotes = {};
  if (!(cached && Date.now() - cached.fetchedAt < 1_000)) {
    try { freshQuotes = (await loadQuotes(market))?.quotes || {}; } catch (error) { console.warn('Live quote refresh failed; using last known prices:', error); }
  }
  const sharedQuotes = cachedQuotesMarket === market ? (cachedQuotes?.quotes || {}) : {};
  const quotes = { ...sharedQuotes, ...(cached?.quotes || {}), ...freshQuotes };
  const storedHistory = cachedHistoryMarket === market ? cachedHistory : null;
  for (const key of MARKET_CUSTOM_ASSETS) {
    const current = Number(quotes[key]?.price);
    if (Number.isFinite(current) && current > 0) continue;
    const series = storedHistory?.histories?.[key] || (key === 'electricity' ? null : persistedMarketHistoryData?.histories?.[key]);
    if (!Array.isArray(series)) continue;
    for (let i = series.length - 1; i >= 0; i--) {
      const value = Number(series[i]?.value), time = Number(series[i]?.time);
      if (Number.isFinite(value) && value > 0) {
        quotes[key] = { ...(quotes[key] || {}), price: value, marketAt: time, currency: quotes[key]?.currency || 'USD', provider: quotes[key]?.provider || 'Last known market data' };
        break;
      }
    }
  }
  if (Object.keys(quotes).length) positionQuoteCache.set(market, { quotes, fetchedAt: Date.now() });
  return quotes;
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

function positionElectricityMarket(position) {
  try { return normalizeElectricityMarket(JSON.parse(position.entry_quotes_json || '{}').__electricityMarket); } catch { return "DE-LU"; }
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
  return Number((Number(position.amount) * (multiplier || 1)).toFixed(3));
}

async function syncMarketPositions(env, userId) {
  const rows = (await env.DB.prepare("SELECT * FROM market_positions WHERE user_id = ? ORDER BY created_at DESC").bind(userId).all()).results || [];
  if (!rows.length) return [];
  const now = Date.now();
  const activeMarkets = [...new Set(rows.filter(row => ['active', 'paused'].includes(row.status)).map(positionElectricityMarket))];
  const marketQuotes = new Map(await Promise.all(activeMarkets.map(async market => [market, await currentMarketQuotes(market)])));
  for (const row of rows) {
    if (!['active', 'paused'].includes(row.status)) continue;
    const quotes = marketQuotes.get(positionElectricityMarket(row)) || {};
    const marketValue = marketPositionValue(row, quotes);
    const amount = Number(row.amount);
    const stop = amount * (1 - Number(row.stop_loss_pct) / 100);
    const value = Math.max(marketValue, stop);
    const maturity = calendarMaturity(Number(row.created_at), Number(row.months));
    let status;
    if (now >= maturity) status = 'matured';
    else status = marketValue <= stop ? 'paused' : 'active';
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
          .bind(userId, 'Plan matured', 'Reference value ' + value.toFixed(3) + ' USDT. Internal balance updated; no market order or external payout was executed.', now).run();
      } else if (status === 'stop_loss') {
        await env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)")
          .bind(userId, 'Plan closed at stop', 'The aggressive plan reached its fixed ' + Number(row.stop_loss_pct) + '% stop. Remaining reference value was moved to your internal released balance.', now).run();
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
        const gain = Number(Math.max(0, value - amount).toFixed(3));
        if (basisReady && Object.keys(nextBasis).length && gain >= 0.001) {
          const transactionId = makeToken(), basisJson = JSON.stringify(nextBasis);
          const redemption = await env.DB.batch([
            env.DB.prepare("UPDATE market_positions SET last_value = ?, entry_quotes_json = ?, last_redemption_id = ?, basis_updated_at = ?, auto_add_anchor_value = ? WHERE id = ? AND user_id = ? AND status = 'active' AND last_value = ? AND entry_quotes_json = ? AND auto_redeem_enabled = 1 AND auto_redeem_pct = ? AND EXISTS (SELECT 1 FROM wallets WHERE user_id = ?)")
              .bind(amount, basisJson, transactionId, now, amount, row.id, userId, value, row.entry_quotes_json, autoPct, userId),
            env.DB.prepare("INSERT INTO market_position_redemptions (position_id, user_id, event_id, amount, reference_value, created_at, segment_start_at, basis_quotes_json) SELECT ?,?,?,?,?,?,?,? WHERE EXISTS (SELECT 1 FROM market_positions WHERE id = ? AND user_id = ? AND status = 'active' AND last_value = ? AND last_redemption_id = ?)")
              .bind(row.id, userId, transactionId, gain, value, now, Number(row.basis_updated_at || row.created_at), row.entry_quotes_json || '{}', row.id, userId, amount, transactionId),
            env.DB.prepare("UPDATE wallets SET earnings = earnings + ? WHERE user_id = ? AND EXISTS (SELECT 1 FROM market_positions WHERE id = ? AND user_id = ? AND status = 'active' AND last_value = ? AND last_redemption_id = ?)")
              .bind(gain, userId, row.id, userId, amount, transactionId),
            env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) SELECT ?, 'transaction', ?, ?, ? WHERE EXISTS (SELECT 1 FROM market_positions WHERE id = ? AND user_id = ? AND status = 'active' AND last_value = ? AND last_redemption_id = ?)")
              .bind(userId, 'Plan gains redeemed', gain.toFixed(3) + ' USDT moved to the redeemable balance after reaching the ' + autoPct + '% gain threshold. The plan remains active.', now, row.id, userId, amount, transactionId),
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
  for (const row of rows) {
    if (!['active','paused'].includes(row.status) || Number(row.auto_add_enabled)!==1) continue;
    const value=Number(row.last_value),amount=Number(row.amount),drawdown=Number(row.auto_add_drawdown_pct||10),anchor=Number(row.auto_add_anchor_value)||value,mode=row.auto_add_mode==='balance_pct'?'balance_pct':'fixed',setting=Number(row.auto_add_value||10);
    if (!(value>0&&amount>0&&anchor>0&&drawdown>=1&&drawdown<=90&&value<=anchor*(1-drawdown/100))) continue;
    const wallet=await env.DB.prepare("SELECT deposit, earnings FROM wallets WHERE user_id = ?").bind(userId).first();if(!wallet)continue;
    const deposit=Number(wallet.deposit)||0,earnings=Number(wallet.earnings)||0,available=deposit+earnings;
    const added=mode==='balance_pct'?Math.floor((available*setting/100+1e-9)*1000)/1000:Number(setting.toFixed(3));
    if (!(available>0&&added>=0.001&&added<=available+1e-9&&(mode!=='fixed'||setting<=available+1e-9))) continue;
    const depositPart=Number(Math.min(deposit,added).toFixed(3)),earningsPart=Number((added-depositPart).toFixed(3)),newAmount=Number((amount+added).toFixed(3)),newValue=Number((value+added).toFixed(3));
    let allocation=[],basis={};try{allocation=JSON.parse(row.allocations_json||'[]');basis=JSON.parse(row.entry_quotes_json||'{}')}catch{continue}
    const updatedBasis={...basis};let ready=true;for(const [key] of allocation){const current=Number(quotes[key]?.price),old=Number(basis[key])||current;if(!(current>0&&old>0)){ready=false;break}updatedBasis[key]=newAmount/(amount/old+added/current)}if(!ready||!allocation.length)continue;
    const basisJson=JSON.stringify(updatedBasis),depositAnchor=Number(row.auto_add_anchor_value)||value;
    const batch=await env.DB.batch([
      env.DB.prepare("UPDATE market_positions SET amount = ?, last_value = ?, entry_quotes_json = ?, basis_updated_at = ?, auto_add_anchor_value = ? WHERE id = ? AND user_id = ? AND status IN ('active','paused') AND last_value = ? AND amount = ? AND auto_add_enabled = 1 AND auto_add_anchor_value = ? AND auto_add_drawdown_pct = ? AND auto_add_mode = ? AND auto_add_value = ? AND EXISTS (SELECT 1 FROM wallets WHERE user_id = ? AND deposit >= ? AND earnings >= ?)")
        .bind(newAmount,newValue,basisJson,now,newValue,row.id,userId,value,amount,depositAnchor,drawdown,mode,setting,userId,depositPart,earningsPart),
      env.DB.prepare("UPDATE wallets SET deposit = deposit - ?, earnings = earnings - ? WHERE user_id = ? AND deposit >= ? AND earnings >= ? AND EXISTS (SELECT 1 FROM market_positions WHERE id = ? AND user_id = ? AND status IN ('active','paused') AND amount = ? AND last_value = ? AND basis_updated_at = ? AND auto_add_anchor_value = ?)")
        .bind(depositPart,earningsPart,userId,depositPart,earningsPart,row.id,userId,newAmount,newValue,now,newValue),
      env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) SELECT ?, 'transaction', ?, ?, ? WHERE EXISTS (SELECT 1 FROM market_positions WHERE id = ? AND user_id = ? AND amount = ? AND last_value = ? AND basis_updated_at = ? AND auto_add_anchor_value = ?)")
        .bind(userId,'Automatic top-up added',added.toFixed(3)+' USDT added automatically after a '+drawdown+'% portfolio decline. Funds were taken from available balances.',now,row.id,userId,newAmount,newValue,now,newValue),
    ]);
    if(batch[0]?.meta?.changes>0){row.amount=newAmount;row.last_value=newValue;row.entry_quotes_json=basisJson;row.basis_updated_at=now;row.auto_add_anchor_value=newValue;}
  }
  return rows.map(row => ({
    id: row.id, planId: row.plan_id, months: Number(row.months), amount: Number(row.amount),
    stopLossPct: Number(row.stop_loss_pct), riskLevel: row.risk_level || 'legacy', earlyCloseFee: Number(row.early_close_fee || 0), basisUpdatedAt: Number(row.basis_updated_at || row.created_at), entryQuotes: (() => { try { return JSON.parse(row.entry_quotes_json || '{}'); } catch { return {}; } })(), currentValue: Number(row.last_value),
    autoRedeemEnabled: Number(row.auto_redeem_enabled) === 1, autoRedeemPct: Number(row.auto_redeem_pct || 10), autoAddEnabled: Number(row.auto_add_enabled) === 1, autoAddDrawdownPct: Number(row.auto_add_drawdown_pct || 10), autoAddMode: row.auto_add_mode === 'balance_pct' ? 'balance_pct' : 'fixed', autoAddValue: Number(row.auto_add_value || 10), autoAddAnchorValue: Number(row.auto_add_anchor_value || row.last_value || row.amount), customFee: Number(row.custom_fee || 0),
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
  if (!await adminAccess(request, env, "fullPanel")) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  return Response.json({ addresses: await readDepositAddresses(env) });
}
async function saveAdminDepositAddresses(request, env) {
  const operator = await privilegedAdminActor(request, env);
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

async function simpleEarnInvest(request, env) {
  const user = await authed(request, env);
  const body = await request.json().catch(() => ({}));
  const amount = Number(Number(body.amount).toFixed(3));
  const source = body.source === "earnings" ? "earnings" : "deposit";
  if (!Number.isFinite(amount) || amount < 200 || amount > 1e12) return bad("L'investimento minimo per Simple Earn è 200 USDT.");
  const wallet = await accrue(env, user.id);
  if (amount > Number(wallet[source] || 0) + 1e-9) return bad("Saldo disponibile insufficiente.");
  const dailyRate = amount <= 500 ? 0.0019 : amount <= 1000 ? 0.0024 : amount <= 2500 ? 0.0029 : 0.0036;
  const now = Date.now();
  const results = await env.DB.batch([
    env.DB.prepare("INSERT INTO simple_earn_positions (user_id, principal, source, daily_rate, status, created_at) SELECT ?, ?, ?, ?, 'active', ? WHERE EXISTS (SELECT 1 FROM wallets WHERE user_id = ? AND " + source + " >= ?)").bind(user.id, amount, source, dailyRate, now, user.id, amount),
    env.DB.prepare("UPDATE wallets SET " + source + " = " + source + " - ? WHERE user_id = ? AND " + source + " >= ?").bind(amount, user.id, amount)
  ]);
  if (!(results[0]?.meta?.changes > 0) || !(results[1]?.meta?.changes > 0)) return bad("Saldo insufficiente o account aggiornato. Aggiorna e riprova.");
  return Response.json({ ok: true, positionId: results[0]?.meta?.last_row_id || null, principal: amount, dailyRate });
}

async function simpleEarnRedeem(request, env) {
  const user = await authed(request, env);
  const body = await request.json().catch(() => ({}));
  const id = Number(body.positionId);
  if (!Number.isInteger(id) || id < 1) return bad("Posizione Simple Earn non valida.");
  const position = await env.DB.prepare("SELECT * FROM simple_earn_positions WHERE id = ? AND user_id = ? AND status = 'active'").bind(id, user.id).first();
  if (!position) return Response.json({ error: "La posizione non è più attiva." }, { status: 409 });
  const now = Date.now(), principal = Number(position.principal);
  const interest = Number((principal * Number(position.daily_rate) * Math.max(0, now - Number(position.created_at)) / 86400000).toFixed(3));
  const result = await env.DB.prepare("UPDATE simple_earn_positions SET status = 'redeemed', interest_paid = ?, closed_at = ? WHERE id = ? AND user_id = ? AND status = 'active'").bind(interest, now, id, user.id).run();
  if (!(result.meta?.changes > 0)) return Response.json({ error: "La posizione è già stata riscattata." }, { status: 409 });
  if (position.source === "earnings") await env.DB.prepare("UPDATE wallets SET earnings = earnings + ? WHERE user_id = ?").bind(Number((principal + interest).toFixed(3)), user.id).run();
  else await env.DB.prepare("UPDATE wallets SET deposit = deposit + ?, earnings = earnings + ? WHERE user_id = ?").bind(principal, interest, user.id).run();
  return Response.json({ ok: true, principal, interest, total: Number((principal + interest).toFixed(3)) });
}

async function marketInvest(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const amount = Number(Number(body.amount).toFixed(3));
  const months = Number(body.months);
  if (body.riskAcknowledged !== true) return bad('È necessario confermare di aver compreso i rischi prima di avviare il piano.');
  const riskLevel = typeof body.riskLevel === 'string' ? body.riskLevel : 'balanced';
  const stopLossPct = MARKET_RISK_STOP_LOSS[riskLevel];
  const defaultAllocation = MARKET_PLAN_ALLOCATIONS[months];
  if (!defaultAllocation) return bad("Durata del piano non valida.");
  let allocation = defaultAllocation;
  if (body.allocations !== undefined) {
    if (!Array.isArray(body.allocations) || body.allocations.length < 3 || body.allocations.length > MARKET_CUSTOM_ASSETS.size) return bad('Seleziona almeno 3 asset.');
    const seen = new Set();
    allocation = [];
    for (const item of body.allocations) {
      const key = Array.isArray(item) ? String(item[0] || '') : String(item?.asset || '');
      const weight = Number(Array.isArray(item) ? item[1] : item?.weight);
      if (!MARKET_CUSTOM_ASSETS.has(key) || seen.has(key) || !Number.isFinite(weight) || weight <= 0 || weight > 100) return bad('Asset o percentuale non validi.');
      seen.add(key);
      allocation.push([key, weight]);
    }
    if (Math.abs(allocation.reduce((sum, item) => sum + item[1], 0) - 100) > 0.01) return bad('Le percentuali degli asset devono sommare esattamente 100%.');
  }
  if (!Number.isFinite(amount) || amount < 50) return bad("L'importo minimo è 50 USDT.");
  if (!Number.isFinite(stopLossPct)) return bad("Profilo stop-loss non disponibile.");
  const source = body.source === 'earnings' ? 'earnings' : 'deposit';
  const wallet = await accrue(env, user.id);
  if (amount > Number(wallet[source] || 0) + 1e-9) return bad("Saldo disponibile insufficiente.");
  const electricityMarket = normalizeElectricityMarket(body.electricityMarket);
  const quotes = await currentMarketQuotes(electricityMarket);
  const entry = { __electricityMarket: electricityMarket };
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
    .bind(user.id, "Market plan started", amount.toFixed(3) + " USDT · duration " + months + " months · risk " + riskLevel + " · fixed stop-loss " + stopLossPct + "%.", now).run();
  return Response.json({ ok: true, positionId: results[0]?.meta?.last_row_id || null });
}

async function customMarketInvest(request, env) {
  const user = await authed(request, env);
  const body = await request.json().catch(() => ({}));
  if (body.riskAcknowledged !== true) return bad('È necessario confermare di aver compreso i rischi prima di avviare il piano.');
  const months = Number(body.months);
  const principal = Number(Number(body.amount).toFixed(3));
  const stopLossPct = Number(body.stopLossPct);
  const fee = CUSTOM_PLAN_FEES[months];
  if (!fee) return bad('Durata del piano personalizzato non valida.');
  if (!Number.isFinite(principal) || principal < 1000) return bad('L’investimento minimo per un portafoglio personalizzato è 1.000 USDT.');
  if (!Number.isFinite(stopLossPct) || stopLossPct < 1 || stopLossPct > 90) return bad('Lo stop loss deve essere tra 1% e 90%.');
  const source = body.source === 'earnings' ? 'earnings' : 'deposit';
  const rawAllocations = Array.isArray(body.allocations) ? body.allocations : [];
  if (rawAllocations.length < 1 || rawAllocations.length > MARKET_CUSTOM_ASSETS.size) return bad('Seleziona almeno 1 asset.');
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
  const totalCharge = Number((principal + fee).toFixed(3));
  const wallet = await accrue(env, user.id);
  if (totalCharge > Number(wallet[source] || 0) + 1e-9) return bad('Saldo insufficiente: servono ' + totalCharge.toFixed(3) + ' USDT, inclusa la quota del piano.');
  const electricityMarket = normalizeElectricityMarket(body.electricityMarket);
  const quotes = await currentMarketQuotes(electricityMarket), entry = { __electricityMarket: electricityMarket };
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
    .bind(user.id, 'Custom market portfolio started', principal.toFixed(3) + ' USDT invested · plan fee ' + fee.toFixed(3) + ' USDT · duration ' + months + ' months · stop loss ' + stopLossPct + '%.', now).run();
  return Response.json({ ok: true, positionId: results[0]?.meta?.last_row_id || null, amount: principal, fee, totalCharged: totalCharge });
}

async function addMarketFunds(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const id = Number(body.positionId), amount = Number(Number(body.amount).toFixed(3));
  const source = body.source === 'earnings' ? 'earnings' : 'deposit';
  if (!Number.isInteger(id) || id < 1 || !Number.isFinite(amount) || amount < 1) return bad("Importo o piano non valido.");
  const initialPosition = await env.DB.prepare("SELECT * FROM market_positions WHERE id = ? AND user_id = ? AND status IN ('active','paused')").bind(id, user.id).first();
  if (!initialPosition) return bad("Piano attivo o sospeso non trovato.");
  if (initialPosition.status === 'active') await syncMarketPositions(env, user.id);
  await accrue(env, user.id);
  const quotes = await currentMarketQuotes(positionElectricityMarket(initialPosition));
  const tx = await db.transaction('write');
  const reject = async message => { await tx.rollback(); return bad(message); };
  try {
    const positionResult = await tx.execute({
      sql: "SELECT * FROM market_positions WHERE id = ? AND user_id = ? AND status IN ('active','paused')",
      args: [id, user.id]
    });
    const position = positionResult.rows[0];
    if (!position) return await reject("Piano attivo o sospeso non trovato.");
    if (Date.now() >= calendarMaturity(Number(position.created_at), Number(position.months))) return await reject("Il piano è scaduto e non accetta fondi aggiuntivi.");
    const walletResult = await tx.execute({ sql: "SELECT " + source + " AS available FROM wallets WHERE user_id = ?", args: [user.id] });
    const available = Number(walletResult.rows[0]?.available || 0);
    if (amount > available + 1e-9) return await reject("Saldo disponibile insufficiente.");
    let basis = {};
    try { basis = JSON.parse(position.entry_quotes_json || '{}'); } catch {}
    let allocation = [];
    try { allocation = JSON.parse(position.allocations_json || '[]'); } catch {}
    const updatedBasis = { ...basis };
    for (const [key] of allocation) {
      const current = Number(quotes[key]?.price);
      if (!(current > 0)) return await reject("Prezzi live temporaneamente non disponibili.");
      const old = Number(basis[key]) || current;
      updatedBasis[key] = (Number(position.amount) + amount) / (Number(position.amount) / old + amount / current);
    }
    if (!allocation.length) return await reject("Valore di mercato non disponibile.");
    const currentValue = marketPositionValue(position, quotes);
    if (!Number.isFinite(currentValue) || currentValue <= 0) return await reject("Valore di mercato non disponibile.");
    const now = Date.now(), newAmount = Number((Number(position.amount) + amount).toFixed(3)), newValue = Number((currentValue + amount).toFixed(3));
    const updated = await tx.execute({
      sql: "UPDATE market_positions SET amount = ?, last_value = ?, entry_quotes_json = ?, basis_updated_at = ?, auto_add_anchor_value = ?, status = 'active', closed_at = NULL WHERE id = ? AND user_id = ? AND status IN ('active','paused')",
      args: [newAmount, newValue, JSON.stringify(updatedBasis), now, newValue, id, user.id]
    });
    if (updated.rowsAffected !== 1) return await reject("Piano non aggiornabile. Aggiorna e riprova.");
    const debited = await tx.execute({
      sql: "UPDATE wallets SET " + source + " = " + source + " - ? WHERE user_id = ? AND " + source + " >= ?",
      args: [amount, user.id, amount]
    });
    if (debited.rowsAffected !== 1) return await reject("Saldo disponibile insufficiente.");
    const notification = await tx.execute({
      sql: "INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)",
      args: [user.id, 'Additional funds added', amount.toFixed(3) + ' USDT added manually to market portfolio #' + id + '.', now]
    });
    await tx.commit();
    if (notification.lastInsertRowid != null) publishNotification(user.id, notification.lastInsertRowid);
    return Response.json({ ok: true });
  } catch (error) {
    try { await tx.rollback(); } catch {}
    throw error;
  }
}
async function redeemMarketGains(request, env) {
  const user = await authed(request, env);
  const body = await request.json().catch(() => ({}));
  const id = Number(body.positionId);
  if (!Number.isInteger(id) || id < 1) return bad('Piano non valido.');
  await syncMarketPositions(env, user.id);
  const position = await env.DB.prepare("SELECT * FROM market_positions WHERE id = ? AND user_id = ? AND status IN ('active','paused')").bind(id, user.id).first();
  if (!position) return Response.json({ error: 'Il piano non è più attivo o riscattabile.' }, { status: 409 });
  const quotes = await currentMarketQuotes(positionElectricityMarket(position));
  const value = marketPositionValue(position, quotes);
  const gain = Number((value - Number(position.amount)).toFixed(3));
  if (!(gain >= 0.001)) return bad('Non ci sono guadagni sopra il capitale investito da riscattare.');
  let allocation = [];
  try { allocation = JSON.parse(position.allocations_json || '[]'); } catch {}
  const nextBasis = { __electricityMarket: positionElectricityMarket(position) };
  for (const [key] of allocation) {
    const price = Number(quotes[key]?.price);
    if (!(price > 0)) return Response.json({ error: 'Prezzi di riferimento non disponibili; riprova tra poco.' }, { status: 503 });
    nextBasis[key] = price;
  }
  const basisJson = JSON.stringify(nextBasis);
  const transactionId = makeToken();
  const oldStatus = position.status;
  const now = Date.now();
  const batch = await env.DB.batch([
    env.DB.prepare("UPDATE market_positions SET last_value = ?, entry_quotes_json = ?, last_redemption_id = ?, basis_updated_at = ?, auto_add_anchor_value = ? WHERE id = ? AND user_id = ? AND status = ? AND last_value = ? AND entry_quotes_json = ? AND EXISTS (SELECT 1 FROM wallets WHERE user_id = ?)")
      .bind(Number(position.amount), basisJson, transactionId, now, Number(position.amount), id, user.id, oldStatus, Number(position.last_value), position.entry_quotes_json, user.id),
    env.DB.prepare("INSERT INTO market_position_redemptions (position_id, user_id, event_id, amount, reference_value, created_at, segment_start_at, basis_quotes_json) SELECT ?,?,?,?,?,?,?,? WHERE EXISTS (SELECT 1 FROM market_positions WHERE id = ? AND user_id = ? AND status = ? AND last_redemption_id = ?)")
      .bind(id, user.id, transactionId, gain, value, now, Number(position.basis_updated_at || position.created_at), position.entry_quotes_json || '{}', id, user.id, oldStatus, transactionId),
    env.DB.prepare("UPDATE wallets SET earnings = earnings + ? WHERE user_id = ? AND EXISTS (SELECT 1 FROM market_positions WHERE id = ? AND user_id = ? AND status = ? AND last_value = ? AND last_redemption_id = ?)")
      .bind(gain, user.id, id, user.id, oldStatus, Number(position.amount), transactionId),
    env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) SELECT ?, 'transaction', ?, ?, ? WHERE EXISTS (SELECT 1 FROM market_positions WHERE id = ? AND user_id = ? AND status = ? AND last_redemption_id = ?)")
      .bind(user.id, 'Plan gains redeemed', gain.toFixed(3) + ' USDT moved to your internal redeemable balance. The invested principal remains in the plan.', now, id, user.id, oldStatus, transactionId),
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
  const quotes = await currentMarketQuotes(positionElectricityMarket(position));
  const value = Number(marketPositionValue(position, quotes).toFixed(3));
  if (!(value > 0)) return bad('Il valore di riferimento non è disponibile per la chiusura.');
  const fee = Number((value * 0.65).toFixed(3));
  const net = Number((value - fee).toFixed(3));
  const now = Date.now();
  const transactionId = makeToken();
  const batch = await env.DB.batch([
    env.DB.prepare("UPDATE market_positions SET status = 'early_closed', closed_at = ?, last_value = ?, early_close_fee = ?, last_redemption_id = ? WHERE id = ? AND user_id = ? AND status = ? AND EXISTS (SELECT 1 FROM wallets WHERE user_id = ?)")
      .bind(now, value, fee, transactionId, id, user.id, position.status, user.id),
    env.DB.prepare("UPDATE wallets SET released = MAX(0, released - ?), earnings = earnings + ? WHERE user_id = ? AND EXISTS (SELECT 1 FROM market_positions WHERE id = ? AND user_id = ? AND status = 'early_closed' AND last_redemption_id = ?)")
      .bind(value, net, user.id, id, user.id, transactionId),
    env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) SELECT ?, 'transaction', ?, ?, ? WHERE EXISTS (SELECT 1 FROM market_positions WHERE id = ? AND user_id = ? AND status = 'early_closed' AND last_redemption_id = ?)")
      .bind(user.id, 'Plan closed early', 'Current reference value ' + value.toFixed(3) + ' USDT; 65% early-closure fee ' + fee.toFixed(3) + ' USDT; net credited to earnings balance and available for withdrawal request ' + net.toFixed(3) + ' USDT. No external payout was made.', now, id, user.id, transactionId),
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

async function savePositionAutoAdd(request, env) {
  const user=await authed(request,env),body=await request.json().catch(()=>({})),positionId=Number(body.positionId),enabled=body.enabled===true,drawdownPct=Number(body.drawdownPct),mode=body.mode==='balance_pct'?'balance_pct':'fixed',rawValue=Number(body.value);
  if(!Number.isInteger(positionId)||positionId<1)return bad('Piano non valido.');
  if(!Number.isFinite(drawdownPct)||drawdownPct<1||drawdownPct>90)return bad('La soglia di calo deve essere tra 1% e 90%.');
  if(!Number.isFinite(rawValue)||(mode==='balance_pct'?(rawValue<1||rawValue>100):rawValue<0.001))return bad('Importo o percentuale di ricarica non validi.');
  const value=mode==='fixed'?Number(rawValue.toFixed(3)):rawValue;
  await syncMarketPositions(env,user.id);
  const position=await env.DB.prepare("SELECT last_value FROM market_positions WHERE id = ? AND user_id = ? AND status IN ('active','paused')").bind(positionId,user.id).first();
  if(!position)return Response.json({error:'Questo piano non è più attivo.'},{status:409});
  const anchor=Number(position.last_value)||0;
  const result=await env.DB.prepare("UPDATE market_positions SET auto_add_enabled = ?, auto_add_drawdown_pct = ?, auto_add_mode = ?, auto_add_value = ?, auto_add_anchor_value = ? WHERE id = ? AND user_id = ? AND status IN ('active','paused')")
    .bind(enabled?1:0,drawdownPct,mode,value,anchor,positionId,user.id).run();
  if(!(result.meta?.changes>0))return Response.json({error:'Questo piano non è più attivo.'},{status:409});
  return Response.json({ok:true,positionId,enabled,drawdownPct,mode,value,anchor});
}

async function transferDepositToEarnings(request, env) {
  const user = await authed(request, env);
  const body = await request.json().catch(() => ({}));
  const amount = Number(Number(body.amount).toFixed(3));
  if (!Number.isFinite(amount) || amount <= 0) return bad("Inserisci un importo valido.");
  const wallet = await accrue(env, user.id);
  if (amount > Number(wallet.deposit || 0) + 1e-9) return bad("Saldo deposito insufficiente.");
  const feeAmount = Number((amount * 0.25).toFixed(3));
  const creditedAmount = Number((amount - feeAmount).toFixed(3));
  if (!(creditedAmount > 0)) return bad("L'importo è troppo piccolo dopo la commissione.");
  const changed = await env.DB.prepare("UPDATE wallets SET deposit = deposit - ?, earnings = earnings + ? WHERE user_id = ? AND deposit >= ?").bind(amount, creditedAmount, user.id, amount).run();
  if (!(changed.meta?.changes > 0)) return Response.json({ error: "Saldo deposito insufficiente o aggiornato. Riprova." }, { status: 409 });
  const created = Date.now();
  try {
    await env.DB.batch([
      env.DB.prepare("INSERT INTO balance_transfers (user_id, amount, fee_amount, credited_amount, created_at) VALUES (?, ?, ?, ?, ?)").bind(user.id, amount, feeAmount, creditedAmount, created),
      env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)").bind(user.id, "Deposit balance transferred", amount.toFixed(3) + " USDT moved from deposit balance; 25% commission " + feeAmount.toFixed(3) + " USDT; " + creditedAmount.toFixed(3) + " USDT credited to earnings.", created),
    ]);
  } catch (error) {
    await env.DB.prepare("UPDATE wallets SET deposit = deposit + ?, earnings = MAX(0, earnings - ?) WHERE user_id = ?").bind(amount, creditedAmount, user.id).run();
    throw error;
  }
  return Response.json({ ok: true, amount, fee: feeAmount, credited: creditedAmount });
}

async function validateWithdrawal(env, user, body) {
  const amount = Number(body.amount);
  const net = String(body.net || "").trim();
  if (!Number.isFinite(amount) || amount < 50) return bad("Il prelievo minimo è di 50 USDT.");
  if (!["BNB", "TRC20", "BTC", "USDC"].includes(net)) return bad("Rete non valida.");
  const wallet = await accrue(env, user.id);
  const source = body.source === 'released' ? 'released' : 'earnings';
  if (amount > Number(wallet[source] || 0) + 1e-9) return bad(source === 'released' ? "Saldo capitale restituito non sufficiente." : "Saldo riscattabile non sufficiente.");
  
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

  if (!address || address === "Indirizzo non specificato dall'utente") return bad("Collega un indirizzo di prelievo per la rete selezionata nel profilo.");
  const chargedAmount = Number(amount.toFixed(3));
  const feeRate = source === 'released' ? 0.25 : 0.08;
  const feeAmount = Number((chargedAmount * feeRate).toFixed(3));
  const payoutAmount = Number((chargedAmount - feeAmount).toFixed(3));
  return { amount: payoutAmount, feeAmount, chargedAmount, net, address, source };
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
  const debited = await env.DB.prepare("UPDATE wallets SET " + prepared.source + " = " + prepared.source + " - ? WHERE user_id = ? AND " + prepared.source + " >= ?").bind(prepared.chargedAmount, user.id, prepared.chargedAmount).run();
  if (!(debited.meta?.changes > 0)) return Response.json({ error: "Saldo insufficiente o aggiornato. Riprova." }, { status: 409 });
  try {
    await env.DB.batch([
      env.DB.prepare("INSERT INTO withdrawals (user_id, amount, net, date, status, wallet_address, source, fee_amount, charged_amount) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, ?)")
        .bind(user.id, prepared.amount, prepared.net, created, prepared.address, prepared.source, prepared.feeAmount, prepared.chargedAmount),
      env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)").bind(user.id, "Withdrawal request submitted", "Payout " + prepared.amount.toFixed(3) + " USDT · commission " + prepared.feeAmount.toFixed(3) + " USDT · " + prepared.net + " · Pending review.", created),
    ]);
  } catch (error) {
    await env.DB.prepare("UPDATE wallets SET " + prepared.source + " = " + prepared.source + " + ? WHERE user_id = ?").bind(prepared.chargedAmount, user.id).run();
    throw error;
  }
  await recordAntifraudEvent(request, env, user, 'withdrawal');
  return Response.json({ ok: true, status: "pending", payout: prepared.amount, fee: prepared.feeAmount, charged: prepared.chargedAmount });
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
  
  if (actionRow.action === "wallet-change") {
    let payload = {};
    try { payload = JSON.parse(actionRow.payload_json || "{}"); } catch {}
    const result = await applyWalletChange(env, user, payload.wallets || {});
    if (result instanceof Response && !result.ok) return result;
    await env.DB.prepare("UPDATE sensitive_action_verifications SET consumed_at = ? WHERE user_id = ? AND consumed_at IS NULL").bind(Date.now(), user.id).run();
    return result;
  }
  await env.DB.prepare("UPDATE sensitive_action_verifications SET consumed_at = ? WHERE user_id = ?").bind(Date.now(), user.id).run();
  return Response.json({ ok: true });
}

const ANTIFRAUD_RETENTION_MS=90*86400000;
const ANTIFRAUD_TEMPLATES={
1:{en:["Account security notice","Activity was detected from an IP address also used by another account. This automated signal is not proof of wrongdoing. Please review your account security and contact support if you do not recognize it."],fr:["Alerte de sécurité du compte","Une activité a été détectée depuis une adresse IP également utilisée par un autre compte. Ce signal automatique ne prouve aucun comportement fautif. Vérifiez votre compte et contactez l’assistance si vous ne reconnaissez pas cette activité."],es:["Aviso de seguridad de la cuenta","Se detectó actividad desde una IP también utilizada por otra cuenta. Esta señal automática no demuestra una conducta indebida. Revise su cuenta y contacte con soporte si no reconoce la actividad."],de:["Sicherheitshinweis zum Konto","Es wurde Aktivität über eine IP-Adresse festgestellt, die auch ein anderes Konto nutzt. Dieses automatische Signal ist kein Nachweis eines Fehlverhaltens. Prüfen Sie Ihr Konto und kontaktieren Sie bei Unklarheiten den Support."],pt:["Aviso de segurança da conta","Foi detetada atividade através de um IP também utilizado por outra conta. Este sinal automático não comprova irregularidade. Verifique a conta e contacte o apoio se não reconhecer a atividade."],hi:["खाता सुरक्षा सूचना","दूसरे खाते में भी उपयोग किए गए IP से गतिविधि मिली है। यह स्वचालित संकेत है, गलत काम का प्रमाण नहीं। खाते की सुरक्षा जाँचें और गतिविधि पहचान में न आए तो सहायता टीम से संपर्क करें।"],zh:["账户安全提醒","检测到使用另一账户也使用过的 IP 地址进行的活动。这是自动安全提示，并非不当行为的证明。请检查账户安全；如非本人操作，请联系支持团队。"],it:["Avviso di sicurezza dell’account","È stata rilevata attività da un indirizzo IP utilizzato anche da un altro account. È un segnale automatico, non una prova di illecito. Verifica la sicurezza dell’account e contatta l’assistenza se non riconosci l’attività."]},
2:{en:["Final security warning","Final warning: activity associated with an IP address also used by another account remains under review. This signal alone does not establish wrongdoing. Contact support if you believe this is a mistake."],fr:["Dernier avertissement de sécurité","Dernier avertissement : une activité associée à une adresse IP également utilisée par un autre compte est toujours examinée. Ce signal seul ne prouve aucun comportement fautif. Contactez l’assistance si vous pensez qu’il s’agit d’une erreur."],es:["Último aviso de seguridad","Último aviso: se sigue revisando actividad asociada a una IP también utilizada por otra cuenta. Esta señal por sí sola no demuestra una conducta indebida. Contacte con soporte si cree que es un error."],de:["Letzte Sicherheitswarnung","Letzte Warnung: Aktivitäten über eine IP-Adresse, die auch ein anderes Konto nutzt, werden weiter geprüft. Dieses Signal allein belegt kein Fehlverhalten. Bei einem Irrtum wenden Sie sich an den Support."],pt:["Último aviso de segurança","Último aviso: continua em análise atividade associada a um IP também utilizado por outra conta. Este sinal, por si só, não comprova irregularidade. Contacte o apoio se considerar que é um erro."],hi:["अंतिम सुरक्षा चेतावनी","अंतिम चेतावनी: दूसरे खाते में भी उपयोग किए गए IP से जुड़ी गतिविधि की समीक्षा जारी है। केवल इस संकेत से गलत काम साबित नहीं होता। गलती लगे तो सहायता टीम से संपर्क करें।"],zh:["最后一次安全警告","最后提醒：与另一账户也使用过的 IP 地址相关的活动仍在审核中。仅凭此提示不能认定存在不当行为。如认为有误，请联系支持团队。"],it:["Ultimo avviso di sicurezza","Ultimo avviso: è ancora in corso la verifica di un’attività associata a un IP utilizzato anche da un altro account. Il segnale da solo non dimostra un illecito. Se ritieni che sia un errore, contatta l’assistenza."]},
3:{en:["Final notice · account review","Continued activity associated with an IP address also used by another account may lead to an account-closure review. No account is closed automatically based on an IP match alone."],fr:["Dernier avis · examen du compte","La poursuite d’une activité associée à une adresse IP également utilisée par un autre compte peut entraîner un examen de clôture du compte. Aucun compte n’est fermé automatiquement sur la seule base d’une correspondance IP."],es:["Aviso final · revisión de la cuenta","La continuidad de actividad asociada a una IP también utilizada por otra cuenta puede dar lugar a una revisión para el cierre de la cuenta. Ninguna cuenta se cierra automáticamente por una coincidencia de IP."],de:["Letzter Hinweis · Kontoprüfung","Anhaltende Aktivitäten über eine IP-Adresse, die auch ein anderes Konto nutzt, können zu einer Prüfung einer Kontoschließung führen. Aufgrund eines IP-Treffers allein wird kein Konto automatisch geschlossen."],pt:["Aviso final · análise da conta","A continuidade de atividade associada a um IP também utilizado por outra conta pode levar a uma análise para encerramento da conta. Nenhuma conta é encerrada automaticamente apenas por correspondência de IP."],hi:["अंतिम सूचना · खाता समीक्षा","दूसरे खाते में भी उपयोग किए गए IP से जुड़ी गतिविधि जारी रहने पर खाता बंद करने की समीक्षा हो सकती है। केवल IP मिलान पर खाता अपने-आप बंद नहीं होगा।"],zh:["最终通知 · 账户审核","若与另一账户也使用过的 IP 地址相关的活动持续，账户可能进入关闭审核。不会仅凭 IP 匹配自动关闭账户。"],it:["Avviso finale · verifica dell’account","Il protrarsi di attività associate a un indirizzo IP utilizzato anche da un altro account può comportare una verifica ai fini della chiusura. Nessun account viene chiuso automaticamente sulla sola base di una corrispondenza IP."]}};
function resolvedClientIp(request){const v=String(request.headers.get('x-platform-client-ip')||'').trim().replace(/^::ffff:/i,'');return v&&v.length<=64&&/^[0-9a-fA-F:.]+$/.test(v)?v:null}
async function recordAntifraudEvent(request,env,user,eventType,eventRef=''){const now=Date.now(),ip=resolvedClientIp(request);await env.DB.prepare("DELETE FROM antifraud_events WHERE created_at < ?").bind(now-ANTIFRAUD_RETENTION_MS).run();const reuse=ip?await env.DB.prepare("SELECT COUNT(DISTINCT user_id) AS accounts FROM antifraud_events WHERE ip_address=? AND user_id!=? AND created_at>=?").bind(ip,user.id,now-ANTIFRAUD_RETENTION_MS).first():null;const shared=Number(reuse?.accounts||0)>0?1:0;if(shared)await env.DB.prepare("UPDATE antifraud_events SET shared_ip=1 WHERE ip_address=? AND created_at>=?").bind(ip,now-ANTIFRAUD_RETENTION_MS).run();await env.DB.prepare("INSERT INTO antifraud_events (user_id,username,event_type,ip_address,event_ref,created_at,shared_ip) VALUES (?,?,?,?,?,?,?)").bind(user.id,String(user.username||'user').slice(0,80),eventType,ip,String(eventRef||'').slice(0,180),now,shared).run()}
async function adminAntifraudEvents(request,env){const actor=await adminAccess(request,env,'antifraud');if(!actor)return Response.json({error:'Permesso antifrode richiesto.'},{status:403});const rows=(await env.DB.prepare("SELECT e.id,e.user_id,e.username,e.event_type,e.ip_address,e.event_ref,e.created_at,e.shared_ip,COALESCE((SELECT MAX(n.stage) FROM antifraud_notice_log n WHERE n.user_id=e.user_id),0) AS notice_stage FROM antifraud_events e ORDER BY e.created_at DESC,e.id DESC LIMIT 250").all()).results;return Response.json({events:rows,retentionDays:90})}
async function sendAntifraudNotice(request,env){const actor=await adminAccess(request,env,'antifraud');if(!actor)return Response.json({error:'Permesso antifrode richiesto.'},{status:403});let body={};try{body=await request.json()}catch{}const eventId=Number(body.eventId),stage=Number(body.stage),language=['en','fr','es','de','pt','hi','zh','it'].includes(body.language)?body.language:'en';if(!Number.isSafeInteger(eventId)||eventId<1||![1,2,3].includes(stage))return bad('Avviso antifrode non valido.');const event=await env.DB.prepare("SELECT id,user_id,shared_ip FROM antifraud_events WHERE id=?").bind(eventId).first();if(!event)return Response.json({error:'Segnalazione non trovata.'},{status:404});if(Number(event.shared_ip)!==1)return bad('Non è possibile inviare un avviso senza una corrispondenza IP tra account distinti.');const latest=await env.DB.prepare("SELECT COALESCE(MAX(stage),0) AS stage FROM antifraud_notice_log WHERE user_id=?").bind(event.user_id).first();if(stage!==Number(latest?.stage||0)+1)return Response.json({error:'Inviare gli avvisi in sequenza: avviso, ultimo avviso, avviso finale.'},{status:409});const msg=ANTIFRAUD_TEMPLATES[stage][language]||ANTIFRAUD_TEMPLATES[stage].en,now=Date.now();const result=await env.DB.batch([env.DB.prepare("INSERT INTO antifraud_notice_log (event_id,user_id,stage,admin_id,created_at) VALUES (?,?,?,?,?)").bind(eventId,event.user_id,stage,actor.id,now),env.DB.prepare("INSERT INTO notifications (user_id,kind,title,message,created_at) VALUES (?, 'antifraud', ?, ?, ?)").bind(event.user_id,msg[0],msg[1],now)]);if(!(result?.[0]?.meta?.changes>0))return Response.json({error:'Questo avviso è già stato inviato o la sequenza è cambiata. Aggiorna la sezione.'},{status:409});return Response.json({ok:true,stage})}

async function adminPermissions(request, env) {
  const actor = await adminActor(request, env);
  if (!actor) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  const permissions = await readAdminPermissions(actor, env);
  const assignedRole = await env.DB.prepare("SELECT role FROM admin_roles WHERE user_id = ?").bind(actor.id).first();
  const isAdmin = isPrimaryAdmin(actor) || assignedRole?.role === "admin" || permissions.allAdmin === true;
  if (isAdmin || permissions.fullPanel) permissions.antifraud = true;
  return Response.json({ permissions, isAdmin });
}

async function listPasswordResetRequests(request, env) {
  const actor = await adminActor(request, env);
  if (!actor) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  const requests = (await env.DB.prepare("SELECT r.user_id, r.requested_at, r.otp_expires_at, r.issued_at, u.username, u.identifier FROM password_reset_requests r JOIN users u ON u.id = r.user_id ORDER BY r.requested_at DESC").all()).results;
  return Response.json({ requests });
}

async function adminDatabaseBackup(request, env) {
  const actor = await privilegedAdminActor(request, env);
  if (!actor) return Response.json({ error: "Accesso amministratore richiesto." }, { status: 403 });
  const objects = (await env.DB.prepare("SELECT type, name, tbl_name, sql FROM sqlite_master WHERE type IN ('table','index','trigger','view') AND name NOT LIKE 'sqlite_%' AND sql IS NOT NULL ORDER BY type, name").all()).results || [];
  const tableObjects = objects.filter((item) => item.type === 'table');
  const tables = {};
  for (const item of tableObjects) {
    const identifier = '"' + String(item.name).replaceAll('"', '""') + '"';
    const result = await env.DB.prepare('SELECT * FROM ' + identifier).all();
    tables[item.name] = result.results || [];
  }
  const createdAt = new Date().toISOString();
  const filename = 'ginzeh-backup-' + createdAt.replace(/[:.]/g, '-') + '.json';
  return new Response(JSON.stringify({ format: 'ginzeh-full-backup-v1', createdAt, tables, schema: objects }), {
    headers: { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', 'Content-Disposition': 'attachment; filename="' + filename + '"' }
  });
}

async function adminTransactions(request, env) {
  const user = await adminActor(request, env);
  if (!user) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  const permissions = await readAdminPermissions(user, env);
  const deposits = permissions.requests ? (await env.DB.prepare("SELECT d.*, u.username FROM deposits d JOIN users u ON u.id = d.user_id WHERE d.status = 'pending' ORDER BY d.date ASC").all()).results : [];
  const withdrawals = permissions.requests ? (await env.DB.prepare("SELECT w.*, u.username FROM withdrawals w JOIN users u ON u.id = w.user_id WHERE w.status = 'pending' ORDER BY w.date ASC").all()).results : [];
  
  const rawWalletChanges = (await env.DB.prepare(`
    SELECT r.id, r.user_id, r.net, r.old_address, r.new_address, r.created_at, r.status, u.username, u.identifier,
       EXISTS(SELECT 1 FROM user_withdrawal_addresses a WHERE a.net = r.net AND lower(a.address) = lower(r.new_address) AND a.user_id != r.user_id) AS address_conflict
    FROM wallet_change_requests r 
    JOIN users u ON u.id = r.user_id 
    WHERE r.status = 'pending' 
    ORDER BY r.created_at ASC
  `).all()).results;

  const walletChanges = permissions.walletChanges ? rawWalletChanges.map(w => {
    const addressDetails = "Vecchio: " + w.old_address + " -> Nuovo: " + w.new_address;
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
  }) : [];
  
  const emailVerifications = permissions.otp ? (await env.DB.prepare("SELECT r.user_id, r.requested_at, r.otp_expires_at, u.username, u.identifier FROM email_verification_requests r JOIN users u ON u.id = r.user_id WHERE r.verified_at IS NULL").all()).results : [];
  const sensitiveActionVerifications = permissions.otp ? (await env.DB.prepare("SELECT r.user_id, r.action, r.payload_json, r.requested_at, r.otp_expires_at, u.username, u.identifier FROM sensitive_action_verifications r JOIN users u ON u.id = r.user_id WHERE r.consumed_at IS NULL").all()).results : [];
  const passwordResets = (await env.DB.prepare("SELECT r.user_id, r.requested_at, r.otp_expires_at, r.issued_at, u.username, u.identifier FROM password_reset_requests r JOIN users u ON u.id = r.user_id ORDER BY r.requested_at DESC").all()).results;

  return Response.json({ deposits, withdrawals, walletChanges, emailVerifications, sensitiveActionVerifications, passwordResets, operator: user.username, permissions });
}

async function adminGlobalLedger(request, env) {
  const actor = await adminAccess(request, env, "fullPanel");
  if (!actor) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  const url = new URL(request.url);
  const requestedCategory = String(url.searchParams.get("category") || "all");
  const categories = new Set(["all", "deposits", "withdrawals", "investments", "redemptions", "referrals"]);
  if (!categories.has(requestedCategory)) return Response.json({ error: "Categoria non valida." }, { status: 400 });
  const category = requestedCategory === "all" ? "" : requestedCategory;
  const user = String(url.searchParams.get("user") || "").trim().slice(0, 100);
  const rawOffset = Number(url.searchParams.get("offset") || 0);
  const offset = Number.isSafeInteger(rawOffset) && rawOffset >= 0 ? Math.min(rawOffset, 100000) : 0;
  const limit = 100;
  const result = await env.DB.prepare([
    "WITH ledger AS (",
    "SELECT 'deposit:' || d.id AS event_id, d.date AS occurred_at, u.id AS user_id, u.username AS username, 'Deposit' AS activity, 'deposits' AS category, ABS(d.amount) AS amount, d.status AS status, COALESCE(NULLIF(d.tx_reference, ''), d.net) AS reference FROM deposits d JOIN users u ON u.id = d.user_id",
    "UNION ALL SELECT 'withdrawal:' || w.id, w.date, u.id, u.username, 'Withdrawal', 'withdrawals', -ABS(w.amount), w.status, COALESCE(NULLIF(w.tx_reference, ''), w.source || ' · ' || w.net) FROM withdrawals w JOIN users u ON u.id = w.user_id",
    "UNION ALL SELECT 'investment:' || i.id, i.created, u.id, u.username, 'Investment', 'investments', -ABS(i.amount), CASE WHEN i.active = 1 THEN 'active' ELSE 'completed' END, 'Investment · ' || i.days || ' days · ' || i.rate || '%' FROM investments i JOIN users u ON u.id = i.user_id",
    "UNION ALL SELECT 'position:' || m.id, m.created_at, u.id, u.username, 'Market position', 'investments', -ABS(m.amount), m.status, m.plan_id || ' · ' || m.months || ' months' FROM market_positions m JOIN users u ON u.id = m.user_id",
    "UNION ALL SELECT 'redemption:' || r.id, r.created_at, u.id, u.username, 'Investment redemption', 'redemptions', ABS(r.amount), 'completed', r.event_id || ' · reference value ' || printf('%.3f', r.reference_value) FROM market_position_redemptions r JOIN users u ON u.id = r.user_id",
    "UNION ALL SELECT 'commission:' || c.id, c.created_at, u.id, u.username, 'Referral commission', 'referrals', ABS(c.amount), 'credited', 'Level ' || c.level || ' · from @' || COALESCE(source.username, 'user') FROM referral_commissions c JOIN users u ON u.id = c.user_id LEFT JOIN users source ON source.id = c.source_user_id",
    ") SELECT event_id, occurred_at, user_id, username, activity, category, amount, status, reference FROM ledger WHERE (? = '' OR category = ?) AND (? = '' OR instr(lower(username), lower(?)) > 0 OR instr(lower(user_id), lower(?)) > 0) ORDER BY occurred_at DESC, event_id DESC LIMIT ? OFFSET ?"
  ].join("\n")).bind(category, category, user, user, user, limit + 1, offset).all();
  const rows = result.results || [];
  const hasMore = rows.length > limit;
  const entries = rows.slice(0, limit);
  return Response.json({ entries, hasMore, nextOffset: offset + entries.length });
}

async function adminUpdateUserProfile(request, env, userId) {
  const actor = await privilegedAdminActor(request, env);
  if (!actor) return Response.json({ error: "Permessi admin richiesti." }, { status: 403 });
  const body = await request.json();
  const username = String(body.username || '').trim();
  const identifier = String(body.identifier || '').trim().toLowerCase();
  if (!/^[a-zA-Z0-9_.-]{3,30}$/.test(username)) return bad("Username non valido: usa 3-30 caratteri alfanumerici, punto, trattino o underscore.");
  if (identifier.length > 160 || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(identifier)) return bad("Email non valida.");
  const target = await env.DB.prepare("SELECT id, identifier FROM users WHERE id = ?").bind(userId).first();
  if (!target) return Response.json({ error: "Utente non trovato." }, { status: 404 });
  const duplicate = await env.DB.prepare("SELECT id FROM users WHERE (lower(username) = ? OR lower(identifier) = ?) AND id != ?").bind(username.toLowerCase(), identifier, userId).first();
  if (duplicate) return bad("Username o email già associati a un altro account.");
  const primary = await env.DB.prepare("SELECT updated_by FROM admin_credentials WHERE id = 1").first();
  if (primary?.updated_by === userId && identifier !== String(target.identifier).toLowerCase()) return bad("L'email dell'account amministratore principale non è modificabile.");
  await env.DB.prepare("UPDATE users SET username = ?, identifier = ?, email_verified = CASE WHEN lower(identifier) != ? THEN 0 ELSE email_verified END WHERE id = ?")
    .bind(username, identifier, identifier, userId).run();
  return Response.json({ ok: true, emailVerified: identifier === String(target.identifier).toLowerCase() });
}

async function adminOperatorAudit(request, env) {
  const actor = await privilegedAdminActor(request, env);
  if (!actor) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  const operators = (await env.DB.prepare(
    "SELECT u.id, u.username, u.identifier, r.granted_at FROM admin_roles r JOIN users u ON u.id = r.user_id WHERE r.role = 'operator' ORDER BY lower(u.username)"
  ).all()).results;
  const activity = (await env.DB.prepare(
    "SELECT id, operator_id, operator_username, request_kind, request_id, user_id, username, action, request_details, created_at FROM operator_review_audit ORDER BY created_at DESC, id DESC LIMIT 1000"
  ).all()).results;
  const feedback = (await env.DB.prepare(
    "SELECT thread_id, user_id, username, operator_id, operator_username, rating, comment, created_at FROM support_feedback ORDER BY created_at DESC LIMIT 1000"
  ).all()).results;
  return Response.json({ operators, activity, feedback });
}

async function adminSetUserRole(request, env) {
  const actor = await privilegedAdminActor(request, env);
  if (!actor) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  const body = await request.json(), userId = String(body.userId || ""), role = String(body.role || "user");
  if (!userId || !["user", "operator"].includes(role)) return bad("Ruolo non valido.");
  const requestedPermissions = {};
  if (role === "operator") for (const key of ADMIN_PERMISSION_KEYS) requestedPermissions[key] = body.permissions?.[key] === true;
  if (requestedPermissions.fullPanel) for (const key of ADMIN_PERMISSION_KEYS.filter(key => key !== "allAdmin")) requestedPermissions[key] = true;
  if (requestedPermissions.allAdmin) for (const key of ADMIN_PERMISSION_KEYS) requestedPermissions[key] = true;
  const target = await env.DB.prepare("SELECT id FROM users WHERE id = ?").bind(userId).first();
  if (!target) return Response.json({ error: "Utente non trovato." }, { status: 404 });
  if (role === 'user') await env.DB.prepare("DELETE FROM admin_roles WHERE user_id = ?").bind(userId).run();
  else await env.DB.prepare("INSERT INTO admin_roles (user_id, role, permissions_json, granted_by, granted_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET role=excluded.role, permissions_json=excluded.permissions_json, granted_by=excluded.granted_by, granted_at=excluded.granted_at").bind(userId, role, JSON.stringify(requestedPermissions), actor.id, Date.now()).run();
  return Response.json({ ok: true, role, permissions: requestedPermissions });
}

async function adminStatistics(request, env) {
  const actor = await adminAccess(request, env, "fullPanel");
  if (!actor) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  const totals = await env.DB.prepare("SELECT (SELECT COUNT(*) FROM users) AS users, (SELECT COUNT(*) FROM deposits) AS deposit_count, (SELECT COUNT(*) FROM deposits WHERE status = 'approved') AS deposits_approved, (SELECT COUNT(*) FROM deposits WHERE status = 'pending') AS deposits_pending, (SELECT COALESCE(SUM(amount),0) FROM deposits WHERE status = 'approved') AS deposits_amount, (SELECT COALESCE(SUM(amount),0) FROM deposits WHERE status = 'pending') AS deposits_pending_amount, (SELECT COUNT(*) FROM withdrawals) AS withdrawal_count, (SELECT COUNT(*) FROM withdrawals WHERE status = 'approved') AS withdrawals_approved, (SELECT COUNT(*) FROM withdrawals WHERE status = 'pending') AS withdrawals_pending, (SELECT COALESCE(SUM(amount),0) FROM withdrawals WHERE status = 'approved') AS withdrawals_amount, (SELECT COALESCE(SUM(amount),0) FROM withdrawals WHERE status = 'pending') AS withdrawals_pending_amount, (SELECT COUNT(*) FROM investments)+(SELECT COUNT(*) FROM market_positions) AS investment_count, (SELECT COALESCE(SUM(amount),0) FROM investments)+(SELECT COALESCE(SUM(amount),0) FROM market_positions) AS investment_amount, (SELECT COUNT(*) FROM investments WHERE active=1)+(SELECT COUNT(*) FROM market_positions WHERE status IN ('active','paused')) AS active_investments, (SELECT COALESCE(SUM(amount),0) FROM investments WHERE active=1)+(SELECT COALESCE(SUM(amount),0) FROM market_positions WHERE status IN ('active','paused')) AS active_investment_amount").first();
  const referrals = (await env.DB.prepare("WITH RECURSIVE referral_tree(inviter_id,user_id,level) AS (SELECT referred_by,id,1 FROM users WHERE referred_by IS NOT NULL AND referred_by!='' UNION ALL SELECT tree.inviter_id,child.id,tree.level+1 FROM referral_tree tree JOIN users child ON child.referred_by=tree.user_id WHERE tree.level<3) SELECT tree.inviter_id,inviter.username AS inviter_username,tree.user_id,invited.username,invited.created_at,tree.level,(SELECT COUNT(*) FROM deposits d WHERE d.user_id=tree.user_id) AS deposit_count,(SELECT COUNT(*) FROM deposits d WHERE d.user_id=tree.user_id AND d.status='approved') AS deposits_approved,(SELECT COALESCE(SUM(d.amount),0) FROM deposits d WHERE d.user_id=tree.user_id AND d.status='approved') AS deposit_amount,(SELECT COUNT(*) FROM withdrawals w WHERE w.user_id=tree.user_id) AS withdrawal_count,(SELECT COUNT(*) FROM withdrawals w WHERE w.user_id=tree.user_id AND w.status='approved') AS withdrawals_approved,(SELECT COALESCE(SUM(w.amount),0) FROM withdrawals w WHERE w.user_id=tree.user_id AND w.status='approved') AS withdrawal_amount,(SELECT COUNT(*) FROM investments i WHERE i.user_id=tree.user_id)+(SELECT COUNT(*) FROM market_positions m WHERE m.user_id=tree.user_id) AS investment_count,(SELECT COALESCE(SUM(i.amount),0) FROM investments i WHERE i.user_id=tree.user_id)+(SELECT COALESCE(SUM(m.amount),0) FROM market_positions m WHERE m.user_id=tree.user_id) AS investment_amount FROM referral_tree tree JOIN users inviter ON inviter.id=tree.inviter_id JOIN users invited ON invited.id=tree.user_id ORDER BY inviter.username COLLATE NOCASE,tree.level,invited.created_at DESC").all()).results;
  return Response.json({ totals, referrals });
}

async function adminUsers(request, env) {
  const user = await adminAccess(request, env, "users");
  if (!user) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  const users = (await env.DB.prepare("SELECT u.id, u.username, u.identifier, u.created_at, u.email_verified, COALESCE(r.role, 'user') AS role FROM users u LEFT JOIN admin_roles r ON r.user_id = u.id ORDER BY u.created_at DESC").all()).results;
  return Response.json({ users });
}

async function adminUserDetails(request, env, userId) {
  const actor = await adminAccess(request, env, "users");
  if (!actor) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  const target = await env.DB.prepare("SELECT u.id, u.username, u.identifier, u.created_at, u.referred_by, u.email_verified, COALESCE(r.role, 'user') AS role, COALESCE(r.permissions_json, '{}') AS permissions_json FROM users u LEFT JOIN admin_roles r ON r.user_id = u.id WHERE u.id = ?").bind(userId).first();
  if (!target) return Response.json({ error: "Utente non trovato." }, { status: 404 });
  const wallet = await accrue(env, target.id);
  const investments = (await env.DB.prepare("SELECT id, amount, days, rate, created, credited, active FROM investments WHERE user_id = ? ORDER BY created DESC").bind(target.id).all()).results;
  const marketPositions = await syncMarketPositions(env, target.id);
  const deposits = (await env.DB.prepare("SELECT id, amount, net, date, status, tx_reference FROM deposits WHERE user_id = ? ORDER BY date DESC LIMIT 100").bind(target.id).all()).results;
  const withdrawals = (await env.DB.prepare("SELECT id, amount, net, date, status, wallet_address, source FROM withdrawals WHERE user_id = ? ORDER BY date DESC LIMIT 100").bind(target.id).all()).results;
  const verificationRequest = await env.DB.prepare("SELECT requested_at, otp_expires_at, verified_at FROM email_verification_requests WHERE user_id = ?").bind(target.id).first();
  const actionVerification = await env.DB.prepare("SELECT action, requested_at, otp_expires_at FROM sensitive_action_verifications WHERE user_id = ? AND consumed_at IS NULL").bind(target.id).first();
  const passwordResetRequest = await env.DB.prepare("SELECT requested_at, otp_expires_at, issued_at FROM password_reset_requests WHERE user_id = ?").bind(target.id).first();
  const settings = await env.DB.prepare("SELECT withdraw_wallets FROM settings WHERE user_id = ?").bind(target.id).first();
  const actorRole = isPrimaryAdmin(actor) || (await readAdminPermissions(actor, env)).allAdmin ? "admin" : "operator";
  const targetPermissions = (() => { try { return JSON.parse(target.permissions_json || "{}"); } catch { return {}; } })();
  const invitedCount = await env.DB.prepare("SELECT COUNT(*) AS count FROM users WHERE referred_by = ?").bind(target.id).first();
  return Response.json({ user: target, wallet, investments, marketPositions, deposits, withdrawals, settings, verificationRequest, actionVerification, passwordResetRequest, invitedCount: Number(invitedCount?.count || 0), actorRole, permissions: targetPermissions });
}

async function adminUpdateUserBalance(request, env, userId) {
  const actor = await privilegedAdminActor(request, env);
  if (!actor) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  const body = await request.json();
  const deposit = Number(body.deposit), earnings = Number(body.earnings), released = Number(body.released || 0);
  if (![deposit, earnings, released].every(n => Number.isFinite(n) && n >= 0 && n <= 1e12)) return bad("Saldo non valido.");
  const now = Date.now();
  await env.DB.batch([
    env.DB.prepare("INSERT INTO wallets (user_id, deposit, earnings, released) VALUES (?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET deposit=excluded.deposit, earnings=excluded.earnings, released=excluded.released").bind(userId, deposit, earnings, released),
    env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)").bind(userId, "Wallet balance updated", "Your available balance has been updated by the administrator.", now)
  ]);
  return Response.json({ ok: true });
}

async function adminDeleteUser(request, env, userId) {
  const actor = await privilegedAdminActor(request, env);
  if (!actor) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  
  const target = await env.DB.prepare("SELECT id, identifier FROM users WHERE id = ?").bind(userId).first();
  if (!target) return Response.json({ error: "Utente non trovato." }, { status: 404 });
  if (target.identifier.toLowerCase() === "supercloud3.invest@yahoo.com".toLowerCase()) {
    return bad("Non è possibile eliminare l'account amministratore principale.");
  }

  await env.DB.batch([
    env.DB.prepare("DELETE FROM sessions WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM admin_sessions WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM admin_roles WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM market_positions WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM email_verification_requests WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM password_reset_requests WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM sensitive_action_verifications WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM notifications WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM wallets WHERE user_id = ?").bind(userId),
    env.DB.prepare("DELETE FROM investments WHERE user_id = ?").bind(userId),
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

async function markWithdrawalSent(request, env) {
  const actor = await privilegedAdminActor(request, env);
  if (!actor) return Response.json({ error: "Permessi admin richiesti." }, { status: 403 });
  const body = await request.json();
  const id = Number(body.id);
  const txReference = String(body.txReference || "").trim();
  if (!Number.isInteger(id) || id < 1) return bad("Richiesta di prelievo non valida.");
  if (!/^(?:0x)?[a-fA-F0-9]{64}$/.test(txReference)) return bad("Inserisci un hash di transazione valido dopo aver inviato il pagamento.");
  const withdrawal = await env.DB.prepare("SELECT id, status FROM withdrawals WHERE id = ?").bind(id).first();
  if (!withdrawal) return bad("Prelievo non trovato.");
  if (withdrawal.status !== "pending") return bad("Il prelievo è già stato esaminato.");
  const result = await env.DB.prepare("UPDATE withdrawals SET tx_reference = ? WHERE id = ? AND status = 'pending'").bind(txReference, id).run();
  if (!result.meta?.changes) return bad("Il prelievo non è più in attesa.");
  return Response.json({ ok: true, status: "pending", txReference });
}

async function reviewTransaction(request, env) {
  const operator = await adminActor(request, env);
  if (!operator) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  const body = await request.json();
  const kind = body.kind;
  const requiredPermission = kind === "wallet_change" ? "walletChanges" : "requests";
  if (!(await readAdminPermissions(operator, env))[requiredPermission]) return Response.json({ error: "Accesso non autorizzato." }, { status: 403 });
  const id = Number(body.id);
  let action = body.action;
  
  let userId = null;
  let amount = 0;
  let net = "";
  let transactionReference = "";

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
          const commAmount = Number((amount * pct).toFixed(3));
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
            ).bind(uplineUser.id, "Referral commission", `Credited ${commAmount.toFixed(3)} USDT referral commission (Level ${level + 1}) on user's first deposit.`, now).run();
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
    if (wit.status !== 'pending') return bad("Questa richiesta è già stata esaminata.");
    userId = wit.user_id;
    amount = wit.amount;
    net = wit.net;

    if (action === "approve") {
      const txReference = String(wit.tx_reference || '').trim();
      const submittedReference = String(body.txReference || '').trim();
      if (!/^(?:0x)?[a-fA-F0-9]{64}$/.test(txReference)) return bad("L’amministratore deve prima registrare l’hash del pagamento.");
      if (submittedReference.toLowerCase() !== txReference.toLowerCase()) return bad("L’hash verificato non corrisponde a quello registrato dall’amministratore.");
      transactionReference = txReference;
      const reviewed = await env.DB.prepare("UPDATE withdrawals SET status = 'approved' WHERE id = ? AND status = 'pending' AND tx_reference = ?").bind(id, txReference).run();
      if (!reviewed.meta?.changes) return bad("Il prelievo non è più in attesa o l’hash è cambiato.");
      await env.DB.prepare("UPDATE wallets SET total_withdrawn = total_withdrawn + ? WHERE user_id = ?").bind(amount, userId).run();
    } else {
      await env.DB.prepare("UPDATE withdrawals SET status = 'rejected' WHERE id = ?").bind(id).run();
      const refundSource = wit.source === 'released' ? 'released' : 'earnings';
      const refundAmount = Number(wit.charged_amount) > 0 ? Number(wit.charged_amount) : amount;
      await env.DB.prepare("UPDATE wallets SET " + refundSource + " = " + refundSource + " + ? WHERE user_id = ?").bind(refundAmount, userId).run();
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
        await env.DB.batch([
          env.DB.prepare("DELETE FROM wallet_change_requests WHERE id = ?").bind(id),
          env.DB.prepare("DELETE FROM sensitive_action_verifications WHERE user_id = ? AND action = 'wallet-change' AND consumed_at IS NULL").bind(userId)
        ]);
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
            .bind(userId, net, newAddress, Date.now()),
          env.DB.prepare("DELETE FROM wallet_change_requests WHERE id = ?").bind(id),
          env.DB.prepare("DELETE FROM sensitive_action_verifications WHERE user_id = ? AND action = 'wallet-change' AND consumed_at IS NULL").bind(userId)
        ]);
      }
    } else {
      await env.DB.batch([
        env.DB.prepare("DELETE FROM wallet_change_requests WHERE id = ?").bind(id),
        env.DB.prepare("DELETE FROM sensitive_action_verifications WHERE user_id = ? AND action = 'wallet-change' AND consumed_at IS NULL").bind(userId)
      ]);
    }
  } else {
    return bad("Tipo di transazione non valido.");
  }

  const actorRole = await env.DB.prepare("SELECT role FROM admin_roles WHERE user_id = ?").bind(operator.id).first();
  if (actorRole?.role === "operator" && (action === "approve" || action === "reject")) {
    const reviewedUser = await env.DB.prepare("SELECT username FROM users WHERE id = ?").bind(userId).first();
    const requestDetails = JSON.stringify({
      amount: Number(amount) || 0,
      network: String(net || ""),
      transactionReference: kind === "withdrawal" && action === "approve" ? transactionReference : ""
    });
    await env.DB.prepare("INSERT INTO operator_review_audit (operator_id, operator_username, request_kind, request_id, user_id, username, action, request_details, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
      .bind(operator.id, String(operator.username || "Operator"), kind, id, String(userId || ""), String(reviewedUser?.username || "user"), action, requestDetails, Date.now()).run();
  }

  if (userId) {
    const now = Date.now();
    const statusLabel = action === "approve" ? "approved" : "rejected";
    const transactionLabel = kind === "deposit" ? "Deposit" : kind === "withdrawal" ? "Withdrawal" : "Wallet change request";
    let message = "";
    if (kind === "deposit") {
      message = `${amount.toFixed(3)} USDT · ${net} · Request ${statusLabel}${action === "approve" ? ". Amount successfully credited to wallet." : ". Amount was not credited."}`;
    } else if (kind === "withdrawal") {
      message = `${amount.toFixed(3)} USDT · ${net} · Request ${statusLabel}${action === "reject" ? ". Amount refunded to wallet successfully." : ". Operation completed. TX: " + transactionReference}`;
    } else {
      message = action === "approve"
        ? `Network ${net} · Address change request successfully approved.`
        : `Network ${net} · Address change request rejected: the new address is already used by another account.`;
    }
    
    await env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'transaction', ?, ?, ?)")
      .bind(userId, `${transactionLabel} ${statusLabel}`, message, now).run();
  }

  return Response.json({ ok: true });
}

async function setAdminCredential(request, env) {
  return Response.json({ ok: true });
}

const TOTP_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
function base32Encode(bytes) {
  let bits = 0, value = 0, result = '';
  for (const byte of bytes) { value = (value << 8) | byte; bits += 8; while (bits >= 5) { result += TOTP_ALPHABET[(value >>> (bits - 5)) & 31]; bits -= 5; } }
  if (bits) result += TOTP_ALPHABET[(value << (5 - bits)) & 31];
  return result;
}
function base32Decode(text) {
  let bits = 0, value = 0; const output = [];
  for (const char of String(text).replace(/=+$/,'').toUpperCase()) {
    const digit = TOTP_ALPHABET.indexOf(char); if (digit < 0) throw new Error('Invalid authenticator secret');
    value = (value << 5) | digit; bits += 5;
    if (bits >= 8) { bits -= 8; output.push((value >>> bits) & 255); }
  }
  return new Uint8Array(output);
}
async function totpCode(secret, timeOffset = 0) {
  const counter = BigInt(Math.floor(Date.now() / 30000) + timeOffset);
  const message = new Uint8Array(8); let n = counter;
  for (let i = 7; i >= 0; i--) { message[i] = Number(n & 255n); n >>= 8n; }
  const key = await crypto.subtle.importKey('raw', base32Decode(secret), { name: 'HMAC', hash: 'SHA-1' }, false, ['sign']);
  const digest = new Uint8Array(await crypto.subtle.sign('HMAC', key, message));
  const offset = digest[digest.length - 1] & 15;
  const number = (digest[offset] & 127) * 0x1000000 + digest[offset + 1] * 0x10000 + digest[offset + 2] * 0x100 + digest[offset + 3];
  return String(number % 1000000).padStart(6, '0');
}
async function validTotp(secret, code) {
  if (!/^\d{6}$/.test(code)) return false;
  for (const offset of [-1, 0, 1]) if (await totpCode(secret, offset) === code) return true;
  return false;
}
async function beginAdminAuthenticator(env, userId) {
  let record = await env.DB.prepare("SELECT secret, enabled FROM admin_authenticator WHERE user_id = ?").bind(userId).first();
  if (!record) {
    const bytes = new Uint8Array(20); crypto.getRandomValues(bytes);
    const secret = base32Encode(bytes);
    await env.DB.prepare("INSERT INTO admin_authenticator (user_id, secret, enabled, updated_at) VALUES (?, ?, 0, ?)").bind(userId, secret, Date.now()).run();
    record = { secret, enabled: 0 };
  }
  const challenge = makeToken();
  await env.DB.prepare("INSERT INTO admin_mfa_challenges (challenge, user_id, expires_at, attempts) VALUES (?, ?, ?, 0)").bind(challenge, userId, Date.now() + 5 * 60 * 1000).run();
  const user = await env.DB.prepare("SELECT identifier FROM users WHERE id = ?").bind(userId).first();
  const setup = Number(record.enabled) !== 1;
  const label = encodeURIComponent('You Invest:' + (user?.identifier || 'admin'));
  const issuer = encodeURIComponent('You Invest');
  const otpauthUri = 'otpauth://totp/' + label + '?secret=' + record.secret + '&issuer=' + issuer + '&algorithm=SHA1&digits=6&period=30';
  return Response.json({ ok: true, mode: 'operator', mfaRequired: true, challenge, setup, secret: setup ? record.secret : '', account: user?.identifier || 'Administrator account', otpauthUri: setup ? otpauthUri : '' });
}
async function verifyAdminAuthenticator(request, env) {
  const body = await request.json();
  const challenge = String(body.challenge || '').trim();
  const code = String(body.code || '').trim();
  const row = await env.DB.prepare("SELECT c.user_id, c.expires_at, c.attempts, a.secret FROM admin_mfa_challenges c JOIN admin_authenticator a ON a.user_id = c.user_id WHERE c.challenge = ?").bind(challenge).first();
  if (!row || Number(row.expires_at) <= Date.now() || Number(row.attempts) >= 5) return Response.json({ error: 'Accesso scaduto. Accedi di nuovo.' }, { status: 401 });
  if (!await validTotp(row.secret, code)) {
    const attempts = Number(row.attempts) + 1;
    if (attempts >= 5) await env.DB.prepare("DELETE FROM admin_mfa_challenges WHERE challenge = ?").bind(challenge).run();
    else await env.DB.prepare("UPDATE admin_mfa_challenges SET attempts = ? WHERE challenge = ?").bind(attempts, challenge).run();
    return Response.json({ error: attempts >= 5 ? 'Troppi tentativi. Accedi di nuovo.' : 'Codice non valido.' }, { status: 401 });
  }
  const now = Date.now();
  await env.DB.batch([
    env.DB.prepare("UPDATE admin_authenticator SET enabled = 1, updated_at = ? WHERE user_id = ?").bind(now, row.user_id),
    env.DB.prepare("DELETE FROM admin_mfa_challenges WHERE user_id = ?").bind(row.user_id)
  ]);
  const token = makeToken();
  await env.DB.prepare("INSERT INTO admin_sessions (token, user_id, expires_at) VALUES (?, ?, ?)").bind(token, row.user_id, now + 30 * DAY).run();
  return Response.json({ ok: true, mode: 'operator', token });
}

async function tryOperatorLogin(env, email, password) {
  const credential = await env.DB.prepare("SELECT email, salt, password_hash, iterations FROM admin_credentials WHERE id = 1").first();
  if (email !== PRIMARY_ADMIN_EMAIL.toLowerCase() || credential?.email?.toLowerCase() !== PRIMARY_ADMIN_EMAIL.toLowerCase()) return null;
  const suppliedHash = await hashAdminSecret(password, credential.salt, credential.iterations || 20000);
  if (suppliedHash !== credential.password_hash) return null;
  return beginAdminAuthenticator(env, PRIMARY_ADMIN_ID);
}

async function adminLogout(request, env) {
  const token = (request.headers.get("x-admin-token") || "").trim();
  if (token) await env.DB.prepare("DELETE FROM admin_sessions WHERE token = ?").bind(token).run();
  return Response.json({ ok: true });
}

async function saveWallet(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  return await createWalletChangeOtpRequest(env, user, body.wallets || {});
}

async function requestWalletChange(request, env) {
  const user = await authed(request, env);
  const body = await request.json();
  const wallets = { ...(body.wallets || {}) };
  if (body.net && body.newAddress) wallets[body.net] = body.newAddress;
  return await createWalletChangeOtpRequest(env, user, wallets);
}

async function createWalletChangeOtpRequest(env, user, wallets) {
  const allowedNetworks = ["BNB", "TRC20", "BTC", "USDC"];
  const requestedWallets = {};
  if (!wallets || typeof wallets !== "object" || Array.isArray(wallets)) return bad("Indirizzi wallet non validi.");
  for (const [net, addr] of Object.entries(wallets)) {
    const value = String(addr || "").trim();
    if (!allowedNetworks.includes(net) || !value || value.length > 256) continue;
    requestedWallets[net] = value;
  }
  if (!Object.keys(requestedWallets).length) return bad("Nessuna modifica o nuovo indirizzo valido specificato.");
  const existingAddrs = await env.DB.prepare("SELECT COUNT(*) as cnt FROM user_withdrawal_addresses WHERE user_id = ?").bind(user.id).first();
  if ((!existingAddrs || existingAddrs.cnt === 0) && !user.email_verified) return Response.json({ error: "Verifica l’email prima di salvare l’indirizzo di prelievo per la prima volta.", verificationRequired: true, email: user.identifier }, { status: 403 });
  const currentSettings = await env.DB.prepare("SELECT withdraw_wallets FROM settings WHERE user_id = ?").bind(user.id).first();
  let currentWallets = {};
  try { currentWallets = JSON.parse(currentSettings?.withdraw_wallets || "{}"); } catch {}
  const changes = Object.entries(requestedWallets).filter(([net, address]) => String(currentWallets[net] || "").toLowerCase() !== address.toLowerCase()).map(([net, newAddress]) => ({ net, oldAddress: currentWallets[net] || "", newAddress }));
  if (!changes.length) return bad("Nessuna modifica wallet da applicare.");
  const openWalletRequest = await env.DB.prepare("SELECT id FROM wallet_change_requests WHERE user_id = ? AND status = 'pending' LIMIT 1").bind(user.id).first();
  if (openWalletRequest) return Response.json({ error: "È già presente una richiesta di modifica wallet. Completa quella in corso prima di inviarne un'altra." }, { status: 409 });
  const pending = await env.DB.prepare("SELECT action FROM sensitive_action_verifications WHERE user_id = ? AND consumed_at IS NULL").bind(user.id).first();
  if (pending) return Response.json({ error: "È già presente una richiesta wallet in corso. Completa quella in corso prima di inviarne un'altra." }, { status: 409 });
  const now = Date.now();
  const created = await env.DB.prepare("INSERT INTO sensitive_action_verifications (user_id, action, payload_json, requested_at, otp_hash, otp_salt, otp_expires_at, otp_attempts, issued_at, issued_by, consumed_at) SELECT ?, 'wallet-change', ?, ?, NULL, NULL, NULL, 0, NULL, NULL, NULL WHERE NOT EXISTS (SELECT 1 FROM sensitive_action_verifications WHERE user_id = ? AND consumed_at IS NULL) AND NOT EXISTS (SELECT 1 FROM wallet_change_requests WHERE user_id = ? AND status = 'pending') ON CONFLICT(user_id) DO UPDATE SET action = excluded.action, payload_json = excluded.payload_json, requested_at = excluded.requested_at, otp_hash = NULL, otp_salt = NULL, otp_expires_at = NULL, otp_attempts = 0, issued_at = NULL, issued_by = NULL, consumed_at = NULL")
    .bind(user.id, JSON.stringify({ wallets: requestedWallets, changes }), now, user.id, user.id).run();
  if (!(created.meta?.changes > 0)) return Response.json({ error: "È già presente una richiesta wallet in corso. Completa quella in corso prima di inviarne un'altra." }, { status: 409 });
  await env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'account', ?, ?, ?)")
    .bind(user.id, "Wallet change verification", "Wallet change requested. Ask the operator to issue a six-digit verification code.", now).run();
  return Response.json({ ok: true, otpRequired: true, message: "Richiesta OTP wallet inviata all’operatore. Inserisci il codice dopo averlo ricevuto." });
}

async function queueWalletAddressConflict(env, user, net, oldAddress, newAddress) {
  const now = Date.now();
  await env.DB.prepare("INSERT INTO wallet_change_requests (user_id, net, old_address, new_address, created_at, status) SELECT ?, ?, ?, ?, ?, 'pending' WHERE NOT EXISTS (SELECT 1 FROM wallet_change_requests WHERE user_id = ? AND net = ? AND lower(new_address) = lower(?) AND status = 'pending')")
    .bind(user.id, net, oldAddress || "", newAddress, now, user.id, net, newAddress).run();
}

async function applyWalletChange(env, user, inputWallets) {
  const currentSettings = await env.DB.prepare("SELECT withdraw_wallets FROM settings WHERE user_id = ?").bind(user.id).first();
  let currentWallets = {};
  try { currentWallets = JSON.parse(currentSettings?.withdraw_wallets || "{}"); } catch {}
  const allowedNetworks = ["BNB", "TRC20", "BTC", "USDC"];
  const finalWallets = { ...currentWallets };
  const now = Date.now();
  const batchStatements = [];
  for (const [net, addr] of Object.entries(inputWallets || {})) {
    const trimmedAddr = String(addr || "").trim();
    if (!trimmedAddr || trimmedAddr.length > 256 || !allowedNetworks.includes(net)) continue;
    const oldAddr = currentWallets[net] || "";
    if (!oldAddr) {
      const existingGlobal = await env.DB.prepare("SELECT user_id FROM user_withdrawal_addresses WHERE net = ? AND lower(address) = ? AND user_id != ?").bind(net, trimmedAddr.toLowerCase(), user.id).first();
      if (existingGlobal) { await queueWalletAddressConflict(env, user, net, oldAddr, trimmedAddr); return bad("Questo indirizzo per la rete " + net + " è già registrato su un altro account."); }
      finalWallets[net] = trimmedAddr;
      batchStatements.push(env.DB.prepare("INSERT INTO user_withdrawal_addresses (user_id, net, address, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(user_id, net, address) DO NOTHING").bind(user.id, net, trimmedAddr, now));
    } else if (oldAddr.toLowerCase() !== trimmedAddr.toLowerCase()) {
      const existingGlobal = await env.DB.prepare("SELECT user_id FROM user_withdrawal_addresses WHERE net = ? AND lower(address) = ? AND user_id != ?").bind(net, trimmedAddr.toLowerCase(), user.id).first();
      if (existingGlobal) { await queueWalletAddressConflict(env, user, net, oldAddr, trimmedAddr); return bad("Il nuovo indirizzo per la rete " + net + " è già registrato su un altro account."); }
      finalWallets[net] = trimmedAddr;
      batchStatements.push(env.DB.prepare("INSERT INTO user_withdrawal_addresses (user_id, net, address, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(user_id, net, address) DO NOTHING").bind(user.id, net, trimmedAddr, now));
      batchStatements.push(env.DB.prepare("INSERT INTO notifications (user_id, kind, title, message, created_at) VALUES (?, 'account', ?, ?, ?)").bind(user.id, "Wallet address updated", "Your wallet address for " + net + " was updated after OTP verification.", now));
    }
  }
  if (batchStatements.length === 0) return bad("Nessuna modifica o nuovo indirizzo valido specificato.");
  batchStatements.unshift(env.DB.prepare("INSERT INTO settings (user_id, withdraw_wallets, notifications) VALUES (?, ?, 1) ON CONFLICT(user_id) DO UPDATE SET withdraw_wallets = excluded.withdraw_wallets").bind(user.id, JSON.stringify(finalWallets)));
  await env.DB.batch(batchStatements);
  return Response.json({ ok: true, message: "Wallet aggiornato dopo verifica OTP.", acc: { withdraw_wallets: finalWallets, notifications: true } });
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
      .bind(user.id, "Password updated", "Your account password has been successfully modified.", Date.now()),
  ]);
  return Response.json({ ok: true });
}

app.get('/api/notifications/stream', async (req, res) => {
  try {
    const authRequest = new Request(`http://${req.get('host') || 'localhost'}${req.originalUrl}`, { headers: new Headers(req.headers) });
    const user = await sessionUser(authRequest, env);
    if (!user) return res.status(401).json({ error: 'Non autenticato.' });
    res.status(200);
    res.set({ 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache, no-transform', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
    res.flushHeaders();
    const userId = String(user.id);
    let clients = notificationStreams.get(userId);
    if (!clients) notificationStreams.set(userId, clients = new Set());
    clients.add(res);
    res.write('event: ready\ndata: {}\n\n');
    const heartbeat = setInterval(() => { if (!res.writableEnded) res.write(': keep-alive\n\n'); }, 20000);
    res.on('close', () => {
      clearInterval(heartbeat);
      clients.delete(res);
      if (!clients.size) notificationStreams.delete(userId);
    });
  } catch (error) {
    console.error('Notification stream error:', error);
    if (!res.headersSent) res.status(500).json({ error: 'Impossibile aprire il canale notifiche.' });
    else res.end();
  }
});

app.get('/api/asset-prices/stream', (req, res) => {
  res.status(200);
  res.set({ 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache, no-transform', Connection: 'keep-alive', 'X-Accel-Buffering': 'no' });
  res.flushHeaders();
  assetPriceStreams.add(res);
  res.write(['event: ready', 'data: {}', '', ''].join(String.fromCharCode(10)));
  if (!assetPriceStreamTimer) assetPriceStreamTimer = setInterval(() => { void publishAssetPriceUpdate(); }, 3000);
  const heartbeat = setInterval(() => { if (!res.writableEnded) res.write(':' + ' keep-alive' + String.fromCharCode(10) + String.fromCharCode(10)); }, 20000);
  res.on('close', () => {
    clearInterval(heartbeat);
    assetPriceStreams.delete(res);
    if (!assetPriceStreams.size && assetPriceStreamTimer) {
      clearInterval(assetPriceStreamTimer);
      assetPriceStreamTimer = null;
    }
  });
  void publishAssetPriceUpdate();
});

app.all('/api/*', async (req, res) => {
  try {
    if (req.method === 'POST' && req.path === '/api/logout') {
      const sessionToken = String(req.get('x-session-token') || req.body?.sessionToken || '').trim();
      const adminToken = String(req.get('x-admin-token') || req.body?.adminToken || '').trim();
      if (sessionToken) await env.DB.prepare("DELETE FROM sessions WHERE token = ?").bind(sessionToken).run();
      if (adminToken) await env.DB.prepare("DELETE FROM admin_sessions WHERE token = ?").bind(adminToken).run();
      return res.json({ ok: true });
    }

    const fullUrl = `${req.protocol}://${req.get('host')}${req.originalUrl}`;
    const forwardedHeaders = new Headers(req.headers); forwardedHeaders.delete('x-platform-client-ip');
    if (req.ip) forwardedHeaders.set('x-platform-client-ip', String(req.ip));
    const webRequest = new Request(fullUrl, {
      method: req.method,
      headers: forwardedHeaders,
      body: ['GET', 'HEAD'].includes(req.method) ? undefined : JSON.stringify(req.body)
    });

    const response = await workerHandler.fetch(webRequest, env);
    const bodyText = await response.text();
    
    res.status(response.status);
    response.headers.forEach((val, key) => res.setHeader(key, val));
    res.send(bodyText);
  } catch (err) {
    const databaseCode = err?.code || err?.cause?.code || err?.cause?.proto?.code;
    if (databaseCode === 'BLOCKED') {
      console.error('Turso database reads are blocked by the current database plan:', err);
      return res.status(503).json({ error: "Il database Turso sta bloccando le letture SQL. Riattiva le letture o aggiorna il piano Turso; il login resterà indisponibile finché il database non le consente." });
    }
    console.error('API Error:', err);
    res.status(500).json({ error: 'Errore interno del server.' });
  }
});

app.listen(PORT, () => {
  console.log(`Server avviato correttamente sulla porta ${PORT}`);
});

const PRIMARY_ADMIN_EMAIL = String(process.env.ADMIN_EMAIL || "").trim().toLowerCase();
const PRIMARY_ADMIN_ID = "__primary_admin__";
const PRIMARY_ADMIN_PASSWORD = String(process.env.ADMIN_PASSWORD || "");
if (!PRIMARY_ADMIN_EMAIL || !PRIMARY_ADMIN_PASSWORD) {
  console.warn("Admin authentication is disabled until ADMIN_EMAIL and ADMIN_PASSWORD are configured in Render.");
}
DEFAULT_DEPOSIT_ADDRESSES.BNB = "0x2859d146Dc8e4cB332736986feE9D32B641fbde8";
DEFAULT_DEPOSIT_ADDRESSES.TRC20 = "TYQgWx4eQ6Js94UMexfyLXbqNE4Fucfg7Y";
DEFAULT_DEPOSIT_ADDRESSES.BTC = "bc1par0exs9cyw9w53xsceyh6wzl7f43gdjn6xsq0kyq4qsqsvr2uynqf5llc6";
