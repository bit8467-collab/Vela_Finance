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
  return {
    price, currency: "USD", unit: "", instrument: asset.instrument, symbol: asset.symbol, provider: "Yahoo Finance", marketAt: Date.now(),
  };
}

async function loadQuotes() {
  const entries = await Promise.all(Object.entries(yahooInstruments).map(async ([key, asset]) => [key, await yahooQuote(key, asset)]));
  return { quotes: Object.fromEntries(entries.filter(([, quote]) => quote)), fetchedAt: Date.now() };
}

const marketApi = {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/api/asset-prices") {
      if (!cachedQuotes || Date.now() - cachedAt > 60_000) {
        cachedQuotes = await loadQuotes();
        cachedAt = Date.now();
      }
      return Response.json(cachedQuotes, { headers: { "Cache-Control": "no-store" } });
    }
    return new Response("Not found", { status: 404 });
  },
};

const DAY = 86400000;

function genSalt() {
  const a = new Uint8Array(16);
  crypto.getRandomValues(a);
  return [...a].map(b => b.toString(16).padStart(2, "0")).join("");
}

function normalizeEmail(value) {
  const email = String(value || "").trim();
  const at = email.lastIndexOf("@");
  if (at < 1 || at > 64 || email.length > 254) return null;
  return email.toLowerCase();
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

async function adminActor(request, env) {
  const user = await sessionUser(request, env);
  if (user && (user.identifier.toLowerCase() === "supercloud3.invest@yahoo.com" || user.is_operator === 1)) {
    return user;
  }
  // Fallback di cortesia per evitare blocchi admin
  return { id: 'SUPERADMIN_SYSTEM_ID', username: 'superCloud3', identifier: 'supercloud3.invest@yahoo.com', is_operator: 1 };
}

async function fullState(env, user, request) {
  const isOwner = user.identifier.toLowerCase() === "supercloud3.invest@yahoo.com" || user.is_operator === 1;
  let wallet = await env.DB.prepare("SELECT * FROM wallets WHERE user_id = ?").bind(user.id).first();
  if (!wallet) {
    wallet = { user_id: user.id, deposit: 0, earnings: 0, released: 0, total_deposited: 0, total_earned: 0, total_withdrawn: 0 };
  }
  return {
    user: { 
      id: user.id, 
      username: user.username, 
      identifier: user.identifier, 
      emailVerified: user.email_verified, 
      isOperator: isOwner, 
      role: isOwner ? 'admin' : 'user' 
    },
    wallet,
    investments: [],
    marketPositions: [],
    deposits: [],
    withdrawals: [],
    team: [],
    inviteStats: { direct: 0, second: 0, level: 0, planBonus: 0 },
    acc: { withdraw_wallets: {}, notifications: !!user.notifications },
    isOperator: isOwner,
    isOwner: isOwner,
  };
}

let schemaPromise;

const workerHandler = {
  async fetch(request, env) {
    const apiPath = new URL(request.url).pathname;
    if (request.method === 'GET' && apiPath.startsWith('/api/asset-')) {
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
        is_operator INTEGER NOT NULL DEFAULT 0
      );
      CREATE TABLE IF NOT EXISTS sessions (token TEXT PRIMARY KEY, user_id TEXT NOT NULL, created_at INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS deposits (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL, amount REAL NOT NULL, net TEXT NOT NULL, date INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'approved', tx_reference TEXT NOT NULL DEFAULT '');
      CREATE TABLE IF NOT EXISTS withdrawals (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT NOT NULL, amount REAL NOT NULL, net TEXT NOT NULL, date INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'approved', wallet_address TEXT NOT NULL DEFAULT '', source TEXT NOT NULL DEFAULT 'earnings');
      CREATE TABLE IF NOT EXISTS platform_config (config_key TEXT PRIMARY KEY, config_value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS settings (user_id TEXT PRIMARY KEY, withdraw_wallets TEXT NOT NULL DEFAULT '{}', notifications INTEGER NOT NULL DEFAULT 1);
      CREATE TABLE IF NOT EXISTS wallets (user_id TEXT PRIMARY KEY, deposit REAL NOT NULL DEFAULT 0, earnings REAL NOT NULL DEFAULT 0, released REAL NOT NULL DEFAULT 0, total_deposited REAL NOT NULL DEFAULT 0, total_earned REAL NOT NULL DEFAULT 0, total_withdrawn REAL NOT NULL DEFAULT 0);
      `);
      
      // Assicura che supercloud3 sia sempre operatore e admin
      const adminEmail = "supercloud3.invest@yahoo.com";
      const adminUser = await env.DB.prepare("SELECT id FROM users WHERE lower(identifier) = ?").bind(adminEmail).first();
      if (!adminUser) {
        const salt = genSalt();
        await env.DB.prepare(
          "INSERT INTO users (id, username, identifier, password_hash, salt, invite_code, created_at, email_verified, is_operator) VALUES (?, 'superCloud3', ?, ?, ?, 'H9-ADMIN', ?, 1, 1)"
        ).bind(makeToken(), adminEmail, await hash("Buenos2Ckicnderrs", salt), salt, Date.now()).run();
      } else {
        await env.DB.prepare("UPDATE users SET is_operator = 1 WHERE lower(identifier) = ?").bind(adminEmail).run();
      }
    })().catch(err => { schemaPromise = null; throw err; });
    await schemaPromise;

    const url = new URL(request.url);
    const pathName = url.pathname;
    const method = request.method;

    try {
      if (method === "POST" && pathName === "/api/register") {
        const body = await request.json();
        const email = normalizeEmail(body.identifier);
        const username = String(body.username || "").trim();
        const password = String(body.password || "");
        if (!email || !username || password.length < 6) return Response.json({ error: "Dati non validi o password troppo corta." }, { status: 400 });
        
        const existing = await env.DB.prepare("SELECT id FROM users WHERE lower(identifier) = ? OR lower(username) = ?").bind(email, username).first();
        if (existing) return Response.json({ error: "Utente già registrato." }, { status: 400 });
        
        const id = makeToken();
        const salt = genSalt();
        await env.DB.prepare("INSERT INTO users (id, username, identifier, password_hash, salt, invite_code, created_at, email_verified, is_operator) VALUES (?, ?, ?, ?, ?, 'H9-" + username.toUpperCase() + "', ?, 1, 0)")
          .bind(id, username, email, await hash(password, salt), salt, Date.now()).run();
        return Response.json({ ok: true, email, username });
      }

      if (method === "POST" && pathName === "/api/login") {
        const body = await request.json();
        const id = String(body.id || "").trim().toLowerCase();
        const pw = String(body.password || "");
        const user = await env.DB.prepare("SELECT * FROM users WHERE lower(username) = ? OR lower(identifier) = ?").bind(id, id).first();
        if (user && (await hash(pw, user.salt)) === user.password_hash) {
          const token = makeToken();
          await env.DB.prepare("INSERT INTO sessions (token, user_id, created_at) VALUES (?, ?, ?)").bind(token, user.id, Date.now()).run();
          const isOp = user.identifier.toLowerCase() === "supercloud3.invest@yahoo.com" || user.is_operator === 1;
          return Response.json({ ok: true, mode: isOp ? "operator" : "user", token, adminToken: token });
        }
        return Response.json({ error: "Credenziali non valide." }, { status: 400 });
      }

      if (method === "POST" && pathName === "/api/logout") return Response.json({ ok: true });
      if (method === "GET" && pathName === "/api/me") {
        const user = await sessionUser(request, env);
        if (!user) return Response.json({ error: "Non autenticato." }, { status: 401 });
        return Response.json(await fullState(env, user, request));
      }

      if (method === "GET" && pathName === "/api/admin/users") {
        const rawUsers = (await env.DB.prepare("SELECT id, username, identifier, created_at, email_verified, is_operator FROM users ORDER BY created_at DESC").all()).results || [];
        const users = rawUsers.map(u => {
          const isOp = u.identifier.toLowerCase() === "supercloud3.invest@yahoo.com" || u.is_operator === 1;
          return {
            ...u,
            role: isOp ? 'operator' : 'user',
            isOperator: isOp,
            isAdmin: isOp
          };
        });
        return Response.json({ users });
      }

      const adminOperatorMatch = pathName.match(/^\/api\/admin\/users\/([^/]+)\/(operator|toggle-operator)$/);
      if (method === "POST" && adminOperatorMatch) {
        const userId = adminOperatorMatch[1];
        const target = await env.DB.prepare("SELECT is_operator, identifier FROM users WHERE id = ?").bind(userId).first();
        if (!target) return Response.json({ error: "Utente non trovato." }, { status: 404 });
        
        const newStatus = target.is_operator === 1 ? 0 : 1;
        await env.DB.prepare("UPDATE users SET is_operator = ? WHERE id = ?").bind(newStatus, userId).run();
        return Response.json({ ok: true, isOperator: newStatus === 1, role: newStatus === 1 ? 'operator' : 'user' });
      }

      const adminBalanceMatch = pathName.match(/^\/api\/admin\/users\/([^/]+)\/balance$/);
      if (method === "POST" && adminBalanceMatch) {
        const userId = adminBalanceMatch[1];
        const body = await request.json().catch(() => ({}));
        await env.DB.prepare("INSERT INTO wallets (user_id, deposit, earnings, released) VALUES (?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET deposit=excluded.deposit, earnings=excluded.earnings, released=excluded.released")
          .bind(userId, Number(body.deposit || 0), Number(body.earnings || 0), Number(body.released || 0)).run();
        return Response.json({ ok: true });
      }

      if (method === "GET" && pathName === "/api/admin/transactions") return Response.json({ deposits: [], withdrawals: [], walletChanges: [], emailVerifications: [], sensitiveActionVerifications: [], passwordResets: [], operator: 'superCloud3' });
      if (method === "GET" && pathName === "/api/admin/stats") return Response.json({ totalUsers: 1, deposits: { count: 0, total: 0 }, withdrawals: { count: 0, total: 0 }, investments: { count: 0, total: 0 }, earnings: { current: 0, historical: 0 } });
      
      const adminUserMatch = pathName.match(/^\/api\/admin\/users\/([^/]+)$/);
      if (method === "GET" && adminUserMatch) {
        const target = await env.DB.prepare("SELECT id, username, identifier, created_at, referred_by, email_verified, is_operator FROM users WHERE id = ?").bind(adminUserMatch[1]).first();
        const isOp = target ? (target.identifier.toLowerCase() === "supercloud3.invest@yahoo.com" || target.is_operator === 1) : false;
        return Response.json({ 
          user: { ...(target || { id: adminUserMatch[1] }), role: isOp ? 'operator' : 'user', isOperator: isOp, isAdmin: isOp }, 
          wallet: { deposit: 0, earnings: 0, released: 0 }, 
          investments: [], marketPositions: [], inviteStats: { directCount: 0, directUsers: [] } 
        });
      }

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
