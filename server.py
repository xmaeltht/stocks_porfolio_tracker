#!/usr/bin/env python3
"""
maelkloud.com — Portfolio Monitor  v3
Multi-user, SQLite-backed, Cloudflare Tunnel ready

Usage:
    python server.py              # dev mode  (opens browser, localhost only)
    python server.py --prod       # prod mode (no browser, listens on 0.0.0.0)
    python server.py --port 9090  # custom port
"""

import json, os, sys, time, threading, webbrowser
import sqlite3, hashlib, secrets
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from http.server import HTTPServer, ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, parse_qs as pqs

# ── Auto-install dependencies ──────────────────────────────────────────────────
def _ensure(pkg, import_as=None):
    try:
        __import__(import_as or pkg)
    except ImportError:
        print(f"📦 Installing {pkg} …")
        os.system(f'"{sys.executable}" -m pip install {pkg} --break-system-packages -q')

_ensure("yfinance")
import yfinance as yf

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, "maelkloud.db")
PORT     = int(next((sys.argv[i+1] for i, a in enumerate(sys.argv) if a == '--port' and i+1 < len(sys.argv)), 8080))
PROD     = '--prod' in sys.argv
VERSION  = "3.1"

MARKET_SYMBOLS = ['^GSPC', '^IXIC', '^DJI', '^RUT', '^VIX']
SECTOR_ETFS = {
    'XLK':'Technology','XLE':'Energy','XLF':'Financials',
    'XLV':'Healthcare','XLI':'Industrials','XLY':'Cons. Discretionary',
    'XLP':'Cons. Staples','XLB':'Materials','XLRE':'Real Estate',
    'XLU':'Utilities','XLC':'Communication',
}
CRYPTO_SYMBOLS = {
    'BTC-USD':'Bitcoin','ETH-USD':'Ethereum','SOL-USD':'Solana',
    'BNB-USD':'BNB','XRP-USD':'XRP','DOGE-USD':'Dogecoin',
}
MOVER_WATCHLIST = [
    'AAPL','MSFT','NVDA','AMZN','META','GOOGL','TSLA','AMD','NFLX','CRM',
    'JPM','BAC','GS','V','MA','PYPL','COIN',
    'XOM','CVX','OXY','LLY','UNH','JNJ','PFE','MRNA','ABBV',
    'BA','CAT','GE','HON','SHOP','UBER','ABNB','INTC','QCOM','AVGO','MU','ARM',
    'PLTR','SNOW','RBLX','SQ','SOFI','HOOD','RIVN','LCID','NIO',
    'SPY','QQQ','IWM','GLD',
]

# ── Cache ──────────────────────────────────────────────────────────────────────
_cache     = {}
CACHE_TTL  = 30
CACHE_MAX  = 600   # max keys before LRU eviction

def _evict_cache():
    """Drop oldest 20% of cache entries when over CACHE_MAX."""
    if len(_cache) > CACHE_MAX:
        sorted_keys = sorted(_cache, key=lambda k: _cache[k]["ts"])
        for k in sorted_keys[:len(_cache) // 5]:
            del _cache[k]

def _from_cache(key, fn, ttl=None):
    now = time.time()
    t = ttl if ttl is not None else CACHE_TTL
    if key in _cache and now - _cache[key]["ts"] < t:
        return _cache[key]["data"]
    data = fn()
    _cache[key] = {"ts": now, "data": data}
    _evict_cache()
    return data

def _yf_retry(fn, max_retries=4, base_delay=10):
    """Call fn() with exponential back-off on Yahoo Finance rate limits."""
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            msg = str(e).lower()
            is_rate = any(k in msg for k in ('429', 'rate limit', 'too many request', 'throttl'))
            if is_rate and attempt < max_retries - 1:
                wait = base_delay * (2 ** attempt)   # 10s, 20s, 40s
                print(f"  ⚠️  Rate limited — waiting {wait}s (retry {attempt+1}/{max_retries-1})…")
                time.sleep(wait)
            else:
                raise
    raise RuntimeError("yfinance: max retries exceeded")


# ── Database ───────────────────────────────────────────────────────────────────
def init_db():
    with sqlite3.connect(DB_PATH) as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                username      TEXT    UNIQUE NOT NULL,
                email         TEXT    DEFAULT '',
                password_hash TEXT    NOT NULL,
                salt          TEXT    NOT NULL,
                created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS portfolios (
                user_id  INTEGER NOT NULL,
                ticker   TEXT    NOT NULL,
                shares   REAL    NOT NULL DEFAULT 0,
                avg_cost REAL    NOT NULL DEFAULT 0,
                PRIMARY KEY (user_id, ticker),
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
            CREATE TABLE IF NOT EXISTS sessions (
                token      TEXT    PRIMARY KEY,
                user_id    INTEGER NOT NULL,
                expires_at DATETIME NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
            CREATE TABLE IF NOT EXISTS journal (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER NOT NULL,
                date       TEXT    NOT NULL,
                ticker     TEXT    NOT NULL DEFAULT '',
                action     TEXT    NOT NULL DEFAULT 'NOTE',
                shares     REAL    DEFAULT 0,
                price      REAL    DEFAULT 0,
                notes      TEXT    DEFAULT '',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
            CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                date        TEXT    NOT NULL,
                total_value REAL    NOT NULL DEFAULT 0,
                total_cost  REAL    NOT NULL DEFAULT 0,
                UNIQUE(user_id, date),
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
            CREATE TABLE IF NOT EXISTS watchlist (
                user_id  INTEGER NOT NULL,
                ticker   TEXT    NOT NULL,
                notes    TEXT    DEFAULT '',
                added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, ticker),
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
            CREATE TABLE IF NOT EXISTS price_targets (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id      INTEGER NOT NULL,
                ticker       TEXT    NOT NULL,
                target_price REAL    NOT NULL,
                direction    TEXT    NOT NULL DEFAULT 'above',
                note         TEXT    DEFAULT '',
                created_at   DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, ticker),
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
        """)

def _hash_pw(password, salt=None):
    if salt is None:
        salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 200_000)
    return dk.hex(), salt

def create_user(username, password, email=''):
    try:
        pw_hash, salt = _hash_pw(password)
        with sqlite3.connect(DB_PATH) as c:
            c.execute(
                "INSERT INTO users (username, email, password_hash, salt) VALUES (?,?,?,?)",
                (username.lower().strip(), email.strip(), pw_hash, salt)
            )
            return c.execute("SELECT last_insert_rowid()").fetchone()[0]
    except sqlite3.IntegrityError:
        return None  # username taken

def authenticate_user(username, password):
    with sqlite3.connect(DB_PATH) as c:
        row = c.execute(
            "SELECT id, password_hash, salt FROM users WHERE username = ?",
            (username.lower().strip(),)
        ).fetchone()
    if not row:
        return None
    uid, pw_hash, salt = row
    check, _ = _hash_pw(password, salt)
    return uid if secrets.compare_digest(check, pw_hash) else None

def create_session(user_id):
    token   = secrets.token_urlsafe(32)
    expires = (datetime.now() + timedelta(days=30)).isoformat()
    with sqlite3.connect(DB_PATH) as c:
        c.execute("INSERT INTO sessions (token, user_id, expires_at) VALUES (?,?,?)",
                  (token, user_id, expires))
    return token

def get_session(token):
    if not token:
        return None
    with sqlite3.connect(DB_PATH) as c:
        row = c.execute("""
            SELECT s.user_id, u.username FROM sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.token = ? AND s.expires_at > datetime('now')
        """, (token,)).fetchone()
    return {"user_id": row[0], "username": row[1]} if row else None

def delete_session(token):
    with sqlite3.connect(DB_PATH) as c:
        c.execute("DELETE FROM sessions WHERE token = ?", (token,))

def db_get_portfolio(user_id):
    with sqlite3.connect(DB_PATH) as c:
        rows = c.execute(
            "SELECT ticker, shares, avg_cost FROM portfolios WHERE user_id = ? ORDER BY ticker",
            (user_id,)
        ).fetchall()
    return [{"ticker": r[0], "shares": r[1], "avg_cost": r[2]} for r in rows]

def db_save_portfolio(user_id, holdings):
    with sqlite3.connect(DB_PATH) as c:
        c.execute("DELETE FROM portfolios WHERE user_id = ?", (user_id,))
        c.executemany(
            "INSERT INTO portfolios (user_id, ticker, shares, avg_cost) VALUES (?,?,?,?)",
            [(user_id, h["ticker"].upper().strip(),
              float(h.get("shares", 0)), float(h.get("avg_cost", 0)))
             for h in holdings if h.get("ticker")]
        )

def snapshot_portfolios():
    """Snapshot every user's portfolio value. Called by background thread."""
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        with sqlite3.connect(DB_PATH) as c:
            users = c.execute("SELECT id FROM users").fetchall()
        for (uid,) in users:
            holdings = db_get_portfolio(uid)
            if not holdings:
                continue
            tickers = [h['ticker'] for h in holdings]
            q = get_quotes(tickers)
            total_value = sum(
                q.get(h['ticker'], {}).get('regularMarketPrice', 0) * h['shares']
                for h in holdings
            )
            total_cost = sum(h['avg_cost'] * h['shares'] for h in holdings)
            with sqlite3.connect(DB_PATH) as c:
                c.execute("""
                    INSERT INTO portfolio_snapshots (user_id, date, total_value, total_cost)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(user_id, date) DO UPDATE SET
                        total_value = excluded.total_value,
                        total_cost  = excluded.total_cost
                """, (uid, today, round(total_value, 2), round(total_cost, 2)))
        print(f"  📸 Portfolio snapshots saved for {len(users)} user(s)")
    except Exception as e:
        print(f"  Snapshot error: {e}")

def get_portfolio_history(user_id, days=90):
    with sqlite3.connect(DB_PATH) as c:
        rows = c.execute("""
            SELECT date, total_value, total_cost FROM portfolio_snapshots
            WHERE user_id = ?
            ORDER BY date ASC
            LIMIT ?
        """, (user_id, days)).fetchall()
    return [{"date": r[0], "value": r[1], "cost": r[2]} for r in rows]

def watchlist_get(user_id):
    with sqlite3.connect(DB_PATH) as c:
        rows = c.execute(
            "SELECT ticker, notes, added_at FROM watchlist WHERE user_id = ? ORDER BY added_at DESC",
            (user_id,)
        ).fetchall()
    return [{"ticker": r[0], "notes": r[1], "added_at": r[2]} for r in rows]

def watchlist_add(user_id, ticker, notes=''):
    try:
        with sqlite3.connect(DB_PATH) as c:
            c.execute(
                "INSERT OR REPLACE INTO watchlist (user_id, ticker, notes) VALUES (?,?,?)",
                (user_id, ticker.upper().strip(), notes.strip())
            )
        return True
    except: return False

def watchlist_remove(user_id, ticker):
    with sqlite3.connect(DB_PATH) as c:
        c.execute("DELETE FROM watchlist WHERE user_id = ? AND ticker = ?", (user_id, ticker.upper().strip()))

# ── Price targets ───────────────────────────────────────────────────────────────
def price_targets_get(user_id):
    with sqlite3.connect(DB_PATH) as c:
        rows = c.execute(
            "SELECT id, ticker, target_price, direction, note FROM price_targets WHERE user_id = ? ORDER BY ticker",
            (user_id,)
        ).fetchall()
    return [{"id": r[0], "ticker": r[1], "target_price": r[2], "direction": r[3], "note": r[4]} for r in rows]

def price_target_set(user_id, ticker, target_price, direction='above', note=''):
    with sqlite3.connect(DB_PATH) as c:
        c.execute("""
            INSERT INTO price_targets (user_id, ticker, target_price, direction, note)
            VALUES (?,?,?,?,?)
            ON CONFLICT(user_id, ticker) DO UPDATE SET
                target_price = excluded.target_price,
                direction    = excluded.direction,
                note         = excluded.note
        """, (user_id, ticker.upper().strip(), float(target_price), direction, note.strip()))

def price_target_remove(user_id, target_id):
    with sqlite3.connect(DB_PATH) as c:
        c.execute("DELETE FROM price_targets WHERE id = ? AND user_id = ?", (target_id, user_id))

# ── Ticker search ───────────────────────────────────────────────────────────────
def search_tickers(query):
    query = query.strip()
    if not query or len(query) < 1:
        return []
    key = f"search:{query.lower()}"
    def fetch():
        try:
            results = yf.Search(query, max_results=8).quotes
            out = []
            for r in (results or [])[:8]:
                sym = r.get("symbol", "")
                if not sym:
                    continue
                out.append({
                    "symbol":   sym,
                    "name":     r.get("shortname") or r.get("longname") or sym,
                    "type":     r.get("typeDisp", "Equity"),
                    "exchange": r.get("exchDisp", ""),
                })
            return out
        except Exception as e:
            print(f"  Search error: {e}")
            return []
    return _from_cache(key, fetch, ttl=3600)

# ── Market news ─────────────────────────────────────────────────────────────────
def get_market_news():
    key = "market_news"
    def fetch():
        articles, seen = [], set()
        for sym in ['SPY', 'QQQ', 'DIA', 'IWM']:
            try:
                for item in (yf.Ticker(sym).news or [])[:4]:
                    a = _parse_news_item(item, 'MARKET')
                    if a["uuid"] not in seen and a["title"]:
                        seen.add(a["uuid"])
                        articles.append(a)
            except:
                pass
        return sorted(articles, key=lambda x: x["providerPublishTime"], reverse=True)[:12]
    return _from_cache(key, fetch, ttl=300)

def _fetch_one_earnings(sym):
    """Fetch earnings date for a single ticker. Returns dict or None."""
    try:
        info    = yf.Ticker(sym).info
        earn_ts = info.get("earningsTimestamp") or info.get("earningsDate")
        if not earn_ts:
            return None
        if isinstance(earn_ts, (list, tuple)):
            earn_ts = earn_ts[0]
        earn_dt   = datetime.fromtimestamp(float(earn_ts))
        days_away = (earn_dt - datetime.now()).days
        if -3 <= days_away <= 45:
            return {
                "ticker":           sym,
                "name":             info.get("shortName", sym),
                "sector":           info.get("sector", ""),
                "earnings_date":    earn_dt.strftime("%Y-%m-%d"),
                "eps_estimate":     float(info.get("epsCurrentYear") or info.get("epsForward") or 0),
                "eps_trailing":     float(info.get("epsTrailingTwelveMonths") or 0),
                "days_away":        days_away,
                "market_cap":       float(info.get("marketCap") or 0),
            }
    except Exception as e:
        print(f"  Earnings {sym}: {e}")
    return None

def get_earnings_calendar(tickers):
    """Get upcoming / recent earnings for given tickers (within -3 to +45 days)."""
    tickers = [t.strip().upper() for t in (tickers or [])[:30] if t.strip()]
    if not tickers:
        return []
    key = "earnings:" + ",".join(sorted(tickers))
    def fetch():
        results = []
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(_fetch_one_earnings, sym): sym for sym in tickers}
            for fut in as_completed(futures):
                item = fut.result()
                if item:
                    results.append(item)
        results.sort(key=lambda x: x["days_away"])
        return results
    return _from_cache(key, fetch, ttl=3600 * 6)

# Default tickers to always check for earnings (augmented by user holdings at query time)
EARNINGS_WATCHLIST = [
    'AAPL','MSFT','NVDA','AMZN','META','GOOGL','TSLA','AMD','NFLX',
    'JPM','BAC','GS','V','MA','XOM','LLY','UNH','JNJ','ABBV',
    'AVGO','CRM','ORCL','SHOP','COIN','HOOD','PLTR','ARM','MU',
]

ADMIN_USER = "ismael"

# Futures / overnight market symbols
FUTURES_SYMBOLS = [
    {'symbol': 'ES=F',    'name': 'S&P 500 Futures',     'icon': '📈', 'group': 'equity'},
    {'symbol': 'NQ=F',    'name': 'Nasdaq 100 Futures',   'icon': '💻', 'group': 'equity'},
    {'symbol': 'YM=F',    'name': 'Dow Jones Futures',    'icon': '🏭', 'group': 'equity'},
    {'symbol': 'RTY=F',   'name': 'Russell 2000 Futures', 'icon': '📊', 'group': 'equity'},
    {'symbol': 'GC=F',    'name': 'Gold',                 'icon': '🥇', 'group': 'commodity'},
    {'symbol': 'CL=F',    'name': 'Crude Oil (WTI)',      'icon': '🛢️', 'group': 'commodity'},
    {'symbol': 'ZB=F',    'name': '30Y Treasury Bond',    'icon': '🏦', 'group': 'bond'},
    {'symbol': 'BTC-USD', 'name': 'Bitcoin',              'icon': '₿',  'group': 'crypto'},
]

def get_admin_stats():
    with sqlite3.connect(DB_PATH) as c:
        users      = c.execute("SELECT id, username, email, created_at FROM users ORDER BY created_at DESC").fetchall()
        portfolios = c.execute("SELECT user_id, COUNT(*) FROM portfolios GROUP BY user_id").fetchall()
        sessions   = c.execute("SELECT user_id, COUNT(*) FROM sessions WHERE expires_at > datetime('now') GROUP BY user_id").fetchall()
        journals   = c.execute("SELECT user_id, COUNT(*) FROM journal GROUP BY user_id").fetchall()
        snapshots  = c.execute("SELECT COUNT(*) FROM portfolio_snapshots").fetchone()[0]
        watchlists = c.execute("SELECT user_id, COUNT(*) FROM watchlist GROUP BY user_id").fetchall()

    port_map  = {r[0]: r[1] for r in portfolios}
    sess_map  = {r[0]: r[1] for r in sessions}
    jour_map  = {r[0]: r[1] for r in journals}
    watch_map = {r[0]: r[1] for r in watchlists}

    user_list = [{
        "id":         u[0],
        "username":   u[1],
        "email":      u[2] or "—",
        "joined":     u[3][:10] if u[3] else "—",
        "holdings":   port_map.get(u[0], 0),
        "sessions":   sess_map.get(u[0], 0),
        "journal":    jour_map.get(u[0], 0),
        "watchlist":  watch_map.get(u[0], 0),
    } for u in users]

    return {
        "total_users":     len(users),
        "total_snapshots": snapshots,
        "users":           user_list,
        "server_version":  VERSION,
        "uptime_since":    datetime.now().strftime("%Y-%m-%d %H:%M"),
    }

def journal_get(user_id):
    with sqlite3.connect(DB_PATH) as c:
        rows = c.execute(
            "SELECT id, date, ticker, action, shares, price, notes, created_at FROM journal WHERE user_id = ? ORDER BY date DESC, created_at DESC",
            (user_id,)
        ).fetchall()
    return [{"id":r[0],"date":r[1],"ticker":r[2],"action":r[3],"shares":r[4],"price":r[5],"notes":r[6],"created_at":r[7]} for r in rows]

def journal_add(user_id, entry):
    with sqlite3.connect(DB_PATH) as c:
        c.execute(
            "INSERT INTO journal (user_id, date, ticker, action, shares, price, notes) VALUES (?,?,?,?,?,?,?)",
            (user_id,
             entry.get('date',''),
             entry.get('ticker','').upper().strip(),
             entry.get('action','NOTE').upper().strip(),
             float(entry.get('shares',0) or 0),
             float(entry.get('price',0) or 0),
             entry.get('notes','').strip())
        )
        return c.execute("SELECT last_insert_rowid()").fetchone()[0]

def journal_delete(user_id, entry_id):
    with sqlite3.connect(DB_PATH) as c:
        c.execute("DELETE FROM journal WHERE id = ? AND user_id = ?", (entry_id, user_id))


# ── Stock data ─────────────────────────────────────────────────────────────────
def _parse_quote(sym, info):
    return {
        "symbol":                     sym,
        "shortName":                  info.get("shortName", sym),
        "regularMarketPrice":         float(info.get("regularMarketPrice") or info.get("currentPrice") or 0),
        "regularMarketChange":        float(info.get("regularMarketChange") or 0),
        "regularMarketChangePercent": float(info.get("regularMarketChangePercent") or 0),
        "regularMarketVolume":        int(  info.get("regularMarketVolume") or 0),
        "averageVolume":              int(  info.get("averageVolume") or 0),
        "marketCap":                  float(info.get("marketCap") or 0),
        "fiftyTwoWeekHigh":           float(info.get("fiftyTwoWeekHigh") or 0),
        "fiftyTwoWeekLow":            float(info.get("fiftyTwoWeekLow") or 0),
        "trailingPE":                 float(info.get("trailingPE") or 0),
        "beta":                       float(info.get("beta") or 0),
        "dividendRate":               float(info.get("dividendRate") or 0),
        "dividendYield":              float(info.get("dividendYield") or 0),
        "sector":                     info.get("sector", ""),
    }

def _empty_quote(sym):
    return {"symbol": sym, "shortName": sym, "regularMarketPrice": 0,
            "regularMarketChange": 0, "regularMarketChangePercent": 0,
            "regularMarketVolume": 0, "averageVolume": 0, "marketCap": 0,
            "fiftyTwoWeekHigh": 0, "fiftyTwoWeekLow": 0, "trailingPE": 0,
            "beta": 0, "dividendRate": 0, "dividendYield": 0, "sector": ""}

def _fetch_one_quote(sym):
    """Fetch a single ticker using fast_info (lightweight) + cached metadata."""
    try:
        fi = yf.Ticker(sym).fast_info
        price = float(fi.last_price   or 0)
        prev  = float(fi.previous_close or price or 1)
        change     = price - prev
        change_pct = (change / prev * 100) if prev else 0

        # Slow metadata (name, sector, PE…) cached separately for 24 h
        meta_key = f"meta:{sym}"
        meta = _cache.get(meta_key, {}).get("data")
        if not meta or (time.time() - _cache[meta_key]["ts"]) > 86400:
            try:
                info = yf.Ticker(sym).info
                meta = {
                    "shortName":     info.get("shortName", sym),
                    "sector":        info.get("sector", ""),
                    "trailingPE":    float(info.get("trailingPE")    or 0),
                    "beta":          float(info.get("beta")           or 0),
                    "dividendRate":  float(info.get("dividendRate")   or 0),
                    "dividendYield": float(info.get("dividendYield")  or 0),
                }
                _cache[meta_key] = {"ts": time.time(), "data": meta}
            except:
                meta = {"shortName": sym, "sector": "", "trailingPE": 0,
                        "beta": 0, "dividendRate": 0, "dividendYield": 0}

        return sym, {
            "symbol":                     sym,
            "shortName":                  meta["shortName"],
            "regularMarketPrice":         round(price, 4),
            "regularMarketChange":        round(change, 4),
            "regularMarketChangePercent": round(change_pct, 4),
            "regularMarketPreviousClose": round(prev, 4),
            "regularMarketVolume":        int(getattr(fi, "last_volume", 0) or 0),
            "averageVolume":              int(getattr(fi, "three_month_average_volume", 0) or 0),
            "marketCap":                  float(getattr(fi, "market_cap", 0) or 0),
            "fiftyTwoWeekHigh":           float(getattr(fi, "fifty_two_week_high", 0) or 0),
            "fiftyTwoWeekLow":            float(getattr(fi, "fifty_two_week_low",  0) or 0),
            "trailingPE":                 meta["trailingPE"],
            "beta":                       meta["beta"],
            "dividendRate":               meta["dividendRate"],
            "dividendYield":              meta["dividendYield"],
            "sector":                     meta["sector"],
        }
    except Exception as e:
        print(f"  ⚠️  {sym}: {e}")
        return sym, _empty_quote(sym)

def get_quotes(symbols):
    if not symbols: return {}
    symbols = [s.strip().upper() for s in symbols if s.strip()]
    key = "quotes:" + ",".join(sorted(symbols))
    def fetch():
        print(f"\n📡 Fetching {len(symbols)} quotes in parallel…")
        results = {}
        with ThreadPoolExecutor(max_workers=min(len(symbols), 10)) as pool:
            for sym, data in pool.map(_fetch_one_quote, symbols):
                results[sym] = data
                p, pct = data["regularMarketPrice"], data["regularMarketChangePercent"]
                print(f"  {'🚨' if abs(pct)>=5 else '✅'} {sym:6s}  ${p:>9.4f}  {'↑' if pct>=0 else '↓'} {pct:+.2f}%")
        return results
    return _from_cache(key, fetch)

# ── Extended hours (pre-market / after-hours) ─────────────────────────────────
def _fetch_one_extended(sym):
    """Fetch pre-market and after-hours prices from yfinance .info for a single symbol."""
    try:
        info = yf.Ticker(sym).info
        def _pct(raw):
            # yfinance sometimes returns as decimal fraction (0.015), sometimes as % (1.5)
            v = float(raw or 0)
            return round(v * 100 if abs(v) < 1 and v != 0 else v, 3)
        pre_price  = float(info.get('preMarketPrice')         or 0)
        pre_change = float(info.get('preMarketChange')        or 0)
        pre_pct    = _pct(info.get('preMarketChangePercent'))
        post_price = float(info.get('postMarketPrice')        or 0)
        post_change= float(info.get('postMarketChange')       or 0)
        post_pct   = _pct(info.get('postMarketChangePercent'))
        return sym, {
            'pre_price':  round(pre_price,  4),
            'pre_change': round(pre_change, 4),
            'pre_pct':    pre_pct,
            'post_price': round(post_price,  4),
            'post_change':round(post_change, 4),
            'post_pct':   post_pct,
        }
    except Exception as e:
        print(f"  ⚠️ ext-hours {sym}: {e}")
        return sym, {'pre_price': 0, 'pre_change': 0, 'pre_pct': 0,
                     'post_price': 0, 'post_change': 0, 'post_pct': 0}

def get_extended_hours(symbols):
    if not symbols: return {}
    symbols = [s.strip().upper() for s in symbols if s.strip()]
    key = 'exthours:' + ','.join(sorted(symbols))
    def fetch():
        print(f"⏰ Fetching extended hours for {len(symbols)} symbols in parallel…")
        with ThreadPoolExecutor(max_workers=min(len(symbols), 10)) as pool:
            return dict(pool.map(_fetch_one_extended, symbols))
    return _from_cache(key, fetch, ttl=60)

# ── Futures / overnight market ────────────────────────────────────────────────
def get_futures():
    key = 'futures'
    def fetch():
        syms = [f['symbol'] for f in FUTURES_SYMBOLS]
        meta = {f['symbol']: f for f in FUTURES_SYMBOLS}
        print(f"🌙 Fetching {len(syms)} futures in parallel…")
        def _one(sym):
            try:
                fi   = yf.Ticker(sym).fast_info
                price = float(fi.last_price    or 0)
                prev  = float(fi.previous_close or price or 1)
                change = price - prev
                pct    = (change / prev * 100) if prev else 0
                m = meta[sym]
                return {'symbol': sym, 'name': m['name'], 'icon': m['icon'],
                        'group': m['group'], 'price': round(price, 2),
                        'change': round(change, 2), 'change_pct': round(pct, 3)}
            except Exception as e:
                print(f"  ⚠️ futures {sym}: {e}")
                m = meta[sym]
                return {'symbol': sym, 'name': m['name'], 'icon': m['icon'],
                        'group': m['group'], 'price': 0, 'change': 0, 'change_pct': 0}
        with ThreadPoolExecutor(max_workers=len(syms)) as pool:
            return list(pool.map(_one, syms))
    return _from_cache(key, fetch, ttl=60)

def _parse_news_item(item, tag):
    content   = item.get("content") if isinstance(item.get("content"), dict) else {}
    title     = content.get("title")     or item.get("title",     "")
    link      = (content.get("clickThroughUrl") or {}).get("url") or item.get("link", "")
    publisher = (content.get("provider")        or {}).get("displayName") or item.get("publisher", "")
    ts        = item.get("providerPublishTime", 0)
    uid       = item.get("uuid") or link or title
    return {"title": title, "link": link, "publisher": publisher,
            "providerPublishTime": ts, "uuid": uid, "_tag": tag}

def get_news(symbols):
    symbols = [s.strip().upper() for s in symbols[:6] if s.strip()]
    key = "news:" + ",".join(symbols)
    def fetch():
        articles, seen = [], set()
        for sym in symbols:
            try:
                for item in (yf.Ticker(sym).news or [])[:3]:
                    a = _parse_news_item(item, sym)
                    if a["uuid"] not in seen and a["title"]:
                        seen.add(a["uuid"]); articles.append(a)
            except: pass
        return sorted(articles, key=lambda x: x["providerPublishTime"], reverse=True)[:12]
    return _from_cache(key, fetch)

def get_stock_detail(symbol):
    symbol = symbol.strip().upper()
    key = f"detail:{symbol}"
    def fetch():
        print(f"  🔍 Detail: {symbol}")
        try:
            stock = yf.Ticker(symbol)
            info  = stock.info
            news  = [_parse_news_item(n, symbol) for n in (stock.news or [])[:6]]
            news  = [n for n in news if n["title"]]
            def flt(k): return float(info.get(k) or 0)
            return {
                "symbol": symbol, "shortName": info.get("shortName", symbol),
                "longName": info.get("longName", ""), "description": info.get("longBusinessSummary", ""),
                "sector": info.get("sector", ""), "industry": info.get("industry", ""),
                "website": info.get("website", ""), "country": info.get("country", ""),
                "employees": info.get("fullTimeEmployees", 0), "exchange": info.get("exchange", ""),
                "price": flt("regularMarketPrice") or flt("currentPrice"),
                "change": flt("regularMarketChange"), "change_pct": flt("regularMarketChangePercent"),
                "open": flt("regularMarketOpen"), "prev_close": flt("previousClose"),
                "day_high": flt("dayHigh"), "day_low": flt("dayLow"),
                "market_cap": flt("marketCap"), "pe_ratio": flt("trailingPE"),
                "forward_pe": flt("forwardPE"), "peg_ratio": flt("pegRatio"),
                "eps": flt("trailingEps"), "forward_eps": flt("forwardEps"),
                "price_to_book": flt("priceToBook"), "price_to_sales": flt("priceToSalesTrailing12Months"),
                "dividend_yield": flt("dividendYield"), "dividend_rate": flt("dividendRate"),
                "payout_ratio": flt("payoutRatio"), "beta": flt("beta"),
                "short_ratio": flt("shortRatio"), "short_pct_float": flt("shortPercentOfFloat"),
                "volume": flt("regularMarketVolume"), "avg_volume": flt("averageVolume"),
                "avg_volume_10d": flt("averageVolume10days"),
                "week52_high": flt("fiftyTwoWeekHigh"), "week52_low": flt("fiftyTwoWeekLow"),
                "week52_change": flt("52WeekChange"), "day50_ma": flt("fiftyDayAverage"),
                "day200_ma": flt("twoHundredDayAverage"),
                "target_price": flt("targetMeanPrice"), "target_high": flt("targetHighPrice"),
                "target_low": flt("targetLowPrice"), "recommendation": info.get("recommendationKey", ""),
                "analyst_count": info.get("numberOfAnalystOpinions", 0),
                "shares_outstanding": flt("sharesOutstanding"), "float_shares": flt("floatShares"),
                "news": news,
            }
        except Exception as e:
            return {"symbol": symbol, "error": str(e)}
    return _from_cache(key, fetch)

def get_stock_history(symbol, period='1mo'):
    symbol = symbol.strip().upper()
    key    = f"hist:{symbol}:{period}"
    def fetch():
        print(f"  📈 History: {symbol} ({period})")
        try:
            # 1-day: 1-min bars with prepost=True → full 24 h session (overnight + pre + regular + AH)
            if period == '1d':
                hist = yf.Ticker(symbol).history(period='1d', interval='1m', prepost=True)
            else:
                interval_map = {'5d':'15m','1mo':'1d','3mo':'1d','6mo':'1wk','1y':'1wk'}
                interval = interval_map.get(period, '1d')
                hist = yf.Ticker(symbol).history(period=period, interval=interval)
            if hist.empty:
                return []
            pts = []
            for dt, row in hist.iterrows():
                pts.append({
                    't': int(dt.timestamp() * 1000),
                    'c': round(float(row['Close']), 4),
                    'o': round(float(row['Open']),  4),
                    'v': int(row.get('Volume', 0)),
                })
            return pts
        except Exception as e:
            print(f"  History error {symbol}: {e}")
            return []
    ttl_map = {'1d': 30, '5d': 300, '1mo': 3600, '3mo': 7200, '6mo': 14400, '1y': 86400}
    return _from_cache(key, fetch, ttl=ttl_map.get(period, 3600))

def get_market_data():
    key = "market_overview"
    def fetch():
        result = {"indices": {}, "sectors": {}, "crypto": {}}
        all_syms = MARKET_SYMBOLS + list(SECTOR_ETFS.keys()) + list(CRYPTO_SYMBOLS.keys())
        print(f"\n📡 Fetching market overview ({len(all_syms)} symbols)…")
        NAME_MAP = {"^GSPC":"S&P 500","^IXIC":"NASDAQ","^DJI":"DOW Jones","^RUT":"Russell 2000","^VIX":"VIX"}
        def _fetch_market():
            batch = yf.Tickers(" ".join(all_syms))
            for sym in MARKET_SYMBOLS:
                try:
                    info = batch.tickers[sym].info
                    result["indices"][sym] = {
                        "name":       NAME_MAP.get(sym, sym),
                        "price":      float(info.get("regularMarketPrice") or info.get("currentPrice") or 0),
                        "change":     float(info.get("regularMarketChange") or 0),
                        "change_pct": float(info.get("regularMarketChangePercent") or 0),
                        "day_high":   float(info.get("dayHigh") or 0),
                        "day_low":    float(info.get("dayLow") or 0),
                        "prev_close": float(info.get("previousClose") or 0),
                    }
                except: result["indices"][sym] = {"name": NAME_MAP.get(sym,sym), "price":0,"change":0,"change_pct":0,"day_high":0,"day_low":0,"prev_close":0}
            for sym, name in SECTOR_ETFS.items():
                try:
                    info = batch.tickers[sym].info
                    result["sectors"][sym] = {
                        "name":       name,
                        "price":      float(info.get("regularMarketPrice") or 0),
                        "change_pct": float(info.get("regularMarketChangePercent") or 0),
                    }
                except: result["sectors"][sym] = {"name": name, "price": 0, "change_pct": 0}
            for sym, name in CRYPTO_SYMBOLS.items():
                try:
                    info = batch.tickers[sym].info
                    price = float(info.get("regularMarketPrice") or info.get("currentPrice") or 0)
                    result["crypto"][sym] = {
                        "name":       name,
                        "price":      price,
                        "change":     float(info.get("regularMarketChange") or 0),
                        "change_pct": float(info.get("regularMarketChangePercent") or 0),
                        "market_cap": float(info.get("marketCap") or 0),
                        "volume":     float(info.get("regularMarketVolume") or 0),
                    }
                except: result["crypto"][sym] = {"name": name, "price":0,"change":0,"change_pct":0,"market_cap":0,"volume":0}
        try:
            _yf_retry(_fetch_market)
        except Exception as e:
            print(f"  Market data error after retries: {e}")
            if key in _cache:
                print("  ↩ Returning stale market cache")
                return _cache[key]["data"]
        return result
    return _from_cache(key, fetch, ttl=60)

def get_movers():
    key = "market_movers"
    def fetch():
        print(f"\n📡 Scanning movers ({len(MOVER_WATCHLIST)} stocks)…")
        results = []
        def _scan():
            batch = yf.Tickers(" ".join(MOVER_WATCHLIST))
            for sym in MOVER_WATCHLIST:
                try:
                    info  = batch.tickers[sym].info
                    price = float(info.get("regularMarketPrice") or info.get("currentPrice") or 0)
                    if price > 0:
                        vol   = int(info.get("regularMarketVolume") or 0)
                        avol  = int(info.get("averageVolume") or 0)
                        mcap  = float(info.get("marketCap") or 0)
                        chg   = float(info.get("regularMarketChange") or 0)
                        pct   = float(info.get("regularMarketChangePercent") or 0)
                        open_ = float(info.get("regularMarketOpen") or 0)
                        hi    = float(info.get("dayHigh") or 0)
                        lo    = float(info.get("dayLow") or 0)
                        w52h  = float(info.get("fiftyTwoWeekHigh") or 0)
                        w52l  = float(info.get("fiftyTwoWeekLow") or 0)
                        results.append({
                            "symbol":     sym,
                            "name":       info.get("shortName", sym),
                            "sector":     info.get("sector", ""),
                            "price":      price,
                            "change":     chg,
                            "change_pct": pct,
                            "open":       open_,
                            "day_high":   hi,
                            "day_low":    lo,
                            "volume":     vol,
                            "avg_volume": avol,
                            "market_cap": mcap,
                            "week52_high": w52h,
                            "week52_low":  w52l,
                            "vol_ratio":  round(vol / avol, 2) if avol else 0,
                        })
                except: pass
        try:
            _yf_retry(_scan)
        except Exception as e:
            print(f"  Movers error: {e}")
            # Return stale cache if available rather than empty
            if key in _cache:
                print("  ↩ Returning stale movers cache")
                return _cache[key]["data"]
        results.sort(key=lambda x: x["change_pct"], reverse=True)
        top = [r for r in results if r["change_pct"] > 0][:15]
        bot = sorted([r for r in results if r["change_pct"] < 0], key=lambda x: x["change_pct"])[:15]
        return {"gainers": top, "losers": bot}
    return _from_cache(key, fetch, ttl=300)


# ── HTTP Handler ───────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args): pass

    # ── Cookie helpers
    def get_cookie(self, name):
        for part in self.headers.get("Cookie", "").split(";"):
            k, _, v = part.strip().partition("=")
            if k.strip() == name:
                return v.strip()
        return None

    def get_user(self):
        return get_session(self.get_cookie("session"))

    def require_auth(self):
        user = self.get_user()
        if not user:
            self.send_redirect("/login")
            return None
        return user

    # ── Response helpers
    def send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type",   "application/json")
        self.send_header("Cache-Control",  "no-cache")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def send_file(self, fpath, ct="text/html; charset=utf-8"):
        with open(fpath, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type",   ct)
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def send_redirect(self, location):
        self.send_response(302)
        self.send_header("Location", location)
        self.end_headers()

    def _session_cookie(self, token, max_age=30*24*3600):
        # In prod the request arrives via Cloudflare Tunnel (HTTPS edge → HTTP origin).
        # The browser sees HTTPS so Secure cookies work, but we must NOT require the
        # cookie to arrive over HTTPS on the origin side — Lax is enough.
        # Dropping the Secure flag here is intentional: the tunnel encrypts the edge.
        return f"session={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age}"

    def _build_admin_page(self, s):
        rows = "".join(f"""
        <tr>
          <td>{u['username']}</td>
          <td style="color:#8b949e">{u['email']}</td>
          <td>{u['joined']}</td>
          <td style="text-align:center">{u['holdings']}</td>
          <td style="text-align:center">{u['journal']}</td>
          <td style="text-align:center">{u['watchlist']}</td>
          <td style="text-align:center">{'🟢' if u['sessions'] else '⚪'}</td>
        </tr>""" for u in s['users'])
        return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>maelkloud Admin</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#0d1117;color:#e6edf3;padding:24px}}
h1{{font-size:1.3rem;font-weight:700;margin-bottom:4px}}
.sub{{color:#8b949e;font-size:.84rem;margin-bottom:24px}}
.cards{{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:24px}}
.stat{{background:#161b22;border:1px solid #21262d;border-radius:8px;padding:14px 20px;min-width:130px}}
.stat-val{{font-size:2rem;font-weight:800;color:#388bfd}}
.stat-lbl{{font-size:.72rem;color:#8b949e;text-transform:uppercase;letter-spacing:.05em;margin-top:2px}}
table{{width:100%;border-collapse:collapse;background:#161b22;border:1px solid #21262d;border-radius:8px;overflow:hidden;font-size:.84rem}}
th{{background:#0d1117;color:#8b949e;font-size:.7rem;text-transform:uppercase;letter-spacing:.04em;padding:9px 14px;text-align:left;border-bottom:1px solid #21262d}}
td{{padding:9px 14px;border-bottom:1px solid #21262d}}
tr:last-child td{{border-bottom:none}}
tr:hover td{{background:rgba(255,255,255,.02)}}
a{{color:#388bfd;text-decoration:none;font-size:.8rem}}
</style></head><body>
<h1>📊 maelkloud — Admin</h1>
<div class="sub">v{s['server_version']} · Generated {s['uptime_since']} · <a href="/">← Back to app</a></div>
<div class="cards">
  <div class="stat"><div class="stat-val">{s['total_users']}</div><div class="stat-lbl">Total Users</div></div>
  <div class="stat"><div class="stat-val">{s['total_snapshots']}</div><div class="stat-lbl">Snapshots</div></div>
</div>
<table>
  <thead><tr><th>Username</th><th>Email</th><th>Joined</th><th>Holdings</th><th>Journal</th><th>Watchlist</th><th>Active</th></tr></thead>
  <tbody>{rows}</tbody>
</table>
</body></html>"""

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    # ── GET ────────────────────────────────────────────────────────────────────
    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path
        qs     = parse_qs(parsed.query)
        try:
            # ── Public
            if path in ("/login", "/register"):
                self.send_file(os.path.join(BASE_DIR, "login.html"))
                return

            # ── PWA static files (public, no auth needed)
            if path == "/manifest.json":
                self.send_file(os.path.join(BASE_DIR, "manifest.json"), "application/manifest+json")
                return
            if path == "/sw.js":
                self.send_file(os.path.join(BASE_DIR, "sw.js"), "text/javascript")
                return
            if path.startswith("/icon.png"):
                self.send_file(os.path.join(BASE_DIR, "icon.png"), "image/png")
                return

            if path == "/api/health":
                self.send_json({"status": "ok", "version": VERSION, "ts": time.time()})
                return

            if path == "/admin":
                user = self.get_user()
                if not user or user["username"] != ADMIN_USER:
                    self.send_response(302)
                    self.send_header("Location", "/login")
                    self.end_headers()
                    return
                stats = get_admin_stats()
                body  = self._build_admin_page(stats).encode()
                self.send_response(200)
                self.send_header("Content-Type",   "text/html; charset=utf-8")
                self.send_header("Content-Length", len(body))
                self.end_headers()
                self.wfile.write(body)
                return

            if path == "/logout":
                token = self.get_cookie("session")
                if token:
                    delete_session(token)
                self.send_response(302)
                self.send_header("Location", "/login")
                self.send_header("Set-Cookie", self._session_cookie("", max_age=0))
                self.end_headers()
                return

            # ── Protected
            user = self.require_auth()
            if not user:
                return

            if path in ("/", "/index.html"):
                self.send_file(os.path.join(BASE_DIR, "portfolio_dashboard.html"))

            elif path == "/api/portfolio":
                holdings = db_get_portfolio(user["user_id"])
                self.send_json({"holdings": holdings, "username": user["username"]})

            elif path == "/api/quotes":
                syms = [s for s in qs.get("symbols", [""])[0].split(",") if s.strip()]
                self.send_json(get_quotes(syms))

            elif path == "/api/news":
                syms = [s for s in qs.get("symbols", [""])[0].split(",") if s.strip()]
                self.send_json(get_news(syms))

            elif path.startswith("/api/stock/history/"):
                parts  = path.split("/api/stock/history/", 1)[-1].split("/")
                sym    = parts[0].strip()
                period = parts[1].strip() if len(parts) > 1 else qs.get("period", ["1mo"])[0]
                self.send_json(get_stock_history(sym, period))

            elif path.startswith("/api/stock/"):
                sym = path.split("/api/stock/", 1)[-1].strip()
                self.send_json(get_stock_detail(sym))

            elif path == "/api/market":
                self.send_json(get_market_data())

            elif path == "/api/movers":
                self.send_json(get_movers())

            elif path == "/api/portfolio/history":
                days = int(qs.get("days", ["90"])[0])
                self.send_json(get_portfolio_history(user["user_id"], days))

            elif path == "/api/watchlist":
                wl = watchlist_get(user["user_id"])
                tickers = [w["ticker"] for w in wl]
                q = get_quotes(tickers) if tickers else {}
                for w in wl:
                    d = q.get(w["ticker"], {})
                    w["price"]      = d.get("regularMarketPrice", 0)
                    w["change"]     = d.get("regularMarketChange", 0)
                    w["change_pct"] = d.get("regularMarketChangePercent", 0)
                    w["name"]       = d.get("shortName", w["ticker"])
                self.send_json(wl)

            elif path.startswith("/api/watchlist/remove/"):
                tk = path.split("/")[-1].upper().strip()
                watchlist_remove(user["user_id"], tk)
                self.send_json({"status": "removed"})

            elif path == "/api/journal":
                self.send_json(journal_get(user["user_id"]))

            elif path.startswith("/api/journal/delete/"):
                try:
                    eid = int(path.split("/")[-1])
                    journal_delete(user["user_id"], eid)
                    self.send_json({"status": "deleted"})
                except: self.send_error(400)

            elif path == "/api/search":
                q = qs.get("q", [""])[0].strip()
                self.send_json(search_tickers(q))

            elif path == "/api/market-news":
                self.send_json(get_market_news())

            elif path == "/api/price-targets":
                self.send_json(price_targets_get(user["user_id"]))

            elif path == "/api/earnings-calendar":
                # Merge user-supplied tickers with default watchlist
                extra = [s for s in qs.get("tickers", [""])[0].split(",") if s.strip()]
                tickers = list(dict.fromkeys(extra + EARNINGS_WATCHLIST))  # dedup, preserve order
                self.send_json(get_earnings_calendar(tickers))

            elif path == "/api/futures":
                self.send_json(get_futures())

            elif path == "/api/extended-hours":
                syms = [s for s in qs.get("symbols", [""])[0].split(",") if s.strip()]
                self.send_json(get_extended_hours(syms))

            elif path.startswith("/api/price-targets/delete/"):
                try:
                    tid = int(path.split("/")[-1])
                    price_target_remove(user["user_id"], tid)
                    self.send_json({"status": "deleted"})
                except: self.send_error(400)

            else:
                self.send_error(404)

        except Exception as e:
            self.send_json({"error": str(e)}, 500)

    # ── POST ───────────────────────────────────────────────────────────────────
    def do_POST(self):
        try:
            length  = int(self.headers.get("Content-Length", 0))
            raw     = self.rfile.read(length)
            ct      = self.headers.get("Content-Type", "")

            def parse_body():
                if "json" in ct:
                    return json.loads(raw)
                # application/x-www-form-urlencoded
                return {k: v[0] for k, v in pqs(raw.decode()).items()}

            # ── Login
            if self.path == "/login":
                data = parse_body()
                uid  = authenticate_user(data.get("username", ""), data.get("password", ""))
                if uid:
                    token = create_session(uid)
                    self.send_response(302)
                    self.send_header("Location",   "/")
                    self.send_header("Set-Cookie", self._session_cookie(token))
                    self.end_headers()
                else:
                    self.send_redirect("/login?error=invalid")
                return

            # ── Register
            if self.path == "/register":
                data     = parse_body()
                username = data.get("username", "").strip()
                password = data.get("password", "").strip()
                email    = data.get("email",    "").strip()
                if not username or len(password) < 6:
                    self.send_redirect("/login?tab=register&error=weak")
                    return
                uid = create_user(username, password, email)
                if uid:
                    token = create_session(uid)
                    self.send_response(302)
                    self.send_header("Location",   "/")
                    self.send_header("Set-Cookie", self._session_cookie(token))
                    self.end_headers()
                else:
                    self.send_redirect("/login?tab=register&error=taken")
                return

            # ── Protected POSTs
            user = self.get_user()
            if not user:
                self.send_json({"error": "Unauthorized"}, 401)
                return

            if self.path == "/api/portfolio":
                data     = json.loads(raw)
                holdings = data.get("holdings", data) if isinstance(data, dict) else data
                db_save_portfolio(user["user_id"], holdings)
                self.send_json({"status": "saved"})

            elif self.path == "/api/watchlist":
                data   = json.loads(raw)
                ticker = data.get("ticker", "").upper().strip()
                notes  = data.get("notes", "")
                if not ticker:
                    self.send_json({"error": "ticker required"}, 400)
                else:
                    watchlist_add(user["user_id"], ticker, notes)
                    self.send_json({"status": "added"})

            elif self.path == "/api/journal":
                data = json.loads(raw)
                eid  = journal_add(user["user_id"], data)
                self.send_json({"status": "added", "id": eid})

            elif self.path == "/api/cache/clear":
                # Admin-only: require auth + admin username
                if not user or user.get("username") != ADMIN_USER:
                    self.send_json({"error": "Forbidden"}, 403)
                    return
                _cache.clear()
                print("  🗑  Cache cleared by admin")
                self.send_json({"status": "ok", "message": "Cache cleared"})

            elif self.path == "/api/price-targets":
                data = json.loads(raw)
                ticker = data.get("ticker", "").upper().strip()
                target = data.get("target_price")
                direction = data.get("direction", "above")
                note  = data.get("note", "")
                if not ticker or target is None:
                    self.send_json({"error": "ticker and target_price required"}, 400)
                else:
                    price_target_set(user["user_id"], ticker, float(target), direction, note)
                    self.send_json({"status": "saved"})

            else:
                self.send_error(404)

        except Exception as e:
            self.send_json({"error": str(e)}, 500)


# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()

    def _snapshot_loop():
        import time as _t
        _t.sleep(30)  # brief startup delay
        while True:
            snapshot_portfolios()
            _t.sleep(2 * 3600)  # every 2 hours

    threading.Thread(target=_snapshot_loop, daemon=True).start()

    # ── Background cache pre-warm ──────────────────────────────────────────
    # Fetch quotes + market data right after startup so the first page load
    # hits the cache instead of waiting on Yahoo Finance.
    def _prewarm():
        import time as _t
        _t.sleep(3)  # let server socket bind first
        print("🔥 Pre-warming cache…")
        try:
            all_tickers = []
            with sqlite3.connect(DB_PATH) as c:
                rows = c.execute("SELECT DISTINCT ticker FROM portfolios").fetchall()
                all_tickers = [r[0] for r in rows if r[0]]
            if all_tickers:
                get_quotes(all_tickers)
                print(f"  ✅ Quotes cached for {len(all_tickers)} tickers")
            get_market_data()
            print("  ✅ Market data cached")
        except Exception as e:
            print(f"  ⚠️  Pre-warm error: {e}")

    threading.Thread(target=_prewarm, daemon=True).start()

    host = "0.0.0.0" if PROD else "localhost"
    url  = f"http://localhost:{PORT}"

    print(f"\n{'═'*54}")
    print(f"  🌐  maelkloud.com Portfolio Monitor  ·  v{VERSION}")
    print(f"{'═'*54}")
    print(f"  URL      :  {url}")
    print(f"  Mode     :  {'🚀 Production (0.0.0.0)' if PROD else '🛠  Development (localhost)'}")
    print(f"  Database :  {DB_PATH}")
    print(f"  Stop     :  Ctrl + C")
    print(f"{'═'*54}\n")

    try:
        server = ThreadingHTTPServer((host, PORT), Handler)
    except OSError as e:
        if "Address already in use" in str(e) or "10048" in str(e):
            print(f"⚠️  Port {PORT} already in use — opening browser…\n")
            if not PROD:
                webbrowser.open(url)
            sys.exit(0)
        raise

    if not PROD:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n\n  ✅  Server stopped.\n")
