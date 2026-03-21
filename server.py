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
import sqlite3, hashlib, secrets, resource, gzip
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Compressed file cache (gzip, keyed by path) ──────────────────────────────
_gz_cache = {}   # path -> {"etag": str, "gz": bytes}
from datetime import datetime, timedelta
from http.server import HTTPServer, ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, parse_qs as pqs

# ── Raise OS file-descriptor limit early ─────────────────────────────────────
# macOS default is 256, which is easily exhausted when many parallel yfinance
# HTTP connections are open simultaneously.  Push to 8192 (soft) / keep hard.
try:
    _soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    _target = min(8192, _hard) if _hard > 0 else 8192
    if _soft < _target:
        resource.setrlimit(resource.RLIMIT_NOFILE, (_target, _hard))
        print(f"📂 FD limit raised: {_soft} → {_target}")
except Exception as _e:
    print(f"⚠️  Could not raise FD limit: {_e}")

# Max workers for parallel yfinance fetches — keep low to avoid FD storms.
# Each yfinance call uses ~3 file descriptors (TCP socket + SSL + internal).
# With 5 workers: at most ~15 FDs per fetch batch.
_YF_WORKERS = 15   # increased from 5 — more parallel fast_info/info calls

# Global semaphore: at most 4 concurrent yfinance fetch batches at any time.
# Prevents FD exhaustion when market refresh + portfolio refresh + movers
# all fire simultaneously (each with _YF_WORKERS threads).
_YF_SEM = threading.Semaphore(4)

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
    # Throttle concurrent network fetches to avoid FD exhaustion
    with _YF_SEM:
        # Re-check cache after acquiring semaphore (another thread may have filled it)
        now = time.time()
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
        if 0 <= days_away <= 45:
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
        with ThreadPoolExecutor(max_workers=_YF_WORKERS) as pool:
            futures = {pool.submit(_fetch_one_earnings, sym): sym for sym in tickers}
            for fut in as_completed(futures):
                item = fut.result()
                if item:
                    results.append(item)
        # Store raw items WITHOUT days_away — it will be recalculated fresh on every serve
        return results
    raw = _from_cache(key, fetch, ttl=3600 * 6)
    # Always recompute days_away from earnings_date vs. NOW so cached data never shows
    # stale "Today!" or wrong day counts after midnight rolls over.
    today = datetime.now().date()
    out = []
    for item in raw:
        try:
            earn_date = datetime.strptime(item["earnings_date"], "%Y-%m-%d").date()
            days_away = (earn_date - today).days
            if 0 <= days_away <= 45:
                out.append({**item, "days_away": days_away})
        except Exception:
            pass
    out.sort(key=lambda x: x["days_away"])
    return out

# Default tickers to always check for earnings (augmented by user holdings at query time)
EARNINGS_WATCHLIST = [
    'AAPL','MSFT','NVDA','AMZN','META','GOOGL','TSLA','AMD','NFLX',
    'JPM','BAC','GS','V','MA','XOM','LLY','UNH','JNJ','ABBV',
    'AVGO','CRM','ORCL','SHOP','COIN','HOOD','PLTR','ARM','MU',
]

# ── Growth picks watchlist ────────────────────────────────────────────────────
GROWTH_WATCHLIST = [
    'NVDA','META','AMZN','GOOGL','MSFT','TSLA','AMD','AVGO','TSM','ASML',
    'PLTR','COIN','RKLB','IONQ','SMCI','CRWD','NET','ZS','DDOG','SNOW',
    'MDB','SHOP','MELI','SE','NU','SOFI','DUOL','CAVA','APP','AXON',
    'ASTS','RCAT','SOUN','LUNR','HWM','GEV','VST','CEG','NRG','FTAI',
]

def _fetch_growth_one(sym):
    try:
        info = yf.Ticker(sym).info
        cur  = info.get('currentPrice') or info.get('regularMarketPrice') or 0
        tgt  = info.get('targetMeanPrice') or 0
        if not cur or not tgt or tgt <= cur:
            return None
        upside = round((tgt / cur - 1) * 100, 1)
        if upside < 25:
            return None
        return {
            'ticker':     sym,
            'name':       info.get('shortName', sym),
            'current':    round(cur, 2),
            'target':     round(tgt, 2),
            'upside':     upside,
            'sector':     info.get('sector', '—'),
            'rev_growth': round((info.get('revenueGrowth') or 0) * 100, 1),
            'fwd_pe':     round(info.get('forwardPE') or 0, 1),
            'analysts':   info.get('numberOfAnalystOpinions') or 0,
        }
    except:
        return None

def get_growth_picks():
    cached = _from_cache('growth_picks', lambda: None, 0)
    if cached is not None:
        return cached
    with ThreadPoolExecutor(max_workers=16) as ex:
        results = list(ex.map(_fetch_growth_one, GROWTH_WATCHLIST))
    picks = sorted([r for r in results if r], key=lambda x: x['upside'], reverse=True)[:10]
    _cache['growth_picks'] = {'ts': time.time(), 'data': picks, 'ttl': 21600}
    return picks

# ── Correlation matrix ────────────────────────────────────────────────────────
def get_correlation_matrix(tickers):
    if not tickers or len(tickers) < 2:
        return {'error': 'Need at least 2 tickers'}
    tickers = tickers[:15]  # cap at 15
    key = 'corr_' + '_'.join(sorted(tickers))
    cached = _from_cache(key, lambda: None, 0)
    if cached is not None:
        return cached
    try:
        raw = yf.download(tickers, period='3mo', progress=False, auto_adjust=True)
        if raw.empty:
            return {'error': 'No price data'}
        closes = raw['Close'] if 'Close' in raw.columns else raw
        if isinstance(closes, type(raw)) and hasattr(closes, 'columns'):
            pass
        else:
            closes = raw['Close']
        corr = closes.pct_change().corr().round(2)
        result = {'tickers': list(corr.columns), 'matrix': corr.values.tolist()}
        _cache[key] = {'ts': time.time(), 'data': result, 'ttl': 86400}
        return result
    except Exception as e:
        return {'error': str(e)}

# ── Entry Point Analyzer ──────────────────────────────────────────────────────
def _calc_rsi(closes, period=14):
    """Wilder's RSI."""
    if len(closes) < period + 1:
        return 50.0
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 2)

def _calc_sma(closes, period):
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period

def _calc_ema(closes, period):
    if len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema

def _calc_bollinger(closes, period=20, std_mult=2):
    if len(closes) < period:
        return None, None, None
    window = closes[-period:]
    mid = sum(window) / period
    variance = sum((p - mid) ** 2 for p in window) / period
    std = math.sqrt(variance)
    return mid - std_mult * std, mid, mid + std_mult * std

def _find_pivot_levels(highs, lows, closes, window=5):
    """Find pivot support/resistance levels from recent price history."""
    levels = []
    n = len(closes)
    for i in range(window, n - window):
        # Pivot low (support)
        if lows[i] == min(lows[i-window:i+window+1]):
            levels.append(('support', lows[i]))
        # Pivot high (resistance)
        if highs[i] == max(highs[i-window:i+window+1]):
            levels.append(('resistance', highs[i]))
    # Cluster nearby levels (within 1.5% of each other)
    clustered = []
    used = set()
    price_ref = closes[-1]
    for i, (kind, lvl) in enumerate(levels):
        if i in used:
            continue
        group = [lvl]
        for j, (k2, l2) in enumerate(levels):
            if j != i and j not in used and abs(lvl - l2) / price_ref < 0.015:
                group.append(l2)
                used.add(j)
        used.add(i)
        clustered.append((kind, round(sum(group) / len(group), 2), len(group)))
    # Sort by strength (hit count) and proximity to current price
    clustered.sort(key=lambda x: (-x[2], abs(x[1] - price_ref)))
    return clustered[:8]

def _calc_fibonacci(closes, highs, lows, lookback=60):
    """Fibonacci retracement of the most recent swing high→low."""
    if len(closes) < lookback:
        lookback = len(closes)
    seg_highs = highs[-lookback:]
    seg_lows  = lows[-lookback:]
    swing_high = max(seg_highs)
    swing_low  = min(seg_lows)
    diff = swing_high - swing_low
    if diff == 0:
        return {}
    return {
        '23.6': round(swing_high - 0.236 * diff, 2),
        '38.2': round(swing_high - 0.382 * diff, 2),
        '50.0': round(swing_high - 0.500 * diff, 2),
        '61.8': round(swing_high - 0.618 * diff, 2),
        '78.6': round(swing_high - 0.786 * diff, 2),
        'swing_high': round(swing_high, 2),
        'swing_low':  round(swing_low, 2),
    }

# ── Earnings Reaction History ─────────────────────────────────────────────────
def get_earnings_history(ticker):
    ticker = ticker.upper().strip()
    cache_key = f'earnings_hist_{ticker}'
    def fetch():
        try:
            t = yf.Ticker(ticker)
            # Quarterly earnings
            qe = t.quarterly_earnings
            hist = t.history(period='2y', interval='1d', auto_adjust=True)
            results = []
            if qe is not None and not qe.empty:
                idx = qe.index.tolist()
                for i, period_label in enumerate(idx[:8]):
                    row = qe.iloc[i]
                    eps_est   = round(float(row.get('Estimate', 0) or 0), 4)
                    eps_act   = round(float(row.get('Reported', 0) or 0), 4)
                    surprise  = round(float(row.get('Surprise', 0) or 0), 4)
                    surp_pct  = round(float(row.get('Surprise%', 0) or 0), 2)
                    # Try to get stock reaction on earnings date
                    reaction_pct = 0
                    try:
                        # period_label is e.g. '2024-09-30' or Quarter string
                        date_str = str(period_label)
                        if hasattr(period_label, 'strftime'):
                            date_str = period_label.strftime('%Y-%m-%d')
                        # Find closest trading day in history
                        hist_dates = [str(d)[:10] for d in hist.index]
                        if date_str in hist_dates:
                            di = hist_dates.index(date_str)
                        else:
                            # Find nearest date
                            from datetime import datetime as _dt
                            target = _dt.strptime(date_str[:10], '%Y-%m-%d')
                            nearest_i = min(range(len(hist_dates)),
                                key=lambda i: abs((_dt.strptime(hist_dates[i][:10],'%Y-%m-%d')-target).days))
                            di = nearest_i
                        if di + 1 < len(hist):
                            o = float(hist.iloc[di+1]['Open'])
                            c = float(hist.iloc[di+1]['Close'])
                            reaction_pct = round((c/o - 1)*100, 2) if o else 0
                        elif di > 0:
                            prev_c = float(hist.iloc[di-1]['Close'])
                            c      = float(hist.iloc[di]['Close'])
                            reaction_pct = round((c/prev_c - 1)*100, 2) if prev_c else 0
                    except: pass
                    results.append({
                        'period':       str(period_label)[:10],
                        'eps_estimate': eps_est,
                        'eps_actual':   eps_act,
                        'surprise':     surprise,
                        'surprise_pct': surp_pct,
                        'reaction_pct': reaction_pct,
                    })
            # Also get next earnings date from calendar
            next_date = ''
            try:
                cal = t.calendar
                if cal is not None and not cal.empty and 'Earnings Date' in cal.index:
                    nd = cal.loc['Earnings Date']
                    next_date = str(nd.iloc[0])[:10] if hasattr(nd,'iloc') else str(nd)[:10]
            except: pass
            name = ''
            try: name = t.info.get('shortName', ticker)
            except: pass
            return {'ticker': ticker, 'name': name, 'history': results, 'next_earnings': next_date}
        except Exception as e:
            return {'error': str(e), 'ticker': ticker, 'history': [], 'next_earnings': ''}
    return _from_cache(cache_key, fetch, ttl=3600)

# ── Insider Trading Tracker ───────────────────────────────────────────────────
def get_insider_trades(ticker):
    ticker = ticker.upper().strip()
    cache_key = f'insider_{ticker}'
    def fetch():
        try:
            t = yf.Ticker(ticker)
            trades = []
            # Try insider_transactions (newer yfinance)
            df = None
            try:
                df = t.insider_transactions
            except: pass
            if df is None or (hasattr(df,'empty') and df.empty):
                try:
                    df = t.get_insider_transactions()
                except: pass
            if df is not None and not (hasattr(df,'empty') and df.empty):
                for _, row in df.iterrows():
                    try:
                        date_val = row.get('Start Date', row.get('Date', ''))
                        if hasattr(date_val,'strftime'):
                            date_val = date_val.strftime('%Y-%m-%d')
                        else:
                            date_val = str(date_val)[:10]
                        shares_val = row.get('Shares', row.get('Value', 0))
                        value_val  = row.get('Value',  0)
                        txn_text   = str(row.get('Text', row.get('Transaction',''))).strip()
                        insider    = str(row.get('Insider', row.get('Name','')))
                        title      = str(row.get('Position', row.get('Title','')))
                        is_buy = any(kw in txn_text.upper() for kw in ['BUY','PURCHASE','ACQUI'])
                        trades.append({
                            'date':    date_val,
                            'insider': insider[:40],
                            'title':   title[:40],
                            'type':    'BUY' if is_buy else 'SELL',
                            'shares':  int(float(shares_val or 0)),
                            'value':   round(float(value_val or 0), 0),
                            'text':    txn_text[:80],
                        })
                    except: continue
            return {'ticker': ticker, 'trades': trades[:20]}
        except Exception as e:
            return {'error': str(e), 'ticker': ticker, 'trades': []}
    return _from_cache(cache_key, fetch, ttl=3600)

# ── Morning Briefing ──────────────────────────────────────────────────────────
def get_morning_briefing(holdings, market_data):
    cache_key = 'morning_brief_' + hashlib.md5(json.dumps(sorted([h['ticker'] for h in holdings])).encode()).hexdigest()
    def fetch():
        try:
            total_value = 0; total_cost = 0; gainers = []; losers = []
            movers_detail = []
            syms = [h['ticker'] for h in holdings if h.get('ticker')]
            if not syms:
                return {'error': 'No holdings'}
            q = get_quotes(syms)
            for h in holdings:
                sym   = h['ticker']
                data  = q.get(sym, {})
                price = data.get('regularMarketPrice', 0)
                pct   = data.get('regularMarketChangePercent', 0)
                shrs  = float(h.get('shares', 0))
                cost  = float(h.get('avg_cost', 0))
                val   = price * shrs
                total_value += val
                total_cost  += cost * shrs
                movers_detail.append({'ticker': sym, 'pct': pct, 'value': val,
                                       'name': data.get('shortName', sym)})
            movers_detail.sort(key=lambda x: x['pct'], reverse=True)
            gainers = [m for m in movers_detail if m['pct'] > 0][:3]
            losers  = [m for m in movers_detail if m['pct'] < 0][-3:]
            losers.reverse()
            total_gain = total_value - total_cost
            total_pct  = (total_gain / total_cost * 100) if total_cost else 0
            # Market summary
            mkt_up = 0; mkt_dn = 0
            for idx in (market_data or {}).get('indices', []):
                if idx.get('change', 0) > 0: mkt_up += 1
                else: mkt_dn += 1
            mkt_mood = 'Risk-On' if mkt_up >= 3 else ('Mixed' if mkt_up >= 2 else 'Risk-Off')
            # Build summary sentences
            today_str = datetime.now().strftime('%A, %B %d %Y')
            lines = []
            lines.append(f"Good morning! Today is {today_str}.")
            if total_cost > 0:
                sign = '+' if total_gain >= 0 else ''
                lines.append(f"Your portfolio is worth ${total_value:,.0f}, {sign}{total_pct:.1f}% overall return.")
            lines.append(f"Markets are showing a {mkt_mood} tone today ({mkt_up} indices up, {mkt_dn} down).")
            if gainers:
                g_str = ', '.join([f"{g['ticker']} (+{g['pct']:.1f}%)" for g in gainers])
                lines.append(f"Today's top movers in your portfolio: {g_str}.")
            if losers:
                l_str = ', '.join([f"{l['ticker']} ({l['pct']:.1f}%)" for l in losers])
                lines.append(f"Watching for weakness in: {l_str}.")
            return {
                'date':        today_str,
                'summary':     ' '.join(lines),
                'total_value': round(total_value, 2),
                'total_pct':   round(total_pct, 2),
                'gainers':     gainers,
                'losers':      losers,
                'mkt_mood':    mkt_mood,
                'mkt_up':      mkt_up,
                'mkt_dn':      mkt_dn,
            }
        except Exception as e:
            return {'error': str(e)}
    return _from_cache(cache_key, fetch, ttl=300)

# ── Portfolio Stress Test ─────────────────────────────────────────────────────
STRESS_SCENARIOS = {
    'covid':    {'label': 'COVID Crash (2020)', 'mkt_drop': -34, 'duration': '33 days', 'recovery': '~5 months'},
    'rate2022': {'label': '2022 Rate Hike Bear', 'mkt_drop': -25, 'duration': '12 months', 'recovery': '~14 months'},
    'gfc2008':  {'label': 'GFC 2008–09',        'mkt_drop': -57, 'duration': '17 months', 'recovery': '~4 years'},
    'dot2000':  {'label': 'Dot-com 2000–02',    'mkt_drop': -49, 'duration': '31 months', 'recovery': '~7 years'},
}

def get_stress_test(holdings):
    try:
        syms = [h['ticker'] for h in holdings if h.get('ticker')]
        if not syms:
            return {'error': 'No holdings'}
        q = get_quotes(syms)
        total_value = sum(
            q.get(h['ticker'],{}).get('regularMarketPrice',0) * float(h.get('shares',0))
            for h in holdings
        )
        if total_value <= 0:
            return {'error': 'Portfolio value is zero'}
        results = {}
        for scen_key, scen in STRESS_SCENARIOS.items():
            mkt_pct = scen['mkt_drop'] / 100
            impact_by_stock = []
            total_impact = 0
            for h in holdings:
                sym   = h['ticker']
                data  = q.get(sym, {})
                price = data.get('regularMarketPrice', 0)
                beta  = data.get('beta', 1.0) or 1.0
                shrs  = float(h.get('shares', 0))
                val   = price * shrs
                # Each stock drops by beta * market_drop (clipped at -80%)
                stock_drop_pct = max(beta * mkt_pct, -0.80)
                impact = val * stock_drop_pct
                impact_by_stock.append({
                    'ticker':     sym,
                    'name':       data.get('shortName', sym),
                    'beta':       round(beta, 2),
                    'value':      round(val, 2),
                    'drop_pct':   round(stock_drop_pct * 100, 1),
                    'impact':     round(impact, 2),
                })
                total_impact += impact
            impact_by_stock.sort(key=lambda x: x['impact'])
            survivor_value = total_value + total_impact
            results[scen_key] = {
                'label':          scen['label'],
                'mkt_drop':       scen['mkt_drop'],
                'duration':       scen['duration'],
                'recovery':       scen['recovery'],
                'portfolio_drop': round(total_impact / total_value * 100, 1),
                'estimated_loss': round(total_impact, 2),
                'survivor_value': round(survivor_value, 2),
                'worst_stocks':   impact_by_stock[:5],
            }
        return {'total_value': round(total_value, 2), 'scenarios': results}
    except Exception as e:
        return {'error': str(e)}

# ── Portfolio Backtesting ─────────────────────────────────────────────────────
def get_backtest(holdings, start_year=None):
    """Compare hypothetical buy-and-hold of current holdings since start_year."""
    if not holdings:
        return {'error': 'No holdings'}
    start_year = start_year or 2020
    start_date = f'{start_year}-01-01'
    cache_key  = f'backtest_{"_".join(sorted(h["ticker"] for h in holdings))}_{start_year}'
    def fetch():
        try:
            syms = [h['ticker'] for h in holdings if h.get('ticker')]
            if not syms:
                return {'error': 'No tickers'}
            # Fetch 1-day interval history for the period
            period_map = {2020:'5y', 2019:'6y', 2018:'7y', 2021:'4y', 2022:'3y', 2023:'2y'}
            period = period_map.get(start_year, '5y')
            # Get all tickers in parallel
            all_hist = {}
            def _one(sym):
                try:
                    h = yf.Ticker(sym).history(period=period, interval='1d', auto_adjust=True)
                    return sym, h
                except:
                    return sym, None
            with ThreadPoolExecutor(max_workers=_YF_WORKERS) as pool:
                for sym, hist in pool.map(_one, syms):
                    if hist is not None and not hist.empty:
                        all_hist[sym] = hist

            if not all_hist:
                return {'error': 'No historical data available'}

            # Find common start date across all tickers
            from datetime import datetime as _dt
            start_dt = _dt.strptime(start_date, '%Y-%m-%d')
            # Build equal-weight portfolio (1 share each, normalised)
            # Use portfolio weights based on current shares × price
            q = get_quotes(syms)
            total_cost = sum(
                float(h.get('shares',0)) * q.get(h['ticker'],{}).get('regularMarketPrice',0)
                for h in holdings
            )
            if total_cost <= 0:
                # Fall back to equal weight
                weights = {s: 1.0/len(syms) for s in syms}
            else:
                weights = {}
                for h in holdings:
                    val = float(h.get('shares',0)) * q.get(h['ticker'],{}).get('regularMarketPrice',0)
                    weights[h['ticker']] = val / total_cost if total_cost > 0 else 1.0/len(syms)

            # Build portfolio time series: weighted sum of normalised prices
            # Normalise each ticker to 100 at first available date
            series = {}
            for sym, hist in all_hist.items():
                dates = [str(d)[:10] for d in hist.index]
                closes = hist['Close'].tolist()
                # Filter to dates >= start_date
                pairs = [(d, c) for d, c in zip(dates, closes) if d >= start_date and c > 0]
                if not pairs:
                    continue
                base = pairs[0][1]
                series[sym] = {d: (c/base)*100 for d,c in pairs}

            if not series:
                return {'error': 'Insufficient historical data from start date'}

            # Get all unique dates sorted
            all_dates = sorted(set(d for s in series.values() for d in s.keys()))
            # Only monthly points to keep payload small
            monthly = []
            last_m = ''
            for d in all_dates:
                m = d[:7]  # YYYY-MM
                if m != last_m:
                    monthly.append(d)
                    last_m = m
            # Build portfolio value series
            port_series = []
            spy_series  = []
            # Fetch SPY for comparison
            try:
                spy_hist = yf.Ticker('SPY').history(period=period, interval='1d', auto_adjust=True)
                spy_dates  = [str(d)[:10] for d in spy_hist.index]
                spy_closes = spy_hist['Close'].tolist()
                spy_pairs = [(d,c) for d,c in zip(spy_dates, spy_closes) if d >= start_date and c > 0]
                spy_base = spy_pairs[0][1] if spy_pairs else 0
                spy_map = {d: (c/spy_base)*100 for d,c in spy_pairs} if spy_base > 0 else {}
            except:
                spy_map = {}

            for d in monthly:
                # weighted average of normalised prices
                val = 0
                total_w = 0
                for sym, s_map in series.items():
                    w = weights.get(sym, 1.0/len(series))
                    # find closest available date
                    if d in s_map:
                        val += s_map[d] * w
                        total_w += w
                    else:
                        # use last known price
                        avail = sorted(k for k in s_map if k <= d)
                        if avail:
                            val += s_map[avail[-1]] * w
                            total_w += w
                if total_w > 0:
                    port_series.append({'date': d, 'value': round(val/total_w, 2)})
                spy_val = spy_map.get(d)
                if not spy_val:
                    avail = sorted(k for k in spy_map if k <= d)
                    spy_val = spy_map[avail[-1]] if avail else None
                spy_series.append({'date': d, 'value': round(spy_val, 2) if spy_val else None})

            # Compute stats
            if port_series:
                start_val = port_series[0]['value']
                end_val   = port_series[-1]['value']
                total_ret = round(end_val - start_val, 2)
                total_pct = round((end_val/start_val - 1)*100, 2) if start_val else 0
                # CAGR
                years = len(monthly) / 12
                cagr  = round(((end_val/start_val)**(1/years) - 1)*100, 2) if years > 0 and start_val > 0 else 0
                # Max drawdown
                peak = start_val; max_dd = 0
                for pt in port_series:
                    if pt['value'] > peak: peak = pt['value']
                    dd = (pt['value'] - peak) / peak * 100 if peak > 0 else 0
                    if dd < max_dd: max_dd = dd
                # Best/worst ticker
                ticker_rets = {}
                for sym, s_map in series.items():
                    vals = sorted(s_map.items())
                    if len(vals) >= 2:
                        ticker_rets[sym] = round((vals[-1][1]/vals[0][1] - 1)*100, 1)
                best_ticker = max(ticker_rets, key=ticker_rets.get) if ticker_rets else None
                worst_ticker = min(ticker_rets, key=ticker_rets.get) if ticker_rets else None
            else:
                total_pct = cagr = max_dd = 0
                best_ticker = worst_ticker = None
                ticker_rets = {}

            spy_ret = 0
            if spy_series:
                s_vals = [p['value'] for p in spy_series if p['value']]
                if len(s_vals) >= 2:
                    spy_ret = round((s_vals[-1]/s_vals[0] - 1)*100, 2)

            return {
                'start_year':   start_year,
                'port_series':  port_series,
                'spy_series':   spy_series,
                'total_pct':    total_pct,
                'cagr':         cagr,
                'max_drawdown': round(max_dd, 2),
                'spy_total':    spy_ret,
                'ticker_rets':  ticker_rets,
                'best_ticker':  best_ticker,
                'worst_ticker': worst_ticker,
                'tickers':      list(series.keys()),
            }
        except Exception as e:
            return {'error': str(e)}
    return _from_cache(cache_key, fetch, ttl=3600)

# ── Economic Calendar ─────────────────────────────────────────────────────────
ECON_EVENTS = [
    # These are key recurring event types; actual dates come from yfinance earnings calendars + hardcoded patterns
    {'event': 'FOMC Meeting', 'category': 'fed', 'impact': 'high'},
    {'event': 'CPI Release', 'category': 'inflation', 'impact': 'high'},
    {'event': 'PCE Inflation', 'category': 'inflation', 'impact': 'high'},
    {'event': 'Jobs Report (NFP)', 'category': 'employment', 'impact': 'high'},
    {'event': 'GDP Estimate', 'category': 'growth', 'impact': 'high'},
    {'event': 'Retail Sales', 'category': 'consumer', 'impact': 'medium'},
    {'event': 'PPI Release', 'category': 'inflation', 'impact': 'medium'},
    {'event': 'Fed Chair Speech', 'category': 'fed', 'impact': 'high'},
    {'event': 'ISM Manufacturing', 'category': 'manufacturing', 'impact': 'medium'},
    {'event': 'Consumer Confidence', 'category': 'consumer', 'impact': 'medium'},
    {'event': 'Jobless Claims', 'category': 'employment', 'impact': 'medium'},
    {'event': 'Housing Starts', 'category': 'housing', 'impact': 'low'},
    {'event': 'Durable Goods Orders', 'category': 'manufacturing', 'impact': 'medium'},
]

def get_economic_calendar(user_tickers=None):
    """Returns upcoming economic events + earnings dates for portfolio tickers."""
    cache_key = f'econ_cal_{("|".join(sorted(user_tickers)) if user_tickers else "")}'
    def fetch():
        from datetime import datetime as _dt, timedelta as _td
        events = []
        today  = _dt.now()
        today_str = today.strftime('%Y-%m-%d')

        # Fetch earnings dates for user tickers
        if user_tickers:
            def _get_next_earnings(sym):
                try:
                    cal = yf.Ticker(sym).calendar
                    if cal is not None and not cal.empty and 'Earnings Date' in cal.index:
                        nd = cal.loc['Earnings Date']
                        d = str(nd.iloc[0])[:10] if hasattr(nd,'iloc') else str(nd)[:10]
                        if d >= today_str:
                            return {'date': d, 'event': f'{sym} Earnings', 'category': 'earnings',
                                    'impact': 'high', 'ticker': sym}
                except: pass
                return None
            with ThreadPoolExecutor(max_workers=min(_YF_WORKERS, len(user_tickers))) as pool:
                for result in pool.map(_get_next_earnings, user_tickers):
                    if result: events.append(result)

        # Add approximate upcoming macro events (simplified — real dates shift monthly)
        # We generate approximate dates for recurring events
        import calendar as _cal
        # First Friday of each month = NFP
        # Second/third Tues/Wed of certain months = FOMC
        for month_offset in range(0, 3):
            m = (today.month + month_offset - 1) % 12 + 1
            y = today.year + ((today.month + month_offset - 1) // 12)
            # First Friday = NFP
            first_day = _dt(y, m, 1)
            days_to_fri = (4 - first_day.weekday()) % 7
            nfp_date = first_day + _td(days=days_to_fri)
            if str(nfp_date)[:10] >= today_str:
                events.append({'date': str(nfp_date)[:10], 'event': 'Jobs Report (NFP)',
                                'category': 'employment', 'impact': 'high', 'ticker': None})
            # ~12th of month = CPI
            cpi_date = _dt(y, m, 12)
            if str(cpi_date)[:10] >= today_str:
                events.append({'date': str(cpi_date)[:10], 'event': 'CPI Release',
                                'category': 'inflation', 'impact': 'high', 'ticker': None})
            # ~28th = PCE
            last_day = _cal.monthrange(y, m)[1]
            pce_day  = min(28, last_day)
            pce_date = _dt(y, m, pce_day)
            if str(pce_date)[:10] >= today_str:
                events.append({'date': str(pce_date)[:10], 'event': 'PCE Inflation',
                                'category': 'inflation', 'impact': 'high', 'ticker': None})
            # Retail Sales ~15th
            rs_date = _dt(y, m, 15)
            if str(rs_date)[:10] >= today_str:
                events.append({'date': str(rs_date)[:10], 'event': 'Retail Sales',
                                'category': 'consumer', 'impact': 'medium', 'ticker': None})
            # Jobless Claims = every Thursday (first Thursday of month shown)
            first_day = _dt(y, m, 1)
            days_to_thu = (3 - first_day.weekday()) % 7
            thu_date = first_day + _td(days=days_to_thu)
            if str(thu_date)[:10] >= today_str:
                events.append({'date': str(thu_date)[:10], 'event': 'Jobless Claims',
                                'category': 'employment', 'impact': 'medium', 'ticker': None})

        # Sort and deduplicate
        seen = set()
        unique = []
        for e in sorted(events, key=lambda x: x['date']):
            key = (e['date'], e['event'])
            if key not in seen:
                seen.add(key)
                unique.append(e)
        return {'events': unique[:40]}
    return _from_cache(cache_key, fetch, ttl=3600)

# ═══════════════════════════════════════════════════════════════
#  STOCK STATISTICS  (RSI, MA50/200, valuation, float, margins)
# ═══════════════════════════════════════════════════════════════
def get_stock_statistics(ticker):
    ticker = ticker.upper().strip()
    cache_key = f'stats_{ticker}'
    def fetch():
        try:
            t = yf.Ticker(ticker)
            info = t.info or {}
            hist2y = t.history(period='2y', interval='1d', auto_adjust=True)
            closes = hist2y['Close'].dropna() if not hist2y.empty else pd.Series([], dtype=float)

            rsi14 = None
            ma50 = ma200 = None
            if len(closes) >= 15:
                delta = closes.diff()
                gain  = delta.clip(lower=0).rolling(14).mean()
                loss  = (-delta.clip(upper=0)).rolling(14).mean()
                rs    = gain / loss.replace(0, float('nan'))
                rsi_s = 100 - (100 / (1 + rs))
                rsi14 = round(float(rsi_s.iloc[-1]), 2) if not pd.isna(rsi_s.iloc[-1]) else None
            if len(closes) >= 50:
                ma50 = round(float(closes.rolling(50).mean().iloc[-1]), 2)
            if len(closes) >= 200:
                ma200 = round(float(closes.rolling(200).mean().iloc[-1]), 2)

            def _f(v):
                try: return float(v) if v is not None else None
                except: return None
            def _pct(v):
                try: return round(float(v) * 100, 2) if v is not None else None
                except: return None

            return {
                'valuation': {
                    'marketCap':         info.get('marketCap'),
                    'enterpriseValue':   info.get('enterpriseValue'),
                    'peRatio':           _f(info.get('trailingPE')),
                    'forwardPE':         _f(info.get('forwardPE')),
                    'pbRatio':           _f(info.get('priceToBook')),
                    'psRatio':           _f(info.get('priceToSalesTrailing12Months')),
                    'evEbitda':          _f(info.get('enterpriseToEbitda')),
                    'evRevenue':         _f(info.get('enterpriseToRevenue')),
                    'pegRatio':          _f(info.get('pegRatio')),
                },
                'shares': {
                    'sharesOutstanding': info.get('sharesOutstanding'),
                    'floatShares':       info.get('floatShares'),
                    'sharesShort':       info.get('sharesShort'),
                    'shortRatio':        _f(info.get('shortRatio')),
                    'shortPctFloat':     _pct(info.get('shortPercentOfFloat')),
                    'heldByInsiders':    _pct(info.get('heldPercentInsiders')),
                    'heldByInstitutions':_pct(info.get('heldPercentInstitutions')),
                    'sharesChangeYoY':   _pct(info.get('sharesPercentSharesOut')),
                },
                'price': {
                    'rsi14':   rsi14,
                    'ma50':    ma50,
                    'ma200':   ma200,
                    'beta':    _f(info.get('beta')),
                    'w52High': _f(info.get('fiftyTwoWeekHigh')),
                    'w52Low':  _f(info.get('fiftyTwoWeekLow')),
                    'avgVolume20d': info.get('averageVolume'),
                    'avgVolume10d': info.get('averageVolume10days'),
                    'w52Change':    _pct(info.get('52WeekChange')),
                },
                'dividends': {
                    'dividendRate':        _f(info.get('dividendRate')),
                    'dividendYield':       _pct(info.get('dividendYield')),
                    'exDividendDate':      str(pd.Timestamp(info['exDividendDate'], unit='s').date()) if info.get('exDividendDate') else None,
                    'payoutRatio':         _pct(info.get('payoutRatio')),
                    'fiveYearAvgYield':    _f(info.get('fiveYearAvgDividendYield')),
                    'trailingAnnualYield': _pct(info.get('trailingAnnualDividendYield')),
                },
                'income': {
                    'revenue':          info.get('totalRevenue'),
                    'netIncome':        info.get('netIncomeToCommon'),
                    'eps':              _f(info.get('trailingEps')),
                    'forwardEps':       _f(info.get('forwardEps')),
                    'ebitda':           info.get('ebitda'),
                    'grossMargin':      _pct(info.get('grossMargins')),
                    'operatingMargin':  _pct(info.get('operatingMargins')),
                    'profitMargin':     _pct(info.get('profitMargins')),
                    'revenueGrowth':    _pct(info.get('revenueGrowth')),
                    'earningsGrowth':   _pct(info.get('earningsGrowth')),
                    'freeCashflow':     info.get('freeCashflow'),
                    'operatingCashflow':info.get('operatingCashflow'),
                    'returnOnEquity':   _pct(info.get('returnOnEquity')),
                    'returnOnAssets':   _pct(info.get('returnOnAssets')),
                    'debtToEquity':     _f(info.get('debtToEquity')),
                    'currentRatio':     _f(info.get('currentRatio')),
                    'quickRatio':       _f(info.get('quickRatio')),
                },
            }
        except Exception as e:
            return {'error': str(e)}
    return _from_cache(cache_key, fetch, ttl=3600)


# ═══════════════════════════════════════════════════════════════
#  DIVIDEND HISTORY
# ═══════════════════════════════════════════════════════════════
def get_dividend_history(ticker):
    ticker = ticker.upper().strip()
    cache_key = f'divhist_{ticker}'
    def fetch():
        try:
            t = yf.Ticker(ticker)
            divs = t.dividends
            if divs is None or divs.empty:
                return {'dividends': []}
            result = []
            for date, amount in divs.items():
                result.append({'date': str(date.date()), 'amount': round(float(amount), 4)})
            result.reverse()  # most recent first
            return {'dividends': result}
        except Exception as e:
            return {'dividends': [], 'error': str(e)}
    return _from_cache(cache_key, fetch, ttl=3600)


# ═══════════════════════════════════════════════════════════════
#  PRICE HISTORY  (OHLCV table)
# ═══════════════════════════════════════════════════════════════
def get_price_history(ticker, period='6m'):
    ticker = ticker.upper().strip()
    cache_key = f'pricehist_{ticker}_{period}'
    period_map = {'1m':'1mo','3m':'3mo','ytd':'ytd','6m':'6mo','1y':'1y','5y':'5y','max':'max'}
    yf_period = period_map.get(period, '6mo')
    def fetch():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period=yf_period, interval='1d', auto_adjust=True)
            if hist.empty:
                return {'history': []}
            rows = []
            prev_close = None
            for date, row in hist.iterrows():
                close = round(float(row['Close']), 2)
                chg_pct = round((close / prev_close - 1) * 100, 2) if prev_close else 0
                rows.append({
                    'date':    str(date.date()),
                    'open':    round(float(row['Open']),   2),
                    'high':    round(float(row['High']),   2),
                    'low':     round(float(row['Low']),    2),
                    'close':   close,
                    'volume':  int(row['Volume']),
                    'change':  chg_pct,
                })
                prev_close = close
            rows.reverse()
            return {'history': rows}
        except Exception as e:
            return {'history': [], 'error': str(e)}
    return _from_cache(cache_key, fetch, ttl=300)


# ═══════════════════════════════════════════════════════════════
#  COMPANY PROFILE  (officers, address, employees)
# ═══════════════════════════════════════════════════════════════
def get_stock_profile(ticker):
    ticker = ticker.upper().strip()
    cache_key = f'profile_{ticker}'
    def fetch():
        try:
            t = yf.Ticker(ticker)
            info = t.info or {}
            officers = []
            for o in info.get('companyOfficers', [])[:10]:
                pay = o.get('totalPay') or o.get('exercisedValue')
                officers.append({
                    'name':  o.get('name', ''),
                    'title': o.get('title', ''),
                    'age':   o.get('age'),
                    'pay':   int(pay) if pay else None,
                })
            addr_parts = [info.get('address1',''), info.get('address2',''),
                          info.get('city',''), info.get('state',''), info.get('country','')]
            address = ', '.join(p for p in addr_parts if p)
            return {
                'name':        info.get('longName', ticker),
                'website':     info.get('website', ''),
                'phone':       info.get('phone', ''),
                'address':     address,
                'employees':   info.get('fullTimeEmployees'),
                'exchange':    info.get('exchange', ''),
                'currency':    info.get('currency', 'USD'),
                'sector':      info.get('sector', ''),
                'industry':    info.get('industry', ''),
                'description': info.get('longBusinessSummary', ''),
                'officers':    officers,
            }
        except Exception as e:
            return {'error': str(e)}
    return _from_cache(cache_key, fetch, ttl=86400)


# ═══════════════════════════════════════════════════════════════
#  STOCK COMPARISON
# ═══════════════════════════════════════════════════════════════
def get_stock_comparison(tickers, period='1y'):
    tickers = [t.upper().strip() for t in tickers if t.strip()][:5]
    period_map = {'1m':'1mo','3m':'3mo','ytd':'ytd','6m':'6mo','1y':'1y','5y':'5y','max':'max'}
    yf_period = period_map.get(period, '1y')
    cache_key = f"compare_{'_'.join(sorted(tickers))}_{period}"
    def fetch():
        from concurrent.futures import ThreadPoolExecutor
        def one(sym):
            try:
                t = yf.Ticker(sym)
                info = t.info or {}
                hist = t.history(period=yf_period, interval='1d', auto_adjust=True)
                if hist.empty:
                    return sym, None
                closes = hist['Close'].dropna()
                first = float(closes.iloc[0])
                chart_pts   = [round(float(v)/first*100, 2) for v in closes]
                chart_dates = [str(d.date()) for d in closes.index]
                def _f(v):
                    try: return float(v) if v is not None else None
                    except: return None
                return sym, {
                    'name':          info.get('shortName', sym),
                    'price':         round(float(closes.iloc[-1]), 2),
                    'totalReturn':   round((float(closes.iloc[-1])/first - 1)*100, 2),
                    'marketCap':     info.get('marketCap'),
                    'peRatio':       _f(info.get('trailingPE')),
                    'forwardPE':     _f(info.get('forwardPE')),
                    'dividendYield': round(float(info.get('dividendYield') or 0)*100, 2),
                    'revenue':       info.get('totalRevenue'),
                    'netMargin':     round(float(info.get('profitMargins') or 0)*100, 2),
                    'revenueGrowth': round(float(info.get('revenueGrowth') or 0)*100, 2),
                    'beta':          _f(info.get('beta')),
                    'sector':        info.get('sector', ''),
                    'chartPts':      chart_pts,
                    'chartDates':    chart_dates,
                }
            except:
                return sym, None
        results = {}
        with ThreadPoolExecutor(max_workers=5) as ex:
            for sym, data in ex.map(one, tickers):
                if data:
                    results[sym] = data
        # Align to shortest date series
        min_len = min((len(d['chartDates']) for d in results.values()), default=0)
        dates = []
        for sym in results:
            d = results[sym]
            d['chartPts']   = d['chartPts'][-min_len:]   if min_len else []
            d['chartDates'] = d['chartDates'][-min_len:] if min_len else []
            if not dates and d['chartDates']:
                dates = d['chartDates']
        return {'stocks': results, 'dates': dates}
    return _from_cache(cache_key, fetch, ttl=1800)


# ── New feature backend functions ─────────────────────────────────────────────

def get_financials_extended(ticker, period='annual'):
    """Financials with Annual/Quarterly/TTM toggle + Ratios historical tab."""
    ticker = ticker.upper().strip()
    cache_key = f"fin_ext:{ticker}:{period}"
    def fetch():
        import math
        t = yf.Ticker(ticker)
        info = t.info or {}
        def _f(v):
            try: return float(v) if v is not None and not (isinstance(v, float) and math.isnan(v)) else None
            except: return None
        def sv(df, metric, col):
            try:
                if metric in df.index:
                    v = df.loc[metric, col]
                    return _f(v)
                return None
            except: return None
        def sv2(df, col, *metrics):
            for m in metrics:
                v = sv(df, m, col)
                if v is not None: return v
            return None

        result = {'income': [], 'balance': [], 'cashflow': [], 'ratios': []}

        if period == 'quarterly':
            inc_df  = getattr(t, 'quarterly_income_stmt', None) or getattr(t, 'quarterly_financials', None)
            bal_df  = getattr(t, 'quarterly_balance_sheet', None) or getattr(t, 'quarterly_balancesheet', None)
            cf_df   = getattr(t, 'quarterly_cashflow', None) or getattr(t, 'quarterly_cash_flow', None)
            label_fn = lambda col: (str(col.date()) if hasattr(col,'date') else str(col)[:10])
            n_cols = 8
        else:  # annual
            inc_df  = getattr(t, 'income_stmt', None) or getattr(t, 'financials', None)
            bal_df  = getattr(t, 'balance_sheet', None) or getattr(t, 'balancesheet', None)
            cf_df   = getattr(t, 'cashflow', None) or getattr(t, 'cash_flow', None)
            label_fn = lambda col: (str(col.year) if hasattr(col,'year') else str(col)[:4])
            n_cols = 5

        # ── Income Statement ──────────────────────────────────────────────
        if inc_df is not None and not inc_df.empty:
            cols = list(inc_df.columns)[:n_cols]
            prev_rev = None
            for col in cols:
                lbl = label_fn(col)
                rev = sv2(inc_df, col, "Total Revenue", "Revenue")
                gp  = sv2(inc_df, col, "Gross Profit")
                oi  = sv2(inc_df, col, "Operating Income", "EBIT")
                ni  = sv2(inc_df, col, "Net Income", "Net Income Common Stockholders")
                eb  = sv2(inc_df, col, "EBITDA", "Normalized EBITDA")
                eps = sv2(inc_df, col, "Basic EPS", "Diluted EPS")
                rev_growth = round((rev/prev_rev - 1)*100, 1) if (rev and prev_rev and prev_rev != 0) else None
                prev_rev = rev
                result['income'].append({
                    'label': lbl, 'revenue': rev, 'gross_profit': gp,
                    'operating_income': oi, 'net_income': ni,
                    'ebitda': eb, 'eps': eps, 'rev_growth': rev_growth
                })

        # ── Balance Sheet ─────────────────────────────────────────────────
        if bal_df is not None and not bal_df.empty:
            cols = list(bal_df.columns)[:n_cols]
            for col in cols:
                lbl = label_fn(col)
                result['balance'].append({
                    'label': lbl,
                    'total_assets':       sv2(bal_df, col, "Total Assets"),
                    'total_liabilities':  sv2(bal_df, col, "Total Liabilities Net Minority Interest", "Total Liab"),
                    'stockholders_equity':sv2(bal_df, col, "Stockholders Equity", "Total Stockholder Equity"),
                    'cash':               sv2(bal_df, col, "Cash And Cash Equivalents", "Cash"),
                    'total_debt':         sv2(bal_df, col, "Total Debt", "Long Term Debt"),
                    'current_assets':     sv2(bal_df, col, "Current Assets", "Total Current Assets"),
                    'current_liabilities':sv2(bal_df, col, "Current Liabilities", "Total Current Liabilities"),
                })

        # ── Cash Flow ─────────────────────────────────────────────────────
        if cf_df is not None and not cf_df.empty:
            cols = list(cf_df.columns)[:n_cols]
            for col in cols:
                lbl = label_fn(col)
                result['cashflow'].append({
                    'label': lbl,
                    'operating':   sv2(cf_df, col, "Operating Cash Flow", "Total Cash From Operating Activities"),
                    'investing':   sv2(cf_df, col, "Investing Cash Flow", "Total Cash From Investing Activities"),
                    'financing':   sv2(cf_df, col, "Financing Cash Flow", "Total Cash From Financing Activities"),
                    'free_cf':     sv2(cf_df, col, "Free Cash Flow"),
                    'capex':       sv2(cf_df, col, "Capital Expenditure", "Capital Expenditures"),
                    'dividends_paid': sv2(cf_df, col, "Common Stock Dividend Paid", "Payment Of Dividends"),
                })

        # ── Ratios (historical) ───────────────────────────────────────────
        # Use annual income + balance for ratio computation
        try:
            ri = getattr(t, 'income_stmt', None) or getattr(t, 'financials', None)
            rb = getattr(t, 'balance_sheet', None) or getattr(t, 'balancesheet', None)
            hist2y = t.history(period='2y', interval='1mo', auto_adjust=True)
            month_closes = hist2y['Close'] if not hist2y.empty else None

            if ri is not None and not ri.empty:
                annual_cols = list(ri.columns)[:5]
                for col in annual_cols:
                    yr = str(col.year) if hasattr(col,'year') else str(col)[:4]
                    rev = sv2(ri, col, "Total Revenue", "Revenue")
                    ni  = sv2(ri, col, "Net Income", "Net Income Common Stockholders")
                    shares = _f(info.get('sharesOutstanding'))
                    mc  = _f(info.get('marketCap'))
                    bv  = None
                    if rb is not None and not rb.empty and col in rb.columns:
                        bv = sv2(rb, col, "Stockholders Equity", "Total Stockholder Equity")
                    pe  = None
                    ps  = None
                    pb  = None
                    if mc and rev and rev != 0: ps = round(mc/rev, 2)
                    if mc and bv and bv != 0:  pb = round(mc/bv, 2)
                    if mc and ni and ni != 0:  pe = round(mc/ni, 2)
                    ev = _f(info.get('enterpriseValue'))
                    ebitda_v = sv2(ri, col, "EBITDA", "Normalized EBITDA")
                    ev_ebitda = round(ev/ebitda_v, 2) if (ev and ebitda_v and ebitda_v != 0) else None
                    result['ratios'].append({
                        'label': yr, 'pe': pe, 'ps': ps, 'pb': pb,
                        'ev_ebitda': ev_ebitda
                    })
        except Exception as e:
            pass

        return result
    return _from_cache(cache_key, fetch, ttl=3600)


def get_stock_screener(filters=None):
    """Screen S&P 500 + Russell 1000 universe with optional filters."""
    cache_key = "screener:sp500"
    def fetch():
        import math
        # Hardcoded S&P 500 representative list — no external scraping
        tickers = [
            'AAPL','MSFT','NVDA','AMZN','GOOGL','META','TSLA','BRK-B','JPM','V',
            'JNJ','PG','HD','MA','UNH','ABBV','MRK','CVX','PEP','KO','WMT','BAC',
            'AVGO','LLY','XOM','TMO','COST','MCD','DIS','ADBE','CRM','NFLX','AMD',
            'INTC','QCOM','TXN','HON','PM','CAT','GE','IBM','GS','AXP','SPGI','BLK',
            'RTX','DE','MMM','T','VZ','WFC','CL','AMGN','GILD','REGN','ISRG',
            'BKNG','CHTR','CMCSA','TMUS','EA','TTWO','VZ','NWSA','WBD',
            'F','GM','NKE','SBUX','LOW','TJX','MAR','HLT','ORLY','AZO','ROST',
            'KMB','SYY','ADM','MO','MDLZ','GIS','K','MKC',
            'DHR','BMY','SYK','BDX','MDT','EW','HCA','CI','CVS','HUM',
            'CB','MMC','PGR','TRV','AON','MET','PRU','AIG','ALL','USB','TFC','PNC','SCHW',
            'UNP','BA','RTX','LMT','FDX','UPS','ETN','EMR','ITW','PH','GD','NOC','CARR',
            'SLB','EOG','MPC','PSX','VLO','OXY','DVN','HES','HAL','KMI','WMB',
            'NEE','DUK','SO','AEP','EXC','SRE','XEL','WEC','AWK',
            'PLD','AMT','EQIX','CCI','PSA','SPG','WELL','DLR','O',
            'LIN','APD','ECL','SHW','FCX','NEM','NUE','VMC','ALB','PPG',
            'PLTR','COIN','UBER','SNAP','RBLX','HOOD','DKNG','ROKU','SOFI','ARM',
        ]
        from concurrent.futures import ThreadPoolExecutor
        import math
        def one(sym):
            try:
                info = yf.Ticker(sym).info or {}
                def _f(k):
                    v = info.get(k)
                    try: return round(float(v),4) if v is not None and not (isinstance(v,float) and math.isnan(v)) else None
                    except: return None
                price = _f('currentPrice') or _f('regularMarketPrice')
                chg   = _f('regularMarketChangePercent')
                if chg is None:
                    prev = _f('regularMarketPreviousClose') or _f('previousClose')
                    chg  = round((price/prev-1)*100,2) if (price and prev and prev!=0) else None
                return {
                    'ticker':   sym,
                    'name':     info.get('shortName',''),
                    'sector':   info.get('sector',''),
                    'industry': info.get('industry',''),
                    'price':    price,
                    'change':   chg,
                    'marketCap':info.get('marketCap'),
                    'pe':       _f('trailingPE'),
                    'forwardPe':_f('forwardPE'),
                    'pb':       _f('priceToBook'),
                    'ps':       _f('priceToSalesTrailing12Months'),
                    'divYield': round(float(info.get('dividendYield') or 0)*100,2),
                    'revenue':  info.get('totalRevenue'),
                    'revGrowth':round(float(info.get('revenueGrowth') or 0)*100,2) if info.get('revenueGrowth') is not None else None,
                    'epsGrowth':round(float(info.get('earningsGrowth') or 0)*100,2) if info.get('earningsGrowth') is not None else None,
                    'beta':     _f('beta'),
                    'w52High':  _f('fiftyTwoWeekHigh'),
                    'w52Low':   _f('fiftyTwoWeekLow'),
                    'volume':   info.get('volume'),
                    'avgVol':   info.get('averageVolume'),
                    'employees':info.get('fullTimeEmployees'),
                }
            except: return None
        stocks = []
        with ThreadPoolExecutor(max_workers=8) as ex:
            for r in ex.map(one, tickers):
                if r and r.get('price'): stocks.append(r)
        return {'stocks': stocks, 'count': len(stocks)}
    return _from_cache(cache_key, fetch, ttl=3600)


def get_market_heatmap():
    """S&P 500 sector heatmap — one batch yf.download() call, hardcoded approx market caps."""
    cache_key = "heatmap:sp500"

    # Hardcoded sector map with approximate market-cap tiers (B = billions)
    # Tiers: 3=mega(>500B), 2=large(100-500B), 1=mid(10-100B), 0=small(<10B)
    SP500_SECTORS = {
        'Technology': [
            ('AAPL',3),('MSFT',3),('NVDA',3),('AVGO',3),('AMD',2),('QCOM',2),('TXN',2),
            ('INTC',2),('MU',2),('AMAT',2),('LRCX',2),('KLAC',2),('ADI',2),('MCHP',1),
            ('CDNS',2),('SNPS',2),('FTNT',2),('PANW',2),('CRWD',2),('MPWR',1),
            ('SWKS',1),('QRVO',1),('TER',1),('ENPH',1),('KEYS',1),
        ],
        'Communication Services': [
            ('GOOGL',3),('META',3),('NFLX',2),('DIS',2),('CMCSA',2),('VZ',2),
            ('T',2),('TMUS',2),('CHTR',2),('EA',1),('TTWO',1),('OMC',1),
            ('IPG',1),('MTCH',1),('LYV',1),('WBD',1),
        ],
        'Consumer Cyclical': [
            ('AMZN',3),('TSLA',3),('HD',2),('MCD',2),('NKE',2),('SBUX',2),('LOW',2),
            ('TJX',2),('BKNG',2),('MAR',2),('F',1),('GM',1),('ORLY',2),('AZO',2),
            ('LEN',1),('PHM',1),('DHI',1),('ROST',2),('ULTA',1),('HLT',2),
            ('WYNN',1),('MGM',1),('LVS',1),('EBAY',1),('DKNG',1),
        ],
        'Consumer Defensive': [
            ('WMT',3),('PG',3),('KO',3),('PEP',3),('COST',3),('PM',2),('MO',2),
            ('MDLZ',2),('CL',2),('GIS',1),('KMB',1),('SYY',1),('ADM',1),
            ('K',1),('MKC',1),('CHD',1),('CLX',1),
        ],
        'Healthcare': [
            ('UNH',3),('JNJ',3),('LLY',3),('ABBV',3),('MRK',3),('TMO',2),('ABT',2),
            ('DHR',2),('BMY',2),('AMGN',2),('GILD',2),('ISRG',2),('VRTX',2),
            ('REGN',2),('SYK',2),('BDX',2),('MDT',2),('EW',2),('HCA',2),
            ('CI',2),('CVS',2),('HUM',2),('IDXX',2),('IQV',2),
        ],
        'Financials': [
            ('JPM',3),('BAC',2),('WFC',2),('GS',2),('MS',2),('AXP',2),('SPGI',2),
            ('BLK',2),('CB',2),('MMC',2),('PGR',2),('TRV',2),('AON',2),('MET',1),
            ('PRU',1),('AIG',1),('ALL',1),('USB',1),('TFC',1),('PNC',1),
            ('SCHW',2),('BX',2),('KKR',2),
        ],
        'Industrials': [
            ('GE',3),('CAT',2),('HON',2),('UNP',2),('BA',2),('RTX',2),('LMT',2),
            ('DE',2),('UPS',2),('FDX',2),('ETN',2),('EMR',2),('ITW',2),('PH',2),
            ('GD',2),('NOC',2),('TDG',2),('CARR',2),('OTIS',1),('CTAS',2),
            ('WM',2),('RSG',1),('FAST',1),
        ],
        'Energy': [
            ('XOM',3),('CVX',3),('COP',2),('SLB',2),('EOG',2),('MPC',2),('PSX',2),
            ('VLO',2),('OXY',2),('DVN',1),('HES',1),('HAL',1),('BKR',1),
            ('APA',1),('MRO',1),('EQT',1),('KMI',2),('WMB',2),
        ],
        'Utilities': [
            ('NEE',2),('DUK',2),('SO',2),('D',1),('AEP',1),('EXC',1),('SRE',1),
            ('XEL',1),('WEC',1),('ES',1),('AWK',1),('ATO',1),('CNP',1),
        ],
        'Real Estate': [
            ('PLD',2),('AMT',2),('EQIX',2),('CCI',2),('PSA',2),('SPG',2),('WELL',2),
            ('DLR',2),('O',1),('AVB',1),('EQR',1),('INVH',1),('VTR',1),
            ('BXP',1),('KIM',1),
        ],
        'Basic Materials': [
            ('LIN',3),('APD',2),('ECL',2),('SHW',2),('FCX',2),('NEM',2),('NUE',1),
            ('VMC',1),('MLM',1),('ALB',1),('PPG',1),('LYB',1),
        ],
    }

    # Approximate market caps in billions by tier (used for sizing tiles)
    TIER_MC = {3: 800e9, 2: 200e9, 1: 40e9, 0: 8e9}

    def fetch():
        import pandas as pd
        # Collect all symbols
        all_syms = [sym for syms in SP500_SECTORS.values() for sym, _ in syms]

        print(f"  🗺  Heatmap: batch downloading {len(all_syms)} symbols…")
        raw = None
        try:
            # One batch call — returns a DataFrame with MultiIndex columns (Ticker, OHLCV)
            raw = yf.download(
                tickers=' '.join(all_syms),
                period='5d',
                interval='1d',
                group_by='ticker',
                auto_adjust=True,
                progress=False,
                threads=True,
            )
        except Exception as e:
            print(f"  Heatmap download error: {e}")

        # Extract last close and previous close per ticker
        price_map = {}
        if raw is not None and not raw.empty:
            try:
                # Multi-ticker download: columns are a MultiIndex (Ticker, OHLCV field)
                if isinstance(raw.columns, pd.MultiIndex):
                    tickers_in = raw.columns.get_level_values(0).unique()
                    for sym in tickers_in:
                        try:
                            closes = raw[sym]['Close'].dropna()
                            if len(closes) >= 2:
                                price_map[sym] = (float(closes.iloc[-1]), float(closes.iloc[-2]))
                            elif len(closes) == 1:
                                price_map[sym] = (float(closes.iloc[0]), float(closes.iloc[0]))
                        except: pass
                else:
                    # Single ticker fallback (shouldn't happen with multi-ticker list)
                    closes = raw['Close'].dropna()
                    if len(closes) >= 1:
                        sym = all_syms[0]
                        prev = float(closes.iloc[-2]) if len(closes) >= 2 else float(closes.iloc[-1])
                        price_map[sym] = (float(closes.iloc[-1]), prev)
            except Exception as e:
                print(f"  Heatmap parse error: {e}")
        else:
            print("  Heatmap: download returned empty — using tier-based placeholders")

        sectors = {}
        for sec, syms in SP500_SECTORS.items():
            sector_stocks = []
            for sym, tier in syms:
                p = price_map.get(sym)
                price = p[0] if p else 0
                prev  = p[1] if p else price
                chg   = round((price / prev - 1) * 100, 2) if prev and prev != 0 else 0.0
                mc    = TIER_MC.get(tier, 40e9)
                sector_stocks.append({
                    'ticker': sym,
                    'name':   sym,
                    'sector': sec,
                    'change': chg,
                    'marketCap': mc,
                    'price': round(price, 2),
                })
            sector_stocks.sort(key=lambda x: x['marketCap'], reverse=True)
            sectors[sec] = sector_stocks

        return {'sectors': sectors}
    return _from_cache(cache_key, fetch, ttl=600)


def get_ipo_list():
    """Recent + upcoming IPOs via yfinance search / hardcoded recent list."""
    cache_key = "ipo:list"
    def fetch():
        # yfinance doesn't have an IPO endpoint; use a curated recent list
        # In production this would pull from a data provider
        import datetime
        recent = [
            {'ticker':'RDDT','name':'Reddit','date':'2024-03-21','ipoPrice':34.0,'sector':'Technology'},
            {'ticker':'ASTERA','name':'Astera Labs','date':'2024-03-20','ipoPrice':36.0,'sector':'Technology'},
            {'ticker':'EMCOR','name':'EMCOR Group','date':'2023-01-01','ipoPrice':None,'sector':'Industrials'},
        ]
        enriched = []
        for item in recent:
            try:
                info = yf.Ticker(item['ticker']).info or {}
                price = info.get('currentPrice') or info.get('regularMarketPrice')
                ipo_p = item.get('ipoPrice')
                ret   = round((price/ipo_p - 1)*100, 2) if (price and ipo_p and ipo_p>0) else None
                enriched.append({**item, 'currentPrice': price, 'return': ret,
                                 'marketCap': info.get('marketCap')})
            except:
                enriched.append(item)
        return {'ipos': enriched}
    return _from_cache(cache_key, fetch, ttl=3600)


def get_trending_stocks():
    """Most-watched / trending stocks via batch yf.download() + 24h cached metadata."""
    cache_key = "trending:stocks"
    def fetch():
        import pandas as pd
        popular = ['NVDA','TSLA','AAPL','AMD','PLTR','AMZN','META','MSFT','GOOGL','NFLX',
                   'SPY','QQQ','SOFI','RIVN','F','GM','INTC','BAC','GS','JPM',
                   'SMCI','ARM','MSTR','COIN','HOOD','RBLX','UBER','LYFT','SNOW','DDOG']
        _meta_ttl = 3600 * 24
        price_map = {}
        try:
            raw = yf.download(
                tickers=' '.join(popular),
                period='5d', interval='1d',
                group_by='ticker', auto_adjust=True,
                progress=False, threads=False,
            )
            if raw is not None and not raw.empty and isinstance(raw.columns, pd.MultiIndex):
                for sym in raw.columns.get_level_values(0).unique():
                    try:
                        closes = raw[sym]['Close'].dropna()
                        if len(closes) < 1: continue
                        price = float(closes.iloc[-1])
                        prev  = float(closes.iloc[-2]) if len(closes) >= 2 else price
                        vols  = raw[sym]['Volume'].dropna()
                        price_map[sym] = {
                            'price':  round(price, 4),
                            'change': round((price / prev - 1) * 100, 2) if prev else 0.0,
                            'volume': int(vols.iloc[-1]) if not vols.empty else 0,
                        }
                    except: pass
        except Exception as e:
            print(f"  ⚠️ trending batch error: {e}")

        stocks = []
        for sym in popular:
            d = price_map.get(sym)
            if d is None or d['price'] <= 0: continue
            mk   = f"meta:{sym}"
            meta = _cache.get(mk, {}).get("data")
            if not meta or (time.time() - _cache.get(mk, {}).get("ts", 0)) > _meta_ttl:
                # Fire background meta refresh — don't block response
                def _bg(s=sym, k=mk):
                    try:
                        info = yf.Ticker(s).info
                        _cache[k] = {"ts": time.time(), "data": {
                            "name":      info.get("shortName", s),
                            "sector":    info.get("sector", ""),
                            "marketCap": info.get("marketCap"),
                            "pe":        float(info.get("trailingPE") or 0) or None,
                        }}
                    except: pass
                threading.Thread(target=_bg, daemon=True).start()
                meta = meta or {"name": sym, "sector": "", "marketCap": None, "pe": None}
            stocks.append({
                'ticker':    sym,
                'name':      meta.get('name', sym),
                'price':     d['price'],
                'change':    d['change'],
                'marketCap': meta.get('marketCap'),
                'volume':    d['volume'],
                'sector':    meta.get('sector', ''),
                'pe':        meta.get('pe'),
            })
        stocks.sort(key=lambda x: x.get('volume') or 0, reverse=True)
        return {'stocks': stocks}
    return _from_cache(cache_key, fetch, ttl=600)


def get_etf_detail(ticker):
    """ETF detail: expense ratio, AUM, holdings, sector breakdown."""
    ticker = ticker.upper().strip()
    cache_key = f"etf:{ticker}"
    def fetch():
        import math
        t = yf.Ticker(ticker)
        info = t.info or {}
        def _f(k):
            v = info.get(k)
            try: return float(v) if v is not None and not (isinstance(v,float) and math.isnan(v)) else None
            except: return None
        # Holdings
        holdings = []
        try:
            h = t.funds_data
            if h is not None:
                # sector weightings
                sw = h.sector_weightings if hasattr(h,'sector_weightings') else {}
                top = h.top_holdings if hasattr(h,'top_holdings') else None
                if top is not None and not top.empty:
                    for _, row in top.head(20).iterrows():
                        holdings.append({
                            'ticker': str(row.get('Symbol','')),
                            'name':   str(row.get('Name','')),
                            'weight': round(float(row.get('Percent Assets',0))*100,2),
                        })
                sectors = [{'sector': k, 'weight': round(v*100,2)} for k,v in sw.items()] if isinstance(sw,dict) else []
            else:
                sectors = []
        except:
            sectors = []
        price = _f('currentPrice') or _f('regularMarketPrice') or _f('navPrice')
        prev  = _f('regularMarketPreviousClose') or _f('previousClose')
        chg   = round((price/prev-1)*100,2) if (price and prev and prev!=0) else 0.0
        return {
            'ticker':       ticker,
            'name':         info.get('longName') or info.get('shortName', ticker),
            'price':        price,
            'change':       chg,
            'aum':          info.get('totalAssets'),
            'expenseRatio': round(float(info.get('annualReportExpenseRatio') or 0)*100, 3),
            'ytdReturn':    round(float(info.get('ytdReturn') or 0)*100, 2),
            'beta3y':       _f('beta3Year'),
            'nav':          _f('navPrice'),
            'category':     info.get('category',''),
            'exchange':     info.get('exchange',''),
            'inception':    info.get('fundInceptionDate',''),
            'description':  info.get('longBusinessSummary',''),
            'holdings':     holdings,
            'sectors':      sectors,
            'numHoldings':  info.get('holdings_count') or len(holdings),
        }
    return _from_cache(cache_key, fetch, ttl=1800)


def get_premarket_movers():
    """Pre-market / after-hours movers via one batch yf.download(prepost=True) call."""
    cache_key = "premarket:movers"
    def fetch():
        import datetime
        watchlist = ['AAPL','MSFT','AMZN','NVDA','GOOGL','META','TSLA','JPM','V','JNJ',
                     'BAC','WFC','GS','AMD','INTC','NFLX','UBER','COIN','PLTR','SOFI',
                     'GME','MSTR','ARM','RIVN','SMCI','SPY','QQQ','DIA','IWM','GLD',
                     'SLV','TLT','XLF','XLE','XLK','SNAP','RBLX','HOOD','DKNG','ROKU']

        print(f"  ⚡ Premarket: batch downloading {len(watchlist)} symbols…")
        raw = None
        try:
            # Single batch call with prepost=True — use 5m bars (not 1m) to cut data 5x
            raw = yf.download(
                tickers=' '.join(watchlist),
                period='1d',
                interval='5m',
                prepost=True,
                group_by='ticker',
                auto_adjust=True,
                progress=False,
                threads=True,
            )
        except Exception as e:
            print(f"  Premarket download error: {e}")

        if raw is None or raw.empty:
            print("  Premarket: download returned empty data")
            return {'premarket': [], 'afterhours': [], 'marketOpen': True}

        now_utc  = datetime.datetime.now(datetime.timezone.utc)
        stocks   = []

        import pandas as pd
        _is_multi = isinstance(raw.columns, pd.MultiIndex)

        def _get_ticker_df(sym):
            """Extract single-ticker DataFrame from multi-ticker or single-ticker download."""
            try:
                if _is_multi:
                    return raw[sym].dropna(how='all')
                return raw.dropna(how='all')  # single ticker
            except: return None

        for sym in watchlist:
            try:
                df = _get_ticker_df(sym)
                if df is None or df.empty:
                    continue

                closes = df['Close'].dropna()
                if len(closes) < 2:
                    continue

                # Identify regular-session last close vs extended-hours
                reg_closes  = []
                pre_closes  = []
                post_closes = []

                for ts, val in closes.items():
                    try:
                        ts_utc = ts.tz_convert('UTC') if ts.tzinfo else ts.replace(tzinfo=datetime.timezone.utc)
                    except: continue
                    age = (now_utc - ts_utc).total_seconds()
                    if age > 2 * 86400: continue   # skip bars older than 2 days
                    h = ts_utc.hour + ts_utc.minute / 60.0
                    # Regular session: 14:30–21:00 UTC (9:30am–4pm ET)
                    if 14.5 <= h < 21.0:
                        reg_closes.append((ts_utc, float(val)))
                    # Pre-market: 09:00–14:30 UTC (4am–9:30am ET)
                    elif 9.0 <= h < 14.5:
                        pre_closes.append((ts_utc, float(val)))
                    # After-hours: 21:00–24:00 and 00:00–01:00 UTC (4pm–8pm ET)
                    elif h >= 21.0 or h < 1.0:
                        post_closes.append((ts_utc, float(val)))

                # Determine regular price and previous close
                if not reg_closes:
                    continue
                reg_closes.sort(key=lambda x: x[0])
                price      = reg_closes[-1][1]
                prev_price = reg_closes[-2][1] if len(reg_closes) >= 2 else price
                reg_chg    = round((price / prev_price - 1) * 100, 2) if prev_price else 0.0

                # Extended-hours prices (only include most-recent bar)
                pre_price = post_price = None
                pre_chg   = post_chg   = None
                if pre_closes:
                    pre_closes.sort(key=lambda x: x[0])
                    pre_price = round(pre_closes[-1][1], 2)
                    pre_chg   = round((pre_price / price - 1) * 100, 2) if price else None
                if post_closes:
                    post_closes.sort(key=lambda x: x[0])
                    post_price = round(post_closes[-1][1], 2)
                    post_chg   = round((post_price / price - 1) * 100, 2) if price else None

                stocks.append({
                    'ticker': sym, 'name': sym,
                    'price': round(price, 2), 'change': reg_chg,
                    'preMarketPrice': pre_price, 'preMarketChange': pre_chg,
                    'postMarketPrice': post_price, 'postMarketChange': post_chg,
                })
            except Exception as e:
                print(f"  PM parse error {sym}: {e}")
                continue

        pre_movers  = [s for s in stocks if s.get('preMarketChange') is not None]
        post_movers = [s for s in stocks if s.get('postMarketChange') is not None]

        if not pre_movers and not post_movers:
            # Regular market hours — show biggest % movers instead
            by_change = sorted(stocks, key=lambda x: abs(x.get('change') or 0), reverse=True)
            return {'premarket': by_change[:20], 'afterhours': by_change[:20], 'marketOpen': True}

        pre_movers.sort(key=lambda x: abs(x.get('preMarketChange') or 0), reverse=True)
        post_movers.sort(key=lambda x: abs(x.get('postMarketChange') or 0), reverse=True)
        return {'premarket': pre_movers[:20], 'afterhours': post_movers[:20], 'marketOpen': False}
    return _from_cache(cache_key, fetch, ttl=120)


def get_market_earnings_calendar(date_str=None):
    """Earnings calendar for all S&P 500 tickers around a given date."""
    import datetime
    if not date_str:
        date_str = datetime.date.today().isoformat()
    cache_key = f"earn_cal:{date_str}"
    def fetch():
        # yfinance doesn't have a bulk earnings calendar endpoint
        # We use a representative watchlist
        watchlist = ['AAPL','MSFT','AMZN','NVDA','GOOGL','META','TSLA','JPM','V','JNJ',
                     'PG','UNH','HD','MA','ABBV','MRK','CVX','PEP','KO','WMT','BAC','AVGO',
                     'LLY','XOM','TMO','COST','MCD','DIS','ADBE','CRM','NFLX','AMD','INTC',
                     'QCOM','TXN','HON','PM','CAT','GE','IBM','GS','AXP','SPGI','BLK','RTX']
        import math
        from concurrent.futures import ThreadPoolExecutor
        target = datetime.date.fromisoformat(date_str)
        window_start = target - datetime.timedelta(days=2)
        window_end   = target + datetime.timedelta(days=7)
        def one(sym):
            try:
                t = yf.Ticker(sym)
                cal = t.calendar
                if cal is None: return None
                # calendar may be dict or DataFrame
                earn_date = None
                if isinstance(cal, dict):
                    ed = cal.get('Earnings Date')
                    if ed:
                        earn_date = ed[0] if isinstance(ed, list) else ed
                elif hasattr(cal, 'T'):
                    try:
                        ed = cal.T.get('Earnings Date')
                        if ed is not None: earn_date = ed.iloc[0]
                    except: pass
                if earn_date is None: return None
                if hasattr(earn_date, 'date'): earn_date = earn_date.date()
                elif isinstance(earn_date, str):
                    try: earn_date = datetime.date.fromisoformat(earn_date[:10])
                    except: return None
                if not (window_start <= earn_date <= window_end): return None
                info = t.info or {}
                def _f(k):
                    v = info.get(k)
                    try: return float(v) if v is not None and not (isinstance(v,float) and math.isnan(v)) else None
                    except: return None
                return {
                    'ticker':       sym,
                    'name':         info.get('shortName', sym),
                    'date':         earn_date.isoformat(),
                    'time':         'BMO' if (isinstance(cal, dict) and 'Before Market Open' in str(cal)) else 'AMC',
                    'epsEstimate':  _f('epsForward') or _f('epsCurrentYear'),
                    'revenueEst':   info.get('totalRevenue'),
                    'marketCap':    info.get('marketCap'),
                    'sector':       info.get('sector',''),
                }
            except: return None
        events = []
        with ThreadPoolExecutor(max_workers=8) as ex:
            for r in ex.map(one, watchlist):
                if r: events.append(r)
        events.sort(key=lambda x: x['date'])
        # Group by date
        by_date = {}
        for e in events:
            by_date.setdefault(e['date'], []).append(e)
        return {'events': events, 'byDate': by_date, 'centerDate': date_str}
    return _from_cache(cache_key, fetch, ttl=3600)


def get_entry_analysis(ticker):
    ticker = ticker.upper().strip()
    cache_key = f'entry_{ticker}'
    cached = _from_cache(cache_key, lambda: None, 0)
    if cached is not None:
        return cached

    try:
        t = yf.Ticker(ticker)
        hist = t.history(period='6mo', interval='1d', auto_adjust=True)
        if hist.empty or len(hist) < 30:
            return {'error': f'Insufficient price history for {ticker}'}

        closes  = hist['Close'].tolist()
        highs   = hist['High'].tolist()
        lows    = hist['Low'].tolist()
        volumes = hist['Volume'].tolist()
        price   = closes[-1]

        # ── RSI ──────────────────────────────────────────────────────
        rsi = _calc_rsi(closes, 14)
        if   rsi < 25:  rsi_score = 28; rsi_signal = 'Extremely Oversold'
        elif rsi < 32:  rsi_score = 22; rsi_signal = 'Oversold'
        elif rsi < 45:  rsi_score = 15; rsi_signal = 'Mildly Oversold'
        elif rsi < 55:  rsi_score = 8;  rsi_signal = 'Neutral'
        elif rsi < 65:  rsi_score = 4;  rsi_signal = 'Slightly Extended'
        elif rsi < 75:  rsi_score = 0;  rsi_signal = 'Overbought'
        else:           rsi_score = -5; rsi_signal = 'Extremely Overbought'

        # ── Moving Averages ───────────────────────────────────────────
        sma20  = _calc_sma(closes, 20)
        sma50  = _calc_sma(closes, 50)
        sma200 = _calc_sma(closes, 200)

        ma_score = 0
        ma_signals = []
        def _pct_from(ma):
            return round((price / ma - 1) * 100, 2) if ma else None

        pct20  = _pct_from(sma20)
        pct50  = _pct_from(sma50)
        pct200 = _pct_from(sma200)

        if sma20 and abs(pct20) < 2:
            ma_score += 8; ma_signals.append(f'At 20-day MA (${sma20:.2f})')
        elif sma20 and -5 < pct20 < 0:
            ma_score += 12; ma_signals.append(f'Just below 20-day MA — potential bounce (${sma20:.2f})')
        if sma50 and abs(pct50) < 2:
            ma_score += 12; ma_signals.append(f'At 50-day MA (${sma50:.2f}) — strong support')
        elif sma50 and -5 < pct50 < 0:
            ma_score += 15; ma_signals.append(f'Testing 50-day MA from below (${sma50:.2f})')
        if sma200 and abs(pct200) < 3:
            ma_score += 18; ma_signals.append(f'At 200-day MA (${sma200:.2f}) — major support')
        elif sma200 and -6 < pct200 < 0:
            ma_score += 15; ma_signals.append(f'Near 200-day MA (${sma200:.2f})')
        # Golden/death cross context
        if sma50 and sma200:
            if sma50 > sma200:
                ma_score += 5; ma_signals.append('Golden Cross active (50MA > 200MA — uptrend)')
            else:
                ma_score -= 5; ma_signals.append('Death Cross active (50MA < 200MA — downtrend)')
        if not ma_signals:
            ma_signals.append(f'Price extended from MAs (20MA: ${sma20:.2f}, 50MA: ${sma50:.2f})' if sma20 and sma50 else 'Insufficient MA data')

        # ── Bollinger Bands ───────────────────────────────────────────
        bb_lower, bb_mid, bb_upper = _calc_bollinger(closes, 20, 2)
        bb_score = 0; bb_signal = 'No BB data'
        if bb_lower and bb_upper:
            bb_width = bb_upper - bb_lower
            bb_pos   = (price - bb_lower) / bb_width if bb_width > 0 else 0.5
            if   bb_pos <= 0.05: bb_score = 22; bb_signal = f'At/below lower band (${bb_lower:.2f}) — squeeze buy zone'
            elif bb_pos <= 0.15: bb_score = 18; bb_signal = f'Near lower band (${bb_lower:.2f}) — strong mean-reversion setup'
            elif bb_pos <= 0.30: bb_score = 12; bb_signal = f'Lower third of Bollinger Bands — favorable zone'
            elif bb_pos <= 0.50: bb_score = 6;  bb_signal = f'Below midline (${bb_mid:.2f}) — neutral to slightly favorable'
            elif bb_pos <= 0.70: bb_score = 2;  bb_signal = f'Above midline — wait for pullback'
            elif bb_pos <= 0.90: bb_score = 0;  bb_signal = f'Approaching upper band (${bb_upper:.2f}) — extended'
            else:                bb_score = -5; bb_signal = f'At/above upper band (${bb_upper:.2f}) — overbought'

        # ── MACD ──────────────────────────────────────────────────────
        macd_line = None; signal_line = None; macd_score = 0; macd_signal = 'Insufficient data'
        if len(closes) >= 35:
            ema12 = _calc_ema(closes, 12)
            ema26 = _calc_ema(closes, 26)
            if ema12 and ema26:
                macd_line = ema12 - ema26
                # build MACD history for signal line (EMA-9 of MACD)
                macd_hist = []
                for i in range(26, len(closes)):
                    e12 = _calc_ema(closes[:i+1], 12)
                    e26 = _calc_ema(closes[:i+1], 26)
                    if e12 and e26:
                        macd_hist.append(e12 - e26)
                signal_line = _calc_ema(macd_hist, 9) if len(macd_hist) >= 9 else None
                if signal_line:
                    histogram = macd_line - signal_line
                    if macd_line > signal_line:
                        if histogram > 0 and len(macd_hist) >= 2 and (macd_hist[-1] - macd_hist[-2]) > 0:
                            macd_score = 18; macd_signal = 'Bullish MACD crossover — momentum building'
                        else:
                            macd_score = 10; macd_signal = 'MACD above signal — bullish momentum'
                    else:
                        if abs(macd_line - signal_line) < abs(signal_line) * 0.05:
                            macd_score = 8; macd_signal = 'MACD approaching crossover — watch for reversal'
                        else:
                            macd_score = 2; macd_signal = 'MACD below signal — bearish momentum'

        # ── Support / Resistance + Fibonacci ──────────────────────────
        pivot_levels = _find_pivot_levels(highs, lows, closes, window=5)
        fib = _calc_fibonacci(closes, highs, lows, lookback=90)

        # Find the nearest support level below current price
        supports = [(kind, lvl, cnt) for kind, lvl, cnt in pivot_levels if kind == 'support' and lvl < price]
        supports.sort(key=lambda x: price - x[1])  # closest first
        nearest_support = supports[0][1] if supports else None
        second_support  = supports[1][1] if len(supports) > 1 else None

        # Find nearby Fibonacci levels acting as support
        fib_supports = []
        if fib:
            for label, lvl in fib.items():
                if label in ('swing_high', 'swing_low'):
                    continue
                if lvl < price and abs(price - lvl) / price < 0.08:
                    fib_supports.append((label, lvl))
        fib_supports.sort(key=lambda x: price - x[1])

        sr_score = 0; sr_signals = []
        if nearest_support:
            pct_above = (price / nearest_support - 1) * 100
            if pct_above < 1.5:
                sr_score += 22; sr_signals.append(f'At pivot support ${nearest_support:.2f} — strong floor')
            elif pct_above < 4:
                sr_score += 15; sr_signals.append(f'Near pivot support ${nearest_support:.2f} ({pct_above:.1f}% above)')
            elif pct_above < 8:
                sr_score += 8;  sr_signals.append(f'Pivot support at ${nearest_support:.2f} ({pct_above:.1f}% below current)')
            else:
                sr_score += 3;  sr_signals.append(f'Nearest support: ${nearest_support:.2f} ({pct_above:.1f}% away)')
        if fib_supports:
            label, lvl = fib_supports[0]
            pct = (price / lvl - 1) * 100
            if pct < 2:
                sr_score += 15; sr_signals.append(f'At Fibonacci {label}% retracement (${lvl:.2f})')
            elif pct < 5:
                sr_score += 10; sr_signals.append(f'Near Fibonacci {label}% retracement (${lvl:.2f})')
        if not sr_signals:
            sr_signals.append('No nearby support levels — price may be in a gap')

        # ── Volume ────────────────────────────────────────────────────
        vol_score = 0; vol_signal = 'Normal volume'
        avg_vol = sum(volumes[-20:]) / min(20, len(volumes))
        recent_vol = sum(volumes[-3:]) / 3 if len(volumes) >= 3 else volumes[-1]
        last3_closes = closes[-3:]
        is_accumulating = all(last3_closes[i] >= last3_closes[i-1] for i in range(1, len(last3_closes)))
        is_distributing  = all(last3_closes[i] <= last3_closes[i-1] for i in range(1, len(last3_closes)))
        vol_ratio = recent_vol / avg_vol if avg_vol > 0 else 1
        if vol_ratio > 1.5 and is_accumulating:
            vol_score = 12; vol_signal = f'High volume on up-moves ({vol_ratio:.1f}x avg) — institutional accumulation'
        elif vol_ratio > 1.5 and is_distributing:
            vol_score = -3; vol_signal = f'High volume on down-moves ({vol_ratio:.1f}x avg) — distribution'
        elif vol_ratio < 0.6:
            vol_score = 5;  vol_signal = f'Low volume pullback ({vol_ratio:.1f}x avg) — healthy consolidation'
        else:
            vol_score = 4;  vol_signal = f'Volume near average ({vol_ratio:.1f}x) — no strong signal'

        # ── Composite Score ───────────────────────────────────────────
        total_score = rsi_score + min(ma_score, 25) + bb_score + macd_score + min(sr_score, 25) + vol_score
        total_score = max(0, min(100, total_score))

        if   total_score >= 75: grade = 'A';  recommendation = 'Strong Entry Zone'
        elif total_score >= 58: grade = 'B';  recommendation = 'Good Entry Zone'
        elif total_score >= 42: grade = 'C';  recommendation = 'Moderate Setup'
        elif total_score >= 25: grade = 'D';  recommendation = 'Wait for Better Setup'
        else:                   grade = 'F';  recommendation = 'Avoid / Extended'

        # ── Entry Zone & Targets ──────────────────────────────────────
        # Entry zone: current price down to nearest support (or -2% if no support close)
        entry_low  = nearest_support if nearest_support and (price - nearest_support) / price < 0.06 else round(price * 0.97, 2)
        entry_high = round(price * 1.005, 2)  # within 0.5% of current
        # Stop loss: below nearest support with a 1.5% buffer, or -6%
        stop_loss = round((nearest_support * 0.985) if nearest_support else price * 0.94, 2)
        risk_pct  = round((price - stop_loss) / price * 100, 2)
        # Targets from Fibonacci swing_high or resistance
        resistances = [(kind, lvl, cnt) for kind, lvl, cnt in pivot_levels if kind == 'resistance' and lvl > price]
        resistances.sort(key=lambda x: x[1])
        tgt1 = resistances[0][1] if resistances else round(price * 1.08, 2)
        tgt2 = resistances[1][1] if len(resistances) > 1 else round(price * 1.15, 2)
        tgt3 = fib.get('swing_high') if fib and fib.get('swing_high', 0) > tgt2 else round(price * 1.25, 2)

        reward1 = round((tgt1 / price - 1) * 100, 2)
        rr1     = round(reward1 / risk_pct, 2) if risk_pct > 0 else 0

        result = {
            'ticker': ticker,
            'price': round(price, 2),
            'score': total_score,
            'grade': grade,
            'recommendation': recommendation,
            'entry_zone': {'low': entry_low, 'high': entry_high},
            'stop_loss': stop_loss,
            'risk_pct': risk_pct,
            'targets': [
                {'price': round(tgt1, 2), 'upside': reward1, 'label': 'Target 1 (Nearest resistance)'},
                {'price': round(tgt2, 2), 'upside': round((tgt2/price-1)*100, 2), 'label': 'Target 2'},
                {'price': round(tgt3, 2), 'upside': round((tgt3/price-1)*100, 2), 'label': 'Target 3 (Full swing)'},
            ],
            'rr_ratio': rr1,
            'signals': {
                'rsi':     {'score': rsi_score,  'value': rsi,   'signal': rsi_signal,  'label': 'RSI (14)'},
                'ma':      {'score': min(ma_score,25), 'signal': '; '.join(ma_signals[:2]), 'label': 'Moving Averages',
                            'values': {'sma20': round(sma20,2) if sma20 else None,
                                       'sma50': round(sma50,2) if sma50 else None,
                                       'sma200': round(sma200,2) if sma200 else None}},
                'bb':      {'score': bb_score,   'signal': bb_signal, 'label': 'Bollinger Bands',
                            'values': {'lower': round(bb_lower,2) if bb_lower else None,
                                       'mid':   round(bb_mid,2) if bb_mid else None,
                                       'upper': round(bb_upper,2) if bb_upper else None}},
                'macd':    {'score': macd_score,  'signal': macd_signal, 'label': 'MACD'},
                'sr':      {'score': min(sr_score,25), 'signal': '; '.join(sr_signals[:2]), 'label': 'Support & Fibonacci',
                            'levels': [{'type': k, 'price': l} for k, l, _ in (supports[:3] if supports else [])]},
                'volume':  {'score': vol_score,  'signal': vol_signal, 'label': 'Volume Analysis'},
            },
            'fibonacci': fib,
            'pivot_levels': [{'type': k, 'price': l, 'strength': c} for k, l, c in pivot_levels[:6]],
        }

        _cache[cache_key] = {'ts': time.time(), 'data': result, 'ttl': 1800}  # 30 min cache
        return result

    except Exception as e:
        return {'error': str(e)}

# Default tickers to always include in dividend calendar
# Covers: Dividend Aristocrats, REITs, Utilities, Financials, Consumer, Healthcare, Energy, Tech, Industrials
DIVIDEND_WATCHLIST = [
    # ── Dividend Aristocrats / Blue-chip payers ─────────────────────────
    'JNJ','PG','KO','PEP','MCD','MMM','CAT','GE','ITW','EMR',
    'ABT','ADP','AFL','ATO','BEN','BKH','CL','CLX','CVX','XOM',
    'ECL','ED','ESS','FRT','GWW','HRL','IBM','CINF','KMB','LEG',
    'LOW','MKC','NDSN','NUE','O','OHI','PH','PPG','SWK','SYY',
    'TGT','VFC','WBA','WMT','XEL','WEC','UGI','T','VZ','TROW',
    'NKE','EXPD','CHRW','CTAS','CBT','DOV','FDS','GPC','SPGI',
    # ── Consumer Staples ────────────────────────────────────────────────
    'KHC','GIS','COST','MDLZ','K','STZ','HSY','CPB','HRL','MO','PM','BTI',
    'DEO','BUD','CHD','EL','ENR','CLX','SJM','CAG','TSN','HRL','PPC','INGR',
    # ── Healthcare ──────────────────────────────────────────────────────
    'ABBV','PFE','MRK','LLY','BMY','AMGN','GILD','CI','CVS',
    'HUM','UNH','MCK','ABC','CAH','MDT','BSX','SYK','EW','BAX',
    # ── Financials / Insurance ──────────────────────────────────────────
    'JPM','BAC','WFC','C','GS','MS','USB','PNC','TFC','RF',
    'CFG','HBAN','KEY','FITB','MTB','PRU','MET','AIG','ALL','CB',
    'TRV','HIG','L','AFL','BLK','IVZ','FII','STT','BK','NTRS',
    'V','MA','AXP','DFS','COF','SYF','NDAQ','ICE',
    # ── BDCs (Business Development Companies) — high-yield monthly/quarterly
    'MAIN','ARCC','GBDC','OBDC','HTGC','FSCO','ORCC','BXSL','TPVG',
    'GAIN','GLAD','GOODX','SLRC','OXSQ','PFLT','PNNT','TCPC',
    # ── Mortgage REITs / mREITs ─────────────────────────────────────────
    'NLY','AGNC','MFA','TWO','ARR','CIM','DX','EARN','NYMT','IVR',
    # ── Utilities ───────────────────────────────────────────────────────
    'NEE','DUK','SO','D','AEP','EXC','SRE','PEG','ED','EIX',
    'WEC','XEL','ES','DTE','ETR','FE','CNP','NI','OGE','POR',
    'AWK','CMS','LNT','EVRG','ATO','NWE','MGE','MGEE',
    # ── Energy ──────────────────────────────────────────────────────────
    'XOM','CVX','COP','EOG','PSX','VLO','MPC','OXY','SLB','HAL',
    'KMI','WMB','OKE','EPD','ET','MMP','MPLX','PAA','AM','TRGP',
    'DVN','PXD','FANG','HES','BKR','NOV',
    # ── Real Estate / REITs ─────────────────────────────────────────────
    'O','SPG','PLD','AMT','EQIX','CCI','PSA','EQR','AVB','VTR',
    'WELL','ARE','BXP','KIM','REG','FRT','NNN','STAG','TRNO','EGP',
    'OHI','SBAC','DLR','IRM','COLD','CBRE','HST','RHP','EPR',
    'MPW','VICI','GLPI','GTY','ADC','NTST','WPC','NHI','IIPR',
    'LAND','GOOD','PINE','BRSP','UNIT','DEA','GMRE',
    # ── Technology (dividend payers) ────────────────────────────────────
    'MSFT','AAPL','CSCO','TXN','QCOM','AVGO','IBM','HPQ','HPE',
    'INTC','ADI','KLAC','LRCX','MCHP','SWKS','AMAT','PAYX','ADP',
    'ORCL','ACN','CTSH','CDW','JNPR','NTAP','STX','WDC',
    # ── Industrials ─────────────────────────────────────────────────────
    'HON','RTX','LMT','GD','NOC','BA','DE','ETN','PH','ROK',
    'EMR','IR','AME','XYL','OTIS','CARR','JCI','TT','TDY','HWM',
    'GWW','MSC','FAST','GPC','CHRW','EXPD','UPS','FDX',
    # ── Telecom ─────────────────────────────────────────────────────────
    'T','VZ','TMUS','LUMN','USM','SHEN',
    # ── International / ADRs (major dividend payers) ────────────────────
    'ENB','TRP','BCE','TD','RY','BNS','BMO','CM',   # Canada
    'BP','SHEL','GSK','AZN','SNY','RHHBY',           # Europe
    'RIO','BHP','VALE',                               # Mining
    'MFC','SLF','POW',                                # Canadian financials
    # ── Materials ───────────────────────────────────────────────────────
    'LIN','APD','SHW','PPG','NUE','STLD','RS','VMC','MLM','FCX',
    'IP','PKG','SEE','SON','AVY','IFF','EMN','CE','RPM','DOW',
    'LYB','OLN','ASH','TREX','UFPI',
    # ── Consumer Discretionary ──────────────────────────────────────────
    'HD','LOW','MCD','SBUX','YUM','DRI','CMG','HBI','RL','PVH',
    'VFC','TPR','TJX','ROST','M','KSS','BBY','WHR','LKQ',
]

# ── Dividend calendar ──────────────────────────────────────────────────────────
def _fetch_one_dividend(sym):
    """Fetch dividend info for a single ticker. Returns dict or None."""
    try:
        t    = yf.Ticker(sym)
        info = t.info

        div_rate  = float(info.get("dividendRate")  or 0)
        div_yield = float(info.get("dividendYield") or 0)
        last_div  = float(info.get("lastDividendValue") or 0)

        # Skip non-payers
        if div_rate <= 0 and last_div <= 0:
            return None

        # Ex-dividend date
        ex_ts = info.get("exDividendDate")
        ex_date_str = None
        if ex_ts:
            try:
                if isinstance(ex_ts, (list, tuple)):
                    ex_ts = ex_ts[0]
                ex_date_str = datetime.utcfromtimestamp(float(ex_ts)).strftime("%Y-%m-%d")
            except Exception:
                pass

        # Payment / last dividend date
        pay_ts = info.get("lastDividendDate")
        pay_date_str = None
        if pay_ts:
            try:
                pay_date_str = datetime.utcfromtimestamp(float(pay_ts)).strftime("%Y-%m-%d")
            except Exception:
                pass

        # Estimate quarterly dividend from annual rate
        quarterly = round(last_div if last_div > 0 else div_rate / 4, 4)

        # Frequency label
        freq_map = {1: "Monthly", 2: "Semi-Annual", 4: "Quarterly", 12: "Monthly"}
        trailing = float(info.get("trailingAnnualDividendRate") or 0)
        freq_label = "Quarterly"  # default
        if quarterly > 0 and trailing > 0:
            approx = round(trailing / quarterly)
            freq_label = freq_map.get(approx, "Quarterly")

        return {
            "ticker":       sym,
            "name":         info.get("shortName", sym),
            "sector":       info.get("sector", ""),
            "ex_div_date":  ex_date_str,
            "pay_date":     pay_date_str,
            "div_rate":     round(div_rate,  4),
            "div_yield":    round(div_yield, 6),
            "last_div":     round(last_div,  4),
            "quarterly":    quarterly,
            "frequency":    freq_label,
        }
    except Exception as e:
        print(f"  Dividend {sym}: {e}")
    return None


def get_dividend_calendar(portfolio_tickers, watchlist_tickers):
    """Fetch ex-div calendar for portfolio + watchlist + default dividend payers."""
    all_tickers = list(dict.fromkeys(
        [t.upper() for t in portfolio_tickers if t.strip()] +
        [t.upper() for t in watchlist_tickers if t.strip()] +
        DIVIDEND_WATCHLIST
    ))[:250]   # allow large market-wide list

    key = "divCal:" + ",".join(sorted(all_tickers))

    def fetch():
        results = []
        # Use more workers for the large list; each call is lightweight (info dict only)
        workers = min(32, len(all_tickers))
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for item in pool.map(_fetch_one_dividend, all_tickers):
                if item:
                    results.append(item)
        print(f"  💰 Dividend calendar: {len(results)} payers found from {len(all_tickers)} tickers")
        return results

    raw = _from_cache(key, fetch, ttl=3600 * 6)

    # Recompute days_away fresh every time so cached data stays accurate
    today = datetime.now().date()
    out   = []
    for item in raw:
        ex_str   = item.get("ex_div_date")
        days_away = None
        if ex_str:
            try:
                days_away = (datetime.strptime(ex_str, "%Y-%m-%d").date() - today).days
            except Exception:
                pass
        out.append({**item, "days_away": days_away})

    # Sort: upcoming → recent (past 30d) → no-date
    upcoming = sorted(
        [x for x in out if x["days_away"] is not None and -30 <= x["days_away"] <= 90],
        key=lambda x: x["days_away"]
    )
    no_date = [x for x in out if x.get("days_away") is None or
               x["days_away"] > 90 or x["days_away"] < -30]
    return upcoming + no_date


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
                ex_div = info.get("exDividendDate")  # epoch int or None
                ex_div_str = ""
                if ex_div:
                    try:
                        import datetime
                        ex_div_str = datetime.datetime.utcfromtimestamp(int(ex_div)).strftime("%Y-%m-%d")
                    except: pass
                meta = {
                    "shortName":            info.get("shortName", sym),
                    "sector":               info.get("sector", ""),
                    "trailingPE":           float(info.get("trailingPE")          or 0),
                    "beta":                 float(info.get("beta")                or 0),
                    "dividendRate":         float(info.get("dividendRate")        or 0),
                    "dividendYield":        float(info.get("dividendYield")       or 0),
                    "exDividendDate":       ex_div_str,
                    "w52ChangePct":         float(info.get("52WeekChange")        or 0) * 100,
                    "shortPercentOfFloat":  float(info.get("shortPercentOfFloat") or 0) * 100,
                    "shortRatio":           float(info.get("shortRatio")          or 0),
                }
                _cache[meta_key] = {"ts": time.time(), "data": meta}
            except:
                meta = {"shortName": sym, "sector": "", "trailingPE": 0,
                        "beta": 0, "dividendRate": 0, "dividendYield": 0,
                        "exDividendDate": "", "w52ChangePct": 0,
                        "shortPercentOfFloat": 0, "shortRatio": 0}

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
            "exDividendDate":             meta.get("exDividendDate", ""),
            "w52ChangePct":               meta.get("w52ChangePct", 0),
            "sector":                     meta["sector"],
            "shortPercentOfFloat":         meta.get("shortPercentOfFloat", 0),
            "shortRatio":                  meta.get("shortRatio", 0),
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
        with ThreadPoolExecutor(max_workers=_YF_WORKERS) as pool:
            for sym, data in pool.map(_fetch_one_quote, symbols):
                results[sym] = data
                p, pct = data["regularMarketPrice"], data["regularMarketChangePercent"]
                print(f"  {'🚨' if abs(pct)>=5 else '✅'} {sym:6s}  ${p:>9.4f}  {'↑' if pct>=0 else '↓'} {pct:+.2f}%")
        return results
    return _from_cache(key, fetch, ttl=60)   # 60s instead of 30s default

# ── Extended hours (pre-market / after-hours) ─────────────────────────────────
def _fetch_one_extended(sym):
    """Fallback: fetch pre/after-hours for a single ticker using yf history (chart API, no quoteSummary 401 issues)."""
    try:
        import datetime as _dt
        hist = yf.Ticker(sym).history(period='2d', interval='5m', prepost=True, auto_adjust=True)
        if hist is None or hist.empty:
            return sym, {'pre_price': 0, 'pre_change': 0, 'pre_pct': 0,
                         'post_price': 0, 'post_change': 0, 'post_pct': 0, 'reg_close': 0}
        idx = hist.index
        if hasattr(idx, 'tz') and idx.tz is not None:
            idx_et = idx.tz_convert('America/New_York')
        else:
            idx_et = idx.tz_localize('UTC').tz_convert('America/New_York')
        closes = hist['Close'].copy()
        closes.index = idx_et
        closes = closes.dropna()
        last_date = closes.index.date[-1]
        day  = closes[closes.index.date == last_date]
        reg  = day[(day.index.time >= _dt.time(9, 30)) & (day.index.time < _dt.time(16, 0))]
        pre  = day[day.index.time < _dt.time(9, 30)]
        post = day[day.index.time >= _dt.time(16, 0)]
        reg_close  = float(reg.iloc[-1])  if not reg.empty  else float(day.iloc[-1]) if not day.empty else 0.0
        pre_price  = float(pre.iloc[-1])  if not pre.empty  else 0.0
        post_price = float(post.iloc[-1]) if not post.empty else 0.0
        def _calc(ext):
            if ext <= 0 or reg_close <= 0: return 0.0, 0.0
            chg = ext - reg_close
            return round(chg, 4), round(chg / reg_close * 100, 3)
        pre_chg,  pre_pct  = _calc(pre_price)
        post_chg, post_pct = _calc(post_price)
        return sym, {
            'pre_price':   round(pre_price,  4), 'pre_change':  pre_chg,  'pre_pct':  pre_pct,
            'post_price':  round(post_price, 4), 'post_change': post_chg, 'post_pct': post_pct,
            'reg_close':   round(reg_close,  4),
        }
    except Exception as e:
        print(f"  ⚠️ ext-hours {sym}: {e}")
        return sym, {'pre_price': 0, 'pre_change': 0, 'pre_pct': 0,
                     'post_price': 0, 'post_change': 0, 'post_pct': 0, 'reg_close': 0}

def get_extended_hours(symbols):
    """Fetch pre-market and after-hours data via one batch yf.download(prepost=True) call."""
    if not symbols: return {}
    symbols = [s.strip().upper() for s in symbols if s.strip()]
    key = 'exthours:' + ','.join(sorted(symbols))
    def fetch():
        import pandas as pd, datetime as _dt
        print(f"⏰ Extended hours — {len(symbols)} symbols (batch download)…")
        _zero = {'pre_price': 0, 'pre_change': 0, 'pre_pct': 0,
                 'post_price': 0, 'post_change': 0, 'post_pct': 0, 'reg_close': 0}
        result = {s: dict(_zero) for s in symbols}
        try:
            raw = yf.download(
                tickers=' '.join(symbols),
                period='2d', interval='5m',
                prepost=True, group_by='ticker',
                auto_adjust=True, progress=False, threads=False,
            )
            if raw is None or raw.empty:
                return result
            # Convert index to Eastern Time for accurate pre/post-market filtering
            idx = raw.index
            if hasattr(idx, 'tz') and idx.tz is not None:
                idx_et = idx.tz_convert('America/New_York')
            else:
                idx_et = idx.tz_localize('UTC').tz_convert('America/New_York')
            _is_multi = isinstance(raw.columns, pd.MultiIndex)
            for sym in symbols:
                try:
                    df_close = (raw[sym]['Close'] if _is_multi else raw['Close']).copy()
                    df_close.index = idx_et
                    closes = df_close.dropna()
                    if closes.empty: continue
                    last_date = closes.index.date[-1]
                    day  = closes[closes.index.date == last_date]
                    reg  = day[(day.index.time >= _dt.time(9, 30)) & (day.index.time < _dt.time(16, 0))]
                    pre  = day[day.index.time < _dt.time(9, 30)]
                    post = day[day.index.time >= _dt.time(16, 0)]
                    reg_close  = float(reg.iloc[-1])  if not reg.empty  else float(day.iloc[-1])
                    pre_price  = float(pre.iloc[-1])  if not pre.empty  else 0.0
                    post_price = float(post.iloc[-1]) if not post.empty else 0.0
                    def _calc(ext, rc=reg_close):
                        if ext <= 0 or rc <= 0: return 0.0, 0.0
                        chg = ext - rc
                        return round(chg, 4), round(chg / rc * 100, 3)
                    pre_chg,  pre_pct  = _calc(pre_price)
                    post_chg, post_pct = _calc(post_price)
                    result[sym] = {
                        'pre_price':   round(pre_price,  4), 'pre_change':  pre_chg,  'pre_pct':  pre_pct,
                        'post_price':  round(post_price, 4), 'post_change': post_chg, 'post_pct': post_pct,
                        'reg_close':   round(reg_close,  4),
                    }
                except Exception as e:
                    print(f"  ⚠️ ext-hours {sym}: {e}")
        except Exception as e:
            print(f"  ⚠️ extended hours batch error: {e}")
            # Fall back to per-symbol history calls
            with ThreadPoolExecutor(max_workers=_YF_WORKERS) as pool:
                for sym, d in pool.map(_fetch_one_extended, symbols):
                    result[sym] = d
        return result
    return _from_cache(key, fetch, ttl=60)

# ── Futures / overnight market ────────────────────────────────────────────────
def get_futures():
    key = 'futures'
    def fetch():
        import pandas as pd
        syms = [f['symbol'] for f in FUTURES_SYMBOLS]
        meta = {f['symbol']: f for f in FUTURES_SYMBOLS}
        print(f"🌙 Fetching {len(syms)} futures (batch download)…")
        price_map = {}
        try:
            raw = yf.download(
                tickers=' '.join(syms),
                period='5d', interval='1d',
                group_by='ticker', auto_adjust=True,
                progress=False, threads=False,
            )
            if raw is not None and not raw.empty:
                if isinstance(raw.columns, pd.MultiIndex):
                    for sym in raw.columns.get_level_values(0).unique():
                        try:
                            closes = raw[sym]['Close'].dropna()
                            if len(closes) < 1: continue
                            price = float(closes.iloc[-1])
                            prev  = float(closes.iloc[-2]) if len(closes) >= 2 else price
                            price_map[sym] = {
                                'price':  round(price, 2),
                                'change': round(price - prev, 2),
                                'pct':    round((price - prev) / prev * 100 if prev else 0, 3),
                            }
                        except: pass
                else:
                    # Single-ticker fallback
                    try:
                        closes = raw['Close'].dropna()
                        if len(closes) >= 1 and syms:
                            sym = syms[0]
                            price = float(closes.iloc[-1])
                            prev  = float(closes.iloc[-2]) if len(closes) >= 2 else price
                            price_map[sym] = {
                                'price':  round(price, 2),
                                'change': round(price - prev, 2),
                                'pct':    round((price - prev) / prev * 100 if prev else 0, 3),
                            }
                    except: pass
        except Exception as e:
            print(f"  ⚠️ futures batch error: {e}")
        results = []
        for m in FUTURES_SYMBOLS:
            sym = m['symbol']
            d = price_map.get(sym, {'price': 0, 'change': 0, 'pct': 0})
            results.append({'symbol': sym, 'name': m['name'], 'icon': m['icon'],
                            'group': m['group'], 'price': d['price'],
                            'change': d['change'], 'change_pct': d['pct']})
        return results
    try:
        return _from_cache(key, fetch, ttl=60)
    except Exception as e:
        print(f"⚠️ get_futures failed: {e}")
        # Return stub list so client always gets valid JSON (price=0 cards)
        return [{'symbol': f['symbol'], 'name': f['name'], 'icon': f['icon'],
                 'group': f['group'], 'price': 0, 'change': 0, 'change_pct': 0}
                for f in FUTURES_SYMBOLS]

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
    return _from_cache(key, fetch, ttl=300)  # 5-min cache — news doesn't change every 30s

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
                "recommendation_mean": flt("recommendationMean"),
                "shares_outstanding": flt("sharesOutstanding"), "float_shares": flt("floatShares"),
                # ── Extra financial fields from info ──────────────────────────
                "total_revenue":      flt("totalRevenue"),
                "net_income":         flt("netIncomeToCommon"),
                "gross_margins":      flt("grossMargins"),
                "operating_margins":  flt("operatingMargins"),
                "profit_margins":     flt("profitMargins"),
                "revenue_growth":     flt("revenueGrowth"),
                "earnings_growth":    flt("earningsGrowth"),
                "return_on_equity":   flt("returnOnEquity"),
                "return_on_assets":   flt("returnOnAssets"),
                "current_ratio":      flt("currentRatio"),
                "debt_to_equity":     flt("debtToEquity"),
                "total_cash":         flt("totalCash"),
                "total_debt":         flt("totalDebt"),
                "free_cashflow":      flt("freeCashflow"),
                "operating_cashflow": flt("operatingCashflow"),
                "forward_eps":        flt("forwardEps"),
                "trailing_eps":       flt("trailingEps"),
                "book_value":         flt("bookValue"),
                "earnings_date": (lambda ts: (
                    datetime.utcfromtimestamp(float(ts[0] if isinstance(ts,(list,tuple)) else ts)).strftime('%b %d, %Y')
                    if ts else None
                ))(info.get("earningsTimestamp") or info.get("earningsDate")),
                "ex_dividend_date": info.get("exDividendDate"),
                "quote_type":       info.get("quoteType", ""),
                "news": news,
            }
        except Exception as e:
            return {"symbol": symbol, "error": str(e)}
    return _from_cache(key, fetch)

def get_stock_financials(symbol):
    """Fetch income statement, balance sheet, cashflow + analyst rating breakdown."""
    symbol = symbol.strip().upper()
    key = f"financials:{symbol}"
    def fetch():
        import math
        print(f"  📊 Financials: {symbol}")
        result = {"symbol": symbol, "income": [], "balance": [], "cashflow": [], "ratings": None}
        def sv(df, metric, col):
            try:
                if metric in df.index:
                    v = df.loc[metric, col]
                    return float(v) if v is not None and not (isinstance(v, float) and math.isnan(v)) else None
                return None
            except: return None
        def _try_df(*attrs):
            """Try multiple yfinance DataFrame attributes, return first non-empty."""
            for attr in attrs:
                try:
                    df = getattr(stock, attr) if isinstance(attr, str) else attr()
                    if df is not None and not df.empty:
                        return df
                except Exception:
                    pass
            return None

        try:
            stock = yf.Ticker(symbol)
            info  = stock.info or {}

            # ── Income Statement ──────────────────────────────────────────────
            try:
                fin = _try_df('financials', 'income_stmt', 'quarterly_financials')
                if fin is not None and not fin.empty:
                    for col in list(fin.columns)[:4]:
                        row = {"year": str(col.year) if hasattr(col,'year') else str(col)[:4]}
                        # try multiple possible row names for each metric
                        def sv2(df, *metrics):
                            for m in metrics:
                                v = sv(df, m, col)
                                if v is not None: return v
                            return None
                        row["revenue"]          = sv2(fin, "Total Revenue", "Revenue")
                        row["gross_profit"]     = sv2(fin, "Gross Profit")
                        row["operating_income"] = sv2(fin, "Operating Income", "EBIT")
                        row["net_income"]       = sv2(fin, "Net Income", "Net Income Common Stockholders")
                        row["ebitda"]           = sv2(fin, "EBITDA", "Normalized EBITDA")
                        row["eps"]              = sv2(fin, "Basic EPS", "Diluted EPS")
                        result["income"].append(row)
                else:
                    # ── Info-based TTM fallback ───────────────────────────────
                    def _inf(k):
                        v = info.get(k)
                        try: return float(v) if v is not None else None
                        except: return None
                    ttm = {"year": "TTM",
                           "revenue":          _inf("totalRevenue"),
                           "gross_profit":      (_inf("totalRevenue") or 0) * (_inf("grossMargins") or 0) or None,
                           "operating_income":  (_inf("totalRevenue") or 0) * (_inf("operatingMargins") or 0) or None,
                           "net_income":        _inf("netIncomeToCommon"),
                           "ebitda":            _inf("ebitda"),
                           "eps":               _inf("trailingEps"),
                    }
                    # Only add if at least revenue is available
                    if ttm["revenue"]:
                        result["income"].append(ttm)
                        result["info_fallback"] = True
            except Exception as e: print(f"  Income err {symbol}: {e}")

            # ── Balance Sheet ─────────────────────────────────────────────────
            try:
                bs = _try_df('balance_sheet', 'quarterly_balance_sheet')
                if bs is not None and not bs.empty:
                    for col in list(bs.columns)[:4]:
                        row = {"year": str(col.year) if hasattr(col,'year') else str(col)[:4]}
                        def bsv(*metrics):
                            for m in metrics:
                                v = sv(bs, m, col)
                                if v is not None: return v
                            return None
                        row["total_assets"]        = bsv("Total Assets")
                        row["total_debt"]          = bsv("Total Debt", "Long Term Debt")
                        row["cash"]                = bsv("Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments")
                        row["equity"]              = bsv("Stockholders Equity", "Common Stock Equity")
                        row["current_assets"]      = bsv("Current Assets")
                        row["current_liabilities"] = bsv("Current Liabilities")
                        result["balance"].append(row)
                else:
                    # Info-based balance fallback
                    def _inf(k):
                        v = info.get(k)
                        try: return float(v) if v is not None else None
                        except: return None
                    bal = {"year": "Latest",
                           "total_assets":        _inf("totalAssets"),
                           "total_debt":          _inf("totalDebt"),
                           "cash":                _inf("totalCash"),
                           "equity":              None,
                           "current_assets":      _inf("currentAssets") if info.get("currentAssets") else None,
                           "current_liabilities": None,
                    }
                    if bal["total_assets"] or bal["total_debt"] or bal["cash"]:
                        result["balance"].append(bal)
            except Exception as e: print(f"  Balance err {symbol}: {e}")

            # ── Cash Flow ─────────────────────────────────────────────────────
            try:
                cf = _try_df('cashflow', 'cash_flow', 'quarterly_cashflow')
                if cf is not None and not cf.empty:
                    for col in list(cf.columns)[:4]:
                        row = {"year": str(col.year) if hasattr(col,'year') else str(col)[:4]}
                        def cfv(*metrics):
                            for m in metrics:
                                v = sv(cf, m, col)
                                if v is not None: return v
                            return None
                        row["operating_cf"]  = cfv("Operating Cash Flow", "Cash Flow From Continuing Operations")
                        row["investing_cf"]  = cfv("Investing Cash Flow")
                        row["financing_cf"]  = cfv("Financing Cash Flow")
                        row["free_cf"]       = cfv("Free Cash Flow")
                        row["capex"]         = cfv("Capital Expenditure")
                        result["cashflow"].append(row)
                else:
                    def _inf(k):
                        v = info.get(k)
                        try: return float(v) if v is not None else None
                        except: return None
                    cfl = {"year": "TTM",
                           "operating_cf":  _inf("operatingCashflow"),
                           "investing_cf":  None,
                           "financing_cf":  None,
                           "free_cf":       _inf("freeCashflow"),
                           "capex":         None,
                    }
                    if cfl["operating_cf"] or cfl["free_cf"]:
                        result["cashflow"].append(cfl)
            except Exception as e: print(f"  Cashflow err {symbol}: {e}")

            # ── Analyst Recommendations Breakdown ─────────────────────────────
            try:
                recs = None
                # Try recommendations_summary first (newer API)
                for attr in ['recommendations_summary', 'recommendations']:
                    try:
                        df = getattr(stock, attr)
                        if df is not None and not df.empty:
                            recs = df; break
                    except: pass
                if recs is not None and not recs.empty:
                    latest = recs.iloc[-1]
                    def _rc(k):
                        v = latest.get(k, 0)
                        try: return int(float(v)) if v is not None else 0
                        except: return 0
                    result["ratings"] = {
                        "strongBuy":  _rc("strongBuy"),
                        "buy":        _rc("buy"),
                        "hold":       _rc("hold"),
                        "sell":       _rc("sell"),
                        "strongSell": _rc("strongSell"),
                        "period":     str(recs.index[-1])[:10] if len(recs) > 0 else "",
                    }
                else:
                    # Build from info dict analyst count + recommendation key
                    rec_key = info.get("recommendationKey", "")
                    n = info.get("numberOfAnalystOpinions", 0) or 0
                    if rec_key and n:
                        # Estimate distribution from recommendationMean (1=Strong Buy, 5=Strong Sell)
                        mean = float(info.get("recommendationMean", 3) or 3)
                        # Simple heuristic distribution
                        buy_pct   = max(0, min(1, (3.5 - mean) / 2.5))
                        sell_pct  = max(0, min(1, (mean - 3.5) / 1.5))
                        hold_pct  = 1 - buy_pct - sell_pct
                        result["ratings"] = {
                            "strongBuy":  round(n * buy_pct * 0.4),
                            "buy":        round(n * buy_pct * 0.6),
                            "hold":       round(n * hold_pct),
                            "sell":       round(n * sell_pct * 0.6),
                            "strongSell": round(n * sell_pct * 0.4),
                            "period":     "estimated",
                        }
            except Exception as e: print(f"  Ratings err {symbol}: {e}")

        except Exception as e:
            result["error"] = str(e)
        return result
    return _from_cache(key, fetch, ttl=43200)  # 12-hour cache

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
                interval_map = {
                    '5d':  '15m',
                    '1mo': '1d',
                    '3mo': '1d',
                    '6mo': '1d',
                    '1y':  '1wk',
                    '2y':  '1wk',
                    '5y':  '1mo',
                    'max': '1mo',
                }
                interval = interval_map.get(period, '1d')
                hist = yf.Ticker(symbol).history(period=period, interval=interval, auto_adjust=True)
            if hist.empty:
                return []
            pts = []
            for dt, row in hist.iterrows():
                pts.append({
                    't': int(dt.timestamp() * 1000),
                    'c': round(float(row['Close']), 4),
                    'o': round(float(row['Open']),  4),
                    'h': round(float(row['High']),  4),
                    'l': round(float(row['Low']),   4),
                    'v': int(row.get('Volume', 0)),
                })
            return pts
        except Exception as e:
            print(f"  History error {symbol}: {e}")
            return []
    ttl_map = {'1d': 30, '5d': 300, '1mo': 3600, '3mo': 7200, '6mo': 14400,
               '1y': 86400, '2y': 86400, '5y': 86400, 'max': 86400}
    return _from_cache(key, fetch, ttl=ttl_map.get(period, 3600))

def get_market_data():
    """Fetch indices, sector ETFs, and crypto via a single yf.download() batch call.
    One HTTP request instead of 22 individual fast_info calls — ~5-10× faster."""
    key = "market_overview"
    def fetch():
        import pandas as pd
        NAME_MAP = {"^GSPC":"S&P 500","^IXIC":"NASDAQ","^DJI":"DOW Jones","^RUT":"Russell 2000","^VIX":"VIX"}
        all_syms = MARKET_SYMBOLS + list(SECTOR_ETFS.keys()) + list(CRYPTO_SYMBOLS.keys())
        print(f"\n📡 Fetching market overview — {len(all_syms)} symbols (batch download)…")

        _zero = {"price":0,"change":0,"change_pct":0,"prev_close":0,
                 "day_high":0,"day_low":0,"market_cap":0,"volume":0}
        price_map = {}
        try:
            raw = yf.download(
                tickers=' '.join(all_syms),
                period='5d',
                interval='1d',
                group_by='ticker',
                auto_adjust=True,
                progress=False,
                threads=False,
            )
            if raw is not None and not raw.empty and isinstance(raw.columns, pd.MultiIndex):
                for sym in raw.columns.get_level_values(0).unique():
                    try:
                        closes = raw[sym]['Close'].dropna()
                        if len(closes) < 1:
                            continue
                        price = float(closes.iloc[-1])
                        prev  = float(closes.iloc[-2]) if len(closes) >= 2 else price
                        chg     = price - prev
                        chg_pct = (chg / prev * 100) if prev else 0
                        highs = raw[sym].get('High', raw[sym]['Close']).dropna()
                        lows  = raw[sym].get('Low',  raw[sym]['Close']).dropna()
                        vols  = raw[sym].get('Volume', raw[sym]['Close']).dropna()
                        price_map[sym] = {
                            "price":      round(price, 4),
                            "change":     round(chg, 4),
                            "change_pct": round(chg_pct, 4),
                            "prev_close": round(prev, 4),
                            "day_high":   round(float(highs.iloc[-1]), 4) if not highs.empty else 0,
                            "day_low":    round(float(lows.iloc[-1]),  4) if not lows.empty  else 0,
                            "market_cap": 0,
                            "volume":     int(vols.iloc[-1]) if not vols.empty else 0,
                        }
                    except: pass
        except Exception as e:
            print(f"  ⚠️ market batch download error: {e}")

        # Fall back to individual fast_info for any missing symbols
        missing = [s for s in all_syms if s not in price_map]
        if missing:
            print(f"  ↩ fast_info fallback for {len(missing)} missing symbols…")
            def _fi(sym):
                try:
                    fi = yf.Ticker(sym).fast_info
                    p  = float(fi.last_price or 0)
                    pc = float(fi.previous_close or p or 1)
                    ch = p - pc
                    return sym, {"price": round(p,4), "change": round(ch,4),
                                 "change_pct": round((ch/pc*100) if pc else 0, 4),
                                 "prev_close": round(pc,4), "day_high": round(float(fi.day_high or 0),4),
                                 "day_low": round(float(fi.day_low or 0),4), "market_cap": 0, "volume": 0}
                except: return sym, dict(_zero)
            with ThreadPoolExecutor(max_workers=min(len(missing), 15)) as pool:
                for sym, d in pool.map(_fi, missing):
                    price_map[sym] = d

        def _d(sym): return price_map.get(sym, dict(_zero))
        result = {"indices": {}, "sectors": {}, "crypto": {}}
        for sym in MARKET_SYMBOLS:
            result["indices"][sym] = {"name": NAME_MAP.get(sym, sym), **_d(sym)}
        for sym, name in SECTOR_ETFS.items():
            d = _d(sym)
            result["sectors"][sym] = {"name": name, "price": d["price"], "change_pct": d["change_pct"]}
        for sym, name in CRYPTO_SYMBOLS.items():
            d = _d(sym)
            result["crypto"][sym] = {"name": name, "price": d["price"], "change": d["change"],
                                     "change_pct": d["change_pct"], "market_cap": d["market_cap"],
                                     "volume": d["volume"]}
        return result

    try:
        return _from_cache(key, fetch, ttl=60)
    except Exception as e:
        print(f"⚠️ get_market_data failed: {e}")
        if key in _cache:
            print("  ↩ Returning stale market cache")
            return _cache[key]["data"]
        return {"indices": {}, "sectors": {}, "crypto": {}}

def get_movers():
    """Scan MOVER_WATCHLIST via a single batch yf.download() call.
    Name/sector/52w/avg_volume metadata is kept in a 24-hour cache and refreshed in background."""
    key = "market_movers"
    _meta_ttl = 3600 * 24

    def _get_meta(sym):
        mk = f"meta:{sym}"
        if mk in _cache and time.time() - _cache[mk]["ts"] < _meta_ttl:
            return _cache[mk]["data"]
        try:
            info = yf.Ticker(sym).info
            meta = {
                "name":        info.get("shortName", sym),
                "sector":      info.get("sector", ""),
                "week52_high": float(info.get("fiftyTwoWeekHigh") or 0),
                "week52_low":  float(info.get("fiftyTwoWeekLow")  or 0),
                "avg_volume":  int(info.get("averageVolume")       or 0),
            }
        except:
            meta = {"name": sym, "sector": "", "week52_high": 0, "week52_low": 0, "avg_volume": 0}
        _cache[mk] = {"ts": time.time(), "data": meta}
        return meta

    def fetch():
        import pandas as pd
        print(f"\n📡 Movers — {len(MOVER_WATCHLIST)} stocks (batch download)…")
        price_map = {}
        try:
            raw = yf.download(
                tickers=' '.join(MOVER_WATCHLIST),
                period='5d', interval='1d',
                group_by='ticker', auto_adjust=True,
                progress=False, threads=False,
            )
            if raw is not None and not raw.empty and isinstance(raw.columns, pd.MultiIndex):
                for sym in raw.columns.get_level_values(0).unique():
                    try:
                        closes = raw[sym]['Close'].dropna()
                        if len(closes) < 1: continue
                        price = float(closes.iloc[-1])
                        prev  = float(closes.iloc[-2]) if len(closes) >= 2 else price
                        highs = raw[sym]['High'].dropna()
                        lows  = raw[sym]['Low'].dropna()
                        vols  = raw[sym]['Volume'].dropna()
                        opens = raw[sym]['Open'].dropna()
                        chg   = price - prev
                        pct   = (chg / prev * 100) if prev else 0
                        price_map[sym] = {
                            "price":      round(price, 4),
                            "change":     round(chg, 4),
                            "change_pct": round(pct, 4),
                            "day_high":   round(float(highs.iloc[-1]), 4) if not highs.empty else price,
                            "day_low":    round(float(lows.iloc[-1]),  4) if not lows.empty  else price,
                            "volume":     int(vols.iloc[-1])  if not vols.empty  else 0,
                            "open":       round(float(opens.iloc[-1]), 4) if not opens.empty else price,
                        }
                    except: pass
        except Exception as e:
            print(f"  ⚠️ movers batch error: {e}")

        # Build results using cached meta (non-blocking for main response)
        results = []
        for sym in MOVER_WATCHLIST:
            d = price_map.get(sym)
            if d is None or d["price"] <= 0:
                continue
            mk = f"meta:{sym}"
            meta = _cache[mk]["data"] if mk in _cache else {
                "name": sym, "sector": "", "week52_high": 0, "week52_low": 0, "avg_volume": 0}
            results.append({
                "symbol":      sym,
                "name":        meta["name"],
                "sector":      meta["sector"],
                "price":       d["price"],
                "change":      d["change"],
                "change_pct":  d["change_pct"],
                "open":        d["open"],
                "day_high":    d["day_high"],
                "day_low":     d["day_low"],
                "volume":      d["volume"],
                "avg_volume":  meta["avg_volume"],
                "market_cap":  0,
                "week52_high": meta["week52_high"],
                "week52_low":  meta["week52_low"],
                "vol_ratio":   round(d["volume"] / meta["avg_volume"], 2) if meta["avg_volume"] else 0,
            })

        # Kick off slow-metadata refresh in background (non-blocking)
        def _refresh_meta():
            syms_no_meta = [s for s in MOVER_WATCHLIST
                            if f"meta:{s}" not in _cache
                            or time.time() - _cache[f"meta:{s}"]["ts"] > _meta_ttl]
            if syms_no_meta:
                with ThreadPoolExecutor(max_workers=_YF_WORKERS) as pool:
                    list(pool.map(_get_meta, syms_no_meta[:20]))
        threading.Thread(target=_refresh_meta, daemon=True).start()

        results.sort(key=lambda x: x["change_pct"], reverse=True)
        top = [r for r in results if r["change_pct"] > 0][:15]
        bot = sorted([r for r in results if r["change_pct"] < 0], key=lambda x: x["change_pct"])[:15]
        return {"gainers": top, "losers": bot}

    try:
        return _from_cache(key, fetch, ttl=300)
    except Exception as e:
        print(f"⚠️ get_movers failed: {e}")
        if key in _cache:
            return _cache[key]["data"]
        return {"gainers": [], "losers": []}


def get_init_bundle(user_id, username):
    """Fetch everything the frontend needs on first load, all in parallel.
    Returns portfolio + quotes + market + futures + news + extended-hours in one server call."""
    holdings = db_get_portfolio(user_id)
    tickers  = [h['ticker'] for h in holdings if h.get('ticker')]
    with ThreadPoolExecutor(max_workers=5) as ex:
        f_market  = ex.submit(get_market_data)
        f_futures = ex.submit(get_futures)
        f_news    = ex.submit(get_market_news)
        f_quotes  = ex.submit(get_quotes, tickers)         if tickers else None
        f_ext     = ex.submit(get_extended_hours, tickers) if tickers else None
        market  = f_market.result()
        futures = f_futures.result()
        news    = f_news.result()
        quotes  = f_quotes.result() if f_quotes else {}
        ext     = f_ext.result()    if f_ext    else {}
    return {
        "portfolio": {"holdings": holdings, "username": username},
        "quotes":    quotes,
        "market":    market,
        "futures":   futures,
        "news":      news,
        "extended":  ext,
    }


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
        accept_enc = self.headers.get("Accept-Encoding", "")
        if "gzip" in accept_enc and len(body) > 2048:
            body = gzip.compress(body, compresslevel=6)
            self.send_response(status)
            self.send_header("Content-Type",     "application/json")
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Cache-Control",    "no-cache")
            self.send_header("Content-Length",   len(body))
            self.end_headers()
        else:
            self.send_response(status)
            self.send_header("Content-Type",   "application/json")
            self.send_header("Cache-Control",  "no-cache")
            self.send_header("Content-Length", len(body))
            self.end_headers()
        self.wfile.write(body)

    def send_file(self, fpath, ct="text/html; charset=utf-8"):
        with open(fpath, "rb") as f:
            body = f.read()

        # ETag for browser cache (304 Not Modified on repeat loads)
        etag = '"' + hashlib.md5(body).hexdigest()[:16] + '"'
        if self.headers.get("If-None-Match") == etag:
            self.send_response(304)
            self.end_headers()
            return

        # Gzip compression — compress once, cache the result in memory
        accept_enc = self.headers.get("Accept-Encoding", "")
        if "gzip" in accept_enc:
            cached = _gz_cache.get(fpath)
            if cached is None or cached["etag"] != etag:
                _gz_cache[fpath] = {"etag": etag, "gz": gzip.compress(body, compresslevel=6)}
            gz_body = _gz_cache[fpath]["gz"]
            self.send_response(200)
            self.send_header("Content-Type",     ct)
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length",   len(gz_body))
            self.send_header("ETag",             etag)
            self.send_header("Cache-Control",    "no-cache")
            self.end_headers()
            self.wfile.write(gz_body)
        else:
            self.send_response(200)
            self.send_header("Content-Type",   ct)
            self.send_header("Content-Length", len(body))
            self.send_header("ETag",           etag)
            self.send_header("Cache-Control",  "no-cache")
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

            elif path == "/api/init":
                self.send_json(get_init_bundle(user["user_id"], user["username"]))

            elif path == "/api/portfolio":
                holdings = db_get_portfolio(user["user_id"])
                self.send_json({"holdings": holdings, "username": user["username"]})

            elif path == "/api/quotes":
                syms = [s for s in qs.get("symbols", [""])[0].split(",") if s.strip()]
                self.send_json(get_quotes(syms))

            elif path == "/api/news":
                syms = [s for s in qs.get("symbols", [""])[0].split(",") if s.strip()]
                self.send_json(get_news(syms))

            elif path.startswith("/api/stock/financials/"):
                sym = path.split("/api/stock/financials/", 1)[-1].strip()
                self.send_json(get_stock_financials(sym))

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

            elif path == "/api/dividends":
                syms = [s for s in qs.get("symbols", [""])[0].split(",") if s.strip()]
                result = []
                for sym in syms[:30]:
                    meta_key = f"meta:{sym.upper()}"
                    m = _cache.get(meta_key, {}).get("data") or {}
                    ex = m.get("exDividendDate", "")
                    rate = m.get("dividendRate", 0)
                    yld  = m.get("dividendYield", 0)
                    if ex or rate:
                        result.append({"ticker": sym.upper(), "exDividendDate": ex,
                                       "dividendRate": rate, "dividendYield": yld})
                self.send_json(result)

            elif path == "/api/dividend-calendar":
                port_tickers = [s for s in qs.get("portfolio", [""])[0].split(",") if s.strip()]
                wl_tickers   = [s for s in qs.get("watchlist", [""])[0].split(",") if s.strip()]
                self.send_json(get_dividend_calendar(port_tickers, wl_tickers))

            elif path == "/api/correlation":
                tickers = [s for s in qs.get("tickers", [""])[0].split(",") if s.strip()]
                self.send_json(get_correlation_matrix(tickers))

            elif path == "/api/growth-picks":
                self.send_json(get_growth_picks())

            elif path == "/api/entry-analysis":
                sym = qs.get("ticker", [""])[0].strip().upper()
                if not sym:
                    self.send_error(400)
                else:
                    self.send_json(get_entry_analysis(sym))

            elif path == "/api/earnings-history":
                sym = qs.get("ticker", [""])[0].strip().upper()
                if not sym: self.send_error(400)
                else: self.send_json(get_earnings_history(sym))

            elif path == "/api/insider-trades":
                sym = qs.get("ticker", [""])[0].strip().upper()
                if not sym: self.send_error(400)
                else: self.send_json(get_insider_trades(sym))

            elif path == "/api/morning-briefing":
                holdings = db_get_portfolio(user["user_id"])
                mkt      = get_market_data()
                self.send_json(get_morning_briefing(holdings, mkt))

            elif path == "/api/stress-test":
                holdings = db_get_portfolio(user["user_id"])
                self.send_json(get_stress_test(holdings))

            elif path == "/api/backtest":
                holdings   = db_get_portfolio(user["user_id"])
                start_year = int(qs.get("start_year", ["2020"])[0])
                self.send_json(get_backtest(holdings, start_year))

            elif path == "/api/economic-calendar":
                holdings  = db_get_portfolio(user["user_id"])
                tickers   = [h['ticker'] for h in holdings if h.get('ticker')]
                self.send_json(get_economic_calendar(tickers))

            elif path == "/api/db-backup":
                import shutil, io
                backup_path = os.path.join(BASE_DIR, 'maelkloud_backup.db')
                shutil.copy2(DB_PATH, backup_path)
                with open(backup_path, 'rb') as f:
                    data = f.read()
                self.send_response(200)
                self.send_header('Content-Type', 'application/octet-stream')
                self.send_header('Content-Disposition', 'attachment; filename="maelkloud_backup.db"')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return

            elif path == "/api/stock-statistics":
                sym = qs.get("ticker", [""])[0].strip().upper()
                if not sym: self.send_error(400)
                else: self.send_json(get_stock_statistics(sym))

            elif path == "/api/dividend-history":
                sym = qs.get("ticker", [""])[0].strip().upper()
                if not sym: self.send_error(400)
                else: self.send_json(get_dividend_history(sym))

            elif path == "/api/price-history":
                sym    = qs.get("ticker", [""])[0].strip().upper()
                period = qs.get("period", ["6m"])[0].strip()
                if not sym: self.send_error(400)
                else: self.send_json(get_price_history(sym, period))

            elif path == "/api/stock-profile":
                sym = qs.get("ticker", [""])[0].strip().upper()
                if not sym: self.send_error(400)
                else: self.send_json(get_stock_profile(sym))

            elif path == "/api/stock-comparison":
                syms   = qs.get("tickers", [""])[0].split(",")
                period = qs.get("period", ["1y"])[0].strip()
                syms   = [s.strip().upper() for s in syms if s.strip()]
                if not syms: self.send_error(400)
                else: self.send_json(get_stock_comparison(syms, period))

            elif path == "/api/financials-extended":
                sym    = qs.get("ticker", [""])[0].strip().upper()
                period = qs.get("period", ["annual"])[0].strip()
                if not sym: self.send_error(400)
                else: self.send_json(get_financials_extended(sym, period))

            elif path == "/api/screener":
                self.send_json(get_stock_screener())

            elif path == "/api/market-heatmap":
                self.send_json(get_market_heatmap())

            elif path == "/api/ipo-list":
                self.send_json(get_ipo_list())

            elif path == "/api/trending":
                self.send_json(get_trending_stocks())

            elif path.startswith("/api/etf/"):
                sym = path.split("/api/etf/",1)[-1].strip().upper()
                if not sym: self.send_error(400)
                else: self.send_json(get_etf_detail(sym))

            elif path == "/api/premarket-movers":
                self.send_json(get_premarket_movers())

            elif path == "/api/market-earnings-calendar":
                date_str = qs.get("date", [""])[0].strip() or None
                self.send_json(get_market_earnings_calendar(date_str))

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

    # Pre-warm the most expensive caches in background so the first request is instant
    def _warmup():
        time.sleep(4)   # let the server socket fully bind first
        print("🔥 Pre-warming caches (market, movers, futures, news)…")
        for fn in (get_market_data, get_futures, get_market_news, get_movers):
            try: fn()
            except Exception as _e: print(f"  ⚠️ warmup {fn.__name__}: {_e}")
        print("✅ Cache pre-warm complete")
    threading.Thread(target=_warmup, daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n\n  ✅  Server stopped.\n")
