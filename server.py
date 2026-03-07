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
import sqlite3, hashlib, secrets, resource
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
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
_YF_WORKERS = 5

# Global semaphore: at most 2 concurrent yfinance fetch batches at any time.
# Prevents FD exhaustion when market refresh + portfolio refresh + movers
# all fire simultaneously (each with _YF_WORKERS threads).
_YF_SEM = threading.Semaphore(2)

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
                    "shortName":       info.get("shortName", sym),
                    "sector":          info.get("sector", ""),
                    "trailingPE":      float(info.get("trailingPE")    or 0),
                    "beta":            float(info.get("beta")           or 0),
                    "dividendRate":    float(info.get("dividendRate")   or 0),
                    "dividendYield":   float(info.get("dividendYield")  or 0),
                    "exDividendDate":  ex_div_str,
                    "w52ChangePct":    float(info.get("52WeekChange")   or 0) * 100,
                }
                _cache[meta_key] = {"ts": time.time(), "data": meta}
            except:
                meta = {"shortName": sym, "sector": "", "trailingPE": 0,
                        "beta": 0, "dividendRate": 0, "dividendYield": 0,
                        "exDividendDate": "", "w52ChangePct": 0}

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
    return _from_cache(key, fetch)

# ── Extended hours (pre-market / after-hours) ─────────────────────────────────
def _fetch_one_extended(sym):
    """Fetch pre-market and after-hours prices from yfinance .info for a single symbol."""
    try:
        info = yf.Ticker(sym).info

        # Use regularMarketPrice as the reference close so dollar change and %
        # are always consistent with each other.
        reg_close = float(info.get('regularMarketPrice') or info.get('previousClose') or 0)

        def _calc(ext_price):
            """Return (change, pct) relative to the regular-session close."""
            if ext_price <= 0 or reg_close <= 0:
                return 0.0, 0.0
            chg = ext_price - reg_close
            pct = chg / reg_close * 100
            return round(chg, 4), round(pct, 3)

        pre_price  = float(info.get('preMarketPrice')  or 0)
        post_price = float(info.get('postMarketPrice') or 0)

        pre_change,  pre_pct  = _calc(pre_price)
        post_change, post_pct = _calc(post_price)

        return sym, {
            'pre_price':   round(pre_price,  4),
            'pre_change':  pre_change,
            'pre_pct':     pre_pct,
            'post_price':  round(post_price,  4),
            'post_change': post_change,
            'post_pct':    post_pct,
            'reg_close':   round(reg_close, 4),
        }
    except Exception as e:
        print(f"  ⚠️ ext-hours {sym}: {e}")
        return sym, {'pre_price': 0, 'pre_change': 0, 'pre_pct': 0,
                     'post_price': 0, 'post_change': 0, 'post_pct': 0,
                     'reg_close': 0}

def get_extended_hours(symbols):
    if not symbols: return {}
    symbols = [s.strip().upper() for s in symbols if s.strip()]
    key = 'exthours:' + ','.join(sorted(symbols))
    def fetch():
        print(f"⏰ Fetching extended hours for {len(symbols)} symbols in parallel…")
        with ThreadPoolExecutor(max_workers=_YF_WORKERS) as pool:
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
                fi    = yf.Ticker(sym).fast_info
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
        with ThreadPoolExecutor(max_workers=_YF_WORKERS) as pool:
            results = list(pool.map(_one, syms))
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
                    'h': round(float(row['High']),  4),
                    'l': round(float(row['Low']),   4),
                    'v': int(row.get('Volume', 0)),
                })
            return pts
        except Exception as e:
            print(f"  History error {symbol}: {e}")
            return []
    ttl_map = {'1d': 30, '5d': 300, '1mo': 3600, '3mo': 7200, '6mo': 14400, '1y': 86400}
    return _from_cache(key, fetch, ttl=ttl_map.get(period, 3600))

def get_market_data():
    """Fetch indices, sector ETFs, and crypto in parallel using individual Ticker calls.
    Replaces the unreliable yf.Tickers() batch approach which frequently silently fails."""
    key = "market_overview"
    def fetch():
        NAME_MAP = {"^GSPC":"S&P 500","^IXIC":"NASDAQ","^DJI":"DOW Jones","^RUT":"Russell 2000","^VIX":"VIX"}
        all_syms = MARKET_SYMBOLS + list(SECTOR_ETFS.keys()) + list(CRYPTO_SYMBOLS.keys())
        print(f"\n📡 Fetching market overview — {len(all_syms)} symbols in parallel…")

        def _fetch_one(sym):
            try:
                fi = yf.Ticker(sym).fast_info
                price      = float(fi.last_price      or 0)
                prev_close = float(fi.previous_close  or price or 1)
                change     = price - prev_close
                change_pct = (change / prev_close * 100) if prev_close else 0
                return sym, {
                    "price":      round(price, 4),
                    "change":     round(change, 4),
                    "change_pct": round(change_pct, 4),
                    "prev_close": round(prev_close, 4),
                    "day_high":   round(float(fi.day_high  or 0), 4),
                    "day_low":    round(float(fi.day_low   or 0), 4),
                    "market_cap": round(float(fi.market_cap or 0), 0),
                    "volume":     round(float(fi.three_month_average_volume or 0), 0),
                }
            except Exception as e:
                print(f"  ⚠️ market {sym}: {e}")
                return sym, {"price":0,"change":0,"change_pct":0,"prev_close":0,
                             "day_high":0,"day_low":0,"market_cap":0,"volume":0}

        with ThreadPoolExecutor(max_workers=_YF_WORKERS) as pool:
            raw = dict(pool.map(_fetch_one, all_syms))

        result = {"indices": {}, "sectors": {}, "crypto": {}}
        for sym in MARKET_SYMBOLS:
            d = raw[sym]
            result["indices"][sym] = {"name": NAME_MAP.get(sym, sym), **d}
        for sym, name in SECTOR_ETFS.items():
            d = raw[sym]
            result["sectors"][sym] = {"name": name, "price": d["price"], "change_pct": d["change_pct"]}
        for sym, name in CRYPTO_SYMBOLS.items():
            d = raw[sym]
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
    """Scan MOVER_WATCHLIST using parallel individual Ticker calls.
    Uses fast_info for live prices + cached slow metadata (name/sector/52w)."""
    key = "market_movers"
    # Slow metadata cache: symbol → {name, sector, week52_high, week52_low, avg_volume}
    # Populated lazily and kept for 24h so we don't slow down every movers refresh.
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
        print(f"\n📡 Scanning movers — {len(MOVER_WATCHLIST)} stocks in parallel…")

        def _scan_one(sym):
            try:
                fi    = yf.Ticker(sym).fast_info
                price = float(fi.last_price     or 0)
                prev  = float(fi.previous_close or price or 1)
                if price <= 0:
                    return None
                chg   = price - prev
                pct   = (chg / prev * 100) if prev else 0
                vol   = int(fi.last_volume or 0)
                mcap  = float(fi.market_cap or 0)
                hi    = float(fi.day_high   or 0)
                lo    = float(fi.day_low    or 0)
                # Slow metadata from cache (non-blocking — uses stale if available)
                mk = f"meta:{sym}"
                meta = _cache[mk]["data"] if mk in _cache else {"name": sym, "sector": "",
                       "week52_high": 0, "week52_low": 0, "avg_volume": 0}
                return {
                    "symbol":      sym,
                    "name":        meta["name"],
                    "sector":      meta["sector"],
                    "price":       round(price, 4),
                    "change":      round(chg,   4),
                    "change_pct":  round(pct,   4),
                    "open":        round(float(fi.open or 0), 4),
                    "day_high":    round(hi, 4),
                    "day_low":     round(lo, 4),
                    "volume":      vol,
                    "avg_volume":  meta["avg_volume"],
                    "market_cap":  mcap,
                    "week52_high": meta["week52_high"],
                    "week52_low":  meta["week52_low"],
                    "vol_ratio":   round(vol / meta["avg_volume"], 2) if meta["avg_volume"] else 0,
                }
            except Exception as e:
                print(f"  ⚠️ movers {sym}: {e}")
                return None

        with ThreadPoolExecutor(max_workers=_YF_WORKERS) as pool:
            raw = list(pool.map(_scan_one, MOVER_WATCHLIST))

        # Kick off slow-metadata refresh in background (non-blocking)
        def _refresh_meta():
            syms_no_meta = [s for s in MOVER_WATCHLIST
                            if f"meta:{s}" not in _cache
                            or time.time() - _cache[f"meta:{s}"]["ts"] > _meta_ttl]
            if syms_no_meta:
                with ThreadPoolExecutor(max_workers=_YF_WORKERS) as pool:
                    list(pool.map(_get_meta, syms_no_meta[:20]))
        threading.Thread(target=_refresh_meta, daemon=True).start()

        results = [r for r in raw if r is not None]
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
