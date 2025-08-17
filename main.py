import os, json, time, math
from datetime import datetime, timedelta, timezone
from typing import List, Any, Tuple, Optional

import gspread
import pandas as pd
import numpy as np
from coinbase.rest import RESTClient  # coinbase-advanced-py

# ============== Config ==============
SHEET_NAME        = os.getenv("SHEET_NAME", "Trading Log")
PRODUCTS_TAB      = os.getenv("CRYPTO_PRODUCTS_TAB", "crypto_products")
SCREENER_TAB      = os.getenv("CRYPTO_SCREENER_TAB", "crypto_screener")

LOOKBACK_4H       = int(os.getenv("LOOKBACK_4H", "220"))                # 4h bars fetched
MIN_24H_NOTIONAL  = float(os.getenv("MIN_24H_NOTIONAL", "2000000"))     # USD (baseline)
RSI_MIN           = float(os.getenv("RSI_MIN", "50"))
RSI_MAX           = float(os.getenv("RSI_MAX", "65"))
MAX_EXT_EMA20_PCT = float(os.getenv("MAX_EXT_EMA20_PCT", "0.08"))       # 8% cap
REQUIRE_7D_HIGH   = os.getenv("REQUIRE_7D_HIGH", "true").lower() in ("1","true","yes")

# New: regime + ranking controls
REGIME_GUARD      = os.getenv("REGIME_GUARD", "true").lower() in ("1","true","yes")
TOP_N             = int(os.getenv("TOP_N", "20"))                        # keep best N picks
RS_LOOKBACK       = int(os.getenv("RS_LOOKBACK", "14"))                  # 14 x 4h ~ 2.3 days
VOLATILITY_PCT_MIN= float(os.getenv("VOLATILITY_PCT_MIN", "0.03"))       # ATR14/EMA20 ≥ 3%
VOL_EXPANSION_MULT= float(os.getenv("VOL_EXPANSION_MULT", "1.20"))       # latest 24h vs prior median
MICRO_PRICE       = float(os.getenv("MICRO_PRICE", "0.10"))              # sub-10c = stricter
MICRO_NOTIONAL_MIN= float(os.getenv("MICRO_NOTIONAL_MIN", "10000000"))   # 10M for micro price
STRICT_MICRO      = os.getenv("STRICT_MICRO", "true").lower() in ("1","true","yes")

PER_PRODUCT_SLEEP = float(os.getenv("PER_PRODUCT_SLEEP", "0.05"))        # polite throttle (sec)

# ============== Small utils ==============
def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def g(obj: Any, *names: str, default=None):
    for n in names:
        if isinstance(obj, dict):
            if n in obj and obj[n] not in (None, ""):
                return obj[n]
        else:
            v = getattr(obj, n, None)
            if v not in (None, ""):
                return v
    return default

def _floor_to_4h(dt_utc: datetime) -> datetime:
    dt_utc = dt_utc.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return dt_utc.replace(hour=(dt_utc.hour // 4) * 4)

# ============== Sheets helpers ==============
def get_google_client():
    raw = os.getenv("GOOGLE_CREDS_JSON")
    if not raw:
        raise RuntimeError("Missing GOOGLE_CREDS_JSON")
    return gspread.service_account_from_dict(json.loads(raw))

def _get_ws(gc, tab_name: str):
    sh = gc.open(SHEET_NAME)
    try:
        return sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=tab_name, rows="2000", cols="50")

def write_products(ws, products: List[str]):
    ws.clear()
    ws.append_row(["Product"])
    if products:
        ws.append_rows([[p] for p in products], value_input_option="USER_ENTERED")

def write_screener(ws, rows: List[List[Any]]):
    ws.clear()
    headers = [
        "Product","Price","EMA_20","SMA_50","RSI_14",
        "MACD","Signal","MACD_Hist","MACD_Hist_Δ",
        "Vol24hUSD","7D_High","Breakout","Bullish Signal",
        "RS14","ROC14","Score","Buy Reason","Timestamp"
    ]
    ws.append_row(headers)
    for i in range(0, len(rows), 100):
        ws.append_rows(rows[i:i+100], value_input_option="USER_ENTERED")

# ============== Coinbase helpers ==============
CB = RESTClient()

def all_usd_products() -> List[str]:
    prods = []
    cursor = None
    while True:
        resp = CB.get_products(limit=250, cursor=cursor) if cursor else CB.get_products(limit=250)
        products = g(resp, "products") or (resp if isinstance(resp, list) else [])
        for p in products:
            pid = g(p, "product_id", "productId", "id")
            if not pid:
                base = g(p, "base_currency", "baseCurrency", "base")
                quote = g(p, "quote_currency", "quoteCurrency", "quote")
                if base and quote:
                    pid = f"{base}-{quote}"
            status = (g(p, "status", "trading_status", "tradingStatus", default="") or "").upper()
            if pid and pid.endswith("-USD") and status == "ONLINE":
                prods.append(pid)
        cursor = g(resp, "cursor")
        if not cursor:
            break
    return sorted(dict.fromkeys(prods))

def get_candles_4h(product_id: str, bars: int) -> pd.DataFrame:
    end_dt = _floor_to_4h(datetime.now(timezone.utc))
    start_dt = end_dt - timedelta(hours=bars * 4)
    resp = CB.get_candles(
        product_id=product_id,
        start=int(start_dt.timestamp()),
        end=int(end_dt.timestamp()),
        granularity="FOUR_HOUR",
    )
    rows = g(resp, "candles") or (resp if isinstance(resp, list) else [])
    out = []
    for c in rows:
        out.append({
            "start":  g(c, "start"),
            "open":   float(g(c, "open",   default=0) or 0),
            "high":   float(g(c, "high",   default=0) or 0),
            "low":    float(g(c, "low",    default=0) or 0),
            "close":  float(g(c, "close",  default=0) or 0),
            "volume": float(g(c, "volume", default=0) or 0),
        })
    if not out:
        return pd.DataFrame()
    df = pd.DataFrame(out).sort_values("start")
    return df.tail(bars).reset_index(drop=True)

# ============== Indicators ==============
def ema(s, w): return s.ewm(span=w, adjust=False).mean()
def sma(s, w): return s.rolling(w).mean()

def rsi(series, window=14):
    delta = series.diff()
    up = delta.clip(lower=0); down = -delta.clip(upper=0)
    avg_gain = up.ewm(alpha=1/window, adjust=False).mean()
    avg_loss = down.ewm(alpha=1/window, adjust=False).mean().replace(0, np.nan)
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def macd(series, fast=12, slow=26, signal=9):
    ef = ema(series, fast); es = ema(series, slow)
    line = ef - es; sig = ema(line, signal)
    hist = line - sig
    return line, sig, hist

def atr(df: pd.DataFrame, w: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat([(h-l).abs(), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(window=w, min_periods=w).mean()

# ============== Regime guard ==============
def regime_ok() -> Tuple[bool, str]:
    btc = get_candles_4h("BTC-USD", LOOKBACK_4H)
    eth = get_candles_4h("ETH-USD", LOOKBACK_4H)
    if btc.empty or eth.empty or len(btc) < 60 or len(eth) < 60:
        return False, "insufficient BTC/ETH data"

    def _ok(df):
        close = pd.to_numeric(df["close"]); e20 = ema(close,20); s50 = sma(close,50)
        m_line, m_sig, m_hist = macd(close)
        cond = (close.iloc[-1] > e20.iloc[-1] > s50.iloc[-1]) \
               and (m_line.iloc[-1] > m_sig.iloc[-1]) \
               and (e20.iloc[-1] > e20.iloc[-2])
        return bool(cond)

    if _ok(btc) and _ok(eth):
        return True, "BTC/ETH uptrend (4h)"
    return False, "BTC/ETH not aligned up (4h)"

# ============== Analyze one ==============
def analyze(pid: str, btc_close: Optional[pd.Series]) -> Optional[List[Any]]:
    df = get_candles_4h(pid, LOOKBACK_4H)
    if df.empty or df.shape[0] < 60:
        return None

    close = pd.to_numeric(df["close"], errors="coerce").astype(float)
    vol   = pd.to_numeric(df["volume"], errors="coerce").astype(float)
    high  = pd.to_numeric(df["high"], errors="coerce").astype(float)
    low   = pd.to_numeric(df["low"],  errors="coerce").astype(float)

    price = float(close.iloc[-1])
    ema20s= ema(close, 20); ema20 = float(ema20s.iloc[-1])
    sma50 = float(sma(close, 50).iloc[-1])
    rsi14 = float(rsi(close, 14).iloc[-1])

    macd_line, macd_sig, macd_hist = macd(close)
    macd_v    = float(macd_line.iloc[-1])
    signal_v  = float(macd_sig.iloc[-1])
    hist_v    = float(macd_hist.iloc[-1])
    hist_prev = float(macd_hist.iloc[-2]) if macd_hist.shape[0] >= 2 else np.nan
    hist_delta= hist_v - hist_prev if not math.isnan(hist_prev) else np.nan

    # 24h notional = sum of last 6x4h (close * volume)
    vol24_usd = float((close.tail(6) * vol.tail(6)).sum())

    # Volume expansion: compare last 24h to median of prior 5x24h windows
    roll_24 = (close * vol).rolling(6).sum()
    prev_median = float(roll_24.iloc[-7:-1].rolling(6).sum().median()) if len(roll_24) >= 12 else 0.0
    vol_exp_ok = (prev_median == 0.0) or (roll_24.iloc[-1] >= VOL_EXPANSION_MULT * prev_median)

    # 7D high (42 x 4h bars)
    high_7d = float(close.tail(42).max())
    breakout = price >= high_7d - 1e-9 if REQUIRE_7D_HIGH else (price >= close.tail(20).max() - 1e-9)

    # Volatility filter (skip stables/chop): ATR14 / EMA20
    a14 = atr(pd.DataFrame({"high":high,"low":low,"close":close}), 14)
    atr_pct = float(a14.iloc[-1] / max(1e-12, ema20)) if not a14.isna().iloc[-1] else 0.0
    if atr_pct < VOLATILITY_PCT_MIN:
        return None

    # Base filters
    if not (price > ema20 > sma50):
        return None
    if (price / ema20 - 1.0) > MAX_EXT_EMA20_PCT:
        return None
    if not (RSI_MIN < rsi14 < RSI_MAX):
        return None
    if not (macd_v > signal_v and hist_v > 0 and (not math.isnan(hist_delta) and hist_delta > 0)):
        return None
    if vol24_usd < MIN_24H_NOTIONAL:
        return None
    if REQUIRE_7D_HIGH and not breakout:
        return None
    if not vol_exp_ok:
        return None

    # Micro-price stricter path
    if STRICT_MICRO and price < MICRO_PRICE:
        if vol24_usd < MICRO_NOTIONAL_MIN:
            return None

    # Relative strength vs BTC (if available)
    RS14 = ROC14 = 0.0
    if btc_close is not None and len(btc_close) == len(close):
        # align lengths if needed
        n = min(len(btc_close), len(close))
        c = close.tail(n).reset_index(drop=True)
        b = pd.to_numeric(btc_close.tail(n)).reset_index(drop=True)
        if n > RS_LOOKBACK:
            ROC14 = float(c.iloc[-1] / c.iloc[-1-RS_LOOKBACK] - 1.0)
            BTC_ROC14 = float(b.iloc[-1] / b.iloc[-1-RS_LOOKBACK] - 1.0)
            RS14 = ROC14 - BTC_ROC14

    # Simple composite score: weight RS over raw momentum
    score = 100.0 * (0.6 * RS14 + 0.4 * ROC14)

    reason = (
        f"4h Uptrend (P>EMA20>SMA50), RSI {RSI_MIN}-{RSI_MAX}, "
        f"MACD>Signal & Hist↑, ≤{int(MAX_EXT_EMA20_PCT*100)}% above EMA20, "
        f"24h notional ≥ ${int(MIN_24H_NOTIONAL):,}"
        + (" + 7D breakout" if REQUIRE_7D_HIGH else "")
        + (f", volExp≥{VOL_EXPANSION_MULT}x" if VOL_EXPANSION_MULT>1.0 else "")
    )

    row = [
        pid,
        round(price, 6),
        round(ema20, 6),
        round(sma50, 6),
        round(rsi14, 2),
        round(macd_v, 6),
        round(signal_v, 6),
        round(hist_v, 6),
        round(hist_delta, 6) if not math.isnan(hist_delta) else "",
        int(vol24_usd),
        round(high_7d, 6),
        "✅" if breakout else "",
        "✅",
        round(RS14, 6),
        round(ROC14, 6),
        round(score, 4),
        reason,
        now_iso(),
    ]
    return row

# ============== Main ==============
def main():
    print("🚀 crypto-finder starting (regime + RS ranking)")
    gc = get_google_client()
    ws_products = _get_ws(gc, PRODUCTS_TAB)
    ws_screener = _get_ws(gc, SCREENER_TAB)

    products = all_usd_products()
    write_products(ws_products, products)
    print(f"📦 ONLINE USD products: {len(products)}")

    # Regime guard (BTC & ETH on 4h)
    if REGIME_GUARD:
        ok, why = regime_ok()
        print(f"🛡️  Regime: {why} | allowed={ok}")
        if not ok:
            write_screener(ws_screener, [])
            print("ℹ️ Regime guard blocking buys; screener cleared.")
            return

    # Pre-fetch BTC close for RS comparison
    btc_df = get_candles_4h("BTC-USD", LOOKBACK_4H)
    btc_close = pd.to_numeric(btc_df["close"]).reset_index(drop=True) if not btc_df.empty else None

    rows = []
    for i, pid in enumerate(products, 1):
        try:
            r = analyze(pid, btc_close if btc_close is not None else None)
            if r:
                rows.append(r)
        except Exception as e:
            print(f"⚠️ {pid} analyze error: {e}")
        if i % 20 == 0:
            print(f"   • analyzed {i}/{len(products)}")
        time.sleep(PER_PRODUCT_SLEEP)

    # Rank by Score desc and keep top N
    if rows:
        df = pd.DataFrame(rows, columns=[
            "Product","Price","EMA_20","SMA_50","RSI_14",
            "MACD","Signal","MACD_Hist","MACD_Hist_Δ",
            "Vol24hUSD","7D_High","Breakout","Bullish Signal",
            "RS14","ROC14","Score","Buy Reason","Timestamp"
        ])
        df = df.sort_values("Score", ascending=False).head(max(1, TOP_N))
        rows = df.values.tolist()
    else:
        rows = []

    write_screener(ws_screener, rows)
    print(f"✅ Screener wrote {len(rows)} picks to {SCREENER_TAB}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("❌ Fatal error:", e)
        traceback.print_exc()
