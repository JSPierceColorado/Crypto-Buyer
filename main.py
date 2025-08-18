import os, json, time, random
from datetime import datetime, timezone
from typing import List, Any, Dict, Optional

import gspread
from coinbase.rest import RESTClient

# =========================
# Config (env or defaults)
# =========================
SHEET_NAME   = os.getenv("SHEET_NAME", "Trading Log")
SCREENER_TAB = os.getenv("CRYPTO_SCREENER_TAB", "crypto_screener")
LOG_TAB      = os.getenv("CRYPTO_LOG_TAB", "crypto_log")

PCT_PER_TRADE = float(os.getenv("PERCENT_PER_TRADE", "5.0"))
MIN_NOTIONAL  = float(os.getenv("MIN_ORDER_NOTIONAL", "1.00"))
SLEEP_SEC     = float(os.getenv("SLEEP_BETWEEN_ORDERS_SEC", "0.8"))
POLL_SEC      = float(os.getenv("POLL_INTERVAL_SEC", "0.8"))
POLL_TRIES    = int(os.getenv("POLL_MAX_TRIES", "25"))

# Auto-convert support (within API key's portfolio only)
AUTO_CONVERT       = os.getenv("AUTO_CONVERT", "true").lower() in ("1","true","yes")
CONVERT_FROM_CCY   = os.getenv("CONVERT_FROM_CCY", "USDC").upper()
CONVERT_TO_CCY     = os.getenv("CONVERT_TO_CCY", "USD").upper()
CONVERT_PAD_PCT    = float(os.getenv("CONVERT_PAD_PCT", "1.0"))  # convert 1% extra to be safe

# Debug / safety
DRY_RUN         = os.getenv("DRY_RUN", "").lower() in ("1","true","yes")
DEBUG_BALANCES  = os.getenv("DEBUG_BALANCES", "").lower() in ("1","true","yes")

CB = RESTClient()  # reads COINBASE_API_KEY / COINBASE_API_SECRET

# =========================
# Sheet layout anchors
# =========================
LOG_HEADERS       = ["Timestamp","Action","Product","QuoteUSD","BaseQty","OrderID","Status","Note"]
LOG_TABLE_RANGE   = "A1:H1"

# =========================
# Utils
# =========================
def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def g(obj: Any, *names: str, default=None):
    """Get first present attr/key from an object or dict."""
    for n in names:
        if isinstance(obj, dict):
            if n in obj and obj[n] not in (None, ""):
                return obj[n]
        else:
            v = getattr(obj, n, None)
            if v not in (None, ""):
                return v
    return default

def parse_amount(x) -> float:
    """Accept float/int/str or {'value': '...'} shapes."""
    if x is None:
        return 0.0
    if isinstance(x, dict):
        v = g(x, "value", "amount")
        try: return float(v or 0.0)
        except: return 0.0
    try:
        return float(x)
    except:
        return 0.0

def norm_ccy(c) -> str:
    if c is None: return ""
    if isinstance(c, str): return c.upper()
    return (g(c, "code", "currency", "symbol", "base", default="") or "").upper()

def get_gc():
    raw = os.getenv("GOOGLE_CREDS_JSON")
    if not raw:
        raise RuntimeError("Missing GOOGLE_CREDS_JSON")
    return gspread.service_account_from_dict(json.loads(raw))

def _ws(gc, tab):
    sh = gc.open(SHEET_NAME)
    try:
        return sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=tab, rows="2000", cols="50")

# =========================
# Sheet helpers
# =========================
def ensure_log(ws):
    vals = ws.get_values("A1:H1")
    if not vals or vals[0] != LOG_HEADERS:
        ws.update("A1:H1", [LOG_HEADERS])
    try:
        ws.freeze(rows=1)
    except Exception:
        pass

def append_logs(ws, rows: List[List[str]]):
    # Force exactly 8 columns; anchor to A:H.
    fixed = []
    for r in rows:
        if len(r) < 8:   r = r + [""] * (8 - len(r))
        elif len(r) > 8: r = r[:8]
        fixed.append(r)
    try:
        for i in range(0, len(fixed), 100):
            ws.append_rows(
                fixed[i:i+100],
                value_input_option="RAW",
                table_range=LOG_TABLE_RANGE
            )
    except TypeError:
        start_row = len(ws.get_all_values()) + 1
        end_row = start_row + len(fixed) - 1
        ws.update(f"A{start_row}:H{end_row}", fixed, value_input_option="RAW")

def read_screener(ws) -> List[str]:
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []
    header = [h.strip() for h in values[0]]
    # Prefer "Product" but accept common fallbacks
    possible = ["Product","product","Ticker","Symbol"]
    idx = 0
    for name in possible:
        if name in header:
            idx = header.index(name)
            break
    out, seen = [], set()
    for r in values[1:]:
        if idx < len(r):
            pid = (r[idx] or "").strip().upper()
            if pid and pid.endswith("-USD") and pid not in seen:
                seen.add(pid); out.append(pid)
    return out

# =========================
# Coinbase helpers
# =========================
def list_accounts() -> List[Dict[str, Any]]:
    resp = CB.get_accounts()
    return g(resp, "accounts") or (resp if isinstance(resp, list) else [])

def usd_usdc_balances() -> Dict[str, float]:
    """Return {'USD': amt, 'USDC': amt} in the key's portfolio."""
    usd = 0.0; usdc = 0.0
    for a in list_accounts():
        ccy = norm_ccy(g(a, "currency", "asset", "currency_symbol"))
        avail = parse_amount(g(a, "available_balance", "available", "balance", "available_balance_value"))
        if ccy == "USD":  usd += avail
        if ccy == "USDC": usdc += avail
    if DEBUG_BALANCES:
        print(f"[BAL] USD={usd:.2f} USDC={usdc:.2f}")
    return {"USD": usd, "USDC": usdc}

def convert_ccy(from_ccy: str, to_ccy: str, amount: float) -> Optional[str]:
    """
    Try to convert from_ccy -> to_ccy within this portfolio.
    Returns conversion id / client id if successful; None otherwise.
    """
    if DRY_RUN:
        return "DRYRUN-CONVERT"
    if amount <= 0:
        return None
    try:
        # The Advanced Trade API supports a conversions endpoint.
        # The exact method name can vary by SDK; try common ones defensively.
        if hasattr(CB, "create_conversion"):
            res = CB.create_conversion(from_ccy=from_ccy, to_ccy=to_ccy, amount=f"{amount:.2f}")
        elif hasattr(CB, "convert_currency"):
            res = CB.convert_currency(from_ccy=from_ccy, to_ccy=to_ccy, amount=f"{amount:.2f}")
        else:
            print("[CONVERT] Conversion method not available in this SDK.")
            return None
        cid = g(res, "conversion_id", "id", "client_id", default=None)
        print(f"[CONVERT] {from_ccy}->{to_ccy} ${amount:.2f} id={cid}")
        # Give Coinbase a moment to settle balances
        time.sleep(1.0)
        return cid or "CONVERTED"
    except Exception as e:
        print(f"[CONVERT] error: {e}")
        return None

def place_buy(product_id: str, usd: float) -> str:
    if DRY_RUN:
        return "DRYRUN"
    client_order_id = f"buy-{product_id}-{int(time.time()*1000)}"
    o = CB.market_order_buy(
        client_order_id=client_order_id,
        product_id=product_id,
        quote_size=f"{usd:.2f}"
    )
    return g(o, "order_id", "id", default=client_order_id)

def poll_fills_sum(order_id: str) -> Dict[str, float]:
    if DRY_RUN:
        return {"base_qty": 0.0, "fill_usd": 0.0}
    for _ in range(POLL_TRIES):
        try:
            f = CB.get_fills(order_id=order_id)  # orders tied to key's portfolio
            fills = g(f, "fills") or (f if isinstance(f, list) else [])
            if fills:
                base_qty = 0.0
                fill_usd = 0.0
                for x in fills:
                    sz = parse_amount(g(x, "size", "filled_quantity"))
                    qv = g(x, "quote_value", "commissionable_value")
                    if qv is not None:
                        fill_usd += parse_amount(qv)
                    else:
                        px = parse_amount(g(x, "price"))
                        fill_usd += px * sz
                    base_qty += sz
                if base_qty > 0 and fill_usd > 0:
                    return {"base_qty": base_qty, "fill_usd": fill_usd}
        except Exception:
            pass
        time.sleep(POLL_SEC)
    return {"base_qty": 0.0, "fill_usd": 0.0}

# =========================
# Main
# =========================
def main():
    print("🛒 crypto-buyer starting")
    gc = get_gc()
    ws_scr = _ws(gc, SCREENER_TAB)
    ws_log = _ws(gc, LOG_TAB);  ensure_log(ws_log)

    products = read_screener(ws_scr)
    if not products:
        print("ℹ️ No products in screener; exiting.")
        return

    # Starting balances
    bal = usd_usdc_balances()
    usd_budget = bal["USD"]
    usdc_bal   = bal["USDC"]

    logs = []

    for i, pid in enumerate(products, 1):
        try:
            # Recompute per-trade notional from CURRENT budget
            notional = usd_budget * (PCT_PER_TRADE / 100.0)

            if notional < MIN_NOTIONAL:
                # Attempt on-demand USDC->USD conversion if enabled
                if AUTO_CONVERT and usdc_bal > 0 and (MIN_NOTIONAL - notional) > 0:
                    need = (MIN_NOTIONAL - notional) * (1 + CONVERT_PAD_PCT/100.0)
                    conv_amt = min(max(need, 0.0), usdc_bal)
                    if conv_amt >= 0.50:  # avoid dust conversions
                        convert_ccy(CONVERT_FROM_CCY, CONVERT_TO_CCY, conv_amt)
                        # Refresh balances/budget after conversion
                        bal = usd_usdc_balances()
                        usd_budget = bal["USD"]
                        usdc_bal   = bal["USDC"]
                        notional   = usd_budget * (PCT_PER_TRADE / 100.0)

            if notional < MIN_NOTIONAL:
                note = f"Notional ${notional:.2f} < ${MIN_NOTIONAL:.2f}"
                print(f"⚠️ {pid} {note}")
                logs.append([now_iso(), "CRYPTO-BUY-SKIP", pid, f"{notional:.2f}", "", "", "SKIPPED", note])
                continue

            # Clamp notional to what's actually left in USD
            notional = min(notional, usd_budget)

            oid = place_buy(pid, notional)
            fills = poll_fills_sum(oid)
            base_qty = fills["base_qty"]
            fill_usd = fills["fill_usd"] if fills["fill_usd"] > 0 else notional

            status = "dry-run" if DRY_RUN else "submitted"
            print(f"✅ BUY {pid} ${notional:.2f} (order {oid}, {status})")
            logs.append([
                now_iso(), "CRYPTO-BUY", pid,
                f"{notional:.2f}",
                f"{base_qty:.12f}" if base_qty else "",
                oid, status, ""
            ])

            # Reduce USD budget by what we just spent (estimate by fill_usd)
            usd_budget = max(0.0, usd_budget - fill_usd)

            # slight jitter to avoid bursty patterns
            time.sleep(SLEEP_SEC * (0.8 + 0.4 * random.random()))
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            print(f"❌ {pid} {msg}")
            logs.append([now_iso(), "CRYPTO-BUY-ERROR", pid, "", "", "", "ERROR", msg])

    if logs:
        append_logs(ws_log, logs)
    print("✅ crypto-buyer done")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("❌ Fatal error:", e)
        traceback.print_exc()
