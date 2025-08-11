import os, json, time
from datetime import datetime, timezone
from typing import List, Any, Dict

import gspread
from coinbase.rest import RESTClient

SHEET_NAME   = os.getenv("SHEET_NAME", "Trading Log")
SCREENER_TAB = os.getenv("CRYPTO_SCREENER_TAB", "crypto_screener")
LOG_TAB      = os.getenv("CRYPTO_LOG_TAB", "crypto_log")
COST_TAB     = os.getenv("CRYPTO_COST_TAB", "crypto_cost")

PCT_PER_TRADE = float(os.getenv("PERCENT_PER_TRADE", "5.0"))
MIN_NOTIONAL  = float(os.getenv("MIN_ORDER_NOTIONAL", "1.00"))
SLEEP_SEC     = float(os.getenv("SLEEP_BETWEEN_ORDERS_SEC", "0.8"))
POLL_SEC      = float(os.getenv("POLL_INTERVAL_SEC", "0.8"))
POLL_TRIES    = int(os.getenv("POLL_MAX_TRIES", "25"))
DRY_RUN       = os.getenv("DRY_RUN", "").lower() in ("1","true","yes")

CB = RESTClient()  # reads COINBASE_API_KEY / COINBASE_API_SECRET

# ---------- utils ----------
def now_iso(): return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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

def get_gc():
    raw = os.getenv("GOOGLE_CREDS_JSON")
    if not raw:
        raise RuntimeError("Missing GOOGLE_CREDS_JSON")
    return gspread.service_account_from_dict(json.loads(raw))

def _ws(gc, tab):
    sh = gc.open(SHEET_NAME)
    try: return sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=tab, rows="2000", cols="50")

def ensure_log(ws):
    if not ws.get_all_values():
        ws.append_row(["Timestamp","Action","Product","QuoteUSD","BaseQty","OrderID","Status","Note"])

def ensure_cost(ws):
    if not ws.get_all_values():
        ws.append_row(["Product","Qty","DollarCost","AvgCostUSD","UpdatedAt"])

def read_screener(ws) -> List[str]:
    vals = ws.get_all_values()
    if not vals or len(vals) < 2: return []
    hdr = [h.strip() for h in vals[0]]
    i = hdr.index("Product") if "Product" in hdr else 0
    out, seen = [], set()
    for r in vals[1:]:
        if i < len(r):
            pid = r[i].strip().upper()
            if pid and pid not in seen:
                seen.add(pid); out.append(pid)
    return out

# ---------- coinbase helpers ----------
def get_usd_available() -> float:
    """Return available USD balance using object/dict-safe access."""
    acc = CB.get_accounts()
    accounts = g(acc, "accounts") or (acc if isinstance(acc, list) else [])
    for a in accounts:
        ccy = g(a, "currency", "currency_symbol")
        if ccy == "USD":
            bal = g(a, "available_balance")
            val = g(bal, "value") if bal is not None else None
            try:
                return float(val or 0.0)
            except:
                return 0.0
    return 0.0

def place_buy(product_id: str, usd: float) -> str:
    if DRY_RUN:
        return "DRYRUN"
    o = CB.market_order_buy(client_order_id="", product_id=product_id, quote_size=f"{usd:.2f}")
    return g(o, "order_id", "id", default="")

def poll_fills_sum(order_id: str) -> Dict[str, float]:
    """Poll fills; return {'base_qty':..., 'fill_usd':...} (best-effort)."""
    if DRY_RUN:
        return {"base_qty": 0.0, "fill_usd": 0.0}
    base_qty = 0.0; fill_usd = 0.0
    for _ in range(POLL_TRIES):
        try:
            f = CB.get_fills(order_id=order_id)
            fills = g(f, "fills") or (f if isinstance(f, list) else [])
            if fills:
                bsum = 0.0; qsum = 0.0
                for x in fills:
                    size = float(g(x, "size", "filled_quantity", default=0) or 0)
                    qv   = g(x, "quote_value", "commissionable_value")
                    if qv is not None:
                        qsum += float(qv)
                    else:
                        px = float(g(x, "price", default=0) or 0)
                        qsum += px * size
                    bsum += size
                if bsum > 0 and qsum > 0:
                    return {"base_qty": bsum, "fill_usd": qsum}
        except Exception:
            pass
        time.sleep(POLL_SEC)
    return {"base_qty": base_qty, "fill_usd": fill_usd}

def upsert_cost(ws, product: str, add_qty: float, add_cost: float):
    vals = ws.get_all_values()
    if vals and len(vals) > 1:
        for r in range(1, len(vals)):
            if vals[r][0].strip().upper() == product:
                cur_qty  = float(vals[r][1] or 0.0)
                cur_cost = float(vals[r][2] or 0.0)
                new_qty  = cur_qty + add_qty
                new_cost = cur_cost + add_cost
                avg = (new_cost / new_qty) if new_qty > 0 else 0.0
                ws.update(f"A{r+1}:E{r+1}", [[product, f"{new_qty:.12f}", f"{new_cost:.2f}", f"{avg:.6f}", now_iso()]])
                return
    avg = (add_cost / add_qty) if add_qty > 0 else 0.0
    ws.append_row([product, f"{add_qty:.12f}", f"{add_cost:.2f}", f"{avg:.6f}", now_iso()])

# ---------- main ----------
def main():
    print("🛒 crypto-buyer starting")
    gc = get_gc()
    ws_scr = _ws(gc, SCREENER_TAB)
    ws_log = _ws(gc, LOG_TAB);  ensure_log(ws_log)
    ws_cost= _ws(gc, COST_TAB); ensure_cost(ws_cost)

    products = read_screener(ws_scr)
    if not products:
        print("ℹ️ No products in screener; exiting.")
        return

    logs = []
    for i, pid in enumerate(products, 1):
        try:
            usd_bal = get_usd_available()
            notional = usd_bal * (PCT_PER_TRADE / 100.0)
            if notional < MIN_NOTIONAL:
                note = f"Notional ${notional:.2f} < ${MIN_NOTIONAL:.2f}"
                print(f"⚠️ {pid} {note}")
                logs.append([now_iso(), "CRYPTO-BUY-SKIP", pid, f"{notional:.2f}", "", "", "SKIPPED", note])
                continue

            oid = place_buy(pid, notional)
            fills = poll_fills_sum(oid)
            base_qty = fills["base_qty"]; fill_usd = fills["fill_usd"] if fills["fill_usd"] > 0 else notional

            status = "submitted" if not DRY_RUN else "dry-run"
            print(f"✅ BUY {pid} ${notional:.2f} (order {oid}, {status})")
            logs.append([now_iso(), "CRYPTO-BUY", pid, f"{notional:.2f}", f"{base_qty:.12f}" if base_qty else "", oid, status, ""])

            if not DRY_RUN and base_qty > 0 and fill_usd > 0:
                upsert_cost(ws_cost, pid, base_qty, fill_usd)

            time.sleep(SLEEP_SEC)
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            print(f"❌ {pid} {msg}")
            logs.append([now_iso(), "CRYPTO-BUY-ERROR", pid, "", "", "", "ERROR", msg])

    # write logs
    for i in range(0, len(logs), 100):
        ws_log.append_rows(logs[i:i+100], value_input_option="USER_ENTERED")
    print("✅ crypto-buyer done")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("❌ Fatal error:", e)
        traceback.print_exc()
