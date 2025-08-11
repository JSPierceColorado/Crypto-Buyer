import os, json, time
from datetime import datetime, timezone
from typing import List, Dict, Any

import gspread
from coinbase.rest import RESTClient

SHEET_NAME   = os.getenv("SHEET_NAME", "Trading Log")
SCREENER_TAB = os.getenv("CRYPTO_SCREENER_TAB", "crypto_screener")
LOG_TAB      = os.getenv("CRYPTO_LOG_TAB", "crypto_log")
COST_TAB     = os.getenv("CRYPTO_COST_TAB", "crypto_cost")

PCT_PER_TRADE = float(os.getenv("PERCENT_PER_TRADE", "5.0"))
SLEEP_SEC     = float(os.getenv("SLEEP_BETWEEN_ORDERS_SEC", "0.8"))
POLL_SEC      = float(os.getenv("POLL_INTERVAL_SEC", "0.8"))
POLL_TRIES    = int(os.getenv("POLL_MAX_TRIES", "25"))

CB = RESTClient()

def now_iso(): return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def get_gc():
    raw = os.getenv("GOOGLE_CREDS_JSON"); 
    if not raw: raise RuntimeError("Missing GOOGLE_CREDS_JSON")
    return gspread.service_account_from_dict(json.loads(raw))

def _ws(gc, tab):
    sh = gc.open(SHEET_NAME)
    try: return sh.worksheet(tab)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=tab, rows="2000", cols="50")

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

def ensure_log(ws):
    if not ws.get_all_values():
        ws.append_row(["Timestamp","Action","Product","QuoteUSD","BaseQty","OrderID","Status","Note"])

def ensure_cost(ws):
    if not ws.get_all_values():
        ws.append_row(["Product","Qty","DollarCost","AvgCostUSD","UpdatedAt"])

def get_usd_available() -> float:
    acc = CB.get_accounts()
    accounts = acc.get("accounts", acc) if isinstance(acc, dict) else acc.accounts
    usd = 0.0
    for a in accounts:
        ccy = a.get("currency") or a.get("currency_symbol") or ""
        if ccy == "USD":
            bal = a.get("available_balance") or {}
            v = bal.get("value") if isinstance(bal, dict) else None
            try: usd = float(v or 0.0)
            except: usd = 0.0
            break
    return usd

def place_buy(product_id: str, usd: float) -> str:
    o = CB.market_order_buy(client_order_id="", product_id=product_id, quote_size=f"{usd:.2f}")
    oid = o.get("order_id", "") if isinstance(o, dict) else getattr(o, "order_id", "")
    return oid

def poll_fills_sum(order_id: str) -> Dict[str, float]:
    """Poll fills; return {'base_qty':..., 'fill_usd':...}"""
    base_qty = 0.0; fill_usd = 0.0
    for _ in range(POLL_TRIES):
        try:
            f = CB.get_fills(order_id=order_id)  # may need paging if many fills
            fills = f.get("fills", f) if isinstance(f, dict) else f.fills
            if fills:
                base_qty = sum(float(x.get("size") or x.get("filled_quantity") or 0.0) for x in fills)
                fill_usd = sum(float(x.get("commissionable_value") or x.get("price")*x.get("size",0.0) or 0.0) for x in fills)  # be defensive
                # If price*size not provided, try 'quote_value'
                for x in fills:
                    qv = x.get("quote_value")
                    if qv:
                        try: fill_usd = float(qv); break
                        except: pass
                if base_qty > 0 and fill_usd > 0:
                    return {"base_qty": base_qty, "fill_usd": fill_usd}
        except Exception:
            pass
        time.sleep(POLL_SEC)
    return {"base_qty": base_qty, "fill_usd": fill_usd}

def upsert_cost(ws, product: str, add_qty: float, add_cost: float):
    vals = ws.get_all_values()
    hdr = vals[0] if vals else []
    idx = 0
    if vals:
        for r in range(1, len(vals)):
            if vals[r][0].strip().upper() == product:
                # update existing
                cur_qty = float(vals[r][1] or 0.0); cur_cost = float(vals[r][2] or 0.0)
                new_qty = cur_qty + add_qty
                new_cost = cur_cost + add_cost
                avg = (new_cost / new_qty) if new_qty > 0 else 0.0
                ws.update(f"A{r+1}:E{r+1}", [[product, f"{new_qty:.12f}", f"{new_cost:.2f}", f"{avg:.6f}", now_iso()]])
                return
    # insert new
    avg = (add_cost / add_qty) if add_qty > 0 else 0.0
    ws.append_row([product, f"{add_qty:.12f}", f"{add_cost:.2f}", f"{avg:.6f}", now_iso()])

def main():
    print("🛒 crypto-buyer starting")
    gc = get_gc()
    ws_scr = _ws(gc, SCREENER_TAB)
    ws_log = _ws(gc, LOG_TAB)
    ws_cost= _ws(gc, COST_TAB)
    ensure_log(ws_log); ensure_cost(ws_cost)

    products = read_screener(ws_scr)
    if not products:
        print("ℹ️ No products in screener; exiting.")
        return

    logs = []
    for i, pid in enumerate(products, 1):
        try:
            usd_bal = get_usd_available()
            notional = usd_bal * (PCT_PER_TRADE / 100.0)
            if notional < 1.00:
                logs.append([now_iso(), "CRYPTO-BUY-SKIP", pid, f"{notional:.2f}", "", "", "SKIPPED", "Notional < $1"])
                continue

            oid = place_buy(pid, notional)
            fills = poll_fills_sum(oid)
            base_qty = fills["base_qty"]; fill_usd = fills["fill_usd"] if fills["fill_usd"] > 0 else notional

            logs.append([now_iso(), "CRYPTO-BUY", pid, f"{notional:.2f}", f"{base_qty:.12f}", oid, "submitted", ""])
            if base_qty > 0 and fill_usd > 0:
                upsert_cost(ws_cost, pid, base_qty, fill_usd)

            time.sleep(SLEEP_SEC)
        except Exception as e:
            logs.append([now_iso(), "CRYPTO-BUY-ERROR", pid, "", "", "", "ERROR", f"{type(e).__name__}: {e}"])
    # write logs
    for i in range(0, len(logs), 100):
        ws_log.append_rows(logs[i:i+100], value_input_option="USER_ENTERED")
    print("✅ crypto-buyer done")

if __name__ == "__main__":
    main()
