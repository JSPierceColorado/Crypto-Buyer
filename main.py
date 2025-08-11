import os, json, time
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
COST_TAB     = os.getenv("CRYPTO_COST_TAB", "crypto_cost")

PCT_PER_TRADE = float(os.getenv("PERCENT_PER_TRADE", "5.0"))
MIN_NOTIONAL  = float(os.getenv("MIN_ORDER_NOTIONAL", "1.00"))
SLEEP_SEC     = float(os.getenv("SLEEP_BETWEEN_ORDERS_SEC", "0.8"))
POLL_SEC      = float(os.getenv("POLL_INTERVAL_SEC", "0.8"))
POLL_TRIES    = int(os.getenv("POLL_MAX_TRIES", "25"))

# Portfolio selection
PORTFOLIO_ID   = os.getenv("COINBASE_PORTFOLIO_ID") or ""
PORTFOLIO_NAME = os.getenv("COINBASE_PORTFOLIO_NAME") or ""

# Debug / safety
DRY_RUN         = os.getenv("DRY_RUN", "").lower() in ("1","true","yes")
DEBUG_BALANCES  = os.getenv("DEBUG_BALANCES", "").lower() in ("1","true","yes")

CB = RESTClient()  # reads COINBASE_API_KEY / COINBASE_API_SECRET


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

def norm_str(x) -> str:
    if x is None: return ""
    return str(x)

def norm_ccy(c) -> str:
    if c is None: return ""
    if isinstance(c, str): return c.upper()
    return (g(c, "code", "currency", "symbol", "base", default="") or "").upper()

def norm_amount(x) -> float:
    if x is None: return 0.0
    if isinstance(x, (int, float, str)):
        try: return float(x)
        except: return 0.0
    return float(g(x, "value", "amount", default=0.0) or 0.0)

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

def ensure_log(ws):
    if not ws.get_all_values():
        ws.append_row(["Timestamp","Action","Product","QuoteUSD","BaseQty","OrderID","Status","Note"])

def ensure_cost(ws):
    if not ws.get_all_values():
        ws.append_row(["Product","Qty","DollarCost","AvgCostUSD","UpdatedAt"])

def read_screener(ws) -> List[str]:
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return []
    header = [h.strip() for h in values[0]]
    idx = header.index("Product") if "Product" in header else 0
    out, seen = [], set()
    for r in values[1:]:
        if idx < len(r):
            pid = r[idx].strip().upper()
            if pid and pid not in seen:
                seen.add(pid); out.append(pid)
    return out


# =========================
# Coinbase – portfolios & balances
# =========================
def list_portfolios() -> List[Dict[str, str]]:
    """Return list of {'id':..., 'name':...} for portfolios visible to this API key."""
    resp = CB.get_portfolios()
    items = g(resp, "portfolios") or (resp if isinstance(resp, list) else [])
    out = []
    for p in items:
        pid = norm_str(g(p, "uuid", "id", "portfolio_uuid"))
        name = norm_str(g(p, "name", "portfolio_name"))
        if pid:
            out.append({"id": pid, "name": name})
    return out

def accounts_for_portfolio(portfolio_uuid: Optional[str] = None):
    if portfolio_uuid:
        return CB.get_accounts(portfolio_uuid=portfolio_uuid)
    return CB.get_accounts()

def usd_available_for_portfolio(portfolio_uuid: Optional[str]) -> float:
    acc = accounts_for_portfolio(portfolio_uuid)
    accounts = g(acc, "accounts") or (acc if isinstance(acc, list) else [])
    usd_total = 0.0
    for a in accounts:
        ccy = norm_ccy(g(a, "currency", "currency_symbol", "asset", "currency_code"))
        avail = norm_amount(g(a, "available_balance", "available", "balance", "available_balance_value"))
        if ccy == "USD":
            usd_total += avail
    if DEBUG_BALANCES:
        label = portfolio_uuid or "(key default)"
        print(f"[BAL] Portfolio {label}: USD {usd_total}")
    return usd_total

def resolve_portfolio_uuid() -> Optional[str]:
    # Priority: explicit ID → name match → (None = use key’s default)
    if PORTFOLIO_ID:
        return PORTFOLIO_ID
    if PORTFOLIO_NAME:
        for p in list_portfolios():
            if p["name"].strip().lower() == PORTFOLIO_NAME.strip().lower():
                return p["id"]
    return None

def pick_portfolio_with_usd_if_needed() -> Optional[str]:
    """If configured portfolio has $0, scan others and suggest one with USD."""
    configured = resolve_portfolio_uuid()
    usd_cfg = usd_available_for_portfolio(configured)
    if usd_cfg > 0:
        return configured
    # scan others
    try:
        ports = list_portfolios()
    except Exception:
        ports = []
    best_id, best_usd = None, 0.0
    for p in ports:
        pid = p["id"]
        usd = usd_available_for_portfolio(pid)
        if usd > best_usd:
            best_id, best_usd = pid, usd
    if DEBUG_BALANCES:
        if best_usd > 0 and (configured != best_id):
            print(f"[BAL] Suggest using portfolio {best_id} (USD {best_usd}). "
                  f"Set COINBASE_PORTFOLIO_ID={best_id} or move USD into your configured/default portfolio.")
    # We still return the configured (or None) so orders don’t target a portfolio your key might not control.
    return configured


# =========================
# Orders & fills (with portfolio)
# =========================
def place_buy(product_id: str, usd: float, portfolio_uuid: Optional[str]) -> str:
    if DRY_RUN:
        return "DRYRUN"
    params = dict(client_order_id="", product_id=product_id, quote_size=f"{usd:.2f}")
    if portfolio_uuid:
        params["portfolio_uuid"] = portfolio_uuid
    o = CB.market_order_buy(**params)
    return g(o, "order_id", "id", default="")

def poll_fills_sum(order_id: str, portfolio_uuid: Optional[str]) -> Dict[str, float]:
    if DRY_RUN:
        return {"base_qty": 0.0, "fill_usd": 0.0}
    for _ in range(POLL_TRIES):
        try:
            params = dict(order_id=order_id)
            if portfolio_uuid:
                params["portfolio_uuid"] = portfolio_uuid
            f = CB.get_fills(**params)
            fills = g(f, "fills") or (f if isinstance(f, list) else [])
            if fills:
                base_qty = 0.0
                fill_usd = 0.0
                for x in fills:
                    sz = float(g(x, "size", "filled_quantity", default=0) or 0)
                    qv = g(x, "quote_value", "commissionable_value")
                    if qv is not None:
                        fill_usd += float(qv)
                    else:
                        px = float(g(x, "price", default=0) or 0)
                        fill_usd += px * sz
                    base_qty += sz
                if base_qty > 0 and fill_usd > 0:
                    return {"base_qty": base_qty, "fill_usd": fill_usd}
        except Exception:
            pass
        time.sleep(POLL_SEC)
    return {"base_qty": 0.0, "fill_usd": 0.0}


# =========================
# Sheet helpers (cost basis)
# =========================
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


# =========================
# Main
# =========================
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

    # pick portfolio
    portfolio_uuid = pick_portfolio_with_usd_if_needed()

    # compute notional (from configured/default portfolio’s USD)
    usd_bal = usd_available_for_portfolio(portfolio_uuid)
    if DEBUG_BALANCES:
        print(f"[BAL] Using portfolio {portfolio_uuid or '(key default)'} with USD {usd_bal}")

    logs = []
    for i, pid in enumerate(products, 1):
        try:
            notional = usd_bal * (PCT_PER_TRADE / 100.0)
            if notional < MIN_NOTIONAL:
                note = f"Notional ${notional:.2f} < ${MIN_NOTIONAL:.2f}"
                print(f"⚠️ {pid} {note}")
                logs.append([now_iso(), "CRYPTO-BUY-SKIP", pid, f"{notional:.2f}", "", "", "SKIPPED", note])
                continue

            oid = place_buy(pid, notional, portfolio_uuid)
            fills = poll_fills_sum(oid, portfolio_uuid)
            base_qty = fills["base_qty"]
            fill_usd = fills["fill_usd"] if fills["fill_usd"] > 0 else notional

            status = "dry-run" if DRY_RUN else "submitted"
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
