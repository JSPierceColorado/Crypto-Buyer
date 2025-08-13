import os, json, time, random
from datetime import datetime, timezone
from typing import List, Any, Dict, Optional
from decimal import Decimal, ROUND_DOWN, getcontext

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

# Portfolio hints (used only for balance reads / debugging)
PORTFOLIO_ID   = os.getenv("COINBASE_PORTFOLIO_ID") or ""
PORTFOLIO_NAME = os.getenv("COINBASE_PORTFOLIO_NAME") or ""

# Debug / safety
DRY_RUN         = os.getenv("DRY_RUN", "").lower() in ("1","true","yes")
DEBUG_BALANCES  = os.getenv("DEBUG_BALANCES", "").lower() in ("1","true","yes")

CB = RESTClient()  # reads COINBASE_API_KEY / COINBASE_API_SECRET

# Decimal / rounding config
getcontext().prec = 28
getcontext().rounding = ROUND_DOWN

# =========================
# Sheet layout anchors
# =========================
LOG_HEADERS       = ["Timestamp","Action","Product","QuoteUSD","BaseQty","OrderID","Status","Note"]
LOG_TABLE_RANGE   = "A1:H1"
COST_HEADERS      = ["Product","Qty","DollarCost","AvgCostUSD","UpdatedAt"]
COST_TABLE_RANGE  = "A1:E1"


# =========================
# Utils
# =========================
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

def _parse_num(x) -> float:
    if isinstance(x, (int, float)):
        return float(x)
    s = (x or "").strip()
    if not s:
        return 0.0
    s = s.replace(",", "").replace("$", "")
    if s.endswith("%"):
        s = s[:-1]
    try:
        return float(s)
    except Exception:
        return 0.0

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
# Sheet helpers (anchored)
# =========================
def ensure_log(ws):
    vals = ws.get_values("A1:H1")
    if not vals or vals[0] != LOG_HEADERS:
        ws.update(range_name="A1:H1", values=[LOG_HEADERS])
    try:
        ws.freeze(rows=1)
    except Exception:
        pass

def ensure_cost(ws):
    vals = ws.get_values("A1:E1")
    if not vals or vals[0] != COST_HEADERS:
        ws.update(range_name="A1:E1", values=[COST_HEADERS])
    try:
        ws.freeze(rows=1)
    except Exception:
        pass

def append_logs(ws, rows: List[List[str]]):
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

def append_cost_rows(ws, rows: List[List[str]]):
    fixed = []
    for r in rows:
        if len(r) < 5:   r = r + [""] * (5 - len(r))
        elif len(r) > 5: r = r[:5]
        fixed.append(r)
    try:
        for i in range(0, len(fixed), 100):
            ws.append_rows(
                fixed[i:i+100],
                value_input_option="RAW",
                table_range=COST_TABLE_RANGE
            )
    except TypeError:
        start_row = len(ws.get_all_values()) + 1
        end_row = start_row + len(fixed) - 1
        ws.update(f"A{start_row}:E{end_row}", fixed, value_input_option="RAW")


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
    if PORTFOLIO_ID:
        return PORTFOLIO_ID
    if PORTFOLIO_NAME:
        for p in list_portfolios():
            if p["name"].strip().lower() == PORTFOLIO_NAME.strip().lower():
                return p["id"]
    return None

def debug_portfolios_and_usd():
    try:
        resp = CB.get_portfolios()
        items = g(resp, "portfolios") or (resp if isinstance(resp, list) else [])
    except Exception as e:
        print(f"[PORT] error listing portfolios: {e}")
        items = []

    usd_default = usd_available_for_portfolio(None)
    print(f"[PORT] (key default) USD={usd_default}")

    for p in items:
        pid  = (g(p, "uuid", "id", "portfolio_uuid") or "").strip()
        name = (g(p, "name", "portfolio_name") or "").strip() or "(unnamed)"
        usd  = usd_available_for_portfolio(pid)
        print(f"[PORT] {name} id={pid} USD={usd}")

def pick_portfolio_with_usd_if_needed() -> Optional[str]:
    configured = resolve_portfolio_uuid()
    usd_cfg = usd_available_for_portfolio(configured)
    if usd_cfg > 0:
        return configured
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
            print(f"[BAL] Suggest portfolio {best_id} (USD {best_usd}).")
    return configured  # orders always use the API key’s portfolio


# =========================
# Helpers: retries & product rules
# =========================
def retry(cb_call, *args, _tries=5, _base=0.6, _j=0.4, **kwargs):
    for i in range(_tries):
        try:
            return cb_call(*args, **kwargs)
        except Exception as e:
            msg = str(e).lower()
            if any(x in msg for x in ("429", "rate limit", "timeout", "temporarily", "5", "service")):
                sleep = _base * (2 ** i) * (0.8 + _j * random.random())
                time.sleep(sleep)
                continue
            raise
    return cb_call(*args, **kwargs)

def quantize_down(x: float, step: float) -> float:
    q = Decimal(str(step))
    return float(Decimal(str(x)).quantize(q))

def fetch_product_rules(pid: str):
    meta = retry(CB.get_product, product_id=pid)
    base_inc = float(g(meta, "base_increment", "base_size_increment", "base_min_size", default=1e-8))
    quote_inc = float(g(meta, "quote_increment", "quote_size_increment", "quote_min_size", default=0.01))
    min_quote = float(g(meta, "min_market_funds", "quote_min_size", "min_funds", default=1.00))
    min_base  = float(g(meta, "min_order_size", "base_min_size", default=1e-8))
    return base_inc, quote_inc, min_quote, min_base


# =========================
# Orders & fills (NO portfolio_uuid)
# =========================
def _fees_sum(fills: list) -> float:
    total_fee = 0.0
    for x in fills:
        fx = g(x, "fee", "fees", "commission", default=0.0)
        try: total_fee += float(fx)
        except: pass
    return total_fee

def place_buy(product_id: str, usd: float) -> str:
    if DRY_RUN:
        return "DRYRUN"
    _, quote_inc, min_quote, _ = fetch_product_rules(product_id)
    usd = max(min_quote, quantize_down(usd, quote_inc))
    client_order_id = f"buy-{product_id}-{int(time.time()*1000)}"
    o = retry(
        CB.market_order_buy,
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
            f = retry(CB.get_fills, order_id=order_id)
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
                fee = _fees_sum(fills)      # fee increases cost
                if base_qty > 0 and fill_usd > 0:
                    return {"base_qty": base_qty, "fill_usd": max(0.0, fill_usd + fee)}
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
            row = vals[r]
            if (row[0] if row else "").strip().upper() == product:
                cur_qty  = _parse_num(row[1] if len(row) > 1 else 0.0)
                cur_cost = _parse_num(row[2] if len(row) > 2 else 0.0)
                new_qty  = cur_qty + add_qty
                new_cost = cur_cost + add_cost
                avg = (new_cost / new_qty) if new_qty > 0 else 0.0
                ws.update(
                    range_name=f"A{r+1}:E{r+1}",
                    values=[[product, f"{new_qty:.12f}", f"{new_cost:.2f}", f"{avg:.6f}", now_iso()]]
                )
                return
    avg = (add_cost / add_qty) if add_qty > 0 else 0.0
    append_cost_rows(ws, [[product, f"{add_qty:.12f}", f"{add_cost:.2f}", f"{avg:.6f}", now_iso()]])


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

    if DEBUG_BALANCES:
        debug_portfolios_and_usd()

    portfolio_uuid = pick_portfolio_with_usd_if_needed()
    usd_bal = usd_available_for_portfolio(portfolio_uuid)
    if DEBUG_BALANCES:
        print(f"[BAL] Using portfolio {portfolio_uuid or '(key default)'} with USD {usd_bal}")

    remaining_usd = usd_bal
    logs = []

    for i, pid in enumerate(products, 1):
        try:
            # planned notional from remaining balance
            notional = remaining_usd * (PCT_PER_TRADE / 100.0)

            # pre-check min_quote to avoid noisy rejections
            _, _, min_quote, _ = fetch_product_rules(pid)
            min_required = max(MIN_NOTIONAL, min_quote)
            if notional < min_required:
                note = f"Notional ${notional:.2f} < min ${min_required:.2f}"
                print(f"⚠️ {pid} {note}")
                logs.append([now_iso(), "CRYPTO-BUY-SKIP", pid, f"{notional:.2f}", "", "", "SKIPPED", note])
                continue

            oid = place_buy(pid, notional)
            fills = poll_fills_sum(oid)
            base_qty = fills["base_qty"]
            fill_usd = fills["fill_usd"] if fills["fill_usd"] > 0 else notional

            # decrement remaining by actual USD used (or planned if unknown)
            remaining_usd = max(0.0, remaining_usd - (fill_usd or notional))

            status = "dry-run" if DRY_RUN else "submitted"
            print(f"✅ BUY {pid} ${notional:.2f} (order {oid}, {status})")
            logs.append([
                now_iso(), "CRYPTO-BUY", pid,
                f"{notional:.2f}",
                f"{base_qty:.12f}" if base_qty else "",
                oid, status, ""
            ])

            if not DRY_RUN and base_qty > 0 and fill_usd > 0:
                upsert_cost(ws_cost, pid, base_qty, fill_usd)

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
