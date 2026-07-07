"""
modules/reports.py
==================
Sprint 14 — Reports domain: Dashboard, P&L, Margin Report, Rep Performance.

Exports (via __all__):
  get_dashboard, get_pl_report, get_rep_performance_report, get_margin_report

Dependencies:
  modules.utils  — r2
  modules.db     — qry, qry1

Lazy imports (inside functions to avoid circular deps):
  modules.invoices   — compute_invoice_balance
  modules.purchasing — compute_bill_balance
  modules.inventory  — get_stock_map, get_finished_stock_map
"""

from datetime import date, timedelta

from modules.utils import *   # r2
from modules.db    import *   # qry, qry1

__all__ = [
    'get_dashboard',
    'get_pl_report',
    'get_rep_performance_report',
    'get_margin_report',
    'reset_for_launch',
]


# Transactional tables purged by the one-time launch reset (children before parents, FK-safe).
_LAUNCH_PURGE_TABLES = [
    'payment_allocations', 'customer_payments', 'invoice_items', 'invoices',
    'order_hold_expiry', 'customer_order_items', 'customer_orders', 'sales',
    'field_otp', 'field_order_items', 'field_orders', 'beat_visits',
    'production_consumption', 'production_batches', 'work_orders',
    'supplier_payment_allocations', 'supplier_payments', 'supplier_bill_items', 'supplier_bills',
    'po_items', 'purchase_orders',
    'inventory_ledger', 'margin_alerts', 'plan_release',
]
# Document counters reset to 0 so the first real doc is 0001. (rep / acct counters left alone.)
_LAUNCH_RESET_COUNTERS = ['sale', 'batch', 'invoice', 'payment', 'bill', 'spay',
                          'work_order', 'customer_order', 'purchase_order', 'field_order']


def reset_for_launch():
    """OWNER-ONLY one-time launch reset. Purges ALL test transactional data — orders,
    invoices, payments, sales, work orders, batches, POs, bills, field orders, stock
    movements, plan releases — and resets document numbering to 0001. KEEPS all master
    data: products, customers, suppliers, zones, reps, ingredients, BOMs, costing, cost
    lines, users, prices, plans. Idempotent (safe to run twice)."""
    from modules.db import _conn, save_db
    c = _conn()
    purged = {}
    try:
        c.execute("PRAGMA foreign_keys=OFF")
        for t in _LAUNCH_PURGE_TABLES:
            if not c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone():
                continue
            n = c.execute(f"SELECT COUNT(*) FROM '{t}'").fetchone()[0]
            if n:
                c.execute(f"DELETE FROM '{t}'")
                purged[t] = n
            try:
                c.execute("DELETE FROM sqlite_sequence WHERE name=?", (t,))
            except Exception:
                pass
        ph = ','.join('?' * len(_LAUNCH_RESET_COUNTERS))
        c.execute(f"UPDATE id_counters SET last_num=0 WHERE entity IN ({ph})",
                  tuple(_LAUNCH_RESET_COUNTERS))
        c.commit()
    finally:
        c.close()
    save_db()
    return {'ok': True, 'purged': purged, 'purged_total': sum(purged.values()),
            'counters_reset': _LAUNCH_RESET_COUNTERS}


# ─────────────────────────────────────────────────────────────────
#  DASHBOARD
# ─────────────────────────────────────────────────────────────────

def get_dashboard():
    """Return all KPI data for the dashboard: today's sales, MTD/LTM revenue, AR/AP totals,
    overdue invoices and bills, raw material stock value, and finished goods stock.
    Uses lazy imports from invoices, purchasing, and inventory to avoid circular deps.
    """
    from modules.invoices   import compute_invoice_balance
    from modules.purchasing import compute_bill_balance
    from modules.inventory  import get_stock_map, get_finished_stock_map

    today_d    = date.today()
    today_str  = today_d.isoformat()
    month_start = today_d.replace(day=1).isoformat()

    last_month_end_d   = today_d.replace(day=1) - timedelta(days=1)
    last_month_start_d = last_month_end_d.replace(day=1)
    last_month_start   = last_month_start_d.isoformat()
    last_month_end     = last_month_end_d.isoformat()

    sales_today = qry1("""
        SELECT COUNT(*) as cnt, COALESCE(SUM(total),0) as revenue,
               COALESCE(SUM(cogs),0) as cogs,
               COALESCE(SUM(gross_profit),0) as gp
        FROM sales WHERE sale_date = ?
    """, (today_str,)) or {}

    sales_month = qry1("""
        SELECT COUNT(*) as cnt, COALESCE(SUM(total),0) as revenue,
               COALESCE(SUM(cogs),0) as cogs,
               COALESCE(SUM(gross_profit),0) as gp
        FROM sales WHERE sale_date >= ?
    """, (month_start,)) or {}

    sales_last_month = qry1("""
        SELECT COUNT(*) as cnt, COALESCE(SUM(total),0) as revenue,
               COALESCE(SUM(cogs),0) as cogs,
               COALESCE(SUM(gross_profit),0) as gp
        FROM sales WHERE sale_date >= ? AND sale_date <= ?
    """, (last_month_start, last_month_end)) or {}

    # All-time net cash movement through the ERP (customer receipts − supplier payments).
    # NOTE: this is NOT a bank balance — it has no opening balance and excludes any cash
    # paid outside supplier_payments (salaries, rent, utilities, capital). Labelled honestly
    # on the dashboard as "Net cash flow (all-time)".
    cash_in  = (qry1("SELECT COALESCE(SUM(amount),0) as v FROM customer_payments",  ()) or {}).get('v', 0)
    cash_out = (qry1("SELECT COALESCE(SUM(amount),0) as v FROM supplier_payments",  ()) or {}).get('v', 0)
    cash_position = r2(cash_in - cash_out)

    # Month-to-date cash movement — the more useful, period-bounded figure.
    cash_in_mtd  = (qry1("SELECT COALESCE(SUM(amount),0) as v FROM customer_payments WHERE payment_date >= ?", (month_start,)) or {}).get('v', 0)
    cash_out_mtd = (qry1("SELECT COALESCE(SUM(amount),0) as v FROM supplier_payments WHERE payment_date >= ?", (month_start,)) or {}).get('v', 0)
    cash_flow_mtd = r2(cash_in_mtd - cash_out_mtd)

    ar_invoices     = qry("SELECT inv.id, inv.status, inv.due_date FROM invoices inv")
    ar_unpaid_count = sum(1 for i in ar_invoices if i['status'] in ('UNPAID', 'PARTIAL'))

    ar_outstanding    = 0.0
    overdue_ar_count  = 0
    overdue_ar_amount = 0.0
    overdue_ar_max_days = 0
    for inv in ar_invoices:
        if inv['status'] in ('UNPAID', 'PARTIAL'):
            _, _, total, paid, balance = compute_invoice_balance(inv['id'])
            ar_outstanding += balance
            due_str = inv.get('due_date') or ''
            if due_str and due_str < today_str:
                days_late = (today_d - date.fromisoformat(due_str)).days
                overdue_ar_count += 1
                overdue_ar_amount += balance
                overdue_ar_max_days = max(overdue_ar_max_days, days_late)

    seven_days_str   = (today_d + timedelta(days=7)).isoformat()
    ap_bills         = qry("SELECT id, status, due_date FROM supplier_bills")
    ap_unpaid_count  = sum(1 for b in ap_bills if b['status'] in ('UNPAID', 'PARTIAL'))
    ap_outstanding   = 0.0
    ap_overdue_count = 0
    ap_overdue_amount = 0.0
    ap_due_soon_count  = 0
    ap_due_soon_amount = 0.0
    for bill in ap_bills:
        if bill['status'] in ('UNPAID', 'PARTIAL'):
            total, paid, balance = compute_bill_balance(bill['id'])
            ap_outstanding += balance
            due_str = bill.get('due_date') or ''
            if due_str and due_str < today_str:
                ap_overdue_count  += 1
                ap_overdue_amount += balance
            elif due_str and due_str <= seven_days_str:
                ap_due_soon_count  += 1
                ap_due_soon_amount += balance

    stock_map = get_stock_map()
    alerts    = qry("SELECT i.code, i.name, i.reorder_level FROM ingredients i WHERE i.reorder_level > 0")
    low_stock = []
    for a in alerts:
        ing_row = qry1("SELECT id FROM ingredients WHERE code=?", (a['code'],))
        bal = stock_map.get(ing_row['id'], 0) if ing_row else 0
        if bal <= a['reorder_level']:
            low_stock.append({'code': a['code'], 'name': a['name'],
                              'balance': bal, 'reorder': a['reorder_level']})

    by_type = qry("""
        SELECT customer_type, COUNT(*) as cnt, SUM(total) as revenue
        FROM sales WHERE sale_date >= ? GROUP BY customer_type
    """, (month_start,))

    fg_stock = get_finished_stock_map()
    fg_list  = []
    if fg_stock:
        var_ids = ','.join(str(i) for i in fg_stock.keys())
        all_vars = {r['id']: r for r in qry(f"""
            SELECT pv.id, pv.sku_code, p.name as product_name, ps.label as pack_size
            FROM product_variants pv
            JOIN products p ON p.id=pv.product_id
            JOIN pack_sizes ps ON ps.id=pv.pack_size_id
            WHERE pv.id IN ({var_ids})
        """)}
        for vid, units in fg_stock.items():
            v = all_vars.get(vid)
            if v and units > 0:
                fg_list.append({'skuCode': v['sku_code'], 'product': v['product_name'],
                                'packSize': v['pack_size'], 'units': units})

    # Net profit MTD = gross profit this month − operating costs recorded for this month.
    # Operating costs are the TOTAL ₨ for the month (all categories), not per-pack.
    month_key = today_d.strftime('%Y-%m')
    op_cost_month = 0.0
    try:
        op_cost_month = r2((qry1(
            "SELECT COALESCE(SUM(amount),0) as v FROM monthly_operating_costs WHERE month = ?",
            (month_key,)) or {}).get('v', 0))
    except Exception:
        op_cost_month = 0.0
    net_profit_mtd = r2(r2(sales_month.get('gp', 0)) - op_cost_month)

    # ── Owner-tab analytics ────────────────────────────────────────
    # 6-month revenue trend (oldest → newest for charting).
    trend_rows = qry("""
        SELECT strftime('%Y-%m', sale_date) AS mo,
               COALESCE(SUM(total),0)        AS revenue,
               COALESCE(SUM(gross_profit),0) AS gp
        FROM sales
        WHERE sale_date >= ?
        GROUP BY mo ORDER BY mo ASC
    """, ((today_d.replace(day=1) - timedelta(days=160)).isoformat(),)) or []
    revenue_trend = [{'month': r['mo'], 'revenue': r2(r['revenue']), 'grossProfit': r2(r['gp'])}
                     for r in trend_rows][-6:]

    # Top products this month (by revenue).
    top_products = [{'product': r['product_name'], 'packSize': r['pack_size'],
                     'units': int(r['units'] or 0), 'revenue': r2(r['revenue'])}
                    for r in (qry("""
        SELECT product_name, pack_size,
               COALESCE(SUM(qty),0)   AS units,
               COALESCE(SUM(total),0) AS revenue
        FROM sales WHERE sale_date >= ?
        GROUP BY product_code, pack_size
        ORDER BY revenue DESC LIMIT 5
    """, (month_start,)) or [])]

    # Top customers this month (by revenue).
    top_customers = [{'name': r['cust_name'], 'customerType': r['customer_type'],
                      'orders': int(r['cnt'] or 0), 'revenue': r2(r['revenue'])}
                     for r in (qry("""
        SELECT cust_name, customer_type,
               COUNT(DISTINCT invoice_id) AS cnt,
               COALESCE(SUM(total),0)     AS revenue
        FROM sales WHERE sale_date >= ?
        GROUP BY cust_code
        ORDER BY revenue DESC LIMIT 5
    """, (month_start,)) or [])]

    # ── Operations-tab: open work orders (to make) ─────────────────
    open_wos = []
    try:
        open_wos = [{'woNumber': w['wo_number'], 'product': w['product_name'],
                     'packSize': w['pack_size'], 'qty': int(w['qty_units'] or 0),
                     'status': w['status'], 'targetDate': w.get('target_date') or ''}
                    for w in (qry("""
            SELECT wo.wo_number, wo.qty_units, wo.status, wo.target_date,
                   p.name AS product_name, ps.label AS pack_size
            FROM work_orders wo
            JOIN product_variants pv ON pv.id = wo.product_variant_id
            JOIN products p ON p.id = pv.product_id
            LEFT JOIN pack_sizes ps ON ps.id = pv.pack_size_id
            WHERE wo.status IN ('planned','in_progress')
            ORDER BY wo.target_date ASC, wo.created_at ASC LIMIT 20
        """) or [])]
    except Exception:
        open_wos = []

    # ── Sales by product (LIVE) — every ACTIVE variant with today + MTD units/revenue ──
    # Off (deactivated) variants are excluded so a retired SKU (e.g. SPGM-25) disappears here.
    sbp_variants = qry("""
        SELECT p.code AS product_code, p.name AS product_name, ps.label AS pack_size, ps.grams AS grams
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id
        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE pv.active_flag = 1 AND p.active = 1
        ORDER BY p.code, ps.grams
    """) or []
    _today_by_sku = {(r['product_code'], r['pack_size']): r for r in (qry("""
        SELECT product_code, pack_size, COALESCE(SUM(qty),0) AS units
        FROM sales WHERE sale_date = ? GROUP BY product_code, pack_size
    """, (today_str,)) or [])}
    _mtd_by_sku = {(r['product_code'], r['pack_size']): r for r in (qry("""
        SELECT product_code, pack_size, COALESCE(SUM(qty),0) AS units, COALESCE(SUM(total),0) AS revenue
        FROM sales WHERE sale_date >= ? GROUP BY product_code, pack_size
    """, (month_start,)) or [])}
    sales_by_product = []
    for v in sbp_variants:
        key = (v['product_code'], v['pack_size'])
        t = _today_by_sku.get(key) or {}
        m = _mtd_by_sku.get(key) or {}
        sales_by_product.append({
            'productCode': v['product_code'], 'product': v['product_name'], 'packSize': v['pack_size'],
            'todayUnits': int(t.get('units') or 0),
            'unitsMtd':   int(m.get('units') or 0),
            'revenueMtd': r2(m.get('revenue') or 0),
        })
    sales_by_product.sort(key=lambda x: x['revenueMtd'], reverse=True)

    # ── Production summary (Quick Glance / Operations) ─────────────
    # "Made this month" = COMPLETED/verified batches (production_batches are verified FG only).
    # "In progress" = active staged batch runs — work-in-progress, NOT yet made.
    made_packs = made_batches = inprog_batches = inprog_packs = 0
    try:
        _mr = qry1("SELECT COALESCE(SUM(qty_units),0) AS packs, COUNT(*) AS n "
                   "FROM production_batches WHERE strftime('%Y-%m', batch_date)=?", (month_key,)) or {}
        made_packs, made_batches = int(_mr.get('packs') or 0), int(_mr.get('n') or 0)
    except Exception:
        pass
    try:
        _ir = qry1("SELECT COUNT(*) AS n, COALESCE(SUM(qty_units),0) AS packs "
                   "FROM batch_runs WHERE status IN ('in_progress','awaiting_verification')") or {}
        inprog_batches, inprog_packs = int(_ir.get('n') or 0), int(_ir.get('packs') or 0)
    except Exception:
        pass
    ready_to_sell = int(sum(fg_stock.values())) if fg_stock else 0
    to_make_packs = sum(int(w.get('qty') or 0) for w in open_wos)

    # ── Raw-material tank levels (Operations tab, worst-first) ─────
    # Fuel-gauge tanks like the Inventory screen: on-hand vs target ('full' tank),
    # with the reorder line. Worst-first so the tanks that need buying surface.
    raw_materials = []
    try:
        _rmrows = qry("SELECT id, code, name, COALESCE(reorder_level,0) AS reorder, "
                      "COALESCE(target_grams,0) AS target FROM ingredients WHERE active=1")
        for _r in _rmrows:
            _bal = float(stock_map.get(_r['id'], 0) or 0)
            _ro  = float(_r.get('reorder') or 0)
            _tg  = float(_r.get('target') or 0)
            # Skip catalog rows that are irrelevant to production glance:
            # nothing in stock AND no reorder level AND no target set.
            if _bal <= 0 and _ro <= 0 and _tg <= 0:
                continue
            raw_materials.append({
                'name':    _r.get('name') or _r.get('code'),
                'balance': int(_bal), 'reorder': int(_ro), 'target': int(_tg),
            })
        def _fill(t):
            top = max(t['target'], t['balance'], t['reorder'] * 1.5, 1)
            return (t['balance'] / top) if top else 1
        raw_materials.sort(key=_fill)
        raw_materials = raw_materials[:8]
    except Exception:
        raw_materials = []

    # Owner-set Rs→$ display rate (dashboard convenience only; 0 = not set / feature off).
    usd_rate = 0.0
    try:
        _u = qry1("SELECT value FROM costing_config WHERE key='usd_rate'")
        if _u and _u.get('value'):
            usd_rate = float(_u['value'])
    except Exception:
        usd_rate = 0.0

    return {
        'salesToday': {
            'count':       int(sales_today.get('cnt', 0)),
            'revenue':     r2(sales_today.get('revenue', 0)),
            'grossProfit': r2(sales_today.get('gp', 0)),
        },
        'salesMonth': {
            'count':       int(sales_month.get('cnt', 0)),
            'revenue':     r2(sales_month.get('revenue', 0)),
            'cogs':        r2(sales_month.get('cogs', 0)),
            'grossProfit': r2(sales_month.get('gp', 0)),
        },
        'salesLastMonth': {
            'count':       int(sales_last_month.get('cnt', 0)),
            'revenue':     r2(sales_last_month.get('revenue', 0)),
            'cogs':        r2(sales_last_month.get('cogs', 0)),
            'grossProfit': r2(sales_last_month.get('gp', 0)),
        },
        'cashPosition': cash_position,
        'cashFlowMTD':  cash_flow_mtd,
        'opCostMonth':  op_cost_month,
        'netProfitMTD': net_profit_mtd,
        'ar': {
            'unpaidCount': ar_unpaid_count,
            'outstanding': r2(ar_outstanding),
        },
        'overdueAR': {
            'count':          overdue_ar_count,
            'amount':         r2(overdue_ar_amount),
            'maxDaysOverdue': overdue_ar_max_days,
        },
        'ap': {
            'unpaidCount': ap_unpaid_count,
            'outstanding': r2(ap_outstanding),
        },
        'apOverdue': {
            'count':  ap_overdue_count,
            'amount': r2(ap_overdue_amount),
        },
        'apDueSoon': {
            'count':  ap_due_soon_count,
            'amount': r2(ap_due_soon_amount),
        },
        'lowStockAlerts': low_stock,
        'salesByType':    by_type,
        'finishedGoods':  fg_list,
        'revenueTrend':     revenue_trend,
        'topProducts':      top_products,
        'topCustomers':     top_customers,
        'openWorkOrders':   open_wos,
        'salesByProduct':   sales_by_product,
        'production': {
            'madeThisMonthPacks':   made_packs,
            'madeThisMonthBatches': made_batches,
            'inProgressBatches':    inprog_batches,
            'inProgressPacks':      inprog_packs,
            'readyToSellUnits':     ready_to_sell,
            'toMakePacks':          to_make_packs,
        },
        'rawMaterials': raw_materials,
        'usdRate': usd_rate,
    }


# ─────────────────────────────────────────────────────────────────
#  P&L REPORT
# ─────────────────────────────────────────────────────────────────

def get_pl_report(year: str) -> dict:
    """Monthly P&L for the given year. Returns per-month and YTD totals."""
    MONTH_LABELS = ['Jan','Feb','Mar','Apr','May','Jun',
                    'Jul','Aug','Sep','Oct','Nov','Dec']

    sales_rows = qry("""
        SELECT strftime('%m', sale_date) AS mo,
               COALESCE(SUM(total),        0) AS revenue,
               COALESCE(SUM(cogs),         0) AS cogs,
               COALESCE(SUM(gross_profit), 0) AS gp,
               COUNT(*)                       AS tx_count
        FROM sales
        WHERE strftime('%Y', sale_date) = ?
        GROUP BY mo
    """, (year,))
    sales_map = {row['mo']: row for row in sales_rows}

    receipt_rows = qry("""
        SELECT strftime('%m', payment_date) AS mo,
               COALESCE(SUM(amount), 0) AS receipts
        FROM customer_payments
        WHERE strftime('%Y', payment_date) = ?
        GROUP BY mo
    """, (year,))
    receipt_map = {row['mo']: row['receipts'] for row in receipt_rows}

    payment_rows = qry("""
        SELECT strftime('%m', payment_date) AS mo,
               COALESCE(SUM(amount), 0) AS paid_out
        FROM supplier_payments
        WHERE strftime('%Y', payment_date) = ?
        GROUP BY mo
    """, (year,))
    payment_map = {row['mo']: row['paid_out'] for row in payment_rows}

    # Non-stock purchases (equipment/supplies/services/labour) → operating expenses on the P&L,
    # kept SEPARATE from the manually-entered monthly operating costs (each cost lives in one place,
    # no double-count). Capital (equipment) is summed apart so a one-off asset doesn't distort the
    # month's expense line.
    from modules.purchasing import category_nature, PURCHASE_CATEGORIES
    _cat_label = {c['key']: c['label'] for c in PURCHASE_CATEGORIES}
    exp_rows = qry("""
        SELECT strftime('%m', sb.bill_date) AS mo, COALESCE(sbi.category,'') AS cat,
               COALESCE(SUM(sbi.line_total), 0) AS amt
        FROM supplier_bill_items sbi
        JOIN supplier_bills sb ON sb.id = sbi.bill_id
        WHERE sbi.line_type = 'other' AND sb.status != 'VOID'
          AND strftime('%Y', sb.bill_date) = ?
        GROUP BY mo, cat
    """, (year,))
    expense_map, capital_map, cat_totals = {}, {}, {}
    for r in exp_rows:
        mo = r['mo']; amt = float(r['amt'] or 0); cat = r['cat'] or ''
        if category_nature(cat) == 'capital':
            capital_map[mo] = capital_map.get(mo, 0) + amt
        else:
            expense_map[mo] = expense_map.get(mo, 0) + amt
        cat_totals[cat] = cat_totals.get(cat, 0) + amt

    months = []
    ytd = {'revenue': 0, 'cogs': 0, 'gp': 0, 'tx_count': 0,
           'receipts': 0, 'paid_out': 0, 'expenses': 0, 'capital': 0}

    for m in range(1, 13):
        mo_str = f"{m:02d}"
        s    = sales_map.get(mo_str, {})
        rev  = float(s.get('revenue', 0) or 0)
        cogs = float(s.get('cogs',    0) or 0)
        gp   = float(s.get('gp',      0) or 0)
        cnt  = int(s.get('tx_count',  0) or 0)
        rec  = float(receipt_map.get(mo_str, 0) or 0)
        out  = float(payment_map.get(mo_str, 0) or 0)
        exp  = float(expense_map.get(mo_str, 0) or 0)
        cap  = float(capital_map.get(mo_str, 0) or 0)
        gp_pct   = round(gp / rev * 100, 1) if rev > 0 else 0.0
        net_cash = r2(rec - out)
        net_profit = r2(gp - exp)          # gross profit minus operating expenses (excludes capital)

        months.append({
            'month':        f"{year}-{mo_str}",
            'label':        f"{MONTH_LABELS[m-1]} {year}",
            'short':        MONTH_LABELS[m-1],
            'revenue':      r2(rev),
            'cogs':         r2(cogs),
            'gross_profit': r2(gp),
            'gp_pct':       gp_pct,
            'tx_count':     cnt,
            'receipts':     r2(rec),
            'paid_out':     r2(out),
            'net_cash':     net_cash,
            'expenses':     r2(exp),
            'capital':      r2(cap),
            'net_profit':   net_profit,
        })
        ytd['revenue']  += rev
        ytd['cogs']     += cogs
        ytd['gp']       += gp
        ytd['tx_count'] += cnt
        ytd['receipts'] += rec
        ytd['paid_out'] += out
        ytd['expenses'] += exp
        ytd['capital']  += cap

    ytd_gp_pct = round(ytd['gp'] / ytd['revenue'] * 100, 1) if ytd['revenue'] > 0 else 0.0

    return {
        'year':   year,
        'months': months,
        'ytd': {
            'revenue':      r2(ytd['revenue']),
            'cogs':         r2(ytd['cogs']),
            'gross_profit': r2(ytd['gp']),
            'gp_pct':       ytd_gp_pct,
            'tx_count':     ytd['tx_count'],
            'receipts':     r2(ytd['receipts']),
            'paid_out':     r2(ytd['paid_out']),
            'net_cash':     r2(ytd['receipts'] - ytd['paid_out']),
            'expenses':     r2(ytd['expenses']),
            'capital':      r2(ytd['capital']),
            'net_profit':   r2(ytd['gp'] - ytd['expenses']),
        },
        'expensesByCategory': [
            {'category': k, 'label': _cat_label.get(k, k or 'Other'),
             'nature': category_nature(k), 'amount': r2(v)}
            for k, v in sorted(cat_totals.items(), key=lambda kv: -kv[1]) if v
        ],
        'available_years': [str(y) for y in sorted(set(
            int(r['yr']) for r in qry(
                "SELECT DISTINCT strftime('%Y', sale_date) AS yr FROM sales WHERE yr IS NOT NULL", ()
            )
        ), reverse=True)] or [str(date.today().year)],
    }


# ─────────────────────────────────────────────────────────────────
#  REP PERFORMANCE REPORT
# ─────────────────────────────────────────────────────────────────

def get_rep_performance_report(period=None):
    """
    Sales rep performance dashboard.
    period = 'YYYY-MM' (defaults to current month).
    Returns per-rep: route_coverage_pct, visit_hit_rate, orders_count,
    revenue_actual, revenue_target, revenue_vs_target_pct, monthly_trend.
    """
    if not period:
        period = date.today().strftime('%Y-%m')

    reps = qry("""
        SELECT id, employee_id, name, phone, designation
        FROM sales_reps
        WHERE status IS NULL OR status='active'
        ORDER BY name
    """)

    result = []
    for rep in reps:
        rid = rep['id']

        total_stops = qry1("""
            SELECT COUNT(rc.id) as cnt
            FROM rep_routes rr
            JOIN route_customers rc ON rc.route_id = rr.route_id
            WHERE rr.rep_id=? AND rr.assigned_to IS NULL
        """, (rid,))['cnt'] or 0

        visited = qry1("""
            SELECT COUNT(DISTINCT customer_id) as cnt
            FROM beat_visits
            WHERE rep_id=? AND strftime('%Y-%m', visit_date)=?
        """, (rid, period))['cnt'] or 0

        route_coverage_pct = round(visited / total_stops * 100, 1) if total_stops else 0.0
        visit_hit_rate     = route_coverage_pct

        # Rep revenue comes from the SAME `sales` table the dashboard/P&L use, attributed via
        # sales → invoice → customer_order.created_by_rep_id. (The old field_orders path is retired;
        # reading it made the rep report disagree with the dashboard for the same rep's sales.)
        orders_count = qry1("""
            SELECT COUNT(DISTINCT co.id) as cnt
            FROM sales s
            JOIN invoices inv       ON inv.id = s.invoice_id
            JOIN customer_orders co ON co.id = inv.customer_order_id
            WHERE co.created_by_rep_id=? AND COALESCE(s.voided,0)=0
              AND strftime('%Y-%m', s.sale_date)=?
        """, (rid, period))['cnt'] or 0

        rev_actual = qry1("""
            SELECT COALESCE(SUM(s.total), 0) as total
            FROM sales s
            JOIN invoices inv       ON inv.id = s.invoice_id
            JOIN customer_orders co ON co.id = inv.customer_order_id
            WHERE co.created_by_rep_id=? AND COALESCE(s.voided,0)=0
              AND strftime('%Y-%m', s.sale_date)=?
        """, (rid, period))['total'] or 0.0

        tgt_row    = qry1("SELECT revenue_target FROM rep_targets WHERE rep_id=? AND month=?", (rid, period))
        rev_target = tgt_row['revenue_target'] if tgt_row else 0.0
        rev_vs_target_pct = round(rev_actual / rev_target * 100, 1) if rev_target else None

        trend_rows = qry("""
            SELECT strftime('%Y-%m', s.sale_date) as month,
                   COALESCE(SUM(s.total), 0) as revenue,
                   COUNT(DISTINCT co.id) as orders
            FROM sales s
            JOIN invoices inv       ON inv.id = s.invoice_id
            JOIN customer_orders co ON co.id = inv.customer_order_id
            WHERE co.created_by_rep_id=? AND COALESCE(s.voided,0)=0
              AND s.sale_date >= date('now','-6 months')
            GROUP BY 1 ORDER BY 1
        """, (rid,))

        result.append({
            'repId':            rid,
            'employeeId':       rep['employee_id'],
            'name':             rep['name'],
            'designation':      rep['designation'],
            'period':           period,
            'totalStops':       total_stops,
            'visitedCustomers': visited,
            'routeCoveragePct': route_coverage_pct,
            'visitHitRate':     visit_hit_rate,
            'ordersCount':      orders_count,
            'revenueActual':    rev_actual,
            'revenueTarget':    rev_target,
            'revsVsTargetPct':  rev_vs_target_pct,
            'monthlyTrend':     [dict(r) for r in trend_rows],
        })

    return {'period': period, 'reps': result}


# ─────────────────────────────────────────────────────────────────
#  MARGIN REPORT
# ─────────────────────────────────────────────────────────────────

def get_margin_report(month=None):
    """Margin report per product, optionally filtered by YYYY-MM."""
    where = f"WHERE s.sale_date LIKE '{month}%'" if month else ""
    rows  = qry(f"""
        SELECT s.product_code, s.product_name, s.pack_size,
               COUNT(*) as orders,
               SUM(s.qty) as units_sold,
               SUM(s.total) as revenue,
               SUM(s.cogs) as cogs,
               SUM(s.gross_profit) as gross_profit
        FROM sales s {where}
        GROUP BY s.product_code, s.product_name, s.pack_size
        ORDER BY gross_profit DESC
    """)
    for r in rows:
        if r['revenue'] and r['revenue'] > 0:
            r['margin_pct'] = r2(r['gross_profit'] / r['revenue'] * 100)
        else:
            r['margin_pct'] = 0.0
    return rows
