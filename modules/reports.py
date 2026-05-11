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
]


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

    cash_in  = (qry1("SELECT COALESCE(SUM(amount),0) as v FROM customer_payments",  ()) or {}).get('v', 0)
    cash_out = (qry1("SELECT COALESCE(SUM(amount),0) as v FROM supplier_payments",  ()) or {}).get('v', 0)
    cash_position = r2(cash_in - cash_out)

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

    months = []
    ytd = {'revenue': 0, 'cogs': 0, 'gp': 0, 'tx_count': 0,
           'receipts': 0, 'paid_out': 0}

    for m in range(1, 13):
        mo_str = f"{m:02d}"
        s    = sales_map.get(mo_str, {})
        rev  = float(s.get('revenue', 0) or 0)
        cogs = float(s.get('cogs',    0) or 0)
        gp   = float(s.get('gp',      0) or 0)
        cnt  = int(s.get('tx_count',  0) or 0)
        rec  = float(receipt_map.get(mo_str, 0) or 0)
        out  = float(payment_map.get(mo_str, 0) or 0)
        gp_pct   = round(gp / rev * 100, 1) if rev > 0 else 0.0
        net_cash = r2(rec - out)

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
        })
        ytd['revenue']  += rev
        ytd['cogs']     += cogs
        ytd['gp']       += gp
        ytd['tx_count'] += cnt
        ytd['receipts'] += rec
        ytd['paid_out'] += out

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
        },
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

        orders_count = qry1("""
            SELECT COUNT(id) as cnt FROM field_orders
            WHERE rep_id=? AND strftime('%Y-%m', order_date)=?
        """, (rid, period))['cnt'] or 0

        rev_actual = qry1("""
            SELECT COALESCE(SUM(foi.quantity * foi.unit_price), 0) as total
            FROM field_orders fo
            JOIN field_order_items foi ON foi.order_id = fo.id
            WHERE fo.rep_id=? AND fo.status='confirmed'
              AND strftime('%Y-%m', fo.order_date)=?
        """, (rid, period))['total'] or 0.0

        tgt_row    = qry1("SELECT revenue_target FROM rep_targets WHERE rep_id=? AND month=?", (rid, period))
        rev_target = tgt_row['revenue_target'] if tgt_row else 0.0
        rev_vs_target_pct = round(rev_actual / rev_target * 100, 1) if rev_target else None

        trend_rows = qry("""
            SELECT strftime('%Y-%m', fo.order_date) as month,
                   COALESCE(SUM(foi.quantity * foi.unit_price), 0) as revenue,
                   COUNT(DISTINCT fo.id) as orders
            FROM field_orders fo
            JOIN field_order_items foi ON foi.order_id = fo.id
            WHERE fo.rep_id=? AND fo.status='confirmed'
              AND fo.order_date >= date('now','-6 months')
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
