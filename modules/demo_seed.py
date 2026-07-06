"""
modules/demo_seed.py — reversible DEMO data seeder for the Executive Dashboard.

Pre-launch the dashboard is empty (no sales yet), so it's hard to evaluate. This
seeds ~6 months of realistic activity by REUSING THE REAL ENGINE (produce batches
→ orders → confirm → invoice → payments), so every number reconciles exactly like
live use. It reuses the real customers/products/BOMs and only ADDS transactions.

Reversible: `clear_demo_data()` runs the same purge as the launch reset (removes all
orders/invoices/sales/payments/WOs/batches/ledger + resets numbering) and drops the
demo operating-cost months — returning the DB to the clean pre-launch state. Master
data (customers, products, ingredients, BOMs, prices, users) is never touched.

Owner-only. Intended for pre-launch evaluation; run the Launch Reset (or Clear demo)
before going live.
"""
import random
from datetime import date

from modules.db import qry, qry1, run

__all__ = ['seed_demo_data', 'clear_demo_data', 'demo_status']

_DEMO_MONTHS_OPCOST = None   # set during seed → months we added operating costs for

# demo selling prices per 50g pack (well above cost so margins look healthy)
_DEMO_PRICE = {'SPCM': 250.0, 'SPGM': 300.0}


def _recent_months(n=6):
    """Last n month strings ['YYYY-MM', …] oldest→newest, ending this month."""
    t = date.today()
    out = []
    y, m = t.year, t.month
    for _ in range(n):
        out.append('%04d-%02d' % (y, m))
        m -= 1
        if m == 0:
            m = 12; y -= 1
    return list(reversed(out))


def _day_in_month(month, rng):
    """A safe ISO date inside `month` (YYYY-MM); never in the future."""
    y, m = [int(x) for x in month.split('-')]
    t = date.today()
    hi = t.day if (y == t.year and m == t.month) else 28
    hi = max(1, min(28, hi))
    return '%s-%02d' % (month, rng.randint(1, hi))


def demo_status():
    """Is demo/any transactional data present? (used to guard against double-seed)."""
    n = qry1("SELECT COUNT(*) AS n FROM sales")['n']
    ni = qry1("SELECT COUNT(*) AS n FROM invoices")['n']
    return {'hasData': (n + ni) > 0, 'sales': n, 'invoices': ni}


def seed_demo_data():
    """Seed ~6 months of demo activity. Refuses if transactional data already exists
    (clear first). Returns a summary. Resilient: per-item failures are skipped, not fatal."""
    st = demo_status()
    if st['hasData']:
        return {'ok': False, 'msg': 'Transactions already exist — Clear demo data first, then seed.',
                'existing': st}

    from modules.inventory import create_adjustment
    from modules.production import create_production_batch, create_work_order
    from modules.orders    import create_customer_order, confirm_customer_order, generate_invoice_from_order
    from modules.invoices  import record_customer_payment, allocate_customer_payment
    from modules.costing   import upsert_operating_cost

    rng = random.Random(20260706)   # deterministic demo
    months = _recent_months(6)
    summary = {'ok': True, 'batches': 0, 'orders': 0, 'invoices': 0, 'payments': 0,
               'workOrders': 0, 'inProgress': 0, 'bills': 0, 'lowStock': 0, 'opCostMonths': 0, 'errors': 0}

    # ── master data ──
    customers = qry("SELECT id, code, name FROM customers WHERE active=1 ORDER BY code")
    # 50g variants of the two launch products (must have an active BOM)
    variants = qry("""
        SELECT pv.id, pv.sku_code, p.code AS pcode, p.name AS pname, ps.label AS pack, ps.grams
        FROM product_variants pv
        JOIN products p    ON p.id = pv.product_id
        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        JOIN bom_versions bv ON bv.product_id = p.id AND bv.active_flag = 1
        WHERE pv.active_flag = 1 AND p.active = 1 AND ps.grams = 50 AND p.code IN ('SPCM','SPGM')
        ORDER BY p.code""")
    if not customers or not variants:
        return {'ok': False, 'msg': 'Need active customers and 50g SPCM/SPGM products with a BOM to seed.',
                'customers': len(customers), 'variants': len(variants)}

    # ingredient ids used by the two active BOMs → these get topped up so batches never short
    bom_ings = qry("""
        SELECT DISTINCT bi.ingredient_id AS id
        FROM bom_items bi
        JOIN bom_versions bv ON bv.id = bi.bom_version_id AND bv.active_flag = 1
        JOIN products p ON p.id = bv.product_id AND p.code IN ('SPCM','SPGM')""")

    # ── 1) top up raw material so demo batches can run (200 kg each BOM ingredient) ──
    for ing in bom_ings:
        try:
            create_adjustment({'ingredientId': ing['id'], 'qtyGrams': 200000, 'notes': 'DEMO stock top-up'})
        except Exception:
            summary['errors'] += 1

    # ── 2) produce finished goods: 3 batches per product across the 6 months (800 units each) ──
    fg_made = {}
    for v in variants:
        for mi in (1, 3, 5):   # incl. the CURRENT month (index 5) so "made this month" isn't 0
            try:
                create_production_batch({'productCode': v['pcode'], 'packSize': v['pack'],
                                         'qtyUnits': 800, 'batchDate': _day_in_month(months[mi], rng),
                                         'notes': 'DEMO batch'})
                summary['batches'] += 1
                fg_made[v['pcode']] = fg_made.get(v['pcode'], 0) + 800
            except Exception:
                summary['errors'] += 1

    # ── 3) orders → confirm → invoice, spread across the 6 months (heavier recent) ──
    per_month = {months[0]: 2, months[1]: 3, months[2]: 3, months[3]: 4, months[4]: 6, months[5]: 6}
    invoiced_units = {}          # per product, to stay under finished-goods produced
    made_invoices = []           # (invoice_id, customer_id, invoiceDate, total, month)
    for month in months:
        for _ in range(per_month.get(month, 3)):
            cust = rng.choice(customers)
            d = _day_in_month(month, rng)
            # 1–2 product lines
            picks = rng.sample(variants, k=rng.randint(1, min(2, len(variants))))
            lines, ok = [], True
            for v in picks:
                qtms = rng.randint(20, 70)
                # cap so we never invoice more than produced
                if invoiced_units.get(v['pcode'], 0) + qtms > fg_made.get(v['pcode'], 0) - 20:
                    continue
                lines.append({'productCode': v['pcode'], 'packSize': v['pack'], 'qty': qtms,
                              'unitPrice': _DEMO_PRICE.get(v['pcode'], 250.0), '_vp': v['pcode']})
            if not lines:
                continue
            try:
                o = create_customer_order({'custCode': cust['code'], 'orderDate': d, 'requiredDate': d,
                                           'notes': 'DEMO order',
                                           'lines': [{'productCode': l['productCode'], 'packSize': l['packSize'],
                                                      'qty': l['qty'], 'unitPrice': l['unitPrice']} for l in lines]})
                oid = o['orderId']
                summary['orders'] += 1
                confirm_customer_order(oid)
                items = qry("SELECT id, qty_ordered FROM customer_order_items WHERE order_id=?", (oid,))
                inv = generate_invoice_from_order(oid, {'invoiceDate': d,
                        'lines': [{'orderItemId': it['id'], 'qty': it['qty_ordered']} for it in items]})
                summary['invoices'] += 1
                for l in lines:
                    invoiced_units[l['_vp']] = invoiced_units.get(l['_vp'], 0) + l['qty']
                otot = sum(l['qty'] * l['unitPrice'] for l in lines)   # compute total ourselves (reliable)
                made_invoices.append({'invoiceId': inv['invoiceId'], 'customerId': cust['id'],
                                      'date': d, 'total': otot, 'month': month})
            except Exception:
                summary['errors'] += 1

    # ── 4) payments: pay most older invoices in full, some partial; leave recent + a few older unpaid ──
    n_inv = len(made_invoices)
    for i, mi in enumerate(made_invoices):
        month = mi['month']
        recent = month in (months[4], months[5])   # last 2 months
        r = rng.random()
        if recent:
            if r < 0.45:   pay = mi['total']            # ~45% paid
            elif r < 0.65: pay = round(mi['total'] * 0.5, 2)  # partial
            else:          pay = 0                      # unpaid (some become overdue as time passes)
        else:
            if r < 0.75:   pay = mi['total']            # older mostly paid
            elif r < 0.88: pay = round(mi['total'] * 0.5, 2)
            else:          pay = 0                      # a few older left unpaid → overdue AR
        if pay and pay > 0:
            try:
                pdt = mi['date']
                p = record_customer_payment({'customerId': mi['customerId'], 'amount': pay,
                                             'paymentDate': pdt, 'paymentMode': 'CASH', 'notes': 'DEMO payment'})
                allocate_customer_payment(p['id'], mi['invoiceId'], pay)
                summary['payments'] += 1
            except Exception:
                summary['errors'] += 1

    # ── 5) open work orders → "to make", and start one staged run → "in progress" ──
    from modules.production import create_work_order, start_batch_run
    first_wo = None
    for v in variants:
        try:
            w = create_work_order({'productVariantId': v['id'], 'qtyUnits': rng.choice([600, 800, 1000]),
                                   'notes': 'DEMO work order'})
            summary['workOrders'] += 1
            if first_wo is None:
                first_wo = w.get('id')
        except Exception:
            summary['errors'] += 1
    try:
        if first_wo:
            start_batch_run(first_wo, qty=None, user='demo')   # → one active run = "in progress"
            summary['inProgress'] = 1
    except Exception:
        summary['errors'] += 1

    # ── 5b) supplier bills → Accounts Payable (and purchasing history) ──
    from modules.purchasing import create_supplier_bill
    sups = qry("SELECT id FROM suppliers ORDER BY id LIMIT 4")
    bom_list = list(bom_ings)
    if sups and bom_list:
        for k in range(4):
            try:
                bmonth = months[rng.randint(3, 5)]
                bd = _day_in_month(bmonth, rng)
                items = [{'ingredientId': bom_list[(k + j) % len(bom_list)]['id'],
                          'quantityKg': rng.randint(20, 60),
                          'unitCostKg': rng.choice([1800, 2400, 3000])}
                         for j in range(rng.randint(1, 3))]
                create_supplier_bill({'supplierId': sups[k % len(sups)]['id'],
                                      'billDate': bd, 'dueDate': bd, 'supplierRef': 'DEMO-BILL-' + str(k + 1),
                                      'notes': 'DEMO bill', 'items': items})
                summary['bills'] += 1
            except Exception:
                summary['errors'] += 1

    # ── 6) knock 2 ingredients below their reorder level → low-stock tile ──
    #     (batches are already produced, so reducing stock now is safe)
    low_candidates = qry("SELECT id, code, reorder_level FROM ingredients WHERE active=1 AND reorder_level > 0 ORDER BY code")
    picked = low_candidates[:2]
    from modules.inventory import get_stock_map
    stock = get_stock_map()
    for ing in picked:
        try:
            cur = stock.get(ing['id'], 0)
            target = float(ing['reorder_level']) * 0.4
            delta = target - cur
            if delta != 0:
                create_adjustment({'ingredientId': ing['id'], 'qtyGrams': round(delta, 2), 'notes': 'DEMO low stock'})
            summary['lowStock'] += 1
        except Exception:
            summary['errors'] += 1

    # ── 7) monthly operating costs for the last 2 months → realistic Net Profit ──
    global _DEMO_MONTHS_OPCOST
    opmonths = months[-2:]
    _DEMO_MONTHS_OPCOST = opmonths
    for month in opmonths:
        try:
            for cat, amt in (('salaries', 20000), ('rent', 10000), ('electricity', 4000), ('gas', 1000)):
                upsert_operating_cost(month, cat, amt, username='demo')
            summary['opCostMonths'] += 1
        except Exception:
            summary['errors'] += 1

    summary['msg'] = ('Seeded demo data across %d months: %d batches, %d orders, %d invoices, %d payments, %d work orders.'
                      % (6, summary['batches'], summary['orders'], summary['invoices'],
                         summary['payments'], summary['workOrders']))
    return summary


def clear_demo_data():
    """Reverse the demo seed: purge all transactional data (same as the launch reset —
    orders/invoices/sales/payments/WOs/batches/ledger, counters back to 0001) and drop the
    demo operating-cost months. Master data is untouched. Safe/idempotent."""
    from modules.reports import reset_for_launch
    from modules.db import _conn, save_db
    res = reset_for_launch()
    removed_opcosts = 0
    c = _conn()
    try:
        # remove operating-cost rows added by demo (by the demo months, or any 'demo' entry)
        rows = c.execute("SELECT COUNT(*) FROM monthly_operating_costs WHERE updated_by='demo'").fetchone()[0]
        if rows:
            c.execute("DELETE FROM monthly_operating_costs WHERE updated_by='demo'")
            removed_opcosts = rows
        c.commit()
    except Exception:
        c.rollback()
    finally:
        c.close()
    save_db()
    return {'ok': True, 'purged_total': res.get('purged_total', 0),
            'opcost_rows_removed': removed_opcosts,
            'msg': 'Cleared demo data — back to the clean pre-launch state.'}
