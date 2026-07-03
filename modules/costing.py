"""
modules/costing.py
==================
Sprint 13 — Costing domain: config, standard costs, prices, margin alerts,
            price history, batch variances.

Exports (via __all__):
  get_costing_config, compute_standard_cost, get_all_standard_costs,
  get_batch_variances, update_costing_config,
  set_product_price, import_prices_master,
  get_price_history, get_ingredient_price_history, seed_price_history,
  get_margin_alerts, dismiss_margin_alert, send_margin_alert_email

Dependencies (resolved at import time via * from sibling modules):
  modules.utils  — r2, today
  modules.db     — _conn, qry, qry1, run, run_many, save_db, audit_log

NOTE: ensure_costing_config, ensure_margin_alerts_table, ensure_price_types_sprint6,
      ensure_price_history_extended are all in modules.migrations (already extracted).

NOTE: get_margin_report is left in server.py — it queries the sales table and
      belongs with the reports module (Sprint 14).
"""

import os

from modules.utils import *   # r2, today
from modules.db    import *   # _conn, qry, qry1, run, run_many, save_db, audit_log

__all__ = [
    'get_costing_config',
    'compute_standard_cost',
    'get_all_standard_costs',
    'get_batch_variances',
    'update_costing_config',
    'set_product_price',
    'import_prices_master',
    'get_price_history',
    'get_ingredient_price_history',
    'seed_price_history',
    'get_margin_alerts',
    'dismiss_margin_alert',
    'send_margin_alert_email',
    'list_operating_costs',
    'upsert_operating_cost',
    'list_cost_lines',
    'create_cost_line',
    'update_cost_line',
    'delete_cost_line',
    'cost_line_rates',
]


# ─────────────────────────────────────────────────────────────────
#  CONFIG HELPERS
# ─────────────────────────────────────────────────────────────────

def get_costing_config():
    """Return all costing config values as a dict keyed by config key."""
    rows = qry("SELECT key, value, label, updated_at, updated_by FROM costing_config ORDER BY key")
    return {r['key']: dict(r) for r in rows}


def _get_config_val(cfg, key, default):
    """Extract float from costing_config dict."""
    try:
        return float(cfg[key]['value'])
    except (KeyError, TypeError, ValueError):
        return default


def _selling_price_for_variant(variant_id):
    """The ACTUAL selling price from the price book (Model B — price set by market,
    not by cost). Primary = distributor; falls back to ex_factory → retail_mrp → web.
    Returns (price, channel_code) or (0.0, None) if no selling price is set."""
    try:
        row = qry1("""
            SELECT pp.price, pt.code
            FROM product_prices pp
            JOIN price_types pt ON pt.id = pp.price_type_id
            WHERE pp.product_variant_id=? AND pp.active_flag=1
              AND pt.code IN ('distributor','ex_factory','retail_mrp','web')
            ORDER BY CASE pt.code
                       WHEN 'distributor' THEN 1 WHEN 'ex_factory' THEN 2
                       WHEN 'retail_mrp'  THEN 3 WHEN 'web'        THEN 4 ELSE 5 END,
                     pp.effective_from DESC
            LIMIT 1
        """, (variant_id,))
        if row and (row['price'] or 0) > 0:
            return (round(float(row['price']), 2), row['code'])
    except Exception:
        pass
    return (0.0, None)


# ─────────────────────────────────────────────────────────────────
#  STANDARD COST COMPUTATION
# ─────────────────────────────────────────────────────────────────

def _operating_unit_rates():
    """Trailing 3-month-average operating cost PER PACK, by category + grouped totals.
    Averages whatever months are available (1 → that month). Denominator = the stable
    normal monthly volume (not the raw month's output). {} if no data → caller falls back."""
    rows = qry("SELECT month, category, amount FROM monthly_operating_costs ORDER BY month DESC")
    if not rows:
        return {}
    normal = _get_config_val(get_costing_config(), 'normal_monthly_volume', 600.0) or 600.0
    months = []
    for r in rows:
        if r['month'] not in months:
            months.append(r['month'])
        if len(months) >= 3:
            break
    by_cat = {}
    for r in rows:
        if r['month'] in months:
            by_cat[r['category']] = by_cat.get(r['category'], 0.0) + float(r['amount'] or 0)
    n = len(months) or 1
    per_pack = {cat: round((tot / n) / normal, 4) for cat, tot in by_cat.items()}
    MAKING = ('labour', 'electricity', 'gas', 'rent')
    OVH    = ('transport', 'salaries', 'admin')
    conversion = round(sum(v for c, v in per_pack.items() if c in MAKING), 4)
    overhead   = round(sum(v for c, v in per_pack.items() if c in OVH), 4)
    return {'per_pack': per_pack, 'conversion': conversion, 'overhead': overhead,
            'total': round(conversion + overhead, 4), 'months_used': n, 'normal_volume': normal}


def list_operating_costs(months=6):
    """Recent monthly operating costs + derived per-pack rates (trailing 3-month average
    ÷ normal volume — the same figure that feeds Cost to Make in Phase 2)."""
    rows = qry("SELECT month, category, amount FROM monthly_operating_costs ORDER BY month DESC, category")
    cfg = get_costing_config()
    normal_vol = _get_config_val(cfg, 'normal_monthly_volume', 600.0)
    by_month = {}
    for r in rows:
        by_month.setdefault(r['month'], {})[r['category']] = float(r['amount'] or 0)
    months_sorted = sorted(by_month.keys(), reverse=True)[:months]
    op = _operating_unit_rates()
    return {
        'normal_monthly_volume': normal_vol,
        'months': [{'month': m,
                    'categories': by_month[m],
                    'total': round(sum(by_month[m].values()), 2)} for m in months_sorted],
        'latest_month': months_sorted[0] if months_sorted else None,
        'derived_per_pack': op.get('per_pack', {}) if op else {},
        'derived_total_per_pack': op.get('total', 0) if op else 0,
        'months_used': op.get('months_used', 0) if op else 0,
    }


def upsert_operating_cost(month, category, amount, username='admin'):
    """Set one month's actual cost for a category (idempotent upsert by month+category)."""
    month    = str(month or '').strip()
    category = str(category or '').strip().lower()
    amt      = float(amount or 0)
    if not month or not category:
        raise ValueError("month and category are required")
    c = _conn()
    try:
        ex = c.execute("SELECT id FROM monthly_operating_costs WHERE month=? AND category=?",
                       (month, category)).fetchone()
        if ex:
            c.execute("UPDATE monthly_operating_costs SET amount=?, updated_at=datetime('now'), updated_by=? WHERE id=?",
                      (amt, username, ex[0]))
        else:
            c.execute("INSERT INTO monthly_operating_costs (month, category, amount, updated_at, updated_by) VALUES (?,?,?,datetime('now'),?)",
                      (month, category, amt, username))
        c.commit()
    finally:
        c.close()
    save_db()
    return list_operating_costs()


# ─────────────────────────────────────────────────────────────────
#  COST LINES (user-managed; two-engine model)
# ─────────────────────────────────────────────────────────────────

_VALID_BUCKETS = ('variable', 'fixed', 'tracking')


def list_cost_lines():
    """All active cost lines, grouped variable → fixed → tracking, then by sort order."""
    return qry("""
        SELECT id, name, bucket, standard_value, sort_order
        FROM cost_lines WHERE active=1
        ORDER BY CASE bucket WHEN 'variable' THEN 1 WHEN 'fixed' THEN 2 ELSE 3 END,
                 sort_order, id
    """)


def create_cost_line(name, bucket='tracking', standard_value=0.0):
    """Add a cost line. bucket defaults to 'tracking' (safest — never touches product cost
    until deliberately set to variable/fixed)."""
    name   = str(name or '').strip()
    bucket = (bucket or 'tracking').strip().lower()
    if not name:
        raise ValueError("name is required")
    if bucket not in _VALID_BUCKETS:
        bucket = 'tracking'
    c = _conn()
    try:
        mx = c.execute("SELECT COALESCE(MAX(sort_order),0) FROM cost_lines").fetchone()[0]
        c.execute("INSERT INTO cost_lines (name, bucket, standard_value, sort_order) VALUES (?,?,?,?)",
                  (name, bucket, float(standard_value or 0), mx + 10))
        c.commit()
    finally:
        c.close()
    save_db()
    return list_cost_lines()


def update_cost_line(line_id, name=None, bucket=None, standard_value=None):
    """Edit a cost line's name, bucket, and/or standard value (partial update)."""
    c = _conn()
    try:
        row = c.execute("SELECT name, bucket, standard_value FROM cost_lines WHERE id=?", (line_id,)).fetchone()
        if not row:
            raise ValueError("cost line not found")
        new_name   = str(name).strip() if name is not None else row[0]
        new_bucket = (bucket or row[1]).strip().lower()
        if new_bucket not in _VALID_BUCKETS:
            new_bucket = row[1]
        new_val    = float(standard_value) if standard_value is not None else row[2]
        c.execute("UPDATE cost_lines SET name=?, bucket=?, standard_value=? WHERE id=?",
                  (new_name, new_bucket, new_val, line_id))
        c.commit()
    finally:
        c.close()
    save_db()
    return list_cost_lines()


def delete_cost_line(line_id):
    """Soft-delete a cost line (active=0). Recorded monthly actuals are left untouched."""
    c = _conn()
    try:
        c.execute("UPDATE cost_lines SET active=0 WHERE id=?", (line_id,))
        c.commit()
    finally:
        c.close()
    save_db()
    return list_cost_lines()


def cost_line_rates(normal_vol=None):
    """Standard cost contribution from user cost lines:
      variable_per_pack = Σ standard_value where bucket='variable'   (₨/pack)
      fixed_monthly     = Σ standard_value where bucket='fixed'      (₨/month)
      fixed_per_pack    = fixed_monthly ÷ normal_vol
    'tracking' lines are ignored (they never touch product cost)."""
    if normal_vol is None:
        normal_vol = _get_config_val(get_costing_config(), 'normal_monthly_volume', 600.0) or 600.0
    rows   = qry("SELECT bucket, standard_value FROM cost_lines WHERE active=1")
    var_pp = round(sum(float(r['standard_value'] or 0) for r in rows if r['bucket'] == 'variable'), 4)
    fix_mo = round(sum(float(r['standard_value'] or 0) for r in rows if r['bucket'] == 'fixed'), 2)
    fix_pp = round(fix_mo / normal_vol, 4) if normal_vol else 0.0
    return {'variable_per_pack': var_pp, 'fixed_monthly': fix_mo,
            'fixed_per_pack': fix_pp, 'normal_volume': normal_vol}


def _live_unit_rate(normal_vol):
    """ACTUAL variable+fixed cost per pack from the latest recorded month's operating costs,
    matched to cost_lines by name (tracking lines like Raw materials excluded — RM comes from
    the BOM). ₨/pack, or None if no month/data recorded yet."""
    row = qry1("SELECT MAX(month) AS m FROM monthly_operating_costs")
    if not row or not row.get('m'):
        return None
    lines = qry("SELECT LOWER(name) AS lname FROM cost_lines WHERE active=1 AND bucket IN ('variable','fixed')")
    keep  = {l['lname'] for l in lines}
    acts  = qry("SELECT LOWER(category) AS cat, amount FROM monthly_operating_costs WHERE month=?", (row['m'],))
    total = sum(float(a['amount'] or 0) for a in acts if a['cat'] in keep)
    if total <= 0 or not normal_vol:
        return None
    return round(total / normal_vol, 4)


def compute_standard_cost(product_code, pack_size_label, cfg=None):
    """
    Compute standard cost for one SKU using current BOM + ingredient costs + costing_config.
    Returns dict with full cost breakdown, or None if variant not found.
    pack_size_label: e.g. '50g', '100g', '1000g'
    """
    if cfg is None:
        cfg = get_costing_config()

    margin_mfr   = _get_config_val(cfg, 'margin_mfr', 1.30)
    margin_dist  = _get_config_val(cfg, 'margin_dist', 1.10)
    margin_mrp   = _get_config_val(cfg, 'margin_mrp', 1.22)
    floor_pct    = _get_config_val(cfg, 'margin_floor_pct', 30.0)

    # Itemized cost lines (₨/pack) → category totals. If the itemized keys are
    # present (post-migration) we use their sum even when 0 (deliberate zero);
    # only fall back to the legacy lumped key when the itemized keys are absent.
    def _sum_cfg(keys, fallback_key=None, fallback_default=0.0):
        present = [k for k in keys if k in cfg]
        if present:
            return round(sum(_get_config_val(cfg, k, 0.0) for k in keys), 2)
        return _get_config_val(cfg, fallback_key, fallback_default) if fallback_key else 0.0

    # ── TWO-ENGINE MODEL: standard cost from USER-MANAGED cost lines (see cost_line_rates) ──
    # Variable lines are ₨/pack; fixed lines are ₨/month absorbed ÷ normal volume; tracking
    # lines (e.g. Raw materials, Marketing) never touch product cost. RM comes from the BOM
    # below. Monthly actuals are surfaced as a LIVE reference beside the standard, not the price.
    normal_vol          = _get_config_val(cfg, 'normal_monthly_volume', 600.0) or 600.0
    _rates              = cost_line_rates(normal_vol)
    variable_pp         = _rates['variable_per_pack']
    fixed_per_pack      = _rates['fixed_per_pack']
    fixed_monthly_total = _rates['fixed_monthly']
    # Back-compat return slots: packaging is now a variable line, so 'packaging'=0;
    # 'labour' carries all variable per-pack, 'overhead' carries absorbed fixed per pack.
    packaging      = 0.0
    conversion     = variable_pp
    overhead_lines = fixed_per_pack
    _ovh_present   = True
    labour         = variable_pp

    variant = qry1("""
        SELECT pv.id, pv.sku_code, p.code as product_code, p.name as product_name,
               ps.label as pack_size, ps.grams as pack_grams, pv.product_id,
               COALESCE(pv.wastage_pct, 0) as wastage_pct
        FROM product_variants pv
        JOIN products p    ON p.id  = pv.product_id
        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE p.code=? AND ps.label=? AND pv.active_flag=1
    """, (product_code, pack_size_label))
    if not variant:
        return None

    wastage_pct = float(variant['wastage_pct'] or 0)

    bom_ver = qry1("""
        SELECT * FROM bom_versions WHERE product_id=? AND active_flag=1
        ORDER BY version_no DESC LIMIT 1
    """, (variant['product_id'],))
    if not bom_ver:
        return {
            'productCode':  product_code,
            'productName':  variant['product_name'],
            'packSize':     pack_size_label,
            'skuCode':      variant['sku_code'],
            'variantId':    variant['id'],
            'has_bom':      False,
            'ingredients':  [],
            'rm_cost':      0,
            'wastage_adj':  0,
            'overhead':     overhead_lines,
            'packaging':    packaging,
            'labour':       conversion,
            'cost_to_make': round(packaging + conversion + overhead_lines, 2),
            'direct_sale':  0,
            'distributor':  0,
            'mrp':          0,
            'gross_margin_pct': 0,
            'below_floor':  True,
            'wastage_pct':  wastage_pct,
            'selling_price':    _selling_price_for_variant(variant['id'])[0],
            'selling_channel':  _selling_price_for_variant(variant['id'])[1],
            'actual_margin_pct': None,    # no BOM → cost is incomplete, don't claim a margin
            'actual_below_floor': False,
            'floor_pct':        _get_config_val(cfg, 'margin_floor_pct', 30.0),
            'fixed_monthly_total': fixed_monthly_total,
            'normal_volume':    normal_vol,
            'has_live':         False,
            'live_cost_to_make': None,
            'live_variance':    None,
            'live_variance_pct': None,
            'live_margin_pct':  None,
        }

    # Scale BOM to 1 unit of this pack size
    pack_grams = float(variant['pack_grams'] or 0)
    scale = pack_grams / float(bom_ver['batch_size_grams']) if bom_ver['batch_size_grams'] else 0

    bom_items = qry("""
        SELECT bi.quantity_grams, i.id as ing_id, i.code as ing_code, i.name as ing_name,
               i.cost_per_kg
        FROM bom_items bi
        JOIN ingredients i ON i.id = bi.ingredient_id
        WHERE bi.bom_version_id=?
        ORDER BY i.code
    """, (bom_ver['id'],))

    ingredients = []
    rm_cost_raw = 0.0
    for b in bom_items:
        qty_kg    = round(b['quantity_grams'] * scale / 1000.0, 6)
        cpkg      = float(b['cost_per_kg'] or 0)
        line_cost = round(qty_kg * cpkg, 4)
        rm_cost_raw += line_cost
        ingredients.append({
            'code':        b['ing_code'],
            'name':        b['ing_name'],
            'qty_kg':      qty_kg,
            'cost_per_kg': cpkg,
            'line_cost':   line_cost,
        })

    rm_cost_raw = round(rm_cost_raw, 2)
    if wastage_pct > 0 and wastage_pct < 1:
        rm_cost_adjusted = round(rm_cost_raw / (1 - wastage_pct), 2)
    else:
        rm_cost_adjusted = rm_cost_raw
    wastage_adj  = round(rm_cost_adjusted - rm_cost_raw, 2)

    # Fixed cost per pack = monthly fixed ÷ normal volume (computed above as overhead_lines).
    overhead     = overhead_lines
    cost_to_make = round(rm_cost_adjusted + overhead + packaging + conversion, 2)
    overhead_pct_of_cost = round(overhead / cost_to_make * 100, 1) if cost_to_make > 0 else 0
    direct_sale  = round(cost_to_make * margin_mfr, 2)
    distributor  = round(direct_sale * margin_dist, 2)
    mrp          = round(distributor * margin_mrp, 2)
    margin_pct   = round((mrp - cost_to_make) / mrp * 100, 1) if mrp > 0 else 0

    # Model B — measure the REAL margin against the actual selling price (price book),
    # not the theoretical cost×markup MRP. The computed mrp above is the "suggested" price.
    sell_price, sell_chan = _selling_price_for_variant(variant['id'])
    actual_margin_pct  = round((sell_price - cost_to_make) / sell_price * 100, 1) if sell_price > 0 else None
    actual_below_floor = (actual_margin_pct is not None and actual_margin_pct < floor_pct)

    # LIVE reference (decision support, NOT the price): this SKU's RM + standard packaging +
    # the ACTUAL conversion/overhead per pack from recorded monthly operating costs. Shown
    # beside the standard so the MRP can be set against reality. None until a month is recorded.
    _live_rate = _live_unit_rate(normal_vol)
    if _live_rate is not None:
        live_cost_to_make = round(rm_cost_adjusted + _live_rate, 2)
        live_variance     = round(live_cost_to_make - cost_to_make, 2)
        live_variance_pct = round(live_variance / cost_to_make * 100, 1) if cost_to_make > 0 else None
        live_margin_pct   = round((sell_price - live_cost_to_make) / sell_price * 100, 1) if sell_price > 0 else None
    else:
        live_cost_to_make = live_variance = live_variance_pct = live_margin_pct = None

    return {
        'productCode':      product_code,
        'productName':      variant['product_name'],
        'packSize':         pack_size_label,
        'skuCode':          variant['sku_code'],
        'variantId':        variant['id'],
        'has_bom':          True,
        'ingredients':      ingredients,
        'rm_cost':          rm_cost_adjusted,
        'wastage_adj':      wastage_adj,
        'wastage_pct':      wastage_pct,
        'overhead':         overhead,
        'overhead_pct_of_cost': overhead_pct_of_cost,
        'packaging':        packaging,
        'labour':           labour,
        'cost_to_make':     cost_to_make,
        'direct_sale':      direct_sale,
        'distributor':      distributor,
        'mrp':              mrp,                       # suggested price (cost × markups)
        'gross_margin_pct': margin_pct,               # margin vs suggested price
        'below_floor':      margin_pct < floor_pct,
        'selling_price':    sell_price,               # ACTUAL price from the book (0 if none set)
        'selling_channel':  sell_chan,
        'actual_margin_pct': actual_margin_pct,       # REAL margin vs actual price (None if no price)
        'actual_below_floor': actual_below_floor,
        'floor_pct':        floor_pct,
        'fixed_monthly_total': fixed_monthly_total,   # sum of fix_* monthly standards
        'normal_volume':    normal_vol,               # absorption denominator
        'has_live':         _live_rate is not None,    # any monthly actuals recorded?
        'live_cost_to_make': live_cost_to_make,       # RM + packaging + ACTUAL conv/ovh (None if no actuals)
        'live_variance':    live_variance,            # live − standard (₨)
        'live_variance_pct': live_variance_pct,       # (live − standard) / standard %
        'live_margin_pct':  live_margin_pct,          # margin vs actual price on the LIVE cost
    }


def get_all_standard_costs():
    """Return standard costs for all active SKUs."""
    cfg = get_costing_config()
    variants = qry("""
        SELECT p.code as product_code, ps.label as pack_size
        FROM product_variants pv
        JOIN products p    ON p.id  = pv.product_id
        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE pv.active_flag=1 AND p.active=1
        ORDER BY p.code, ps.grams
    """)
    results = []
    for v in variants:
        cost = compute_standard_cost(v['product_code'], v['pack_size'], cfg)
        if cost:
            results.append(cost)
    return results


def get_batch_variances(days=90):
    """Compare unit_cost_at_posting vs computed standard cost for recent batches."""
    cfg = get_costing_config()
    batches = qry("""
        SELECT pb.id, pb.batch_id, pb.batch_date, pb.qty_units,
               pb.unit_cost_at_posting,
               p.code as product_code, p.name as product_name,
               ps.label as pack_size
        FROM production_batches pb
        JOIN products p ON p.id = pb.product_id
        LEFT JOIN product_variants pv ON pv.id = pb.product_variant_id
        LEFT JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE pb.batch_date >= date('now', ? || ' days')
        ORDER BY pb.batch_date DESC
        LIMIT 200
    """, ('-' + str(days),))

    results = []
    for b in batches:
        actual   = float(b['unit_cost_at_posting'] or 0)
        std_data = compute_standard_cost(b['product_code'], b['pack_size'] or '', cfg) if b['pack_size'] else None
        standard = std_data['cost_to_make'] if std_data and std_data.get('has_bom') else None
        variance     = round(actual - standard, 2) if standard is not None else None
        variance_pct = round((variance / standard) * 100, 1) if standard and variance is not None else None
        results.append({
            'batchId':     b['batch_id'],
            'batchDate':   b['batch_date'],
            'productCode': b['product_code'],
            'productName': b['product_name'],
            'packSize':    b['pack_size'],
            'qtyUnits':    b['qty_units'],
            'actual_mfg':  actual,
            'standard_mfg': standard,
            'variance':    variance,
            'variance_pct': variance_pct,
            'favourable':  variance_pct is not None and variance_pct < 0,
            'flag':        variance_pct is not None and variance_pct > 5,
        })
    return results


# ─────────────────────────────────────────────────────────────────
#  CONFIG UPDATE
# ─────────────────────────────────────────────────────────────────

def update_costing_config(key, value, username):
    """Update a single costing config key. Logs change to costing_config_history. Returns full config."""
    row = qry1("SELECT * FROM costing_config WHERE key=?", (key,))
    if not row:
        raise ValueError("Unknown config key: {}".format(key))
    old_val = float(row['value'])
    new_val = float(value)
    pct     = round((new_val - old_val) / old_val * 100, 2) if old_val != 0 else 0
    c = _conn()
    try:
        c.execute("""
            INSERT INTO costing_config_history
                (config_key, old_value, new_value, pct_change, changed_by)
            VALUES (?, ?, ?, ?, ?)
        """, (key, str(old_val), str(new_val), pct, username))
        c.execute("""
            UPDATE costing_config
            SET value=?, updated_at=datetime('now'), updated_by=?
            WHERE key=?
        """, (str(new_val), username, key))
        c.commit()
    finally:
        c.close()
    save_db()
    return get_costing_config()


# ─────────────────────────────────────────────────────────────────
#  PRODUCT PRICES
# ─────────────────────────────────────────────────────────────────

def set_product_price(data):
    """
    Set a price for a product variant + price type.
    Deactivates any existing active price first.
    data: {productVariantId, priceTypeId, price, effectiveFrom}
    """
    pv_id = data.get('productVariantId')
    pt_id = data.get('priceTypeId')
    price = r2(data.get('price', 0))
    eff   = data.get('effectiveFrom', today())

    if price < 0:
        raise ValueError("Price cannot be negative")

    ops = [
        ("UPDATE product_prices SET active_flag=0 WHERE product_variant_id=? AND price_type_id=?",
         (pv_id, pt_id)),
        ("""INSERT INTO product_prices (product_variant_id, price_type_id, price, effective_from, active_flag)
            VALUES (?,?,?,?,1)""",
         (pv_id, pt_id, price, eff)),
    ]
    audit_log(ops, 'product_prices', f"{pv_id}-{pt_id}", 'UPDATE', new_val=data)
    run_many(ops)
    return qry1("""
        SELECT pp.*, pt.code as price_type_code, pt.label as price_type_label,
               pv.sku_code
        FROM product_prices pp
        JOIN price_types pt ON pt.id = pp.price_type_id
        JOIN product_variants pv ON pv.id = pp.product_variant_id
        WHERE pp.product_variant_id=? AND pp.price_type_id=? AND pp.active_flag=1
    """, (pv_id, pt_id))


def import_prices_master(rows):
    """Full replace of prices from master rows. Returns {imported, errors}."""
    imported = 0
    errors   = []
    for i, row in enumerate(rows, 1):
        pcode     = row.get('product_code', '').strip().upper()
        psize     = row.get('pack_size', '').strip()
        ptype     = row.get('price_type', '').strip().lower()
        price_str = row.get('price', '0')
        eff       = row.get('effective_from', today()).strip() or today()
        if not pcode or not psize or not ptype:
            errors.append(f"Row {i}: product_code, pack_size, price_type required")
            continue
        try:
            price_val = float(str(price_str).replace(',', ''))
        except ValueError:
            errors.append(f"Row {i}: invalid price '{price_str}'")
            continue
        pt = qry1("SELECT id FROM price_types WHERE code=?", (ptype,))
        if not pt:
            errors.append(f"Row {i}: unknown price_type '{ptype}'")
            continue
        var = qry1("""
            SELECT pv.id FROM product_variants pv
            JOIN products p    ON p.id  = pv.product_id
            JOIN pack_sizes ps ON ps.id = pv.pack_size_id
            WHERE p.code=? AND ps.label=? AND pv.active_flag=1
        """, (pcode, psize))
        if not var:
            errors.append(f"Row {i}: product '{pcode}' / pack '{psize}' not found")
            continue
        try:
            set_product_price({
                'productVariantId': var['id'],
                'priceTypeId':      pt['id'],
                'price':            price_val,
                'effectiveFrom':    eff,
            })
            imported += 1
        except Exception as e:
            errors.append(f"Row {i}: {e}")
    return {'imported': imported, 'errors': errors}


# ─────────────────────────────────────────────────────────────────
#  PRICE HISTORY
# ─────────────────────────────────────────────────────────────────

def get_ingredient_price_history(ingredient_id=None, limit=50):
    """Return ingredient cost change history with ingredient code and name.
    Pass ingredient_id to filter to one ingredient; omit for all (most recent 50).
    """
    sql = """
        SELECT ph.*, i.code, i.name
        FROM ingredient_price_history ph
        JOIN ingredients i ON i.id = ph.ingredient_id
        {}
        ORDER BY ph.changed_at DESC
        LIMIT ?
    """.format("WHERE ph.ingredient_id=?" if ingredient_id else "")
    params = (ingredient_id, limit) if ingredient_id else (limit,)
    return qry(sql, params)


def seed_price_history():
    """
    If an ingredient has cost_per_kg > 0 but no history record, seed one
    with old_cost=NULL and source='initial'. Makes current price visible
    in the price change column from day one.
    """
    ings = qry("""
        SELECT i.id, i.code, i.cost_per_kg
        FROM ingredients i
        LEFT JOIN ingredient_price_history ph ON ph.ingredient_id = i.id
        WHERE i.cost_per_kg > 0 AND ph.id IS NULL
    """)
    if not ings:
        return
    c = _conn()
    try:
        for i in ings:
            c.execute("""
                INSERT INTO ingredient_price_history
                    (ingredient_id, old_cost_per_kg, new_cost_per_kg, pct_change, source)
                VALUES (?, NULL, ?, NULL, 'initial')
            """, (i['id'], i['cost_per_kg']))
        c.commit()
        print(f"  ✓ Price history: seeded {len(ings)} initial price record(s)")
    except Exception as e:
        print(f"  ⚠ Price history seed error: {e}")
        c.rollback()
    finally:
        c.close()
    save_db()


def get_price_history(limit=100, change_type=None, days=None):
    """
    Return unified price history: ingredient cost changes + costing config changes.
    Merges ingredient_price_history (change_type='ingredient') and
    costing_config_history (change_type='config').
    """
    rows = []

    # ── Ingredient price history ──────────────────────────────────
    ing_where  = "WHERE 1=1"
    ing_params = []
    if change_type and change_type != 'config':
        ing_where += " AND COALESCE(iph.change_type, 'ingredient') = ?"
        ing_params.append(change_type)
    if days:
        ing_where += " AND iph.changed_at >= datetime('now', ? || ' days')"
        ing_params.append('-' + str(days))

    ing_rows = qry("""
        SELECT
            'ingredient'                        AS change_type,
            iph.id                              AS id,
            i.code                              AS entity_code,
            i.name                              AS entity_name,
            iph.old_cost_per_kg                 AS old_value,
            iph.new_cost_per_kg                 AS new_value,
            iph.pct_change,
            COALESCE(iph.changed_by, 'system')  AS changed_by,
            iph.changed_at,
            COALESCE(iph.source, 'manual')      AS source,
            iph.note
        FROM ingredient_price_history iph
        JOIN ingredients i ON i.id = iph.ingredient_id
        {where}
        ORDER BY iph.changed_at DESC
        LIMIT ?
    """.format(where=ing_where), ing_params + [limit])

    for r in ing_rows:
        rows.append(dict(r))

    # ── Config history ────────────────────────────────────────────
    if change_type in (None, 'config'):
        cfg_where  = "WHERE 1=1"
        cfg_params = []
        if days:
            cfg_where += " AND changed_at >= datetime('now', ? || ' days')"
            cfg_params.append('-' + str(days))

        cfg_rows = qry("""
            SELECT
                'config'                            AS change_type,
                cch.id                              AS id,
                cch.config_key                      AS entity_code,
                cc.label                            AS entity_name,
                cch.old_value,
                cch.new_value,
                cch.pct_change,
                COALESCE(cch.changed_by, 'system')  AS changed_by,
                cch.changed_at,
                'manual'                            AS source,
                cch.note
            FROM costing_config_history cch
            LEFT JOIN costing_config cc ON cc.key = cch.config_key
            {where}
            ORDER BY cch.changed_at DESC
            LIMIT ?
        """.format(where=cfg_where), cfg_params + [limit])

        for r in cfg_rows:
            rows.append(dict(r))

    rows.sort(key=lambda x: x.get('changed_at') or '', reverse=True)
    return rows[:limit]


# ─────────────────────────────────────────────────────────────────
#  MARGIN ALERTS
# ─────────────────────────────────────────────────────────────────

def get_margin_alerts(include_dismissed=False):
    """
    Compute current margin alerts by checking all active SKUs against floor.
    Logs new alerts to margin_alerts table if not already recorded.
    Returns list of active (undismissed) alerts.
    """
    all_costs = get_all_standard_costs()
    cfg       = get_costing_config()
    floor_pct = _get_config_val(cfg, 'margin_floor_pct', 30.0)

    alerts = []
    for sku in all_costs:
        if not sku.get('has_bom'):
            continue
        gm = sku.get('gross_margin_pct', 0)
        if gm < floor_pct:
            existing = qry1("""
                SELECT * FROM margin_alerts
                WHERE product_code=? AND pack_size=? AND dismissed_at IS NULL
                ORDER BY detected_at DESC LIMIT 1
            """, (sku['productCode'], sku['packSize']))

            if not existing:
                c = _conn()
                try:
                    c.execute("""
                        INSERT INTO margin_alerts
                            (product_code, pack_size, sku_code, margin_pct, floor_pct)
                        VALUES (?, ?, ?, ?, ?)
                    """, (sku['productCode'], sku['packSize'],
                          sku.get('skuCode'), round(gm, 2), floor_pct))
                    c.commit()
                    existing = qry1("""
                        SELECT * FROM margin_alerts
                        WHERE product_code=? AND pack_size=? AND dismissed_at IS NULL
                        ORDER BY id DESC LIMIT 1
                    """, (sku['productCode'], sku['packSize']))
                finally:
                    c.close()

            if existing:
                alerts.append({
                    'alertId':     existing['id'],
                    'productCode': sku['productCode'],
                    'productName': sku['productName'],
                    'packSize':    sku['packSize'],
                    'skuCode':     sku.get('skuCode'),
                    'margin_pct':  round(gm, 2),
                    'floor_pct':   floor_pct,
                    'gap':         round(floor_pct - gm, 2),
                    'detectedAt':  existing['detected_at'],
                    'emailSent':   bool(existing['email_sent']),
                    'exFactory':   sku.get('ex_factory'),
                })

    if include_dismissed:
        dismissed = qry("""
            SELECT * FROM margin_alerts WHERE dismissed_at IS NOT NULL
            ORDER BY dismissed_at DESC LIMIT 50
        """)
        for d in dismissed:
            alerts.append({
                'alertId':     d['id'],
                'productCode': d['product_code'],
                'packSize':    d['pack_size'],
                'skuCode':     d['sku_code'],
                'margin_pct':  d['margin_pct'],
                'floor_pct':   d['floor_pct'],
                'detectedAt':  d['detected_at'],
                'dismissedAt': d['dismissed_at'],
                'dismissedBy': d['dismissed_by'],
                'dismissed':   True,
            })

    return alerts


def dismiss_margin_alert(alert_id, username):
    """Mark a margin alert as dismissed."""
    row = qry1("SELECT * FROM margin_alerts WHERE id=?", (alert_id,))
    if not row:
        raise ValueError("Alert not found: {}".format(alert_id))
    run("""
        UPDATE margin_alerts
        SET dismissed_at=datetime('now'), dismissed_by=?
        WHERE id=?
    """, (username, alert_id))
    save_db()
    return {'ok': True, 'alertId': alert_id}


def send_margin_alert_email(alerts):
    """
    Send email notification for margin alerts.
    Reads ALERT_EMAIL env var. Returns True if sent, False if not configured.
    """
    import smtplib
    from email.mime.text      import MIMEText
    from email.mime.multipart import MIMEMultipart

    to_addr   = os.environ.get('ALERT_EMAIL', '')
    smtp_host = os.environ.get('SMTP_HOST', '')
    smtp_port = int(os.environ.get('SMTP_PORT', '587'))
    smtp_user = os.environ.get('SMTP_USER', '')
    smtp_pass = os.environ.get('SMTP_PASS', '')

    if not to_addr or not smtp_host:
        return False

    subject = 'Spicetopia: %d Margin Alert(s) Below Floor' % len(alerts)
    lines   = ['The following SKUs are below the margin floor:\n']
    for a in alerts:
        lines.append('  • %s %s — margin %.1f%% (floor %.1f%%, gap %.1f%%)' % (
            a['productCode'], a['packSize'],
            a['margin_pct'], a['floor_pct'], a['gap']))
    lines.append('\nLog in to review: Prices & Costs → Margin Alerts')

    msg = MIMEMultipart()
    msg['From']    = smtp_user or 'noreply@spicetopia.com'
    msg['To']      = to_addr
    msg['Subject'] = subject
    msg.attach(MIMEText('\n'.join(lines), 'plain'))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as s:
            s.starttls()
            if smtp_user:
                s.login(smtp_user, smtp_pass)
            s.send_message(msg)
        for a in alerts:
            run("UPDATE margin_alerts SET email_sent=1 WHERE id=?", (a['alertId'],))
        return True
    except Exception:
        return False
