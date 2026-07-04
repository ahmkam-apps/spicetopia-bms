"""
modules/production.py
=====================
Sprint 12 — Production domain: Work Orders, Production Batches, BOM management.

Exports (via __all__):
  check_wo_feasibility, get_procurement_list,
  list_work_orders, create_work_order, convert_wo_to_batch,
  update_work_order, update_work_order_status,
  create_production_batch, create_or_update_bom, import_bom_master

Dependencies (resolved at import time via * from sibling modules):
  modules.utils   — r2, today, require, ValidationError, validate_fields
  modules.db      — _conn, qry, qry1, run, save_db, audit_log
  modules.id_gen  — next_id, _sync_counter_to_max

Lazy imports (inside functions to avoid circular deps):
  modules.inventory — get_stock_map, get_wo_reserved_stock_map

NOTE: get_batch_variances is left in server.py — it depends on
      compute_standard_cost / get_costing_config which are not yet extracted.
"""

import json
from collections import defaultdict

from modules.utils  import *   # r2, today, require, ValidationError, validate_fields
from modules.db     import *   # _conn, qry, qry1, run, save_db, audit_log
from modules.id_gen import *   # next_id, _sync_counter_to_max

__all__ = [
    'check_wo_feasibility',
    'get_procurement_list',
    'list_work_orders',
    'create_work_order',
    'convert_wo_to_batch',
    'update_work_order',
    'update_work_order_status',
    'delete_work_order',
    'get_production_dashboard',
    'create_production_batch',
    'create_or_update_bom',
    'import_bom_master',
    'list_batch_stages',
    'start_batch_run',
    'advance_batch_run',
    'verify_batch_run',
    'cancel_batch_run',
    'list_batch_runs',
    'get_batch_run',
]


# ─────────────────────────────────────────────────────────────────
#  INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────

def _lookup_variant_by_sku(product_code, pack_size):
    """Replace ref['var_by_sku'] with a direct DB JOIN."""
    return qry1("""
        SELECT pv.*, ps.label as pack_size, ps.grams as pack_grams,
               p.code as product_code, p.name as product_name
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id
        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE p.code=? AND ps.label=? AND pv.active_flag=1
    """, (product_code, pack_size))


def _lookup_variant_by_id(variant_id):
    """Replace ref['var_by_id'] with a direct DB JOIN."""
    return qry1("""
        SELECT pv.*, ps.label as pack_size, ps.grams as pack_grams,
               p.code as product_code, p.name as product_name
        FROM product_variants pv
        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        JOIN products p ON p.id = pv.product_id
        WHERE pv.id=?
    """, (variant_id,))


# ─────────────────────────────────────────────────────────────────
#  FEASIBILITY
# ─────────────────────────────────────────────────────────────────

def check_wo_feasibility(variant_id, qty_units, wo_id=None):
    """
    Check if stock is sufficient to produce qty_units of the given variant.
    wo_id: if provided, excludes this WO's own reservations from the available
           calculation (prevents a WO blocking itself when re-checking).
    Returns dict with feasible bool, shortfalls list, requirements list.
    Each requirement includes physicalGrams, reservedGrams, availableGrams.
    """
    from modules.inventory import get_stock_map, get_wo_reserved_stock_map

    var = qry1("""
        SELECT pv.*, ps.grams as pack_grams
        FROM product_variants pv
        LEFT JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE pv.id=?
    """, (variant_id,))
    if not var:
        return {'feasible': False, 'shortfalls': ['Product variant not found'], 'requirements': []}

    bom_ver = qry1("""
        SELECT * FROM bom_versions WHERE product_id=? AND active_flag=1
        ORDER BY version_no DESC LIMIT 1
    """, (var['product_id'],))
    if not bom_ver:
        return {'feasible': False, 'shortfalls': ['No active BOM for this product'], 'requirements': []}

    bom_items_list = qry("""
        SELECT bi.*, i.code as ing_code, i.name as ing_name
        FROM bom_items bi JOIN ingredients i ON i.id=bi.ingredient_id
        WHERE bi.bom_version_id=?
    """, (bom_ver['id'],))

    pack_grams  = var.get('pack_grams', 0) or 0
    total_grams = qty_units * pack_grams
    scale       = total_grams / float(bom_ver['batch_size_grams'])
    stock_map    = get_stock_map()
    reserved_map = get_wo_reserved_stock_map(exclude_wo_id=wo_id)

    requirements = []
    shortfalls   = []
    for b in bom_items_list:
        needed    = r2(b['quantity_grams'] * scale)
        physical  = stock_map.get(b['ingredient_id'], 0)
        reserved  = reserved_map.get(b['ingredient_id'], 0)
        available = max(0.0, r2(physical - reserved))
        deficit   = max(0.0, needed - available)
        requirements.append({
            'ingCode':        b['ing_code'],
            'ingName':        b['ing_name'] or b['ing_code'],
            'neededGrams':    needed,
            'physicalGrams':  physical,
            'reservedGrams':  reserved,
            'availableGrams': available,
            'deficitGrams':   deficit,
            'ok':             deficit < 0.001,
        })
        if deficit >= 0.001:
            shortfalls.append(
                f"{b['ing_code']}: need {needed:.0f}g, "
                f"available {available:.0f}g (physical {physical:.0f}g − reserved {reserved:.0f}g)"
            )
    return {'feasible': len(shortfalls) == 0, 'shortfalls': shortfalls, 'requirements': requirements}


# ─────────────────────────────────────────────────────────────────
#  PROCUREMENT LIST
# ─────────────────────────────────────────────────────────────────

def get_procurement_list(wo_id):
    """
    Full ingredient procurement list for a work order.
    Returns: work order meta + per-ingredient breakdown:
      needed_grams, available_grams, to_procure_grams, cost_per_kg,
      estimated_cost_pkr, in_stock (bool)
    """
    from modules.inventory import get_stock_map

    wo = qry1("""
        SELECT wo.*, p.name as product_name, p.code as product_code,
               ps.label as pack_size, ps.grams as pack_grams, pv.sku_code,
               p.id as product_id
        FROM work_orders wo
        JOIN product_variants pv ON pv.id = wo.product_variant_id
        JOIN products p ON p.id = pv.product_id
        LEFT JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE wo.id=?
    """, (wo_id,))
    if not wo:
        raise ValueError("Work order not found")

    bom_ver = qry1("""
        SELECT * FROM bom_versions WHERE product_id=? AND active_flag=1
        ORDER BY version_no DESC LIMIT 1
    """, (wo['product_id'],))
    if not bom_ver:
        raise ValueError(
            f"No active BOM for {wo['product_name']}. "
            f"Go to Production → BOM Setup → click the red chip for {wo['product_code']} to define ingredients."
        )

    bom_items_list = qry("""
        SELECT bi.quantity_grams,
               i.id as ingredient_id, i.code as ing_code, i.name as ing_name,
               i.cost_per_kg
        FROM bom_items bi JOIN ingredients i ON i.id = bi.ingredient_id
        WHERE bi.bom_version_id = ?
        ORDER BY i.code
    """, (bom_ver['id'],))

    total_grams = wo['qty_units'] * (wo['pack_grams'] or 0)
    scale       = total_grams / float(bom_ver['batch_size_grams']) if bom_ver['batch_size_grams'] else 0
    stock_map   = get_stock_map()

    lines              = []
    total_needed_cost  = 0.0
    total_procure_cost = 0.0

    for b in bom_items_list:
        needed   = r2(b['quantity_grams'] * scale)
        avail    = r2(stock_map.get(b['ingredient_id'], 0))
        procure  = r2(max(0.0, needed - avail))
        cpk      = float(b['cost_per_kg'] or 0)
        est_cost = r2(procure / 1000 * cpk)
        total_needed_cost  += r2(needed / 1000 * cpk)
        total_procure_cost += est_cost
        lines.append({
            'ingCode':          b['ing_code'],
            'ingName':          b['ing_name'] or b['ing_code'],
            'neededGrams':      needed,
            'availableGrams':   avail,
            'toProcureGrams':   procure,
            'toProcureKg':      r2(procure / 1000),
            'costPerKg':        cpk,
            'estimatedCostPKR': est_cost,
            'inStock':          procure < 0.001,
        })

    return {
        'woNumber':             wo['wo_number'],
        'productName':          wo['product_name'],
        'productCode':          wo['product_code'],
        'packSize':             wo['pack_size'],
        'skuCode':              wo['sku_code'],
        'qtyUnits':             wo['qty_units'],
        'totalGrams':           total_grams,
        'targetDate':           wo['target_date'],
        'status':               wo['status'],
        'bomVersion':           bom_ver['version_no'],
        'batchSizeGrams':       bom_ver['batch_size_grams'],
        'lines':                lines,
        'totalNeededCostPKR':   r2(total_needed_cost),
        'totalProcureCostPKR':  r2(total_procure_cost),
        'itemsToProc':          sum(1 for l in lines if not l['inStock']),
        'allInStock':           all(l['inStock'] for l in lines),
    }


# ─────────────────────────────────────────────────────────────────
#  WORK ORDER CRUD
# ─────────────────────────────────────────────────────────────────

def list_work_orders():
    """Return the 200 most recent work orders with product, SKU, and linked order number."""
    return qry("""
        SELECT wo.*, p.name as product_name, p.code as product_code,
               ps.label as pack_size, pv.sku_code,
               co.order_number as customer_order_number
        FROM work_orders wo
        JOIN product_variants pv ON pv.id = wo.product_variant_id
        JOIN products p ON p.id = pv.product_id
        LEFT JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        LEFT JOIN customer_orders co ON co.id = wo.customer_order_id
        ORDER BY wo.created_at DESC LIMIT 200
    """)


def create_work_order(data):
    """Create a standalone work order (not linked to a customer order).
    Required: productVariantId, qtyUnits. Optional: targetDate, notes.
    For WOs linked to a customer order item use POST /api/customer-orders/:id/items/:item_id/work-order.
    """
    validate_fields(data, [
        {'field': 'productVariantId', 'label': 'Product variant', 'type': 'int', 'min': 1},
        {'field': 'qtyUnits',         'label': 'Quantity',         'type': 'int', 'min': 1},
        {'field': 'targetDate',       'label': 'Target date',      'required': False, 'type': 'date'},
    ])
    variant_id = int(data.get('productVariantId', 0))
    qty_units  = int(data.get('qtyUnits', 0))
    if qty_units <= 0:
        raise ValueError("Quantity must be positive")

    var = _lookup_variant_by_id(variant_id)
    if not var:
        raise ValueError("Product variant not found")

    feasibility = check_wo_feasibility(variant_id, qty_units)
    _sync_counter_to_max('work_order', 'work_orders', 'wo_number', 'SP-WO-')
    wo_number = next_id('work_order', 'WO')

    c = _conn()
    try:
        c.execute("""
            INSERT INTO work_orders
                (wo_number, product_variant_id, qty_units, target_date, status, notes, feasibility_ok)
            VALUES (?,?,?,?,?,?,?)
        """, (wo_number, variant_id, qty_units,
              data.get('targetDate') or today(),
              'planned',
              data.get('notes', ''),
              1 if feasibility['feasible'] else 0))
        c.commit()
        wo_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        c.close()
    save_db()
    return {
        'id':           wo_id,
        'woNumber':     wo_number,
        'feasible':     feasibility['feasible'],
        'shortfalls':   feasibility['shortfalls'],
        'requirements': feasibility['requirements'],
    }


def convert_wo_to_batch(wo_id, qty=None):
    """Convert all — or PART — of a work order into a production batch.

    qty=None  → make the full remaining quantity and complete the WO (original behaviour).
    qty=N     → make N units now; the WO stays 'in_progress' with the remainder, and only
                completes once produced_units reaches its target (e.g. 250/week → 1000 WO).
    """
    wo = qry1("SELECT * FROM work_orders WHERE id=?", (wo_id,))
    if not wo:
        raise ValueError("Work order not found")
    if wo['status'] not in ('planned', 'in_progress'):
        raise ValueError(f"Work order is {wo['status']} — cannot convert")

    target    = int(wo['qty_units'])
    produced  = int(wo.get('produced_units') or 0)
    remaining = target - produced
    if remaining <= 0:
        raise ValueError("Work order is already fully produced")
    make_qty = remaining if qty is None else int(qty)
    if make_qty <= 0:
        raise ValueError("Quantity must be positive")
    if make_qty > remaining:
        make_qty = remaining   # never overshoot the WO target

    var = _lookup_variant_by_id(wo['product_variant_id'])
    if not var:
        raise ValueError("Product variant not found")

    prod = qry1("SELECT * FROM products WHERE id=?", (var['product_id'],))

    # Re-use create_production_batch; pass wo_id so the WO's own reservation is
    # excluded from the availability check (prevents self-blocking).
    result = create_production_batch({
        'productCode': prod['code'],
        'packSize':    var['pack_size'],
        'qtyUnits':    make_qty,
        'batchDate':   today(),
        'mfgDate':     today(),
        'bestBefore':  '',
        'notes':       f"From Work Order {wo['wo_number']}" + (f" (partial {make_qty}/{target})" if make_qty < remaining or produced else ''),
    }, exclude_wo_id=wo_id)

    new_produced = produced + make_qty
    new_status   = 'completed' if new_produced >= target else 'in_progress'
    run("UPDATE work_orders SET status=?, produced_units=?, batch_id=?, updated_at=datetime('now') WHERE id=?",
        (new_status, new_produced, result['batchId'], wo_id))
    result['woNumber']  = wo['wo_number']
    result['produced']  = new_produced
    result['remaining'] = target - new_produced
    result['woStatus']  = new_status
    return result


def update_work_order(wo_id, data):
    """Edit qty, target date, and notes on a planned or in_progress work order."""
    wo = qry1("SELECT * FROM work_orders WHERE id=?", (wo_id,))
    if not wo:
        raise ValueError("Work order not found")
    if wo['status'] not in ('planned', 'in_progress'):
        raise ValueError(f"Cannot edit a work order that is {wo['status']}")

    qty_units = int(data.get('qtyUnits', wo['qty_units']))
    if qty_units <= 0:
        raise ValueError("Quantity must be positive")

    feasibility = check_wo_feasibility(wo['product_variant_id'], qty_units)
    run("""
        UPDATE work_orders
        SET qty_units=?, target_date=?, notes=?, feasibility_ok=?, updated_at=datetime('now')
        WHERE id=?
    """, (qty_units,
          data.get('targetDate') or wo['target_date'] or today(),
          data.get('notes', wo['notes'] or ''),
          1 if feasibility['feasible'] else 0,
          wo_id))
    save_db()
    return {'id': wo_id, 'feasible': feasibility['feasible'], 'shortfalls': feasibility['shortfalls']}


def update_work_order_status(wo_id, status):
    """Set WO status directly. Allowed values: planned, in_progress, cancelled.
    Use convert_wo_to_batch() to move a WO to 'completed'.
    """
    allowed = ('planned', 'in_progress', 'cancelled')
    if status not in allowed:
        raise ValueError(f"Invalid status. Must be one of: {', '.join(allowed)}")
    wo = qry1("SELECT id FROM work_orders WHERE id=?", (wo_id,))
    if not wo:
        raise ValueError("Work order not found")
    run("UPDATE work_orders SET status=?, updated_at=datetime('now') WHERE id=?", (status, wo_id))
    return {'id': wo_id, 'status': status}


def get_production_dashboard():
    """Overview data for the Production section: output this month vs target, batch count,
    open work orders (with feasibility), and material fuel-gauge data (stock vs full/reorder)."""
    from modules.inventory import get_stock_map
    t = today()
    month_start = t[:8] + '01'

    made = int((qry1("SELECT COALESCE(SUM(qty_units),0) v FROM production_batches WHERE batch_date>=?",
                     (month_start,)) or {}).get('v') or 0)
    batches_month = int((qry1("SELECT COUNT(*) v FROM production_batches WHERE batch_date>=?",
                              (month_start,)) or {}).get('v') or 0)
    last_batch = (qry1("SELECT MAX(batch_date) v FROM production_batches") or {}).get('v')

    tv = qry1("SELECT value FROM costing_config WHERE key='normal_monthly_volume'")
    try:
        target = int(float(tv['value'])) if tv and tv.get('value') else 2000
    except (TypeError, ValueError):
        target = 2000

    open_rows = qry("""
        SELECT wo.wo_number, wo.qty_units, COALESCE(wo.produced_units,0) AS produced,
               wo.feasibility_ok, p.name AS product_name, ps.label AS pack_size
        FROM work_orders wo
        JOIN product_variants pv ON pv.id = wo.product_variant_id
        JOIN products p ON p.id = pv.product_id
        LEFT JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE wo.status IN ('planned','in_progress')
        ORDER BY wo.target_date ASC, wo.created_at ASC
    """) or []
    open_wos, packs_to_make = [], 0
    for w in open_rows:
        rem = max(0, int(w['qty_units']) - int(w['produced'] or 0))
        packs_to_make += rem
        open_wos.append({'woNumber': w['wo_number'], 'product': w['product_name'],
                         'packSize': w['pack_size'], 'remaining': rem,
                         'canMake': bool(w['feasibility_ok'])})

    stock = get_stock_map()
    tanks, below = [], 0
    for i in qry("""SELECT id, code, name, COALESCE(reorder_level,0) AS reorder_level,
                           COALESCE(target_grams,0) AS target_grams
                    FROM ingredients WHERE active=1 ORDER BY code"""):
        bal_g = stock.get(i['id'], 0)
        ro_g  = float(i['reorder_level'] or 0)     # reorder_level stored in grams
        tg_g  = float(i['target_grams'] or 0)
        if bal_g <= ro_g:
            status = 'red'; below += 1
        elif tg_g > 0 and bal_g <= tg_g * 0.45:
            status = 'amber'
        elif tg_g == 0 and bal_g <= ro_g * 1.5:
            status = 'amber'
        else:
            status = 'green'
        tanks.append({'code': i['code'], 'name': i['name'],
                      'balanceKg': r2(bal_g / 1000), 'targetKg': r2(tg_g / 1000),
                      'reorderKg': r2(ro_g / 1000), 'status': status})

    return {
        'madeThisMonth': made, 'target': target,
        'batchesThisMonth': batches_month, 'lastBatch': last_batch,
        'openWoCount': len(open_wos), 'packsToMake': packs_to_make,
        'openWos': open_wos, 'tanks': tanks, 'belowReorder': below,
    }


def delete_work_order(wo_id):
    """Hard-delete a work order that has produced NOTHING (planned/in_progress with no batch,
    or cancelled). Refuses if any batch was recorded against it — that would break production
    history, so cancel those instead. Unlinks any plan release so the plan can re-release."""
    wo = qry1("SELECT * FROM work_orders WHERE id=?", (wo_id,))
    if not wo:
        raise ValueError("Work order not found")
    if wo['status'] == 'completed' or wo.get('batch_id') or int(wo.get('produced_units') or 0) > 0:
        raise ValueError("This work order already has production against it — cancel it instead of deleting.")
    try:
        run("DELETE FROM plan_release WHERE work_order_id=?", (wo_id,))
    except Exception:
        pass
    run("DELETE FROM work_orders WHERE id=?", (wo_id,))
    return {'ok': True, 'id': wo_id}


# ─────────────────────────────────────────────────────────────────
#  PRODUCTION BATCH
# ─────────────────────────────────────────────────────────────────

def create_production_batch(data, exclude_wo_id=None):
    """
    Create a production batch using the active BOM.
    data: {productCode, packSize, qtyUnits, batchDate, mfgDate, bestBefore, notes}
    exclude_wo_id: WO being converted to this batch — excluded from reserved stock
                   so the WO's own reservation doesn't block itself.
    Automatically deducts raw material ingredients.
    """
    from modules.inventory import get_stock_map, get_wo_reserved_stock_map

    var = _lookup_variant_by_sku(data.get('productCode', ''), data.get('packSize', ''))
    if not var:
        raise ValueError(
            f"Product variant not found: {data.get('productCode')}/{data.get('packSize')}"
        )

    prod = qry1("SELECT * FROM products WHERE id=?", (var['product_id'],))
    qty_units = int(data.get('qtyUnits', 0))
    if qty_units <= 0:
        raise ValueError("Quantity must be positive")

    pack_grams  = var['pack_grams']
    total_grams = r2(qty_units * pack_grams)

    # Get active BOM version
    bom_ver = qry1("""
        SELECT * FROM bom_versions WHERE product_id=? AND active_flag=1
        ORDER BY version_no DESC LIMIT 1
    """, (var['product_id'],))
    if not bom_ver:
        raise ValueError(
            f"No active BOM found for {var['product_name']}. "
            f"Go to Production → BOM Setup → click the red chip for "
            f"{data.get('productCode', 'this product')} to define ingredients."
        )

    bom_items_list = qry("""
        SELECT bi.*, i.code as ing_code, i.id as ingredient_id
        FROM bom_items bi JOIN ingredients i ON i.id=bi.ingredient_id
        WHERE bi.bom_version_id=?
    """, (bom_ver['id'],))

    # Compute ingredient requirements (BOM is per batch_size_grams of output)
    scale = total_grams / float(bom_ver['batch_size_grams'])
    requirements = []
    for b in bom_items_list:
        needed      = r2(b['quantity_grams'] * scale)
        ing_row     = qry1("SELECT cost_per_kg FROM ingredients WHERE id=?", (b['ingredient_id'],))
        cost_per_kg = r2(ing_row['cost_per_kg']) if ing_row else 0.0
        requirements.append({
            'ingredient_id': b['ingredient_id'],
            'ing_code':      b['ing_code'],
            'needed_grams':  needed,
            'cost_per_kg':   cost_per_kg,
        })

    # Freeze total ingredient cost at posting time
    total_ingredient_cost = r2(sum(
        (req['needed_grams'] / 1000.0) * req['cost_per_kg'] for req in requirements
    ))
    unit_cost_at_posting = r2(total_ingredient_cost / qty_units) if qty_units else 0.0

    # Check stock availability (negative stock prevention)
    stock_map    = get_stock_map()
    reserved_map = get_wo_reserved_stock_map(exclude_wo_id=exclude_wo_id)
    shortfalls   = []
    for req in requirements:
        physical  = stock_map.get(req['ingredient_id'], 0)
        reserved  = reserved_map.get(req['ingredient_id'], 0)
        available = max(0.0, r2(physical - reserved))
        if req['needed_grams'] > available + 0.001:
            shortfalls.append(
                f"{req['ing_code']}: need {req['needed_grams']:.1f}g, "
                f"available {available:.1f}g (physical {physical:.1f}g − reserved {reserved:.1f}g)"
            )
    if shortfalls:
        raise ValueError("Insufficient stock:\n" + "\n".join(shortfalls))

    _sync_counter_to_max('batch', 'production_batches', 'batch_id', 'SP-BATCH-')
    batch_id   = next_id('batch', 'BATCH')
    batch_date = data.get('batchDate', today())

    c = _conn()
    try:
        c.execute("""
            INSERT INTO production_batches
                (batch_id, batch_date, product_id, product_variant_id, bom_version_id,
                 qty_grams, qty_units, pack_size, mfg_date, best_before, notes,
                 unit_cost_at_posting)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (batch_id, batch_date, var['product_id'], var['id'], bom_ver['id'],
              total_grams, qty_units, var['pack_size'],
              data.get('mfgDate', ''), data.get('bestBefore', ''), data.get('notes', ''),
              unit_cost_at_posting))
        batch_db_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        for req in requirements:
            c.execute("""
                INSERT INTO production_consumption (batch_id, ingredient_id, qty_grams)
                VALUES (?,?,?)
            """, (batch_db_id, req['ingredient_id'], req['needed_grams']))

            c.execute("""
                INSERT INTO inventory_ledger
                    (ingredient_id, movement_type, qty_grams, reference_id, notes)
                VALUES (?,?,?,?,?)
            """, (req['ingredient_id'], 'PRODUCTION_USE',
                  -req['needed_grams'], batch_id,
                  f"Production batch {batch_id}"))

        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('production_batches',?,'INSERT',?)
        """, (batch_id, json.dumps({
              'product':     data.get('productCode'),
              'qty_units':   qty_units,
              'pack_size':   var['pack_size'],
              'total_grams': total_grams,
        })))

        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()

    return {
        'batchId':             batch_id,
        'totalGrams':          total_grams,
        'qtyUnits':            qty_units,
        'ingredients':         requirements,
        'bomVersion':          bom_ver['version_no'],
        'totalIngredientCost': total_ingredient_cost,
        'unitCostAtPosting':   unit_cost_at_posting,   # frozen — never changes after posting
    }


# ─────────────────────────────────────────────────────────────────
#  STAGED BATCH RUNS  (granular, stage-by-stage execution)
# ─────────────────────────────────────────────────────────────────
#
#  A "batch run" is the making of (part of) a work order, tracked as a process:
#     Received → Cleaning → Roasting → Cooling → Blending → Packaging → Done
#  Raw material is consumed when the run STARTS (the floor has physically taken it).
#  The run is advanced stage-by-stage (timestamped). On owner VERIFICATION at the
#  final stage it becomes a finished-goods production_batch at the ACTUAL yield —
#  which is what surfaces wastage (planned 500, verified 485).
#
#  production_batches stays exactly "verified finished goods" — an unverified run
#  never counts as FG, so stock/dashboard/reports accounting is undisturbed.

def list_batch_stages():
    """Ordered active process stages (configurable via the batch_stage_defs table)."""
    return qry("SELECT id, name, sort_order FROM batch_stage_defs "
               "WHERE active=1 ORDER BY sort_order, id") or []


def _bom_requirements(variant_id, qty_units, bom_version_id=None):
    """BOM ingredient requirements (grams) + frozen cost for qty_units of a variant.
    bom_version_id: pin to a specific BOM version (used at verify so consumption matches
    exactly what was taken at start, even if the active BOM changed since).
    Returns (var, bom_ver, requirements[], total_grams, total_ingredient_cost, unit_cost)."""
    var = _lookup_variant_by_id(variant_id)
    if not var:
        raise ValueError("Product variant not found")
    qty_units = int(qty_units)
    if qty_units <= 0:
        raise ValueError("Quantity must be positive")
    total_grams = r2(qty_units * var['pack_grams'])
    if bom_version_id:
        bom_ver = qry1("SELECT * FROM bom_versions WHERE id=?", (bom_version_id,))
    else:
        bom_ver = qry1("""SELECT * FROM bom_versions WHERE product_id=? AND active_flag=1
                          ORDER BY version_no DESC LIMIT 1""", (var['product_id'],))
    if not bom_ver:
        raise ValueError(f"No active BOM found for {var['product_name']}.")
    items = qry("""SELECT bi.quantity_grams, i.code as ing_code, i.id as ingredient_id
                   FROM bom_items bi JOIN ingredients i ON i.id=bi.ingredient_id
                   WHERE bi.bom_version_id=?""", (bom_ver['id'],))
    scale = total_grams / float(bom_ver['batch_size_grams']) if bom_ver['batch_size_grams'] else 0
    reqs = []
    for b in items:
        needed  = r2(b['quantity_grams'] * scale)
        ing_row = qry1("SELECT cost_per_kg FROM ingredients WHERE id=?", (b['ingredient_id'],))
        cpk     = r2(ing_row['cost_per_kg']) if ing_row else 0.0
        reqs.append({'ingredient_id': b['ingredient_id'], 'ing_code': b['ing_code'],
                     'needed_grams': needed, 'cost_per_kg': cpk})
    total_cost = r2(sum((rq['needed_grams'] / 1000.0) * rq['cost_per_kg'] for rq in reqs))
    unit_cost  = r2(total_cost / qty_units) if qty_units else 0.0
    return var, bom_ver, reqs, total_grams, total_cost, unit_cost


def start_batch_run(wo_id, qty=None, user=None):
    """Begin a staged batch run from a work order. Consumes raw material NOW, sets the run at
    the first stage, logs the first event, and moves the WO to in_progress.
    qty=None → the WO's full remaining quantity; qty=N → a partial (one weekly run).
    Raises with a clear shortfall message if stock is insufficient (run NOT created)."""
    from modules.inventory import get_stock_map, get_wo_reserved_stock_map

    wo = qry1("SELECT * FROM work_orders WHERE id=?", (wo_id,))
    if not wo:
        raise ValueError("Work order not found")
    if wo['status'] not in ('planned', 'in_progress'):
        raise ValueError(f"Work order is {wo['status']} — cannot start a batch")

    active = qry1("""SELECT id, run_code FROM batch_runs
                     WHERE wo_id=? AND status IN ('in_progress','awaiting_verification')""", (wo_id,))
    if active:
        raise ValueError(f"This work order already has an active batch run ({active['run_code']}). "
                         "Finish or cancel it first.")

    target    = int(wo['qty_units'])
    produced  = int(wo.get('produced_units') or 0)
    remaining = target - produced
    if remaining <= 0:
        raise ValueError("Work order is already fully produced")
    make_qty = remaining if qty is None else int(qty)
    if make_qty <= 0:
        raise ValueError("Quantity must be positive")
    if make_qty > remaining:
        make_qty = remaining

    var, bom_ver, reqs, total_grams, total_cost, unit_cost = _bom_requirements(
        wo['product_variant_id'], make_qty)

    # stock guard — exclude this WO's own reservation (else it blocks itself)
    stock_map    = get_stock_map()
    reserved_map = get_wo_reserved_stock_map(exclude_wo_id=wo_id)
    shortfalls = []
    for req in reqs:
        physical  = stock_map.get(req['ingredient_id'], 0)
        reserved  = reserved_map.get(req['ingredient_id'], 0)
        available = max(0.0, r2(physical - reserved))
        if req['needed_grams'] > available + 0.001:
            shortfalls.append(f"{req['ing_code']}: need {req['needed_grams']:.1f}g, "
                              f"available {available:.1f}g")
    if shortfalls:
        raise ValueError("Insufficient stock:\n" + "\n".join(shortfalls))

    stages = list_batch_stages()
    if not stages:
        raise ValueError("No batch stages configured")
    first_stage = stages[0]

    _sync_counter_to_max('batchrun', 'batch_runs', 'run_code', 'SP-RUN-')
    run_code = next_id('batchrun', 'RUN')

    c = _conn()
    try:
        c.execute("""INSERT INTO batch_runs
            (run_code, wo_id, product_id, product_variant_id, bom_version_id, pack_size,
             qty_units, qty_grams, planned_ingredient_cost, planned_unit_cost,
             current_stage_id, status, started_by, notes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,'in_progress',?,?)""",
            (run_code, wo_id, var['product_id'], var['id'], bom_ver['id'], var['pack_size'],
             make_qty, total_grams, total_cost, unit_cost, first_stage['id'],
             user or '', f"From Work Order {wo['wo_number']}"))
        run_db_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        for req in reqs:   # consume RM now (the floor has taken it)
            c.execute("""INSERT INTO inventory_ledger
                         (ingredient_id, movement_type, qty_grams, reference_id, notes)
                         VALUES (?, 'PRODUCTION_USE', ?, ?, ?)""",
                      (req['ingredient_id'], -req['needed_grams'], run_code,
                       f"Batch run {run_code}"))

        c.execute("""INSERT INTO batch_run_events (run_id, stage_id, stage_name, event, by_user)
                     VALUES (?,?,?, 'entered', ?)""",
                  (run_db_id, first_stage['id'], first_stage['name'], user or ''))
        c.execute("UPDATE work_orders SET status='in_progress', updated_at=datetime('now') WHERE id=?",
                  (wo_id,))
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return {'runId': run_db_id, 'runCode': run_code, 'woNumber': wo['wo_number'],
            'qtyUnits': make_qty, 'stage': first_stage['name'], 'status': 'in_progress'}


def advance_batch_run(run_id, note=None, user=None):
    """Complete the current stage and move to the next, with a timestamped event. Advancing
    INTO the final stage flips the run to 'awaiting_verification' (owner confirms yield to finish)."""
    r = qry1("SELECT * FROM batch_runs WHERE id=?", (run_id,))
    if not r:
        raise ValueError("Batch run not found")
    if r['status'] != 'in_progress':
        raise ValueError(f"Batch run is {r['status']} — cannot advance")
    stages = list_batch_stages()
    ids = [s['id'] for s in stages]
    idx = ids.index(r['current_stage_id']) if r['current_stage_id'] in ids else 0
    if idx >= len(stages) - 1:
        raise ValueError("Batch is at the final stage — verify it to finish.")
    cur = stages[idx]
    nxt = stages[idx + 1]
    new_status = 'awaiting_verification' if (idx + 1) == len(stages) - 1 else 'in_progress'
    c = _conn()
    try:
        c.execute("""INSERT INTO batch_run_events (run_id, stage_id, stage_name, event, note, by_user)
                     VALUES (?,?,?, 'completed', ?, ?)""",
                  (run_id, cur['id'], cur['name'], note or '', user or ''))
        c.execute("""INSERT INTO batch_run_events (run_id, stage_id, stage_name, event, by_user)
                     VALUES (?,?,?, 'entered', ?)""",
                  (run_id, nxt['id'], nxt['name'], user or ''))
        c.execute("UPDATE batch_runs SET current_stage_id=?, status=? WHERE id=?",
                  (nxt['id'], new_status, run_id))
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return {'runCode': r['run_code'], 'stage': nxt['name'], 'status': new_status,
            'stageNum': idx + 2, 'stageTotal': len(stages)}


def verify_batch_run(run_id, actual_qty=None, user=None):
    """Owner confirms the run → finalises it into a finished-goods production_batch at the ACTUAL
    yield (defaults to planned qty). RM was already deducted at start, so this only ADDS finished
    goods; it records consumption (for reporting) from the run's frozen BOM version, links the
    batch, marks the run completed, and advances/completes the work order."""
    r = qry1("SELECT * FROM batch_runs WHERE id=?", (run_id,))
    if not r:
        raise ValueError("Batch run not found")
    if r['status'] not in ('awaiting_verification', 'in_progress'):
        raise ValueError(f"Batch run is {r['status']} — cannot verify")
    make_qty = int(actual_qty) if actual_qty not in (None, '') else int(r['qty_units'])
    if make_qty <= 0:
        raise ValueError("Actual quantity must be positive")

    var = _lookup_variant_by_id(r['product_variant_id'])
    if not var:
        raise ValueError("Product variant not found")
    actual_grams = r2(make_qty * var['pack_grams'])

    # consumption from the run's FROZEN bom version + PLANNED qty (matches RM taken at start)
    bom_ver = qry1("SELECT batch_size_grams FROM bom_versions WHERE id=?", (r['bom_version_id'],))
    items   = qry("SELECT quantity_grams, ingredient_id FROM bom_items WHERE bom_version_id=?",
                  (r['bom_version_id'],)) or []
    planned_grams = r2(int(r['qty_units']) * var['pack_grams'])
    scale = planned_grams / float(bom_ver['batch_size_grams']) if bom_ver and bom_ver['batch_size_grams'] else 0
    consumption = [(b['ingredient_id'], r2(b['quantity_grams'] * scale)) for b in items]

    _sync_counter_to_max('batch', 'production_batches', 'batch_id', 'SP-BATCH-')
    batch_id = next_id('batch', 'BATCH')

    c = _conn()
    try:
        c.execute("""INSERT INTO production_batches
            (batch_id, batch_date, product_id, product_variant_id, bom_version_id,
             qty_grams, qty_units, pack_size, mfg_date, best_before, notes, unit_cost_at_posting)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (batch_id, today(), r['product_id'], r['product_variant_id'], r['bom_version_id'],
             actual_grams, make_qty, r['pack_size'], today(), '',
             f"Batch run {r['run_code']}", r2(r['planned_unit_cost'] or 0)))
        batch_db_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        for ing_id, g in consumption:   # reporting only — RM ledger deducted at start
            c.execute("INSERT INTO production_consumption (batch_id, ingredient_id, qty_grams) VALUES (?,?,?)",
                      (batch_db_id, ing_id, g))

        c.execute("""UPDATE batch_runs SET status='completed', actual_qty_units=?, batch_id=?,
                     verified_by=?, finished_at=datetime('now') WHERE id=?""",
                  (make_qty, batch_id, user or '', run_id))

        wo = c.execute("SELECT wo_number, qty_units, COALESCE(produced_units,0) FROM work_orders WHERE id=?",
                       (r['wo_id'],)).fetchone() if r['wo_id'] else None
        wo_status = None
        if wo:
            # Advance the WO by the run's PLANNED quantity (the RM was committed for it),
            # not the actual yield — otherwise a wastage shortfall (made 485 of 500) would
            # leave a phantom 15-unit remainder and prompt another run buying more RM.
            # The actual yield is captured on the production_batch (finished goods) above.
            target = int(wo[1]); produced = int(wo[2]) + int(r['qty_units'])
            wo_status = 'completed' if produced >= target else 'in_progress'
            c.execute("UPDATE work_orders SET status=?, produced_units=?, batch_id=?, updated_at=datetime('now') WHERE id=?",
                      (wo_status, produced, batch_id, r['wo_id']))

        c.execute("""INSERT INTO batch_run_events (run_id, stage_id, stage_name, event, note, by_user)
                     VALUES (?,?,?, 'verified', ?, ?)""",
                  (run_id, r['current_stage_id'], 'Done', f"Verified {make_qty} packs", user or ''))
        c.execute("""INSERT INTO change_log (table_name, record_id, action, new_value)
                     VALUES ('production_batches',?,'INSERT',?)""",
                  (batch_id, json.dumps({'from_run': r['run_code'], 'qty_units': make_qty,
                                         'pack_size': r['pack_size'], 'total_grams': actual_grams})))
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return {'batchId': batch_id, 'runCode': r['run_code'], 'qtyUnits': make_qty,
            'plannedQty': int(r['qty_units']), 'yieldLoss': int(r['qty_units']) - make_qty,
            'woStatus': wo_status}


def cancel_batch_run(run_id, reason=None, user=None):
    """Cancel an unfinished run and RESTORE the raw material consumed at start (positive
    ADJUSTMENT movements). The work order stays open so it can be re-run; produced_units is
    untouched (nothing was verified)."""
    r = qry1("SELECT * FROM batch_runs WHERE id=?", (run_id,))
    if not r:
        raise ValueError("Batch run not found")
    if r['status'] not in ('in_progress', 'awaiting_verification'):
        raise ValueError(f"Batch run is {r['status']} — cannot cancel")
    consumed = qry("""SELECT ingredient_id, SUM(qty_grams) AS g FROM inventory_ledger
                      WHERE reference_id=? AND movement_type='PRODUCTION_USE'
                      GROUP BY ingredient_id""", (r['run_code'],)) or []
    c = _conn()
    try:
        for row in consumed:
            g = r2(row['g'] or 0)   # negative — add back the opposite
            if g != 0:
                c.execute("""INSERT INTO inventory_ledger
                             (ingredient_id, movement_type, qty_grams, reference_id, notes)
                             VALUES (?, 'ADJUSTMENT', ?, ?, ?)""",
                          (row['ingredient_id'], -g, r['run_code'],
                           f"Batch run {r['run_code']} cancelled — RM restored"))
        c.execute("UPDATE batch_runs SET status='cancelled', finished_at=datetime('now') WHERE id=?",
                  (run_id,))
        c.execute("""INSERT INTO batch_run_events (run_id, stage_id, stage_name, event, note, by_user)
                     VALUES (?,?,?, 'cancelled', ?, ?)""",
                  (run_id, r['current_stage_id'], '', reason or '', user or ''))
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return {'runCode': r['run_code'], 'status': 'cancelled', 'restored': len(consumed)}


def list_batch_runs(status=None):
    """Batch runs for the Production overview / Batch Runner. status='active' → in-progress +
    awaiting-verification; otherwise filter by the exact status, or all."""
    where, params = "", ()
    if status == 'active':
        where = "WHERE br.status IN ('in_progress','awaiting_verification')"
    elif status:
        where = "WHERE br.status=?"; params = (status,)
    rows = qry(f"""
        SELECT br.id, br.run_code, br.wo_id, br.qty_units, br.actual_qty_units, br.status,
               br.batch_id, br.started_at, br.finished_at, br.current_stage_id,
               p.name AS product_name, br.pack_size, sd.name AS stage_name, wo.wo_number
        FROM batch_runs br
        LEFT JOIN products p ON p.id = br.product_id
        LEFT JOIN batch_stage_defs sd ON sd.id = br.current_stage_id
        LEFT JOIN work_orders wo ON wo.id = br.wo_id
        {where}
        ORDER BY (br.status='completed'), br.started_at DESC
    """, params) or []
    stages = list_batch_stages()
    total = len(stages)
    out = []
    for r in rows:
        d = dict(r)
        d['stageNum'] = next((i + 1 for i, s in enumerate(stages) if s['id'] == r['current_stage_id']), 0)
        d['stageTotal'] = total
        out.append(d)
    return out


def get_batch_run(run_id):
    """One run with its stage timeline + event log. Exposes pack counts only — no ingredient
    quantities (the recipe secret stays out of this view)."""
    r = qry1("""SELECT br.*, p.name AS product_name, wo.wo_number
                FROM batch_runs br
                LEFT JOIN products p ON p.id=br.product_id
                LEFT JOIN work_orders wo ON wo.id=br.wo_id
                WHERE br.id=?""", (run_id,))
    if not r:
        raise ValueError("Batch run not found")
    d = dict(r)
    stages = list_batch_stages()
    events = qry("""SELECT stage_id, stage_name, event, note, at, by_user
                    FROM batch_run_events WHERE run_id=? ORDER BY id""", (run_id,)) or []
    cur_pos = next((i for i, s in enumerate(stages) if s['id'] == r['current_stage_id']), -1)
    completed = (r['status'] == 'completed')
    timeline = []
    for i, s in enumerate(stages):
        state = 'done' if (completed or i < cur_pos) else ('now' if i == cur_pos else 'todo')
        ev = next((e for e in events if e['stage_id'] == s['id']), None)
        timeline.append({'id': s['id'], 'name': s['name'], 'state': state,
                         'at': ev['at'] if ev else None})
    d['stages'] = timeline
    d['events'] = [dict(e) for e in events]
    d['stageNum'] = cur_pos + 1 if cur_pos >= 0 else 0
    d['stageTotal'] = len(stages)
    return d


# ─────────────────────────────────────────────────────────────────
#  BOM MANAGEMENT
# ─────────────────────────────────────────────────────────────────

def create_or_update_bom(data):
    """
    Create or replace the active BOM for a product.
    data: {
      productCode: str,
      batchSizeGrams: float,
      effectiveFrom: 'YYYY-MM-DD',
      items: [{ ingCode: str, quantityGrams: float }]
    }
    Deactivates any existing active BOM for the product, then inserts a new
    bom_version (version_no = prev_max + 1) and its bom_items.
    """
    prod = qry1("SELECT id, code, name FROM products WHERE code=?",
                (data.get('productCode', '').upper().strip(),))
    if not prod:
        raise ValueError(f"Product not found: {data.get('productCode')}")

    items = data.get('items', [])
    if not items:
        raise ValueError("BOM must have at least one ingredient")

    batch_size = float(data.get('batchSizeGrams', 1000) or 1000)
    if batch_size <= 0:
        raise ValueError("batchSizeGrams must be positive")

    eff_from = data.get('effectiveFrom') or today()

    # Resolve ingredient codes → ids
    resolved_items = []
    for it in items:
        ing_code = str(it.get('ingCode', '')).strip()
        qty_g    = float(it.get('quantityGrams', 0))
        if not ing_code:
            raise ValueError("Each item must have ingCode")
        if qty_g <= 0:
            raise ValueError(f"quantityGrams must be positive for {ing_code}")
        ing = qry1("SELECT id, code FROM ingredients WHERE code=? AND COALESCE(active,1)=1", (ing_code,))
        if not ing:
            raise ValueError(f"Ingredient not found or inactive: {ing_code}")
        resolved_items.append({'ing_id': ing['id'], 'qty_g': qty_g})

    c = _conn()
    try:
        c.execute("UPDATE bom_versions SET active_flag=0 WHERE product_id=? AND active_flag=1",
                  (prod['id'],))

        row = c.execute("SELECT MAX(version_no) FROM bom_versions WHERE product_id=?",
                        (prod['id'],)).fetchone()
        next_ver = (row[0] or 0) + 1

        c.execute("""
            INSERT INTO bom_versions (product_id, version_no, batch_size_grams, effective_from, active_flag, notes)
            VALUES (?,?,?,?,1,?)
        """, (prod['id'], next_ver, batch_size, eff_from, data.get('notes', '')))
        bom_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        for it in resolved_items:
            c.execute("""
                INSERT INTO bom_items (bom_version_id, ingredient_id, quantity_grams)
                VALUES (?,?,?)
            """, (bom_id, it['ing_id'], it['qty_g']))

        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()

    bv = qry1("SELECT * FROM bom_versions WHERE id=?", (bom_id,))
    bi = qry("""
        SELECT bi.*, i.code as ing_code, i.name as ing_name
        FROM bom_items bi JOIN ingredients i ON i.id=bi.ingredient_id
        WHERE bi.bom_version_id=?
    """, (bom_id,))
    return {**bv, 'productCode': prod['code'], 'productName': prod['name'], 'items': bi}


# ─────────────────────────────────────────────────────────────────
#  BOM MASTER IMPORT
# ─────────────────────────────────────────────────────────────────

def import_bom_master(rows):
    """
    Upload BOMs from a CSV/XLSX file.

    Required columns: product_code, ing_code, quantity_grams
    Optional columns: batch_size_grams (default 1000), notes, effective_from

    One row per ingredient per product. Multiple rows with the same product_code
    are grouped into a single BOM. Any existing active BOM for that product is
    replaced with a new version.

    Example:
        product_code | batch_size_grams | ing_code | quantity_grams
        SPCM         | 1000             | ING-001  | 300
        SPCM         | 1000             | ING-002  | 700
        SPGM         | 1000             | ING-003  | 400
    """
    errors   = []
    imported = 0
    skipped  = 0

    def _col(row, *names):
        """Case-insensitive column lookup across aliased CSV/XLSX headers."""
        for n in names:
            for k, v in row.items():
                if k.strip().lower() == n.lower():
                    return str(v).strip()
        return ''

    # Group rows by product_code
    groups = defaultdict(list)
    for i, row in enumerate(rows, 1):
        pcode = _col(row, 'product_code').upper()
        if not pcode:
            errors.append(f"Row {i}: product_code is required")
            skipped += 1
            continue
        groups[pcode].append((i, row))

    for pcode, group_rows in groups.items():
        # Validate product exists — try exact match first, then fuzzy on code/name
        prod = qry1("SELECT id, code, name FROM products WHERE UPPER(code)=?", (pcode.upper(),))
        if not prod:
            # Fuzzy fallback: pcode contains GM → Garam Masala, CM → Chaat Masala
            if 'GM' in pcode.upper():
                prod = qry1("""SELECT id, code, name FROM products WHERE active=1
                               AND (UPPER(code) LIKE '%GM%' OR LOWER(name) LIKE '%garam%')
                               ORDER BY id LIMIT 1""")
            elif 'CM' in pcode.upper():
                prod = qry1("""SELECT id, code, name FROM products WHERE active=1
                               AND (UPPER(code) LIKE '%CM%' OR LOWER(name) LIKE '%chaat%')
                               ORDER BY id LIMIT 1""")
        if not prod:
            for i, _ in group_rows:
                errors.append(f"Row {i}: product '{pcode}' not found")
            skipped += len(group_rows)
            continue

        # Extract BOM-level fields from first row
        first_i, first_row = group_rows[0]
        batch_size_str = _col(first_row, 'batch_size_grams')
        try:
            batch_size = float(batch_size_str) if batch_size_str else 1000.0
        except ValueError:
            errors.append(f"Row {first_i}: invalid batch_size_grams '{batch_size_str}'")
            skipped += len(group_rows)
            continue

        eff_from = _col(first_row, 'effective_from') or today()
        notes    = _col(first_row, 'notes')

        # Validate and resolve all items in this group
        resolved = []
        group_ok = True
        for i, row in group_rows:
            ing_code = _col(row, 'ing_code').upper()
            qty_str  = _col(row, 'quantity_grams')
            if not ing_code:
                errors.append(f"Row {i}: ing_code is required")
                group_ok = False
                continue
            try:
                qty_g = float(qty_str)
            except ValueError:
                errors.append(f"Row {i}: invalid quantity_grams '{qty_str}'")
                group_ok = False
                continue
            if qty_g <= 0:
                errors.append(f"Row {i}: quantity_grams must be positive")
                group_ok = False
                continue
            ing = qry1("SELECT id FROM ingredients WHERE code=? AND COALESCE(active,1)=1", (ing_code,))
            if not ing:
                errors.append(f"Row {i}: ingredient '{ing_code}' not found or inactive")
                group_ok = False
                continue
            resolved.append({'ing_id': ing['id'], 'qty_g': qty_g})

        if not group_ok:
            skipped += len(group_rows)
            continue

        # Write BOM
        try:
            c = _conn()
            try:
                c.execute(
                    "UPDATE bom_versions SET active_flag=0 WHERE product_id=? AND active_flag=1",
                    (prod['id'],)
                )
                row_ver = c.execute(
                    "SELECT MAX(version_no) FROM bom_versions WHERE product_id=?",
                    (prod['id'],)
                ).fetchone()
                next_ver = (row_ver[0] or 0) + 1
                c.execute("""
                    INSERT INTO bom_versions
                        (product_id, version_no, batch_size_grams, effective_from, active_flag, notes)
                    VALUES (?,?,?,?,1,?)
                """, (prod['id'], next_ver, batch_size, eff_from, notes))
                bom_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
                for it in resolved:
                    c.execute("""
                        INSERT INTO bom_items (bom_version_id, ingredient_id, quantity_grams)
                        VALUES (?,?,?)
                    """, (bom_id, it['ing_id'], it['qty_g']))
                c.commit()
            except Exception:
                c.rollback()
                raise
            finally:
                c.close()
            imported += 1
        except Exception as e:
            errors.append(f"Product {pcode}: {e}")
            skipped += len(group_rows)

    save_db()
    return {'imported': imported, 'skipped': skipped, 'errors': errors}
