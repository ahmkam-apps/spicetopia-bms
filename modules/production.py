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
    'create_production_batch',
    'create_or_update_bom',
    'import_bom_master',
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


def convert_wo_to_batch(wo_id):
    """Convert a planned work order into a production batch."""
    wo = qry1("SELECT * FROM work_orders WHERE id=?", (wo_id,))
    if not wo:
        raise ValueError("Work order not found")
    if wo['status'] not in ('planned', 'in_progress'):
        raise ValueError(f"Work order is {wo['status']} — cannot convert")

    var = _lookup_variant_by_id(wo['product_variant_id'])
    if not var:
        raise ValueError("Product variant not found")

    prod = qry1("SELECT * FROM products WHERE id=?", (var['product_id'],))

    # Re-use create_production_batch; pass wo_id so the WO's own reservation is
    # excluded from the availability check (prevents self-blocking).
    result = create_production_batch({
        'productCode': prod['code'],
        'packSize':    var['pack_size'],
        'qtyUnits':    wo['qty_units'],
        'batchDate':   today(),
        'mfgDate':     today(),
        'bestBefore':  '',
        'notes':       f"From Work Order {wo['wo_number']}",
    }, exclude_wo_id=wo_id)

    # Mark work order as completed
    run("UPDATE work_orders SET status='completed', batch_id=?, updated_at=datetime('now') WHERE id=?",
        (result['batchId'], wo_id))
    result['woNumber'] = wo['wo_number']
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
    allowed = ('planned', 'in_progress', 'cancelled')
    if status not in allowed:
        raise ValueError(f"Invalid status. Must be one of: {', '.join(allowed)}")
    wo = qry1("SELECT id FROM work_orders WHERE id=?", (wo_id,))
    if not wo:
        raise ValueError("Work order not found")
    run("UPDATE work_orders SET status=?, updated_at=datetime('now') WHERE id=?", (status, wo_id))
    return {'id': wo_id, 'status': status}


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
        # Validate product exists
        prod = qry1("SELECT id, code, name FROM products WHERE code=?", (pcode,))
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
