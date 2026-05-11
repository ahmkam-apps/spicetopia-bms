"""
modules/inventory.py — Raw material stock, finished goods stock, ingredient CRUD,
                        inventory adjustments, and bulk ingredient imports.

Extracted from server.py (Sprint 7). Overrides originals via bottom-import pattern:
    from modules.inventory import *   # just before __main__ in server.py

_refresh_ref is a no-op until wired at startup:
    import modules.inventory as _inv_mod
    _inv_mod._refresh_ref = load_ref
"""

from modules.db    import _conn, qry, qry1, run_many, save_db, audit_log
from modules.utils import r2, today

__all__ = [
    # Raw material stock
    'get_stock_map',
    'get_wo_reserved_stock_map',
    # Finished goods stock
    'get_finished_stock_map',
    'get_soft_hold_qty',
    'get_hard_reserved_qty',
    'get_available_for_soft_hold',
    'get_stock_situation',
    # Inventory adjustment
    'create_adjustment',
    # Ingredient CRUD
    'create_ingredient',
    'update_ingredient',
    'bulk_update_ingredient_costs',
    'deactivate_ingredient',
    'reactivate_ingredient',
    # Bulk import
    'import_ingredients_master',
]

# ── Callback wired at startup by server.py ────────────────────────────────────
# import modules.inventory as _inv_mod; _inv_mod._refresh_ref = load_ref
_refresh_ref = lambda: None


# ═══════════════════════════════════════════════════════════════════
#  RAW MATERIAL STOCK
# ═══════════════════════════════════════════════════════════════════

def get_stock_map():
    """Return {ingredient_id: current_balance_grams}."""
    rows = qry("""
        SELECT ingredient_id, SUM(qty_grams) as balance
        FROM inventory_ledger GROUP BY ingredient_id
    """)
    return {r['ingredient_id']: r2(r['balance']) for r in rows}


def get_wo_reserved_stock_map(exclude_wo_id=None):
    """
    Return {ingredient_id: grams_reserved} for all planned/in_progress work orders.

    Uses a single CTE JOIN to compute ingredient requirements via the active BOM
    for each open WO without N+1 queries.

    exclude_wo_id: exclude a specific WO from the reservation total — used when
    checking feasibility for that WO itself (it shouldn't reserve against itself).
    """
    exclude_clause = f"AND wo.id != {int(exclude_wo_id)}" if exclude_wo_id else ""
    rows = qry(f"""
        WITH active_boms AS (
            SELECT product_id, MAX(id) AS bom_id
            FROM bom_versions
            WHERE active_flag = 1
            GROUP BY product_id
        )
        SELECT bi.ingredient_id,
               ROUND(SUM(
                   bi.quantity_grams
                   * (wo.qty_units * COALESCE(ps.grams, 0))
                   / NULLIF(bv.batch_size_grams, 0)
               ), 2) AS reserved_grams
        FROM work_orders wo
        JOIN product_variants pv  ON pv.id  = wo.product_variant_id
        LEFT JOIN pack_sizes ps   ON ps.id  = pv.pack_size_id
        JOIN active_boms ab       ON ab.product_id = pv.product_id
        JOIN bom_versions bv      ON bv.id  = ab.bom_id
        JOIN bom_items bi         ON bi.bom_version_id = bv.id
        WHERE wo.status IN ('planned', 'in_progress')
          {exclude_clause}
        GROUP BY bi.ingredient_id
    """)
    return {r['ingredient_id']: r2(r['reserved_grams'] or 0) for r in rows}


# ═══════════════════════════════════════════════════════════════════
#  FINISHED GOODS STOCK
# ═══════════════════════════════════════════════════════════════════

def get_finished_stock_map():
    """Return {product_variant_id: qty_units_available}.
    Voided sales (voided=1) are excluded so that voiding an invoice restores
    finished-goods stock without physically recreating a production batch.
    """
    produced = qry("""
        SELECT product_variant_id, SUM(qty_units) as units
        FROM production_batches WHERE product_variant_id IS NOT NULL
        GROUP BY product_variant_id
    """)
    sold = qry("""
        SELECT product_variant_id, SUM(qty) as units
        FROM sales
        WHERE product_variant_id IS NOT NULL
          AND (voided IS NULL OR voided = 0)
        GROUP BY product_variant_id
    """)
    prod_map = {r['product_variant_id']: r2(r['units']) for r in produced}
    sold_map = {r['product_variant_id']: r2(r['units']) for r in sold}
    all_ids  = set(list(prod_map.keys()) + list(sold_map.keys()))
    return {vid: r2(prod_map.get(vid, 0) - sold_map.get(vid, 0)) for vid in all_ids}


def get_soft_hold_qty(product_variant_id):
    """
    Total qty soft-held by ALL pending_review orders for a given variant.
    This stock is reserved but not confirmed — lower priority than hard reservations.
    """
    row = qry1("""
        SELECT COALESCE(SUM(coi.qty_soft_hold), 0) as held
        FROM customer_order_items coi
        JOIN customer_orders co ON co.id = coi.order_id
        WHERE co.status = 'pending_review'
          AND coi.product_variant_id = ?
    """, (product_variant_id,))
    return r2(row['held']) if row else 0.0


def get_hard_reserved_qty(product_variant_id):
    """
    Total qty committed to confirmed/invoiced orders (hard reservations).
    = qty_ordered for all items on confirmed, partially_invoiced orders
      minus units already invoiced (which have already left stock).
    """
    row = qry1("""
        SELECT COALESCE(SUM(coi.qty_ordered - coi.qty_invoiced), 0) as reserved
        FROM customer_order_items coi
        JOIN customer_orders co ON co.id = coi.order_id
        WHERE co.status IN ('confirmed', 'partially_invoiced')
          AND coi.product_variant_id = ?
    """, (product_variant_id,))
    return max(0.0, r2(row['reserved'])) if row else 0.0


def get_available_for_soft_hold(product_variant_id):
    """
    Stock available for a NEW soft hold placement.
    = physical stock − hard reservations − existing soft holds
    Returns max(0, result) — never negative.
    """
    physical      = get_finished_stock_map().get(product_variant_id, 0.0)
    hard_reserved = get_hard_reserved_qty(product_variant_id)
    soft_held     = get_soft_hold_qty(product_variant_id)
    return max(0.0, r2(physical - hard_reserved - soft_held))


def get_stock_situation(product_variant_id):
    """
    Full stock breakdown for a variant — used in the review queue to give
    admin context before approving/rejecting an order.
    Returns physical, hard_reserved, soft_held, available_for_hold, and
    active production batch info if stock is short.
    """
    physical      = get_finished_stock_map().get(product_variant_id, 0.0)
    hard_reserved = get_hard_reserved_qty(product_variant_id)
    soft_held     = get_soft_hold_qty(product_variant_id)
    available     = max(0.0, r2(physical - hard_reserved - soft_held))

    # Active production batch for this variant (latest non-completed WO)
    active_batch = qry1("""
        SELECT wo.wo_number, wo.qty_units, wo.target_date, wo.status
        FROM work_orders wo
        WHERE wo.product_variant_id = ?
          AND wo.status IN ('planned', 'in_progress')
        ORDER BY wo.target_date ASC LIMIT 1
    """, (product_variant_id,))

    return {
        'physical':      physical,
        'hard_reserved': hard_reserved,
        'soft_held':     soft_held,
        'available':     available,        # for new soft holds
        'active_wo':     dict(active_batch) if active_batch else None,
    }


# ═══════════════════════════════════════════════════════════════════
#  INVENTORY ADJUSTMENT
# ═══════════════════════════════════════════════════════════════════

def create_adjustment(data):
    """
    Manual inventory adjustment.
    data: {ingredientId, qtyGrams, notes}  — positive or negative
    """
    ing = qry1("SELECT * FROM ingredients WHERE id=?", (data.get('ingredientId'),))
    if not ing:
        raise ValueError("Ingredient not found")
    qty = r2(data.get('qtyGrams', 0))
    if qty == 0:
        raise ValueError("Adjustment quantity cannot be zero")

    # Negative adjustment: ensure we don't go below zero
    if qty < 0:
        stock_map = get_stock_map()
        current = stock_map.get(ing['id'], 0)
        if current + qty < -0.001:
            raise ValueError(f"Adjustment would create negative stock. Current: {current:.1f}g")

    ops = [("""
        INSERT INTO inventory_ledger
            (ingredient_id, movement_type, qty_grams, notes)
        VALUES (?,?,?,?)
    """, (ing['id'], 'ADJUSTMENT', qty, data.get('notes', 'Manual adjustment')))]
    audit_log(ops, 'inventory_ledger', str(ing['id']), 'INSERT',
              new_val={'ingredient': ing['code'], 'qty_grams': qty})
    run_many(ops)
    return {'ingredientCode': ing['code'], 'adjustment': qty}


# ═══════════════════════════════════════════════════════════════════
#  INGREDIENT CRUD
# ═══════════════════════════════════════════════════════════════════

def create_ingredient(data):
    """Create a new ingredient."""
    code = str(data.get('code', '')).strip().upper()
    if not code:
        raise ValueError("Code is required (e.g. ING-020SP)")
    if qry1("SELECT id FROM ingredients WHERE code=?", (code,)):
        raise ValueError(f"Ingredient '{code}' already exists")
    name    = str(data.get('name', '')).strip()
    cost    = float(str(data.get('cost_per_kg', 0)).replace(',', '') or 0)
    if cost < 0:
        raise ValueError("Cost cannot be negative")
    unit    = str(data.get('unit', 'kg')).strip() or 'kg'
    reorder = float(str(data.get('reorder_level', 0)).replace(',', '') or 0)
    c = _conn()
    try:
        cur = c.execute("""
            INSERT INTO ingredients (code, name, unit, cost_per_kg, reorder_level, active, created_at)
            VALUES (?, ?, ?, ?, ?, 1, ?)
        """, (code, name, unit, cost, reorder, today()))
        iid = cur.lastrowid
        if cost > 0:
            c.execute("""
                INSERT INTO ingredient_price_history (ingredient_id, new_cost_per_kg, source)
                VALUES (?, ?, 'manual')
            """, (iid, cost))
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    _refresh_ref()
    return qry1("SELECT * FROM ingredients WHERE code=?", (code,))


def update_ingredient(code, data):
    """Update name, cost_per_kg, unit, or reorder_level for an ingredient."""
    ing = qry1("SELECT * FROM ingredients WHERE code=?", (code,))
    if not ing:
        raise ValueError(f"Ingredient not found: {code}")
    set_parts, vals = [], []
    if 'name' in data:
        set_parts.append("name=?"); vals.append(str(data['name']).strip())
    if 'cost_per_kg' in data:
        new_cost = float(str(data['cost_per_kg']).replace(',', '') or 0)
        if new_cost < 0:
            raise ValueError("Cost cannot be negative")
        old_cost = float(ing.get('cost_per_kg') or 0)
        pct = round(((new_cost - old_cost) / old_cost * 100), 2) if old_cost > 0 else None
        set_parts.append("cost_per_kg=?"); vals.append(new_cost)
        # Log price history immediately (separate connection to avoid nesting)
        changed_by = data.get('changed_by', 'admin')
        c2 = _conn()
        try:
            c2.execute("""
                INSERT INTO ingredient_price_history
                    (ingredient_id, old_cost_per_kg, new_cost_per_kg, pct_change,
                     change_type, changed_by, source)
                VALUES (?, ?, ?, ?, 'ingredient', ?, 'manual')
            """, (ing['id'], old_cost if old_cost > 0 else None, new_cost, pct, changed_by))
            c2.commit()
        finally:
            c2.close()
        # If volatile ingredient, trigger margin alert check in background
        if ing.get('price_volatile'):
            import threading
            def _check():
                try:
                    from modules.costing import get_margin_alerts
                    get_margin_alerts()
                except Exception:
                    pass
            threading.Thread(target=_check, daemon=True).start()
    if 'price_volatile' in data:
        set_parts.append("price_volatile=?"); vals.append(1 if data['price_volatile'] else 0)
    if 'unit' in data:
        set_parts.append("unit=?"); vals.append(str(data['unit']).strip() or 'kg')
    if 'reorder_level' in data:
        set_parts.append("reorder_level=?"); vals.append(float(str(data['reorder_level']).replace(',', '') or 0))
    if not set_parts:
        return dict(ing)
    set_parts.append("updated_at=?"); vals.append(today())
    vals.append(code)
    c = _conn()
    try:
        c.execute(f"UPDATE ingredients SET {', '.join(set_parts)} WHERE code=?", vals)
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    _refresh_ref()
    return qry1("SELECT * FROM ingredients WHERE code=?", (code,))


def bulk_update_ingredient_costs(rows, username):
    """
    Bulk update cost_per_kg for multiple ingredients.
    rows: list of {code, cost_per_kg}
    Logs each change to ingredient_price_history.
    Returns {updated, skipped, errors}.
    """
    updated, skipped, errors = 0, 0, []
    for row in rows:
        code = str(row.get('code', '')).strip()
        if not code:
            skipped += 1
            continue
        try:
            new_cost = float(str(row.get('cost_per_kg', '')).replace(',', '') or 0)
        except (ValueError, TypeError):
            errors.append({'code': code, 'error': 'Invalid cost_per_kg'})
            continue
        if new_cost < 0:
            errors.append({'code': code, 'error': 'Cost cannot be negative'})
            continue
        ing = qry1("SELECT id, cost_per_kg FROM ingredients WHERE code=? AND COALESCE(active,1)=1", (code,))
        if not ing:
            skipped += 1
            continue
        old_cost = float(ing['cost_per_kg'] or 0)
        if old_cost == new_cost:
            skipped += 1
            continue
        pct = round(((new_cost - old_cost) / old_cost * 100), 2) if old_cost > 0 else None
        c = _conn()
        try:
            c.execute("UPDATE ingredients SET cost_per_kg=?, updated_at=? WHERE code=?",
                      (new_cost, today(), code))
            c.execute("""
                INSERT INTO ingredient_price_history
                    (ingredient_id, old_cost_per_kg, new_cost_per_kg, pct_change,
                     change_type, changed_by, source)
                VALUES (?, ?, ?, ?, 'ingredient', ?, 'bulk')
            """, (ing['id'], old_cost if old_cost > 0 else None, new_cost, pct, username))
            c.commit()
            updated += 1
        except Exception as e:
            errors.append({'code': code, 'error': str(e)})
        finally:
            c.close()
    if updated > 0:
        save_db()
        _refresh_ref()
    return {'updated': updated, 'skipped': skipped, 'errors': errors}


def deactivate_ingredient(code):
    """Soft-delete an ingredient (sets active=0). Blocks if stock is held."""
    ing = qry1("SELECT id, code FROM ingredients WHERE code=?", (code,))
    if not ing:
        raise ValueError(f"Ingredient not found: {code}")
    stock = get_stock_map().get(ing['id'], 0)
    if stock > 0:
        raise ValueError(
            f"Cannot deactivate {code} — {stock:,.0f}g still in stock. "
            "Consume or write off the balance first."
        )
    c = _conn()
    try:
        c.execute("UPDATE ingredients SET active=0, updated_at=? WHERE code=?", (today(), code))
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    _refresh_ref()
    return {'ok': True, 'deactivated': code}


def reactivate_ingredient(code):
    """Re-activate a previously deactivated ingredient."""
    if not qry1("SELECT id FROM ingredients WHERE code=?", (code,)):
        raise ValueError(f"Ingredient not found: {code}")
    c = _conn()
    try:
        c.execute("UPDATE ingredients SET active=1, updated_at=? WHERE code=?", (today(), code))
        c.commit()
    finally:
        c.close()
    save_db()
    _refresh_ref()
    return {'ok': True, 'reactivated': code}


# ═══════════════════════════════════════════════════════════════════
#  BULK IMPORT
# ═══════════════════════════════════════════════════════════════════

def import_ingredients_master(rows):
    """Full-sync ingredients from master rows.
    Columns: code (required), cost_per_kg (required), name (optional),
             unit (optional), reorder_level (optional — ignored, set manually in app)
    Accepts 'name' or 'Ingredient Name (English)' as the name column.

    FULL SYNC behaviour: after upserting all rows in the file, any existing
    active ingredient whose code is NOT in the file is deactivated. This means
    uploading a fresh file always produces exactly the ingredients in that file —
    no stale duplicates from previous uploads with different codes.

    Returns {imported, updated, deactivated, errors}.
    """
    imported    = 0
    updated     = 0
    deactivated = 0
    errors      = []
    incoming_codes = set()

    # ── Pass 1: validate all rows and collect codes ──────────────────
    parsed = []
    for i, row in enumerate(rows, 1):
        code = row.get('code', '').strip().upper()
        if not code:
            errors.append(f"Row {i}: code is required"); continue
        cost_str = str(row.get('cost_per_kg', '0')).replace(',', '')
        try:
            cost = float(cost_str)
        except ValueError:
            errors.append(f"Row {i}: invalid cost_per_kg '{cost_str}'"); continue
        if cost < 0:
            errors.append(f"Row {i}: cost_per_kg cannot be negative"); continue
        name = str(row.get('name') or row.get('Ingredient Name (English)') or '').strip()
        unit = str(row.get('unit', 'kg')).strip() or 'kg'
        vol_raw = str(row.get('price_volatile', '')).strip().lower()
        volatile = 1 if vol_raw in ('1', 'yes', 'true', 'y') else 0
        incoming_codes.add(code)
        parsed.append((i, code, cost, name, unit, volatile))

    # ── Pass 2: upsert each valid row ────────────────────────────────
    for i, code, cost, name, unit, volatile in parsed:
        existing = qry1("SELECT id FROM ingredients WHERE code=?", (code,))
        try:
            if existing:
                c = _conn()
                try:
                    c.execute("""UPDATE ingredients
                                 SET cost_per_kg=?, unit=?, name=?, active=1,
                                     price_volatile=?, updated_at=?
                                 WHERE code=?""",
                              (cost, unit, name, volatile, today(), code))
                    c.commit()
                finally:
                    c.close()
                updated += 1
            else:
                c = _conn()
                try:
                    c.execute("""INSERT INTO ingredients
                                     (code, name, unit, cost_per_kg, reorder_level,
                                      active, price_volatile, created_at)
                                 VALUES (?, ?, ?, ?, 0, 1, ?, ?)""",
                              (code, name, unit, cost, volatile, today()))
                    c.commit()
                finally:
                    c.close()
                imported += 1
        except Exception as e:
            errors.append(f"Row {i} ({code}): {e}")

    # ── Pass 3: deactivate any active ingredient NOT in the new file ─
    if incoming_codes:
        existing_active = qry("SELECT code FROM ingredients WHERE COALESCE(active,1)=1")
        stale = [r['code'] for r in existing_active if r['code'] not in incoming_codes]
        for code in stale:
            try:
                c = _conn()
                try:
                    c.execute("UPDATE ingredients SET active=0, updated_at=? WHERE code=?",
                              (today(), code))
                    c.commit()
                finally:
                    c.close()
                deactivated += 1
            except Exception as e:
                errors.append(f"Deactivate {code}: {e}")

    save_db()
    _refresh_ref()
    return {'imported': imported, 'updated': updated, 'deactivated': deactivated, 'errors': errors}
