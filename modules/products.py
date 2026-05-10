"""
modules/products.py — Product and variant CRUD, startup migrations, bulk import.

Extracted from server.py (Sprint 6). Overrides originals via bottom-import pattern:
    from modules.products import *   # just before __main__ in server.py

_refresh_ref is a no-op until wired at startup:
    import modules.products as _prod_mod
    _prod_mod._refresh_ref = load_ref
"""

import json

from modules.db import _conn, qry, qry1, save_db

__all__ = [
    # Startup migrations
    'ensure_variant_wastage_pct', 'ensure_variant_gtin',
    # CRUD
    'create_product', 'update_product', 'deactivate_product', 'deactivate_variant',
    # Bulk import
    'import_products_master',
]

# ── Callback wired at startup by server.py ────────────────────────────────────
# import modules.products as _prod_mod; _prod_mod._refresh_ref = load_ref
_refresh_ref = lambda: None


# ═══════════════════════════════════════════════════════════════════
#  STARTUP MIGRATIONS
# ═══════════════════════════════════════════════════════════════════

def ensure_variant_wastage_pct():
    """Add wastage_pct column to product_variants. Idempotent."""
    c = _conn()
    try:
        existing = {r[1] for r in c.execute("PRAGMA table_info(product_variants)").fetchall()}
        if 'wastage_pct' not in existing:
            c.execute("ALTER TABLE product_variants ADD COLUMN wastage_pct REAL DEFAULT 0")
            print("  ✓ product_variants: added wastage_pct")
        c.commit()
    finally:
        c.close()


def ensure_variant_gtin():
    """Add gtin column to product_variants and seed known GTINs. Idempotent."""
    c = _conn()
    try:
        existing = {r[1] for r in c.execute("PRAGMA table_info(product_variants)").fetchall()}
        if 'gtin' not in existing:
            c.execute("ALTER TABLE product_variants ADD COLUMN gtin TEXT DEFAULT NULL")
            print("  ✓ product_variants: added gtin")

        # Seed GTINs — match by product name + pack size grams
        # Only seeds if gtin is currently NULL (never overwrites manually-entered values)
        seeds = [
            ('Chaat Masala', 50,  '8966000086913'),
            ('Garam Masala', 50,  '8966000086920'),
        ]
        for prod_name, grams, gtin_val in seeds:
            cur = c.execute("""
                UPDATE product_variants
                SET gtin = ?
                WHERE gtin IS NULL
                  AND product_id IN (SELECT id FROM products WHERE name = ?)
                  AND pack_size_id IN (SELECT id FROM pack_sizes WHERE grams = ?)
            """, (gtin_val, prod_name, grams))
            if cur.rowcount:
                print(f"  ✓ gtin seeded: {prod_name} {grams}g -> {gtin_val}")
        c.commit()
    finally:
        c.close()


# ═══════════════════════════════════════════════════════════════════
#  PRODUCT CRUD
# ═══════════════════════════════════════════════════════════════════

def create_product(data):
    """
    Each pack size is treated as a separate product.
    One call = one product + one pack-size variant (one SKU).
    """
    code         = data.get('code', '').strip().upper()
    name         = data.get('name', '').strip()
    name_urdu    = data.get('nameUrdu', '').strip()
    blend_code   = data.get('blendCode', '').strip()
    pack_size_id = data.get('packSizeId')

    if not code:
        raise ValueError("Product code is required")
    if not name:
        raise ValueError("Product name is required")
    if not pack_size_id:
        raise ValueError("Pack size is required")

    pack_size_id = int(pack_size_id)
    ps = qry1("SELECT id, label FROM pack_sizes WHERE id=?", (pack_size_id,))
    if not ps:
        raise ValueError("Invalid pack size")

    pack_grams = ps['label'].replace('g', '')
    base       = code if code.startswith('SP-') else f"SP-{code}"
    full_code  = f"{base}-{pack_grams}"
    if qry1("SELECT id FROM products WHERE code=?", (full_code,)):
        raise ValueError(f"Product '{full_code}' ({name} {ps['label']}) already exists")

    c = _conn()
    try:
        c.execute("""
            INSERT INTO products (code, name, name_urdu, blend_code, active)
            VALUES (?,?,?,?,1)
        """, (full_code, name, name_urdu, blend_code))
        prod_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        c.execute("INSERT OR IGNORE INTO id_counters (entity, last_num) VALUES ('sku', 0)")
        c.execute("UPDATE id_counters SET last_num=last_num+1 WHERE entity='sku'")
        num      = c.execute("SELECT last_num FROM id_counters WHERE entity='sku'").fetchone()[0]
        sku_code = f"SP-SKU-{num:04d}"
        c.execute("""
            INSERT INTO product_variants (sku_code, product_id, pack_size_id, active_flag)
            VALUES (?,?,?,1)
        """, (sku_code, prod_id, pack_size_id))
        c.commit()
    finally:
        c.close()

    save_db()
    _refresh_ref()
    return {'code': full_code, 'name': name, 'packSize': ps['label'], 'sku': sku_code}


def update_product(code, data):
    """Edit product name, Urdu name, or blend code. Code is immutable."""
    prod = qry1("SELECT * FROM products WHERE code=?", (code,))
    if not prod:
        raise ValueError(f"Product not found: {code}")
    set_parts, vals = [], []
    if 'name' in data and str(data['name']).strip():
        set_parts.append("name=?");       vals.append(str(data['name']).strip())
    if 'nameUrdu' in data:
        set_parts.append("name_urdu=?");  vals.append(str(data.get('nameUrdu') or '').strip())
    if 'blendCode' in data:
        set_parts.append("blend_code=?"); vals.append(str(data.get('blendCode') or '').strip())
    if not set_parts:
        return dict(prod)
    vals.append(code)
    c = _conn()
    try:
        c.execute(f"UPDATE products SET {', '.join(set_parts)} WHERE code=?", vals)
        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('products',?,'UPDATE',?)
        """, (code, json.dumps({k: data[k] for k in data})))
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    _refresh_ref()
    return qry1("SELECT * FROM products WHERE code=?", (code,))


def deactivate_product(code):
    prod = qry1("SELECT id, name FROM products WHERE code=?", (code,))
    if not prod:
        raise ValueError(f"Product '{code}' not found")

    open_orders = qry("""
        SELECT COUNT(*) as cnt FROM invoice_items ii
        JOIN invoices inv ON inv.id = ii.invoice_id
        JOIN product_variants pv ON pv.id = ii.product_variant_id
        WHERE pv.product_id=? AND inv.status IN ('UNPAID','PARTIAL')
    """, (prod['id'],))
    if open_orders and open_orders[0]['cnt'] > 0:
        raise ValueError(f"Cannot remove: {open_orders[0]['cnt']} open invoice(s) reference this product. Close them first.")

    c = _conn()
    try:
        c.execute("UPDATE products SET active=0 WHERE id=?", (prod['id'],))
        c.execute("UPDATE product_variants SET active_flag=0 WHERE product_id=?", (prod['id'],))
        c.commit()
    finally:
        c.close()

    save_db()
    _refresh_ref()
    return {'removed': code, 'name': prod['name']}


def deactivate_variant(variant_id):
    """Remove a single pack-size SKU."""
    v = qry1("""
        SELECT pv.id, pv.sku_code, pv.product_id, p.name, ps.label as pack_size
        FROM product_variants pv
        JOIN products p    ON p.id  = pv.product_id
        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE pv.id=?
    """, (variant_id,))
    if not v:
        raise ValueError(f"SKU id {variant_id} not found")

    open_cnt = qry1("""
        SELECT COUNT(*) as cnt FROM invoice_items ii
        JOIN invoices inv ON inv.id = ii.invoice_id
        WHERE ii.product_variant_id=? AND inv.status IN ('UNPAID','PARTIAL')
    """, (variant_id,))
    if open_cnt and open_cnt['cnt'] > 0:
        raise ValueError(f"Cannot remove: {open_cnt['cnt']} open invoice(s) use this SKU. Close them first.")

    c = _conn()
    try:
        c.execute("UPDATE product_variants SET active_flag=0 WHERE id=?", (variant_id,))
        remaining = c.execute(
            "SELECT COUNT(*) FROM product_variants WHERE product_id=? AND active_flag=1",
            (v['product_id'],)
        ).fetchone()[0]
        if remaining == 0:
            c.execute("UPDATE products SET active=0 WHERE id=?", (v['product_id'],))
        c.commit()
    finally:
        c.close()

    save_db()
    _refresh_ref()
    return {'removed': v['sku_code'], 'name': v['name'], 'packSize': v['pack_size']}


# ═══════════════════════════════════════════════════════════════════
#  BULK IMPORT
# ═══════════════════════════════════════════════════════════════════

def import_products_master(rows):
    """Upsert products and their variants from master rows.

    Supports one-row-per-variant format with explicit sku_code:
      product_code, product_name, sku_code, pack_size, gtin (optional)

    Returns {imported, updated, variants_imported, variants_updated, errors}.
    """
    imported = updated = 0
    variant_imported = variant_updated = 0
    errors       = []
    seen_products = {}   # product_code -> prod_id
    c = _conn()
    try:
        for i, row in enumerate(rows, 1):
            code     = (row.get('product_code') or row.get('code') or '').strip().upper()
            name     = (row.get('product_name') or row.get('name') or '').strip()
            sku_code = row.get('sku_code', '').strip()
            ps_label = row.get('pack_size', '').strip()

            if not code or not name:
                errors.append(f"Row {i}: product_code and product_name are required"); continue
            if not sku_code or not ps_label:
                errors.append(f"Row {i}: sku_code and pack_size are required"); continue

            # Upsert product (once per unique product_code)
            if code not in seen_products:
                existing = c.execute("SELECT id FROM products WHERE code=?", (code,)).fetchone()
                if existing:
                    prod_id = existing[0]
                    c.execute("UPDATE products SET name=?, active=1 WHERE code=?", (name, code))
                    updated += 1
                else:
                    cur     = c.execute("""INSERT INTO products (code, name, name_urdu, blend_code, active)
                                 VALUES (?,?,?,?,1)""", (code, name, '', ''))
                    prod_id = cur.lastrowid
                    imported += 1
                seen_products[code] = prod_id
            else:
                prod_id = seen_products[code]

            # Ensure pack_size exists
            ps_row = c.execute("SELECT id FROM pack_sizes WHERE label=?", (ps_label,)).fetchone()
            if not ps_row:
                grams = int(''.join(filter(str.isdigit, ps_label)) or 0)
                if 'kg' in ps_label.lower():
                    grams *= 1000
                c.execute("INSERT OR IGNORE INTO pack_sizes (label, grams) VALUES (?,?)", (ps_label, grams))
                ps_row = c.execute("SELECT id FROM pack_sizes WHERE label=?", (ps_label,)).fetchone()
            ps_id = ps_row[0]

            # Validate optional gtin
            gtin_val = row.get('gtin', '').strip() or None
            if gtin_val:
                if not gtin_val.isdigit() or not (8 <= len(gtin_val) <= 14):
                    errors.append(f"Row {i}: gtin '{gtin_val}' must be 8–14 digits — skipping gtin")
                    gtin_val = None

            # Upsert variant
            existing_var = c.execute(
                "SELECT id FROM product_variants WHERE sku_code=?", (sku_code,)).fetchone()
            if existing_var:
                if gtin_val is not None:
                    c.execute("""UPDATE product_variants SET product_id=?, pack_size_id=?, active_flag=1, gtin=?
                                 WHERE sku_code=?""", (prod_id, ps_id, gtin_val, sku_code))
                else:
                    c.execute("""UPDATE product_variants SET product_id=?, pack_size_id=?, active_flag=1
                                 WHERE sku_code=?""", (prod_id, ps_id, sku_code))
                variant_updated += 1
            else:
                c.execute("""INSERT INTO product_variants (sku_code, product_id, pack_size_id, active_flag, gtin)
                             VALUES (?,?,?,1,?)""", (sku_code, prod_id, ps_id, gtin_val))
                variant_imported += 1

        c.commit()
    finally:
        c.close()
    save_db()
    _refresh_ref()
    return {
        'imported': imported, 'updated': updated,
        'variants_imported': variant_imported, 'variants_updated': variant_updated,
        'errors': errors,
    }
