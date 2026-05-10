"""
modules/suppliers.py — Supplier CRUD, zone migration, and bulk import.

Extracted from server.py (Sprint 5). Overrides originals via bottom-import pattern:
    from modules.suppliers import *   # just before __main__ in server.py

_refresh_ref is a no-op until wired at startup:
    import modules.suppliers as _sup_mod
    _sup_mod._refresh_ref = load_ref
"""

from modules.db     import _conn, qry, qry1, run_many, save_db, audit_log
from modules.utils  import validate_fields
from modules.id_gen import _sync_counter_to_max, next_id

__all__ = [
    # Startup migrations
    '_ensure_supplier_zone_col', 'ensure_clean_supplier_codes',
    # Queries
    '_suppliers_with_zones',
    # CRUD
    'create_supplier', 'update_supplier',
    # Bulk import
    'import_suppliers_master',
]

# ── Callback wired at startup by server.py ────────────────────────────────────
# import modules.suppliers as _sup_mod; _sup_mod._refresh_ref = load_ref
_refresh_ref = lambda: None


# ═══════════════════════════════════════════════════════════════════
#  STARTUP MIGRATIONS
# ═══════════════════════════════════════════════════════════════════

def _ensure_supplier_zone_col():
    """Safe migration: add zone_id column to suppliers if not present.
    Also syncs the supplier id_counter to the actual max existing supplier number."""
    c = _conn()
    try:
        c.execute("ALTER TABLE suppliers ADD COLUMN zone_id INTEGER REFERENCES zones(id)")
        c.commit()
        print("  ✓ Suppliers: added zone_id column")
    except Exception:
        pass  # column already exists
    finally:
        c.close()
    _sync_counter_to_max('supplier', 'suppliers', 'code', 'SUP-')


def ensure_clean_supplier_codes():
    """Assign clean SUP-NNN codes to any SP-SUP-* suppliers.
    Finds the current max SUP-NNN and assigns next sequential codes.
    Idempotent — safe to run on every startup. Never crashes server."""
    c = _conn()
    try:
        bad = c.execute(
            "SELECT id, code FROM suppliers WHERE code LIKE 'SP-SUP-%' ORDER BY code"
        ).fetchall()
        if not bad:
            c.close()
            return
        max_row = c.execute(
            "SELECT code FROM suppliers WHERE code LIKE 'SUP-%' ORDER BY code DESC LIMIT 1"
        ).fetchone()
        try:
            next_num = int(max_row['code'].split('-')[1]) + 1 if max_row else 1
        except Exception:
            next_num = 100
        fixed = 0
        for row in bad:
            old_code = row['code'] if isinstance(row, dict) else row[1]
            row_id   = row['id']   if isinstance(row, dict) else row[0]
            new_code = f"SUP-{next_num:03d}"
            next_num += 1
            c.execute("UPDATE suppliers SET code=? WHERE id=?", (new_code, row_id))
            print(f"  ✓ supplier code: {old_code} → {new_code}")
            fixed += 1
        c.commit()
        print(f"  ✓ Normalized {fixed} supplier code(s) to SUP-NNN format")
    except Exception as e:
        print(f"  ⚠ ensure_clean_supplier_codes error (non-fatal): {e}")
        try: c.rollback()
        except: pass
    finally:
        c.close()


# ═══════════════════════════════════════════════════════════════════
#  QUERIES
# ═══════════════════════════════════════════════════════════════════

def _suppliers_with_zones():
    _ensure_supplier_zone_col()
    return qry("""
        SELECT s.*, z.name as zone_name
        FROM suppliers s
        LEFT JOIN zones z ON z.id = s.zone_id
        WHERE s.active_flag=1
        ORDER BY s.name
    """)


# ═══════════════════════════════════════════════════════════════════
#  SUPPLIER CRUD
# ═══════════════════════════════════════════════════════════════════

def create_supplier(data):
    validate_fields(data, [
        {'field': 'name',  'label': 'Supplier name', 'type': 'str', 'min': 2, 'max': 120},
        {'field': 'phone', 'label': 'Phone',         'required': False, 'type': 'str', 'max': 30},
        {'field': 'email', 'label': 'Email',         'required': False, 'type': 'str', 'max': 120},
        {'field': 'city',  'label': 'City',          'required': False, 'type': 'str', 'max': 60},
    ])
    _ensure_supplier_zone_col()
    _sync_counter_to_max('supplier', 'suppliers', 'code', 'SUP-')
    # Generate SUP-NNN format (next_id returns SP-SUP-N, we reformat)
    _raw  = next_id('supplier', 'SUP')
    _num  = int(_raw.split('-')[-1])
    code  = f"SUP-{_num:03d}"
    zone_id = data.get('zoneId') or None
    if zone_id is not None:
        zone_id = int(zone_id)
    ops = [("""
        INSERT INTO suppliers (code, name, contact, phone, email, city, address, zone_id)
        VALUES (?,?,?,?,?,?,?,?)
    """, (code, data['name'].strip(),
          data.get('contact', ''), data.get('phone', ''),
          data.get('email', ''), data.get('city', ''),
          data.get('address', ''), zone_id))]
    audit_log(ops, 'suppliers', code, 'INSERT', new_val=data)
    run_many(ops)
    _refresh_ref()
    return qry1("SELECT * FROM suppliers WHERE code=?", (code,))


def update_supplier(sup_id, data):
    _ensure_supplier_zone_col()
    existing = qry1("SELECT * FROM suppliers WHERE id=?", (sup_id,))
    if not existing:
        raise ValueError(f"Supplier not found: {sup_id}")
    mapping = {
        'name':        'name',
        'contact':     'contact',
        'phone':       'phone',
        'email':       'email',
        'city':        'city',
        'address':     'address',
        'active_flag': 'active_flag',
    }
    set_parts, vals = [], []
    for js_key, db_col in mapping.items():
        if js_key in data:
            set_parts.append(f"{db_col}=?")
            vals.append(data[js_key])
    if 'zoneId' in data:
        set_parts.append("zone_id=?")
        vals.append(int(data['zoneId']) if data['zoneId'] else None)
    if not set_parts:
        return existing
    vals.append(sup_id)
    ops = [(f"UPDATE suppliers SET {', '.join(set_parts)} WHERE id=?", vals)]
    audit_log(ops, 'suppliers', str(sup_id), 'UPDATE', old_val=dict(existing), new_val=data)
    run_many(ops)
    _refresh_ref()
    return qry1("SELECT * FROM suppliers WHERE id=?", (sup_id,))


# ═══════════════════════════════════════════════════════════════════
#  BULK IMPORT
# ═══════════════════════════════════════════════════════════════════

def import_suppliers_master(rows):
    """Upsert suppliers from master rows. Returns {imported, updated, errors}."""
    imported = updated = 0
    errors   = []
    c = _conn()
    try:
        for i, row in enumerate(rows, 1):
            code = row.get('code', '').strip().upper()
            name = row.get('name', '').strip()
            if not code or not name:
                errors.append(f"Row {i}: code and name are required"); continue
            existing = c.execute("SELECT id FROM suppliers WHERE code=?", (code,)).fetchone()
            if existing:
                c.execute("""UPDATE suppliers SET name=?, contact=?, phone=?, email=?,
                             city=?, address=?, active_flag=1 WHERE code=?""",
                          (name, row.get('contact', ''), row.get('phone', ''),
                           row.get('email', ''), row.get('city', ''),
                           row.get('address', ''), code))
                updated += 1
            else:
                c.execute("""INSERT INTO suppliers (code, name, contact, phone, email,
                             city, address, active_flag)
                             VALUES (?,?,?,?,?,?,?,1)""",
                          (code, name, row.get('contact', ''), row.get('phone', ''),
                           row.get('email', ''), row.get('city', ''),
                           row.get('address', '')))
                imported += 1
        c.commit()
    finally:
        c.close()
    save_db()
    _refresh_ref()
    return {'imported': imported, 'updated': updated, 'errors': errors}
