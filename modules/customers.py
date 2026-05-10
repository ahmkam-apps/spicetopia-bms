"""
modules/customers.py — Customer CRUD, imports, route assignment, field lookup.

Extracted from server.py (Sprint 4). Overrides originals via bottom-import pattern:
    from modules.customers import *   # just before __main__ in server.py

_refresh_ref is a no-op until wired at startup:
    import modules.customers as _cust_mod
    _cust_mod._refresh_ref = load_ref
"""

import json

from modules.db     import _conn, qry, qry1, run, run_many, save_db, audit_log
from modules.utils  import validate_fields
from modules.id_gen import _sync_counter_to_max, next_id, generate_account_number

__all__ = [
    # Startup migration
    'ensure_clean_customer_codes',
    # CRUD
    'create_customer', 'update_customer',
    # Bulk import
    'import_customers_master',
    # Route helpers
    'assign_customer_route', 'list_route_customers',
    # Field portal
    'field_lookup_customers', 'field_create_customer',
]

# ── Callback wired at startup by server.py ────────────────────────────────────
# import modules.customers as _cust_mod; _cust_mod._refresh_ref = load_ref
_refresh_ref = lambda: None


# ═══════════════════════════════════════════════════════════════════
#  STARTUP MIGRATION
# ═══════════════════════════════════════════════════════════════════

def ensure_clean_customer_codes():
    """One-time migration: fix SP-SP-CUST-XXXX double-prefix → SP-CUST-XXXX.
    Updates customers table + denormalized cust_code in sales + customer_orders.
    Idempotent — safe to run on every startup."""
    c = _conn()
    try:
        bad = c.execute(
            "SELECT id, code FROM customers WHERE code LIKE 'SP-SP-CUST-%'"
        ).fetchall()
        if not bad:
            c.close()
            return
        sales_cols = {r['name'] for r in c.execute("PRAGMA table_info(sales)")}
        co_cols    = {r['name'] for r in c.execute("PRAGMA table_info(customer_orders)")}
        for row in bad:
            old_code = row['code'] if isinstance(row, dict) else row[1]
            row_id   = row['id']  if isinstance(row, dict) else row[0]
            new_code = old_code.replace('SP-SP-CUST-', 'SP-CUST-', 1)
            c.execute("UPDATE customers SET code=? WHERE id=?", (new_code, row_id))
            if 'cust_code' in sales_cols:
                c.execute("UPDATE sales SET cust_code=? WHERE cust_code=?", (new_code, old_code))
            if 'cust_code' in co_cols:
                c.execute("UPDATE customer_orders SET cust_code=? WHERE cust_code=?", (new_code, old_code))
            print(f"  ✓ customer code fixed: {old_code} → {new_code}")
        c.commit()
        print(f"  ✓ Fixed {len(bad)} customer code(s) — double-prefix removed")
    finally:
        c.close()


# ═══════════════════════════════════════════════════════════════════
#  CUSTOMER CRUD
# ═══════════════════════════════════════════════════════════════════

def create_customer(data):
    validate_fields(data, [
        {'field': 'name',         'label': 'Customer name',  'type': 'str',  'min': 2, 'max': 120},
        {'field': 'city',         'label': 'City',           'type': 'str',  'min': 2, 'max': 60},
        {'field': 'address',      'label': 'Full address',   'required': False, 'type': 'str', 'min': 0, 'max': 250},
        {'field': 'customerType', 'label': 'Customer type',  'required': False,
         'choices': ['RETAIL', 'retail', 'DIRECT', 'direct', 'WHOLESALE', 'wholesale']},
        {'field': 'phone',        'label': 'Phone',          'required': False, 'type': 'str', 'max': 30},
        {'field': 'email',        'label': 'Email',          'required': False, 'type': 'str', 'max': 120},
    ])
    # Sync counter before use to prevent UNIQUE constraint failures on Railway
    _sync_counter_to_max('customer', 'customers', 'code', 'SP-CUST-')
    code  = next_id('customer', 'CUST')
    ctype = data.get('customerType', 'RETAIL').upper()
    if ctype not in ('RETAIL', 'DIRECT', 'WHOLESALE'):
        raise ValueError(f"Invalid customer type: {ctype}")
    city           = data.get('city', '').strip()
    account_number = generate_account_number(city, ctype)
    ops = [("""
        INSERT INTO customers
            (code, account_number, name, customer_type, city, address,
             phone, email, default_pack, payment_terms_days)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (code, account_number, data['name'].strip(), ctype,
          city,
          data.get('address', '').strip(),
          data.get('phone', ''), data.get('email', ''),
          data.get('defaultPack', '50g'),
          int(data.get('paymentTermsDays', 30))))]
    audit_log(ops, 'customers', code, 'INSERT', new_val=data)
    run_many(ops)
    _refresh_ref()
    return qry1("SELECT * FROM customers WHERE code=?", (code,))


def update_customer(cust_id, data):
    """Update customer by integer id."""
    existing = qry1("SELECT * FROM customers WHERE id=?", (cust_id,))
    if not existing:
        raise ValueError(f"Customer not found: {cust_id}")
    mapping = {
        'name':             'name',
        'customerType':     'customer_type',
        'city':             'city',
        'address':          'address',
        'phone':            'phone',
        'email':            'email',
        'defaultPack':      'default_pack',
        'paymentTermsDays': 'payment_terms_days',
        'creditLimit':      'credit_limit',
        'active':           'active',
    }
    set_parts, vals = [], []
    for js_key, db_col in mapping.items():
        if js_key in data:
            set_parts.append(f"{db_col}=?")
            vals.append(data[js_key])
    if not set_parts:
        return existing
    vals.append(cust_id)
    ops = [(f"UPDATE customers SET {', '.join(set_parts)} WHERE id=?", vals)]
    audit_log(ops, 'customers', existing['code'], 'UPDATE', old_val=dict(existing), new_val=data)
    run_many(ops)
    _refresh_ref()
    return qry1("SELECT * FROM customers WHERE id=?", (cust_id,))


# ═══════════════════════════════════════════════════════════════════
#  BULK IMPORT
# ═══════════════════════════════════════════════════════════════════

def import_customers_master(rows):
    """Upsert customers from master rows. Returns {imported, updated, errors}."""
    imported = updated = 0
    errors = []
    c = _conn()
    try:
        for i, row in enumerate(rows, 1):
            code = row.get('code', '').strip().upper()
            name = row.get('name', '').strip()
            if not code or not name:
                errors.append(f"Row {i}: code and name are required"); continue
            ctype = row.get('customer_type', 'RETAIL').strip().upper()
            if ctype not in ('RETAIL', 'DIRECT', 'WHOLESALE'):
                ctype = 'RETAIL'
            address  = row.get('address', '').strip()
            existing = c.execute("SELECT id FROM customers WHERE code=?", (code,)).fetchone()
            if existing:
                c.execute("""UPDATE customers SET name=?, customer_type=?,
                             city=?, address=?, phone=?, email=?, payment_terms_days=?, active=1
                             WHERE code=?""",
                          (name, ctype,
                           row.get('city', ''), address,
                           row.get('phone', ''), row.get('email', ''),
                           int(row.get('payment_terms_days', 30) or 30), code))
                updated += 1
            else:
                # account_number left NULL — backfill_customer_account_numbers() assigns
                # it automatically on next server startup using city+type convention
                c.execute("""INSERT INTO customers (code, name, customer_type,
                             city, address, phone, email, payment_terms_days, active)
                             VALUES (?,?,?,?,?,?,?,?,1)""",
                          (code, name, ctype,
                           row.get('city', ''), address,
                           row.get('phone', ''), row.get('email', ''),
                           int(row.get('payment_terms_days', 30) or 30)))
                imported += 1
        c.commit()
    finally:
        c.close()
    save_db()
    _refresh_ref()
    return {'imported': imported, 'updated': updated, 'errors': errors}


# ═══════════════════════════════════════════════════════════════════
#  ROUTE ASSIGNMENT
# ═══════════════════════════════════════════════════════════════════

def assign_customer_route(customer_id, route_id, shop_name='', address=''):
    existing = qry1(
        "SELECT id FROM route_customers WHERE customer_id=? AND route_id=?",
        (customer_id, route_id))
    if existing:
        return {'ok': True, 'message': 'Already assigned'}
    c = _conn()
    try:
        c.execute("""
            INSERT INTO route_customers (route_id, customer_id, shop_name, address)
            VALUES (?,?,?,?)
        """, (route_id, customer_id, shop_name, address))
        c.commit()
    finally:
        c.close()
    save_db()
    return {'ok': True}


def list_route_customers(route_id):
    return qry("""
        SELECT rc.*, c.name as customer_name, c.code as customer_code,
               c.phone as customer_phone
        FROM route_customers rc
        JOIN customers c ON c.id=rc.customer_id
        WHERE rc.route_id=?
        ORDER BY rc.stop_sequence, c.name
    """, (route_id,))


# ═══════════════════════════════════════════════════════════════════
#  FIELD PORTAL
# ═══════════════════════════════════════════════════════════════════

def field_lookup_customers(query, rep_id):
    """Search customers by account number, name, or phone.
    Returns match list with onRoute flag for each customer."""
    q         = f'%{query}%'
    customers = qry("""
        SELECT id, code, account_number, name, customer_type, city, phone
        FROM customers
        WHERE active=1 AND (account_number LIKE ? OR name LIKE ? OR phone LIKE ?)
        ORDER BY name LIMIT 20
    """, (q, q, q))

    rep_routes = qry("""
        SELECT route_id FROM rep_routes
        WHERE rep_id=? AND (assigned_to IS NULL OR assigned_to >= date('now'))
    """, (rep_id,))
    on_route_ids = set()
    if rep_routes:
        rids         = [r['route_id'] for r in rep_routes]
        placeholders = ','.join('?' * len(rids))
        on_route     = qry(
            f"SELECT customer_id FROM route_customers WHERE route_id IN ({placeholders})",
            rids
        )
        on_route_ids = {r['customer_id'] for r in on_route}

    return [{
        'id':            c['id'],
        'code':          c['code'],
        'accountNumber': c['account_number'],
        'name':          c['name'],
        'customerType':  c['customer_type'],
        'city':          c['city'],
        'phone':         c['phone'],
        'onRoute':       c['id'] in on_route_ids,
    } for c in customers]


def field_create_customer(data, rep_id):
    """Create a customer from the B2B field portal. Delegates to create_customer()."""
    return create_customer(data)
