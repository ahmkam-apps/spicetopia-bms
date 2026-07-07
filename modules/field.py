"""
modules/field.py
================
Sprint 14 — Field domain: Zones, Routes, Reps, Payroll, Beat Visits,
            Field Orders, Direct Sales (create_sale / create_multi_sale),
            field_get_products, _is_out_of_route, _wa_notify_out_of_route.

Exports (via __all__):
  list_zones, create_zone, update_zone,
  list_routes, create_route, update_route,
  list_reps, get_rep, create_rep, update_rep,
  assign_rep_route, unassign_rep_route,
  set_rep_target, record_advance, record_beat_visit,
  get_field_order, list_field_orders,
  calculate_payroll, run_payroll, finalize_payroll, list_payroll_runs,
  get_rep_today_route, field_get_products,
  _is_out_of_route, _wa_notify_out_of_route,
  create_sale, create_multi_sale

Note: _is_out_of_route / _wa_notify_out_of_route must be re-wired as callbacks
      to orders.py at startup (see server.py _apply_startup_config block).

Already extracted (NOT re-exported here):
  field_login, _get_field_session              → modules.auth
  field_lookup_customers, field_create_customer → modules.customers
  assign_customer_route, list_route_customers   → modules.customers
"""

import hashlib
import json
import random
import string
from datetime import date, timedelta, datetime

from modules.utils  import *   # r2, today, fmtpkr, validate_fields
from modules.db     import *   # _conn, qry, qry1, run, save_db
from modules.id_gen import *   # next_id, _sync_counter_to_max

__all__ = [
    # Zones
    'list_zones', 'create_zone', 'update_zone',
    # Routes
    'list_routes', 'create_route', 'update_route',
    # Reps
    'list_reps', 'get_rep', 'create_rep', 'update_rep',
    'assign_rep_route', 'unassign_rep_route',
    # Targets & advances
    'set_rep_target', 'record_advance', 'set_rep_zones', 'set_rep_app_access',
    # Beat visits
    'record_beat_visit',
    # Field orders (list/detail only — legacy create/confirm write path removed, P1-1)
    'get_field_order', 'list_field_orders',
    # Payroll
    'calculate_payroll', 'run_payroll', 'finalize_payroll', 'list_payroll_runs',
    # Field app helpers
    'get_rep_today_route', 'field_get_products',
    # Route / WA helpers (wired as callbacks in orders.py)
    '_is_out_of_route', '_wa_notify_out_of_route',
    # Direct sales
    'create_sale', 'create_multi_sale',
    # OTP login
    'send_field_otp', 'verify_field_otp',
    # One-tap order → invoice (field app)
    'field_place_and_invoice',
    # Two-step: place order (booking) + invoice existing order
    'field_place_order', 'field_invoice_order',
]


# ─────────────────────────────────────────────────────────────────
#  INTERNAL DB HELPERS (replace ref[] lookups)
# ─────────────────────────────────────────────────────────────────

def _lookup_customer_by_code(code):
    """Fetch an active customer row by code (e.g. SP-CUST-0001). Returns None if not found."""
    return qry1("SELECT * FROM customers WHERE code=? AND active=1", (code,))


def _lookup_variant_by_sku_pair(product_code, pack_size):
    """Fetch an active variant row by (product_code, pack_size label), e.g. ('SPGM', '100g').
    Replaces the old ref['var_by_sku'] in-memory dict lookup.
    """
    return qry1("""
        SELECT pv.*, ps.label as pack_size, ps.grams as pack_grams,
               p.code as product_code, p.name as product_name
        FROM product_variants pv
        JOIN products p    ON p.id  = pv.product_id
        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE p.code=? AND ps.label=? AND pv.active_flag=1
    """, (product_code, pack_size))


# ─────────────────────────────────────────────────────────────────
#  ZONES
# ─────────────────────────────────────────────────────────────────

def list_zones():
    """Return all active zones ordered by name."""
    return qry("SELECT * FROM zones WHERE active=1 ORDER BY name")


def create_zone(data):
    """Create a new zone. Required: name. Optional: city/description."""
    name = data.get('name', '').strip()
    if not name:
        raise ValueError("Zone name is required")
    c = _conn()
    try:
        c.execute("INSERT INTO zones (name, city) VALUES (?,?)",
                  (name, data.get('city', data.get('description', ''))))
        c.commit()
        zone_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        c.close()
    save_db()
    return qry1("SELECT * FROM zones WHERE id=?", (zone_id,))


def update_zone(zone_id, data):
    """Update an existing zone. Accepts name, city/description, active."""
    z = qry1("SELECT * FROM zones WHERE id=?", (zone_id,))
    if not z:
        raise ValueError(f"Zone not found: {zone_id}")
    mapping   = {'name': 'name', 'city': 'city', 'description': 'city', 'active': 'active'}
    set_parts, vals, seen_cols = [], [], set()
    for k, col in mapping.items():
        if k in data and col not in seen_cols:
            set_parts.append(f"{col}=?")
            vals.append(data[k])
            seen_cols.add(col)
    if set_parts:
        vals.append(zone_id)
        c = _conn()
        try:
            c.execute(f"UPDATE zones SET {', '.join(set_parts)} WHERE id=?", vals)
            c.commit()
        finally:
            c.close()
        save_db()
    return qry1("SELECT * FROM zones WHERE id=?", (zone_id,))


# ─────────────────────────────────────────────────────────────────
#  ROUTES
# ─────────────────────────────────────────────────────────────────

def list_routes(zone_id=None):
    """Return active routes with zone name. Pass zone_id to filter to one zone."""
    if zone_id:
        return qry("""
            SELECT r.*, z.name as zone_name
            FROM routes r JOIN zones z ON z.id=r.zone_id
            WHERE r.zone_id=? AND r.active=1 ORDER BY r.name
        """, (zone_id,))
    return qry("""
        SELECT r.*, z.name as zone_name
        FROM routes r JOIN zones z ON z.id=r.zone_id
        WHERE r.active=1 ORDER BY z.name, r.name
    """)


def create_route(data):
    """Create a route under a zone. Required: zoneId, name. Optional: visitDays (e.g. 'mon,wed,fri')."""
    zone_id = data.get('zoneId') or data.get('zone_id')
    name    = data.get('name', '').strip()
    if not zone_id or not name:
        raise ValueError("zoneId and name are required")
    z = qry1("SELECT id FROM zones WHERE id=?", (int(zone_id),))
    if not z:
        raise ValueError(f"Zone not found: {zone_id}")
    c = _conn()
    try:
        c.execute("""
            INSERT INTO routes (zone_id, name, visit_days)
            VALUES (?,?,?)
        """, (int(zone_id), name, data.get('visitDays', data.get('visit_days', ''))))
        c.commit()
        route_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        c.close()
    save_db()
    return qry1("""
        SELECT r.*, z.name as zone_name FROM routes r JOIN zones z ON z.id=r.zone_id
        WHERE r.id=?
    """, (route_id,))


def update_route(route_id, data):
    """Update a route's name, visitDays, or active flag."""
    r = qry1("SELECT * FROM routes WHERE id=?", (route_id,))
    if not r:
        raise ValueError(f"Route not found: {route_id}")
    mapping   = {'name': 'name', 'visitDays': 'visit_days', 'visit_days': 'visit_days', 'active': 'active'}
    set_parts, vals = [], []
    for k, col in mapping.items():
        if k in data:
            set_parts.append(f"{col}=?")
            vals.append(data[k])
    if set_parts:
        vals.append(route_id)
        c = _conn()
        try:
            c.execute(f"UPDATE routes SET {', '.join(set_parts)} WHERE id=?", vals)
            c.commit()
        finally:
            c.close()
        save_db()
    return qry1("""
        SELECT r.*, z.name as zone_name FROM routes r JOIN zones z ON z.id=r.zone_id
        WHERE r.id=?
    """, (route_id,))


# ─────────────────────────────────────────────────────────────────
#  SALES REPS
# ─────────────────────────────────────────────────────────────────

def list_reps(active_only=True):
    """Return sales reps with their primary zone name. Set active_only=False to include inactive."""
    sql = """
        SELECT sr.*, z.name as zone_name
        FROM sales_reps sr
        LEFT JOIN zones z ON z.id=sr.primary_zone_id
        {}
        ORDER BY sr.name
    """.format("WHERE (sr.status IS NULL OR sr.status='active')" if active_only else "")
    return qry(sql)


def get_rep(rep_id):
    """Return full rep profile: base row + routes, active salary, active commission rule, outstanding advances."""
    rep = qry1("SELECT * FROM sales_reps WHERE id=?", (rep_id,))
    if not rep:
        return None
    rep['routes'] = qry("""
        SELECT rr.*, r.name as route_name, z.name as zone_name
        FROM rep_routes rr
        JOIN routes r ON r.id=rr.route_id
        JOIN zones  z ON z.id=r.zone_id
        WHERE rr.rep_id=? AND rr.assigned_to IS NULL
    """, (rep_id,))
    try:
        rep['zones'] = qry("""
            SELECT rz.zone_id, z.name AS zone_name
            FROM rep_zones rz JOIN zones z ON z.id=rz.zone_id
            WHERE rz.rep_id=? ORDER BY z.name
        """, (rep_id,))
    except Exception:
        rep['zones'] = []
    rep['salary'] = qry1("""
        SELECT * FROM rep_salary_components WHERE rep_id=? AND active=1
        ORDER BY effective_from DESC LIMIT 1
    """, (rep_id,))
    rep['commission'] = qry1("""
        SELECT * FROM rep_commission_rules WHERE rep_id=? AND active=1
        ORDER BY effective_from DESC LIMIT 1
    """, (rep_id,))
    rep['advances'] = qry("""
        SELECT * FROM rep_advances WHERE rep_id=? AND recovered=0 ORDER BY advance_date DESC
    """, (rep_id,))
    return rep


def create_rep(data):
    """Create a new sales rep. Required: name, phone.
    Optional: email, notes, joinDate, designation, pin, baseSalary, fuelAllowance,
              mobileAllowance, otherAllowance, commissionPct, acceleratorPct, flatTargetBonus.
    Salary and commission rules are inserted as effective-today records if provided.
    """
    name  = data.get('name', '').strip()
    phone = data.get('phone', '').strip()
    if not name:
        raise ValueError("Rep name is required")
    if not phone:
        raise ValueError("Phone number is required")
    existing = qry1(
        "SELECT id FROM sales_reps WHERE phone=? AND (status IS NULL OR status='active')",
        (phone,))
    if existing:
        raise ValueError(f"Phone {phone} already registered to another rep")
    pin      = data.get('pin', '')
    pin_hash = hashlib.sha256(pin.encode()).hexdigest() if pin else ''
    c = _conn()
    try:
        last_id = c.execute("SELECT COUNT(*) FROM sales_reps").fetchone()[0]
        emp_id  = f"SR-{(last_id + 1):04d}"
        c.execute("""
            INSERT INTO sales_reps
                (employee_id, name, phone, pin_hash, email, notes,
                 joining_date, status, designation)
            VALUES (?,?,?,?,?,?,?,'active',?)
        """, (emp_id, name, phone, pin_hash,
              data.get('email', ''),
              data.get('notes', ''),
              data.get('joinDate', str(date.today())),
              data.get('designation', 'SR')))
        c.commit()
        rep_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        c.close()

    base_salary = float(data.get('baseSalary', 0) or 0)
    if base_salary:
        c2 = _conn()
        try:
            c2.execute("""
                INSERT INTO rep_salary_components
                    (rep_id, basic_salary, fuel_allowance, mobile_allowance,
                     other_allowance, effective_from, active)
                VALUES (?,?,?,?,?,?,1)
            """, (rep_id, base_salary,
                  float(data.get('fuelAllowance', 0) or 0),
                  float(data.get('mobileAllowance', 0) or 0),
                  float(data.get('otherAllowance', 0) or 0),
                  str(date.today())))
            c2.commit()
        finally:
            c2.close()

    comm_pct = float(data.get('commissionPct', 0) or 0)
    if comm_pct:
        c3 = _conn()
        try:
            c3.execute("""
                INSERT INTO rep_commission_rules
                    (rep_id, base_commission_pct, accelerator_pct, target_bonus, effective_from, active)
                VALUES (?,?,?,?,?,1)
            """, (rep_id, comm_pct,
                  float(data.get('acceleratorPct', 0) or 0),
                  float(data.get('flatTargetBonus', 0) or 0),
                  str(date.today())))
            c3.commit()
        finally:
            c3.close()

    save_db()
    return get_rep(rep_id)


def update_rep(rep_id, data):
    """Update rep profile fields and/or salary/commission rules.
    Passing baseSalary creates a new salary record (deactivating the old one).
    Passing commissionPct creates a new commission rule (deactivating the old one).
    Passing pin re-hashes it with SHA-256.
    """
    rep = qry1("SELECT * FROM sales_reps WHERE id=?", (rep_id,))
    if not rep:
        raise ValueError(f"Rep not found: {rep_id}")
    mapping = {
        'name': 'name', 'phone': 'phone', 'email': 'email',
        'joinDate': 'joining_date', 'notes': 'notes',
        'status': 'status', 'designation': 'designation',
        'whatsapp_apikey': 'whatsapp_apikey',
        'zoneId': 'primary_zone_id',
    }
    set_parts, vals = [], []
    for k, col in mapping.items():
        if k in data:
            set_parts.append(f"{col}=?")
            vals.append(data[k])
    if 'pin' in data and data['pin']:
        set_parts.append("pin_hash=?")
        vals.append(hashlib.sha256(str(data['pin']).encode()).hexdigest())
    if set_parts:
        vals.append(rep_id)
        c = _conn()
        try:
            c.execute(f"UPDATE sales_reps SET {', '.join(set_parts)} WHERE id=?", vals)
            c.commit()
        finally:
            c.close()

    if 'baseSalary' in data:
        c = _conn()
        try:
            c.execute("UPDATE rep_salary_components SET active=0 WHERE rep_id=?", (rep_id,))
            c.execute("""
                INSERT INTO rep_salary_components
                    (rep_id, basic_salary, fuel_allowance, mobile_allowance,
                     other_allowance, effective_from, active)
                VALUES (?,?,?,?,?,?,1)
            """, (rep_id, float(data['baseSalary']),
                  float(data.get('fuelAllowance', 0) or 0),
                  float(data.get('mobileAllowance', 0) or 0),
                  float(data.get('otherAllowance', 0) or 0),
                  str(date.today())))
            c.commit()
        finally:
            c.close()

    if 'commissionPct' in data:
        c = _conn()
        try:
            c.execute("UPDATE rep_commission_rules SET active=0 WHERE rep_id=?", (rep_id,))
            c.execute("""
                INSERT INTO rep_commission_rules
                    (rep_id, base_commission_pct, accelerator_pct, target_bonus, effective_from, active)
                VALUES (?,?,?,?,?,1)
            """, (rep_id, float(data['commissionPct']),
                  float(data.get('acceleratorPct', 0) or 0),
                  float(data.get('flatTargetBonus', 0) or 0),
                  str(date.today())))
            c.commit()
        finally:
            c.close()

    save_db()
    return get_rep(rep_id)


def assign_rep_route(rep_id, route_id):
    """Assign a rep to a route (idempotent — silently succeeds if already assigned)."""
    if not qry1("SELECT id FROM sales_reps WHERE id=?", (rep_id,)):
        raise ValueError(f"Rep not found: {rep_id}")
    if not qry1("SELECT id FROM routes WHERE id=? AND active=1", (route_id,)):
        raise ValueError(f"Route not found: {route_id}")
    existing = qry1(
        "SELECT id FROM rep_routes WHERE rep_id=? AND route_id=? AND assigned_to IS NULL",
        (rep_id, route_id))
    if existing:
        return {'ok': True, 'message': 'Already assigned'}
    c = _conn()
    try:
        c.execute("INSERT INTO rep_routes (rep_id, route_id, assigned_from) VALUES (?,?,?)",
                  (rep_id, route_id, str(date.today())))
        c.commit()
    finally:
        c.close()
    save_db()
    return {'ok': True, 'repId': rep_id, 'routeId': route_id}


def unassign_rep_route(assign_id):
    """End a rep-route assignment by setting assigned_to=today on the rep_routes row."""
    c = _conn()
    try:
        c.execute("UPDATE rep_routes SET assigned_to=? WHERE id=?",
                  (str(date.today()), assign_id))
        c.commit()
    finally:
        c.close()
    save_db()
    return {'ok': True}


# ─────────────────────────────────────────────────────────────────
#  TARGETS & ADVANCES
# ─────────────────────────────────────────────────────────────────

def set_rep_target(rep_id, data):
    """Upsert monthly targets for a rep. Required: period (YYYY-MM).
    Optional: targetAmount (revenue target in PKR), visitTarget (int).
    """
    period = (data.get('period') or data.get('month', '') or '').strip()
    if not period:
        raise ValueError("period (YYYY-MM) is required")
    revenue_target = float(data.get('targetAmount') or data.get('revenueTarget', 0))
    visit_target   = int(data.get('visitTarget', 0) or 0)
    existing = qry1("SELECT id FROM rep_targets WHERE rep_id=? AND month=?", (rep_id, period))
    c = _conn()
    try:
        if existing:
            c.execute("UPDATE rep_targets SET revenue_target=?, visit_target=? WHERE id=?",
                      (revenue_target, visit_target, existing['id']))
        else:
            c.execute("""
                INSERT INTO rep_targets (rep_id, month, revenue_target, visit_target)
                VALUES (?,?,?,?)
            """, (rep_id, period, revenue_target, visit_target))
        c.commit()
    finally:
        c.close()
    save_db()
    return qry1("SELECT * FROM rep_targets WHERE rep_id=? AND month=?", (rep_id, period))


def record_advance(rep_id, data):
    """Record a salary advance for a rep. Required: amount (PKR > 0).
    Optional: advanceDate (YYYY-MM-DD), monthlyRecovery (PKR/month), notes.
    """
    amount = float(data.get('amount', 0) or 0)
    if amount <= 0:
        raise ValueError("Amount must be positive")
    monthly_recovery = float(data.get('monthlyRecovery', 0) or 0)
    c = _conn()
    try:
        c.execute("""
            INSERT INTO rep_advances
                (rep_id, advance_date, amount, monthly_recovery, outstanding, notes)
            VALUES (?,?,?,?,?,?)
        """, (rep_id, data.get('advanceDate', str(date.today())),
              amount, monthly_recovery, amount, data.get('notes', '')))
        c.commit()
        adv_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        c.close()
    save_db()
    return qry1("SELECT * FROM rep_advances WHERE id=?", (adv_id,))


# ─────────────────────────────────────────────────────────────────
#  BEAT VISITS
# ─────────────────────────────────────────────────────────────────

def _coerce_geo(data):
    """Pull optional lat/lng/accuracy from a request payload → (lat, lng, acc) floats
    or (None, None, None) if absent/invalid. Used to geo-stamp visits and orders."""
    def _f(v):
        if v in (None, ''):
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None
    lat = _f(data.get('lat'))
    lng = _f(data.get('lng'))
    acc = _f(data.get('accuracy') if data.get('accuracy') is not None else data.get('geo_accuracy_m'))
    if lat is None or lng is None or not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
        return None, None, None
    return lat, lng, acc


def record_beat_visit(data):
    """Log a beat visit. Required: repId, customerId. routeId OPTIONAL — a rep can
    log a visit to any shop even if it isn't on a formal route (route_id 0 = none).
    Optional: visitDate (YYYY-MM-DD, default today), outcome (default 'visited'), notes,
    and lat/lng/accuracy (geo-stamp — Track B). If the shop has no coordinate yet, the
    first geo-stamped visit also sets the shop's location (first visit = the pin)."""
    rep_id     = data.get('repId')
    route_id   = data.get('routeId') or 0
    cust_id    = data.get('customerId')
    visit_date = data.get('visitDate', str(date.today()))
    outcome    = data.get('outcome', 'visited')
    notes      = data.get('notes', '')
    if not rep_id or not cust_id:
        raise ValueError("repId and customerId are required")
    lat, lng, acc = _coerce_geo(data)
    c = _conn()
    try:
        cols = [r[1] for r in c.execute("PRAGMA table_info(beat_visits)").fetchall()]
        if 'lat' in cols:
            c.execute("""
                INSERT INTO beat_visits (rep_id, route_id, customer_id, visit_date, outcome,
                                         payment_collected, notes, lat, lng, geo_accuracy_m)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (int(rep_id), int(route_id), int(cust_id), visit_date, outcome, 0, notes,
                  lat, lng, acc))
        else:
            c.execute("""
                INSERT INTO beat_visits (rep_id, route_id, customer_id, visit_date, outcome, payment_collected, notes)
                VALUES (?,?,?,?,?,?,?)
            """, (int(rep_id), int(route_id), int(cust_id), visit_date, outcome, 0, notes))
        c.commit()
        visit_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        c.close()
    # Opportunistic: first geo-stamped visit sets the shop's location if it has none.
    if lat is not None:
        try:
            from modules.customers import set_shop_location
            set_shop_location(int(cust_id), lat, lng, acc, force=False)
        except Exception:
            pass
    save_db()
    return qry1("SELECT * FROM beat_visits WHERE id=?", (visit_id,))


# ─────────────────────────────────────────────────────────────────
#  FIELD ORDERS
# ─────────────────────────────────────────────────────────────────

# create_field_order() REMOVED (P1-1, 2026-07-05): the legacy field_orders WRITE path was
# AR-only (create_invoice, no FG decrement / no sales row / no COGS) and unused by the app,
# which now books rep sales as rep_assisted customer_orders through the real invoice engine.
# get_field_order()/list_field_orders() are KEPT — the admin Field Orders list still reads them.


def get_field_order(order_id):
    """Return a single legacy field order with its items, rep name, customer name, and route name."""
    order = qry1("""
        SELECT fo.*, sr.name as rep_name, c.name as customer_name,
               r.name as route_name
        FROM field_orders fo
        JOIN sales_reps sr ON sr.id=fo.rep_id
        JOIN customers  c  ON c.id=fo.customer_id
        LEFT JOIN routes r ON r.id=fo.route_id
        WHERE fo.id=?
    """, (order_id,))
    if not order:
        return None
    order['items'] = qry("""
        SELECT foi.*, pv.sku_code, p.name as product_name, ps.label as pack_size
        FROM field_order_items foi
        JOIN product_variants pv ON pv.id=foi.product_variant_id
        JOIN products p          ON p.id=pv.product_id
        JOIN pack_sizes ps       ON ps.id=pv.pack_size_id
        WHERE foi.order_id=?
    """, (order_id,))
    return order


def list_field_orders(rep_id=None, status=None, date_from=None, date_to=None):
    """
    Returns portal orders (order.html, order_source='rep_assisted') UNION ALL
    legacy field_orders. Status 'draft' normalised to 'pending'.
    """
    portal_wheres = ["co.order_source='rep_assisted'"]
    portal_params = []
    if rep_id:
        portal_wheres.append("co.created_by_rep_id=?")
        portal_params.append(int(rep_id))
    if status == 'pending':
        portal_wheres.append("co.status IN ('draft','pending_review')")
    elif status == 'confirmed':
        portal_wheres.append("co.status IN ('confirmed','invoiced','partially_invoiced')")
    elif status == 'cancelled':
        portal_wheres.append("co.status='cancelled'")
    if date_from:
        portal_wheres.append("co.order_date>=?"); portal_params.append(date_from)
    if date_to:
        portal_wheres.append("co.order_date<=?"); portal_params.append(date_to)

    portal_sql = """
        SELECT
            co.id,
            co.order_number AS order_ref,
            co.order_date,
            CASE
                WHEN co.status IN ('draft','pending_review') THEN 'pending'
                WHEN co.status IN ('confirmed','invoiced','partially_invoiced') THEN 'confirmed'
                ELSE co.status
            END AS status,
            sr.name AS rep_name,
            c.name  AS customer_name,
            NULL    AS route_name,
            COALESCE((
                SELECT SUM(coi.qty_ordered * coi.unit_price)
                FROM customer_order_items coi WHERE coi.order_id = co.id
            ), 0) AS order_total,
            (SELECT inv.id FROM invoices inv
             WHERE inv.customer_order_id = co.id ORDER BY inv.id DESC LIMIT 1) AS invoice_id,
            co.notes,
            'portal' AS _source
        FROM customer_orders co
        LEFT JOIN sales_reps sr ON sr.id = co.created_by_rep_id
        LEFT JOIN customers  c  ON c.id  = co.customer_id
        WHERE {portal_where}
    """.format(portal_where=' AND '.join(portal_wheres))

    legacy_wheres = []
    legacy_params = []
    if rep_id:
        legacy_wheres.append("fo.rep_id=?"); legacy_params.append(int(rep_id))
    if status:
        legacy_wheres.append("fo.status=?"); legacy_params.append(status)
    if date_from:
        legacy_wheres.append("fo.order_date>=?"); legacy_params.append(date_from)
    if date_to:
        legacy_wheres.append("fo.order_date<=?"); legacy_params.append(date_to)
    legacy_where_str = ("WHERE " + " AND ".join(legacy_wheres)) if legacy_wheres else ""

    legacy_sql = """
        SELECT
            fo.id,
            fo.order_ref,
            fo.order_date,
            fo.status,
            sr.name AS rep_name,
            c.name  AS customer_name,
            r.name  AS route_name,
            COALESCE((
                SELECT SUM(quantity * unit_price) FROM field_order_items WHERE order_id = fo.id
            ), 0) AS order_total,
            COALESCE(fo.confirmed_invoice_id, fo.invoice_id) AS invoice_id,
            fo.notes,
            'legacy' AS _source
        FROM field_orders fo
        JOIN sales_reps sr ON sr.id = fo.rep_id
        JOIN customers  c  ON c.id  = fo.customer_id
        LEFT JOIN routes r ON r.id  = fo.route_id
        {legacy_where}
    """.format(legacy_where=legacy_where_str)

    union_sql = f"""
        SELECT * FROM ({portal_sql} UNION ALL {legacy_sql})
        ORDER BY order_date DESC, id DESC LIMIT 200
    """
    return qry(union_sql, portal_params + legacy_params)


# confirm_field_order() REMOVED (P1-1, 2026-07-05): it created an AR-only invoice via
# invoices.create_invoice — no FG-stock decrement, no sales row, no COGS — so a confirmed
# legacy field order billed the customer but never moved inventory or hit the dashboard.
# Rep sales now flow through orders.generate_invoice_from_order (the correct engine).


# ─────────────────────────────────────────────────────────────────
#  PAYROLL
# ─────────────────────────────────────────────────────────────────

def calculate_payroll(rep_id, period):
    """Calculate payroll for a rep/period. Does NOT save."""
    rep = qry1("SELECT * FROM sales_reps WHERE id=?", (rep_id,))
    if not rep:
        raise ValueError(f"Rep not found: {rep_id}")

    salary_comp = qry1("""
        SELECT * FROM rep_salary_components
        WHERE rep_id=? AND active=1 AND effective_from<=?
        ORDER BY effective_from DESC LIMIT 1
    """, (rep_id, period + '-31'))
    base_salary      = float(salary_comp['basic_salary']     or 0) if salary_comp else 0.0
    fuel_allowance   = float(salary_comp['fuel_allowance']   or 0) if salary_comp else 0.0
    mobile_allowance = float(salary_comp['mobile_allowance'] or 0) if salary_comp else 0.0
    other_allowance  = float(salary_comp['other_allowance']  or 0) if salary_comp else 0.0
    total_fixed = base_salary + fuel_allowance + mobile_allowance + other_allowance

    comm_rule = qry1("""
        SELECT * FROM rep_commission_rules WHERE rep_id=? AND active=1
        ORDER BY effective_from DESC LIMIT 1
    """, (rep_id,))
    base_comm_pct   = float(comm_rule['base_commission_pct'] or 0) if comm_rule else 0.0
    accelerator_pct = float(comm_rule.get('accelerator_pct') or 0) if comm_rule else 0.0
    flat_bonus      = float(comm_rule.get('target_bonus')    or 0) if comm_rule else 0.0

    target_row    = qry1("SELECT * FROM rep_targets WHERE rep_id=? AND month=?", (rep_id, period))
    target_amount = float(target_row['revenue_target'] or 0) if target_row else 0.0

    sales_row    = qry1("""
        SELECT COALESCE(SUM(foi.quantity * foi.unit_price), 0) as total_sales
        FROM field_orders fo
        JOIN field_order_items foi ON foi.order_id=fo.id
        WHERE fo.rep_id=? AND fo.order_date LIKE ? AND fo.status='confirmed'
    """, (rep_id, period + '%'))
    actual_sales = float(sales_row['total_sales'] or 0) if sales_row else 0.0

    base_commission   = r2(actual_sales * base_comm_pct / 100.0)
    accelerator_bonus = 0.0
    target_bonus      = 0.0
    if target_amount > 0 and actual_sales >= target_amount:
        above_target      = actual_sales - target_amount
        accelerator_bonus = r2(above_target * accelerator_pct / 100.0)
        target_bonus      = flat_bonus
    total_commission = r2(base_commission + accelerator_bonus + target_bonus)

    advances_row   = qry1("""
        SELECT COALESCE(SUM(amount),0) as total_advances
        FROM rep_advances
        WHERE rep_id=? AND advance_date LIKE ? AND recovered=0
    """, (rep_id, period + '%'))
    total_advances = float(advances_row['total_advances'] or 0) if advances_row else 0.0

    gross = r2(total_fixed + total_commission)
    net   = r2(gross - total_advances)

    return {
        'repId':            rep_id,
        'repName':          rep['name'],
        'period':           period,
        'baseSalary':       base_salary,
        'fuelAllowance':    fuel_allowance,
        'mobileAllowance':  mobile_allowance,
        'otherAllowance':   other_allowance,
        'totalFixed':       total_fixed,
        'actualSales':      actual_sales,
        'targetAmount':     target_amount,
        'baseCommission':   base_commission,
        'acceleratorBonus': accelerator_bonus,
        'targetBonus':      target_bonus,
        'totalCommission':  total_commission,
        'totalAdvances':    total_advances,
        'grossPay':         gross,
        'netPay':           net,
    }


def run_payroll(period, rep_ids=None):
    """Compute and save draft payroll rows for all active reps (or a subset via rep_ids list).
    Skips reps already finalized for this period. Overwrites existing draft rows.
    Returns list of payroll dicts with status='draft' or 'already_finalized'.
    """
    all_reps = qry("SELECT id FROM sales_reps WHERE (status IS NULL OR status='active')")
    if rep_ids:
        all_reps = [r for r in all_reps if r['id'] in rep_ids]
    results = []
    c = _conn()
    try:
        for rep_row in all_reps:
            calc = calculate_payroll(rep_row['id'], period)
            existing = c.execute(
                "SELECT id FROM payroll_runs WHERE rep_id=? AND period=? AND status='final'",
                (rep_row['id'], period)
            ).fetchone()
            if existing:
                calc['status'] = 'already_finalized'
                results.append(calc)
                continue
            prev = c.execute(
                "SELECT id FROM payroll_runs WHERE rep_id=? AND period=?",
                (rep_row['id'], period)
            ).fetchone()
            if prev:
                c.execute("""
                    UPDATE payroll_runs SET
                        base_salary=?, actual_sales=?, target_amount=?,
                        base_commission=?, accelerator_bonus=?, target_bonus=?,
                        total_commission=?, total_advances=?, gross_pay=?, net_pay=?,
                        status='draft', run_at=datetime('now')
                    WHERE id=?
                """, (calc['baseSalary'], calc['actualSales'], calc['targetAmount'],
                      calc['baseCommission'], calc['acceleratorBonus'], calc['targetBonus'],
                      calc['totalCommission'], calc['totalAdvances'],
                      calc['grossPay'], calc['netPay'], prev[0]))
            else:
                c.execute("""
                    INSERT INTO payroll_runs
                        (rep_id, month, period, base_salary, actual_sales, target_amount,
                         base_commission, accelerator_bonus, target_bonus,
                         total_commission, total_advances, gross_pay, net_pay, status,
                         run_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'draft',datetime('now'))
                """, (rep_row['id'], period, period,
                      calc['baseSalary'], calc['actualSales'], calc['targetAmount'],
                      calc['baseCommission'], calc['acceleratorBonus'], calc['targetBonus'],
                      calc['totalCommission'], calc['totalAdvances'],
                      calc['grossPay'], calc['netPay']))
            calc['status'] = 'draft'
            results.append(calc)
        c.commit()
    finally:
        c.close()
    save_db()
    return results


def finalize_payroll(rep_id, period):
    """Finalize a draft payroll run: marks it 'final' and marks all period advances as recovered."""
    run = qry1("SELECT * FROM payroll_runs WHERE rep_id=? AND period=?", (rep_id, period))
    if not run:
        raise ValueError("No payroll run found for this rep/period")
    if run['status'] == 'final':
        raise ValueError("Already finalized")
    c = _conn()
    try:
        c.execute("""
            UPDATE rep_advances SET recovered=1
            WHERE rep_id=? AND advance_date LIKE ? AND recovered=0
        """, (rep_id, period + '%'))
        c.execute("UPDATE payroll_runs SET status='final' WHERE rep_id=? AND period=?",
                  (rep_id, period))
        c.commit()
    finally:
        c.close()
    save_db()
    return qry1("SELECT * FROM payroll_runs WHERE rep_id=? AND period=?", (rep_id, period))


def list_payroll_runs(period=None):
    """List payroll runs with rep names. Pass period (YYYY-MM) to filter; otherwise returns last 200."""
    if period:
        return qry("""
            SELECT pr.*, sr.name as rep_name
            FROM payroll_runs pr JOIN sales_reps sr ON sr.id=pr.rep_id
            WHERE pr.period=? ORDER BY sr.name
        """, (period,))
    return qry("""
        SELECT pr.*, sr.name as rep_name
        FROM payroll_runs pr JOIN sales_reps sr ON sr.id=pr.rep_id
        ORDER BY pr.period DESC, sr.name LIMIT 200
    """)


# ─────────────────────────────────────────────────────────────────
#  FIELD APP HELPERS
# ─────────────────────────────────────────────────────────────────

def get_rep_today_route(rep_id):
    """Get today's route info and stop list for the field app."""
    from modules.customers import list_route_customers
    today_day = date.today().strftime('%A')[:3].lower()
    routes = qry("""
        SELECT r.*, z.name as zone_name
        FROM rep_routes rr
        JOIN routes r ON r.id=rr.route_id
        JOIN zones  z ON z.id=r.zone_id
        WHERE rr.rep_id=? AND rr.assigned_to IS NULL
          AND (r.visit_days LIKE ? OR r.visit_days LIKE '%all%')
    """, (rep_id, f'%{today_day}%'))
    return [{**route, 'stops': list_route_customers(route['id'])} for route in routes]


def field_get_products(customer_type='RETAIL'):
    """Return active + LIVE products/variants as a flat list with price for the given
    customer type. Honours the same "Live/Off" (show_online) toggle as the website —
    a SKU switched Off in the ERP disappears from the field app too. Never exposes
    mfg_cost or ex_factory prices.
    """
    type_map      = {'RETAIL': 'retail_mrp', 'DIRECT': 'distributor', 'WHOLESALE': 'distributor'}
    price_type_cd = type_map.get((customer_type or 'RETAIL').upper(), 'retail_mrp')
    products      = qry("SELECT * FROM products WHERE active=1 ORDER BY name")
    result        = []
    for prod in products:
        variants = qry("""
            SELECT pv.id, pv.sku_code, ps.label AS pack_size, ps.grams,
                   pp.price
            FROM product_variants pv
            JOIN pack_sizes ps ON ps.id = pv.pack_size_id
            LEFT JOIN product_prices pp
                   ON pp.product_variant_id = pv.id
                  AND pp.active_flag = 1
                  AND pp.price_type_id = (SELECT id FROM price_types WHERE code=?)
            WHERE pv.product_id=? AND pv.active_flag=1
              AND COALESCE(pv.show_online, 0) = 1
            ORDER BY ps.grams
        """, (price_type_cd, prod['id']))
        for v in variants:
            result.append({
                'variant_id':   v['id'],
                'product_code': prod['code'],
                'product_name': prod['name'],
                'sku_code':     v['sku_code'],
                'pack_size':    v['pack_size'],
                'grams':        v['grams'],
                'price':        v['price'],
            })
    return result


# ─────────────────────────────────────────────────────────────────
#  OUT-OF-ROUTE DETECTION (callback wired to orders.py)
# ─────────────────────────────────────────────────────────────────

def _is_out_of_route(rep_id, customer_id):
    """Out of route if the customer's zone is not among the rep's assigned zones.
    Uses the multi-zone rep_zones set; falls back to the single primary_zone_id for
    reps not yet migrated. If the rep has no zones at all, nothing is flagged."""
    customer = qry1("SELECT zone_id FROM customers WHERE id=?", (customer_id,))
    if not customer or not customer.get('zone_id'):
        return False
    cust_zone = int(customer['zone_id'])
    zones = []
    try:
        zones = [int(r['zone_id']) for r in qry("SELECT zone_id FROM rep_zones WHERE rep_id=?", (rep_id,))]
    except Exception:
        zones = []
    if not zones:
        rep = qry1("SELECT primary_zone_id FROM sales_reps WHERE id=?", (rep_id,))
        if rep and rep.get('primary_zone_id'):
            zones = [int(rep['primary_zone_id'])]
    if not zones:
        return False
    return cust_zone not in zones


def set_rep_zones(rep_id, zone_ids):
    """Replace a rep's assigned zones (multi-zone coverage). Also sets primary_zone_id
    to the first zone for back-compat (planning forecast rollup). zone_ids: list of ints."""
    rep = qry1("SELECT id FROM sales_reps WHERE id=?", (rep_id,))
    if not rep:
        raise ValueError("Rep not found")
    ids = []
    for z in (zone_ids or []):
        try:
            zi = int(z)
        except (TypeError, ValueError):
            continue
        if zi and zi not in ids:
            ids.append(zi)
    c = _conn()
    try:
        c.execute("DELETE FROM rep_zones WHERE rep_id=?", (rep_id,))
        for zi in ids:
            c.execute("INSERT OR IGNORE INTO rep_zones (rep_id, zone_id) VALUES (?,?)", (rep_id, zi))
        c.execute("UPDATE sales_reps SET primary_zone_id=? WHERE id=?", (ids[0] if ids else None, rep_id))
        c.commit()
    finally:
        c.close()
    save_db()
    return get_rep(rep_id)


def set_rep_app_access(rep_id, batch=None, field=None):
    """Grant/revoke the phone-app sections for a rep's phone identity (one login fronts both).
    Pass batch and/or field as bools; only the ones provided are changed."""
    rep = qry1("SELECT id FROM sales_reps WHERE id=?", (rep_id,))
    if not rep:
        raise ValueError("Rep not found")
    sets, params = [], []
    if batch is not None:
        sets.append("app_batch=?"); params.append(1 if batch else 0)
    if field is not None:
        sets.append("app_field=?"); params.append(1 if field else 0)
    if sets:
        params.append(rep_id)
        run("UPDATE sales_reps SET " + ", ".join(sets) + " WHERE id=?", params)
        save_db()
    return get_rep(rep_id)


def _wa_notify_out_of_route(order_id, rep_id):
    """WhatsApp admin + rep's manager when a rep places an order outside their assigned route."""
    from modules.orders import _wa_admin, _wa_send
    order    = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    rep      = qry1("SELECT * FROM sales_reps WHERE id=?", (rep_id,))
    customer = qry1("SELECT * FROM customers WHERE id=?", (order['customer_id'],)) if order else None
    if not order or not rep or not customer:
        return
    msg = (
        f"⚠️ OUT-OF-ROUTE ORDER ALERT\n"
        f"Rep: {rep['name']} ({rep['phone']})\n"
        f"Customer: {customer['name']} ({customer.get('account_number','')})\n"
        f"Order: {order['order_number']}\n"
        f"City: {customer.get('city','')}\n"
        f"This customer is NOT on the rep's assigned routes."
    )
    _wa_admin(msg)
    if rep.get('reporting_to'):
        manager = qry1("SELECT * FROM sales_reps WHERE id=?", (rep['reporting_to'],))
        if manager and manager.get('whatsapp_apikey') and manager.get('phone'):
            _wa_send(manager['phone'], manager['whatsapp_apikey'], msg)


# ─────────────────────────────────────────────────────────────────
#  DIRECT SALES
# ─────────────────────────────────────────────────────────────────

def _unit_cost_to_make(product_code, pack_size, variant_id):
    """Cost to make ONE pack, for COGS at sale time. Prefers the BOM-computed
    standard cost (so the dashboard margin uses real cost); falls back to the
    typed `mfg_cost` price-book entry, then 0."""
    try:
        from modules.costing import compute_standard_cost
        sc = compute_standard_cost(product_code, pack_size)
        if sc and (sc.get('cost_to_make') or 0) > 0:
            return r2(sc['cost_to_make'])
    except Exception:
        pass
    try:
        mfg = qry1("SELECT id FROM price_types WHERE code='mfg_cost'")
        if mfg:
            cp = qry1("""SELECT price FROM product_prices
                         WHERE product_variant_id=? AND price_type_id=? AND active_flag=1
                         ORDER BY effective_from DESC LIMIT 1""", (variant_id, mfg['id']))
            if cp:
                return r2(cp['price'])
    except Exception:
        pass
    return 0.0


def create_sale(data):
    """
    Create a single-line sale + invoice.
    data: {custCode, productCode, packSize, qty, unitPrice, saleDate, notes}
    """
    validate_fields(data, [
        {'field': 'custCode',    'label': 'Customer',     'type': 'str'},
        {'field': 'productCode', 'label': 'Product code', 'type': 'str'},
        {'field': 'packSize',    'label': 'Pack size',    'type': 'str'},
        {'field': 'qty',         'label': 'Quantity',     'type': 'int', 'min': 1},
        {'field': 'unitPrice',   'label': 'Unit price',   'type': 'float', 'min': 0},
        {'field': 'saleDate',    'label': 'Sale date',    'required': False, 'type': 'date'},
    ])
    from modules.inventory import get_finished_stock_map

    cust = _lookup_customer_by_code(data.get('custCode', ''))
    if not cust:
        raise ValueError(f"Customer not found: {data.get('custCode')}")

    var = _lookup_variant_by_sku_pair(data.get('productCode', ''), data.get('packSize', ''))
    if not var:
        raise ValueError(f"Product variant not found: {data.get('productCode')}/{data.get('packSize')}")

    qty        = int(data.get('qty', 0))
    unit_price = r2(data.get('unitPrice', 0))
    if qty <= 0:
        raise ValueError("Quantity must be positive")
    if unit_price < 0:
        raise ValueError("Unit price cannot be negative")

    fg_stock = get_finished_stock_map()
    avail    = fg_stock.get(var['id'], 0)
    if qty > avail:
        raise ValueError(f"Insufficient finished goods: {avail:.0f} units available, {qty} requested")

    sale_date = data.get('saleDate', today())
    total     = r2(qty * unit_price)
    terms     = int(cust.get('payment_terms_days', 30))
    try:
        due_date = (date.fromisoformat(sale_date) + timedelta(days=terms)).isoformat()
    except Exception:
        due_date = sale_date

    cogs_price   = r2(_unit_cost_to_make(data.get('productCode', ''), data.get('packSize', ''), var['id']) * qty)
    gross_profit = r2(total - cogs_price)

    _sync_counter_to_max('sale',    'sales',    'sale_id',        'SP-SALE-')
    _sync_counter_to_max('invoice', 'invoices', 'invoice_number', 'SP-INV-')
    sale_id = next_id('sale', 'SALE')
    inv_num = next_id('invoice', 'INV')

    c = _conn()
    try:
        c.execute("""
            INSERT INTO invoices (invoice_number, customer_id, invoice_date, due_date, status)
            VALUES (?,?,?,?,'UNPAID')
        """, (inv_num, cust['id'], sale_date, due_date))
        inv_db_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        c.execute("""
            INSERT INTO sales
                (sale_id, sale_date, customer_id, cust_code, cust_name, customer_type,
                 product_variant_id, product_code, product_name, pack_size,
                 qty, unit_price, total, cogs, gross_profit, invoice_id, notes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (sale_id, sale_date, cust['id'], cust['code'], cust['name'],
              cust.get('customer_type', 'RETAIL'),
              var['id'], var['product_code'], var['product_name'], var['pack_size'],
              qty, unit_price, total, cogs_price, gross_profit, inv_db_id,
              data.get('notes', '')))

        c.execute("""
            INSERT INTO invoice_items
                (invoice_id, product_variant_id, product_code, product_name,
                 pack_size, quantity, unit_price, line_total)
            VALUES (?,?,?,?,?,?,?,?)
        """, (inv_db_id, var['id'], var['product_code'], var['product_name'],
              var['pack_size'], qty, unit_price, total))

        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('sales',?,'INSERT',?)
        """, (sale_id, json.dumps({'custCode': data.get('custCode'),
              'productCode': data.get('productCode'), 'qty': qty, 'total': total})))

        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()
    return {'saleId': sale_id, 'invoiceNumber': inv_num, 'total': total, 'invoiceId': inv_db_id}


def create_multi_sale(data):
    """
    Create a multi-line sale + single invoice.
    data: {custCode, saleDate, notes, lines: [{productCode, packSize, qty, unitPrice}]}
    """
    from modules.inventory import get_finished_stock_map
    from modules.orders    import _enforce_credit_limit

    cust = _lookup_customer_by_code(data.get('custCode', ''))
    if not cust:
        raise ValueError(f"Customer not found: {data.get('custCode')}")

    lines = data.get('lines', [])
    if not lines:
        raise ValueError("At least one line item is required")

    sale_date = data.get('saleDate', today())
    notes     = data.get('notes', '')
    terms     = int(cust.get('payment_terms_days', 30))
    try:
        due_date = (date.fromisoformat(sale_date) + timedelta(days=terms)).isoformat()
    except Exception:
        due_date = sale_date

    fg_stock = get_finished_stock_map()

    resolved = []
    for i, line in enumerate(lines):
        var = _lookup_variant_by_sku_pair(line.get('productCode', ''), line.get('packSize', ''))
        if not var:
            raise ValueError(f"Line {i+1}: Product variant not found: {line.get('productCode')}/{line.get('packSize')}")
        qty        = int(line.get('qty', 0))
        unit_price = r2(line.get('unitPrice', 0))
        if qty <= 0:
            raise ValueError(f"Line {i+1}: Quantity must be positive")
        if unit_price < 0:
            raise ValueError(f"Line {i+1}: Unit price cannot be negative")
        avail = fg_stock.get(var['id'], 0)
        if qty > avail:
            raise ValueError(
                f"Line {i+1} ({var['product_name']} {var['pack_size']}): "
                f"Insufficient stock — {avail:.0f} available, {qty} requested")
        cogs_price   = r2(_unit_cost_to_make(line.get('productCode', ''), line.get('packSize', ''), var['id']) * qty)
        line_total   = r2(qty * unit_price)
        gross_profit = r2(line_total - cogs_price)
        resolved.append({'var': var, 'qty': qty, 'unit_price': unit_price,
                         'line_total': line_total, 'cogs': cogs_price,
                         'gross_profit': gross_profit})

    invoice_total = r2(sum(r['line_total'] for r in resolved))
    _enforce_credit_limit(cust['id'], invoice_total)

    # Generate all IDs before opening transaction (WAL deadlock prevention)
    _sync_counter_to_max('invoice', 'invoices', 'invoice_number', 'SP-INV-')
    _sync_counter_to_max('sale',    'sales',    'sale_id',        'SP-SALE-')
    inv_num  = next_id('invoice', 'INV')
    sale_ids = [next_id('sale', 'SALE') for _ in resolved]

    c = _conn()
    try:
        c.execute("""INSERT INTO invoices (invoice_number, customer_id, invoice_date, due_date, status)
                     VALUES (?,?,?,?,'UNPAID')""",
                  (inv_num, cust['id'], sale_date, due_date))
        inv_db_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        for sale_id, r in zip(sale_ids, resolved):
            var = r['var']
            c.execute("""INSERT INTO sales
                (sale_id, sale_date, customer_id, cust_code, cust_name, customer_type,
                 product_variant_id, product_code, product_name, pack_size,
                 qty, unit_price, total, cogs, gross_profit, invoice_id, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (sale_id, sale_date, cust['id'], cust['code'], cust['name'],
                 cust.get('customer_type', 'RETAIL'),
                 var['id'], var['product_code'], var['product_name'], var['pack_size'],
                 r['qty'], r['unit_price'], r['line_total'], r['cogs'], r['gross_profit'],
                 inv_db_id, notes))
            c.execute("""INSERT INTO invoice_items
                (invoice_id, product_variant_id, product_code, product_name,
                 pack_size, quantity, unit_price, line_total)
                VALUES (?,?,?,?,?,?,?,?)""",
                (inv_db_id, var['id'], var['product_code'], var['product_name'],
                 var['pack_size'], r['qty'], r['unit_price'], r['line_total']))

        c.execute("""INSERT INTO change_log (table_name, record_id, action, new_value)
                     VALUES ('sales',?,'INSERT',?)""",
                  (inv_num, json.dumps({'custCode': data.get('custCode'),
                   'lines': len(resolved), 'total': invoice_total})))
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()
    return {'invoiceNumber': inv_num, 'invoiceId': inv_db_id,
            'total': invoice_total, 'saleIds': sale_ids, 'lineCount': len(resolved)}


# ─────────────────────────────────────────────────────────────────
#  WHATSAPP OTP LOGIN
# ─────────────────────────────────────────────────────────────────

def send_field_otp(phone):
    """Generate and send a 6-digit OTP via CallMeBot WhatsApp.
    Rate-limited to 3 requests per phone per 10 minutes.
    Raises ValueError if rep not found, no WA API key, or rate limited.
    """
    rep = qry1("SELECT * FROM sales_reps WHERE phone=? AND status='active'", (phone,))
    if not rep:
        raise ValueError("No active rep found with this phone number")
    if not rep.get('whatsapp_apikey'):
        raise ValueError("WhatsApp OTP not configured for this number. Contact admin.")

    # Rate limit: max 3 OTPs per phone per 10 minutes
    recent = qry("""
        SELECT COUNT(*) as cnt FROM field_otp
        WHERE phone=? AND created_at > datetime('now', '-10 minutes')
    """, (phone,))
    if recent and recent[0]['cnt'] >= 3:
        raise ValueError("Too many OTP requests. Please wait 10 minutes and try again.")

    code = ''.join(random.choices(string.digits, k=6))

    c = _conn()
    try:
        c.execute("""INSERT INTO field_otp (phone, code, expires_at)
                     VALUES (?, ?, datetime('now', '+5 minutes'))""", (phone, code))
        c.commit()
    finally:
        c.close()

    from modules.orders import _wa_send
    msg = f"Your Spicetopia login code is: {code}\nValid for 5 minutes. Do not share this code."
    _wa_send(phone, rep['whatsapp_apikey'], msg)
    masked = phone[:3] + '****' + phone[-3:] if len(phone) > 6 else '****'
    return {'sent': True, 'phone': masked}


def verify_field_otp(phone, code):
    """Verify a WhatsApp OTP. Returns the sales_rep row on success.
    Raises ValueError if code invalid, expired, or already used.
    """
    row = qry1("""
        SELECT * FROM field_otp
        WHERE phone=? AND code=? AND used=0
          AND expires_at > datetime('now')
        ORDER BY id DESC LIMIT 1
    """, (phone, code))
    if not row:
        raise ValueError("Invalid or expired OTP. Please request a new code.")

    # Mark used immediately (single-use)
    c = _conn()
    try:
        c.execute("UPDATE field_otp SET used=1 WHERE id=?", (row['id'],))
        c.commit()
    finally:
        c.close()

    rep = qry1("SELECT * FROM sales_reps WHERE phone=? AND status='active'", (phone,))
    if not rep:
        raise ValueError("Rep account not found or inactive.")
    return rep


# ─────────────────────────────────────────────────────────────────
#  FIELD REP: ONE-TAP ORDER → INVOICE (on-the-spot, no review queue)
# ─────────────────────────────────────────────────────────────────

def _stamp_order_geo(order_id, data):
    """Geo-stamp a customer_order from a field payload (Track B) and, if the shop has no
    coordinate yet, set the shop location from where the order was taken. Best-effort:
    never raises (a geo failure must not fail an order)."""
    lat, lng, acc = _coerce_geo(data)
    if lat is None:
        return
    try:
        cols = [r['name'] for r in qry("PRAGMA table_info(customer_orders)")]
        if 'lat' in cols:
            run("UPDATE customer_orders SET lat=?, lng=?, geo_accuracy_m=? WHERE id=?",
                (lat, lng, acc, order_id))
    except Exception:
        pass
    try:
        cust = qry1("SELECT customer_id FROM customer_orders WHERE id=?", (order_id,))
        if cust and cust.get('customer_id'):
            from modules.customers import set_shop_location
            set_shop_location(int(cust['customer_id']), lat, lng, acc, force=False)
    except Exception:
        pass


def field_place_and_invoice(data, rep_id):
    """Field-rep one-tap (the DEFAULT action): create a rep_assisted customer order,
    confirm it, and invoice ALL lines through the standard engine
    (orders.generate_invoice_from_order).

    This routes field-app sales through the SAME pipe as the B2B portal, so a single
    action correctly updates: finished-goods inventory (decremented), the sales table
    (dashboard revenue + BOM-based COGS + gross profit), and the invoice + AR. No
    review queue — the rep produces the invoice on the spot. The engine still enforces
    finished-goods availability (blocks overselling) and the customer credit limit, so
    removing human review does NOT remove those guards.

    Graceful fallback: if invoicing fails (e.g. finished goods short), the ORDER is
    preserved and this returns {invoiced: False, orderId, ...} instead of raising, so
    the one-tap button degrades to a saved booking the rep can invoice later once stock
    is made — no lost order, no scary error. Lazy imports from modules.orders avoid a
    circular import at module load."""
    from modules.orders import (create_customer_order_external,
                                 confirm_customer_order,
                                 generate_invoice_from_order)
    payload = dict(data or {})
    payload['order_source'] = 'rep_assisted'
    if rep_id:
        payload['created_by_rep_id'] = rep_id

    order    = create_customer_order_external(payload)
    order_id = order.get('orderId') or order.get('id')
    if not order_id:
        raise ValueError("Order creation failed")
    _stamp_order_geo(order_id, payload)

    order_number = order.get('orderNumber') or order.get('order_number')
    out_of_route = bool(order.get('outOfRoute', False))

    try:
        confirm_customer_order(order_id)
        items = qry("SELECT id, qty_ordered FROM customer_order_items WHERE order_id=?", (order_id,))
        lines = [{'orderItemId': it['id'], 'qty': it['qty_ordered']} for it in items]
        if not lines:
            raise ValueError("Order has no items to invoice")
        inv = generate_invoice_from_order(order_id, {
            'lines':       lines,
            'invoiceDate': payload.get('orderDate') or str(date.today()),
        })
    except ValueError as e:
        # Booking succeeded, invoicing did not (usually FG short / credit) → keep the
        # order, tell the app it can invoice later. NOT an error the rep should fear.
        return {
            'orderId':      order_id,
            'orderNumber':  order_number,
            'invoiced':     False,
            'invoiceError': str(e),
            'outOfRoute':   out_of_route,
        }

    return {
        'orderId':       order_id,
        'orderNumber':   order_number,
        'invoiced':      True,
        'invoiceId':     inv.get('invoiceId'),
        'invoiceNumber': inv.get('invoiceNumber'),
        'total':         inv.get('total'),
        'outOfRoute':    out_of_route,
    }


def field_place_order(data, rep_id):
    """Field-rep: PLACE an order only (a booking) — creates a rep_assisted customer
    order and stops. Does NOT confirm, invoice, or touch inventory, so it always
    succeeds even when finished-goods stock is zero. Invoice it afterwards via
    field_invoice_order (or later in the ERP). Geo-stamps the order if coords are sent."""
    from modules.orders import create_customer_order_external
    payload = dict(data or {})
    payload['order_source'] = 'rep_assisted'
    if rep_id:
        payload['created_by_rep_id'] = rep_id
    order    = create_customer_order_external(payload)
    order_id = order.get('orderId') or order.get('id')
    if not order_id:
        raise ValueError("Order creation failed")
    _stamp_order_geo(order_id, payload)
    return {
        'orderId':     order_id,
        'orderNumber': order.get('orderNumber') or order.get('order_number'),
        'outOfRoute':  bool(order.get('outOfRoute', False)),
    }


def field_invoice_order(order_id, rep_id):
    """Field-rep: invoice a placed order. Confirms it if needed, then runs
    generate_invoice_from_order for all still-uninvoiced lines (FG-stock check +
    decrement + sales row/COGS + AR). Any field rep may invoice any order — reps
    are trusted internal staff (no per-rep ownership lock). If stock is short it
    raises (the order is preserved — invoice it later once stock is made)."""
    from modules.orders import confirm_customer_order, generate_invoice_from_order
    order = qry1("SELECT id, status FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] in ('cancelled', 'invoiced'):
        raise ValueError(f"Order cannot be invoiced ({order['status']})")
    if order['status'] in ('draft', 'pending_review'):
        confirm_customer_order(order_id)

    items = qry("SELECT id, qty_ordered, qty_invoiced FROM customer_order_items WHERE order_id=?", (order_id,))
    lines = [{'orderItemId': it['id'], 'qty': it['qty_ordered'] - it['qty_invoiced']}
             for it in items if (it['qty_ordered'] - it['qty_invoiced']) > 0]
    if not lines:
        raise ValueError("Nothing left to invoice on this order")

    inv = generate_invoice_from_order(order_id, {'lines': lines, 'invoiceDate': str(date.today())})
    return {
        'orderId':       order_id,
        'invoiceId':     inv.get('invoiceId'),
        'invoiceNumber': inv.get('invoiceNumber'),
        'total':         inv.get('total'),
    }
