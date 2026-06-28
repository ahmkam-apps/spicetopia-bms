"""
modules/planning.py — Admin Planning Input System (logic + calculations).

M1 scope: plan_version CRUD, sales forecast + target CRUD, projected-sales calc.
Tables are created by the idempotent migrations in server.py
(ensure_planning_foundations / ensure_plan_version_horizon / ensure_plan_sales_tables).

Activated via `from modules.planning import *` in server.py (with the other modules).

Design rules honoured here:
  • Assumptions are data (these tables); calculations are code (this module) — never
    user-stored formulas. No recipe/BOM data touched.
  • Approved plan versions are immutable: edits raise — clone to a 'revised' version.
  • Every write records a change_log row (who/what) + the new `reason` column (why).
  • All planning endpoints are admin-gated at the API layer (server.py); revenue
    (money) is only ever returned to admins.
"""

import json

from modules.db import _conn, qry, qry1, save_db

__all__ = [
    'SCENARIO_TYPES', 'PLAN_CHANNELS',
    'list_plan_versions', 'get_plan_version', 'create_plan_version', 'update_plan_version',
    'approve_plan_version', 'unapprove_plan_version',
    'list_sales_forecast', 'upsert_sales_forecast', 'delete_sales_forecast',
    'list_sales_targets', 'upsert_sales_target', 'delete_sales_target',
    'projected_sales',
    # M2 — manufacturing / financial / pricing
    'list_manufacturers', 'create_manufacturer',
    'list_manufacturing', 'upsert_manufacturing', 'delete_manufacturing',
    'get_financial', 'upsert_financial',
    'list_pricing', 'upsert_pricing', 'delete_pricing',
    # M2 — outputs
    'capacity_vs_demand', 'production_required', 'cash_flow',
    # M3 — risk + scenario comparison
    'risk_assessment', 'compare_scenarios',
    # UI helper
    'list_active_variants',
    # Ingredients to buy (reuses existing BOM engine)
    'ingredient_requirements',
    # Plan → manufacturing handoff (in-house WO release)
    'release_to_manufacturing', 'list_releases',
]

# Scenario type = the SHAPE of the forecast only. Plan lifecycle lives in the
# separate `status` column (draft → approved, via approve/unapprove).
SCENARIO_TYPES = ('expected', 'conservative', 'aggressive')
PLAN_CHANNELS  = ('retail', 'distributor', 'ecommerce', 'other')


# ═══════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════

def _now():
    from datetime import datetime
    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')


def _table_exists(name):
    return bool(qry1("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)))


def list_active_variants():
    """Active SKUs for planning UI dropdowns: {variant_id, sku_code, product_name, pack_size}."""
    return qry("""
        SELECT pv.id AS variant_id, pv.sku_code, p.name AS product_name, ps.label AS pack_size
        FROM product_variants pv
        JOIN products p    ON p.id  = pv.product_id
        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE pv.active_flag = 1
        ORDER BY p.name, ps.grams
    """)


def _norm_month(val):
    """Normalize 'YYYY-MM' or 'YYYY-MM-DD' to first-of-month 'YYYY-MM-01'. Raises on bad input."""
    s = (val or '').strip()
    parts = s.split('-')
    if len(parts) < 2 or not parts[0].isdigit() or not parts[1].isdigit():
        raise ValueError(f"Invalid month '{val}' — expected YYYY-MM")
    y, m = int(parts[0]), int(parts[1])
    if not (1 <= m <= 12):
        raise ValueError(f"Invalid month '{val}' — month must be 01–12")
    return f"{y:04d}-{m:02d}-01"


def _month_index(month_str):
    """'YYYY-MM-01' -> integer year*12+month for arithmetic/comparison."""
    y, m, _ = month_str.split('-')
    return int(y) * 12 + (int(m) - 1)


def _log_change(c, record_id, action, new_value, changed_by, reason, table='plan_version'):
    """Write a change_log row using the shared audit table (+ the planning `reason`)."""
    c.execute(
        """INSERT INTO change_log (table_name, record_id, action, new_value, changed_by, reason)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (table, str(record_id), action,
         json.dumps(new_value, default=str) if new_value is not None else None,
         changed_by, reason)
    )


def _window_forecast(v):
    """Forecast rows within the version's horizon window.
    Returns (start_month, window_months, rows) where each row has keys
    period_month, variant_id, channel, units, offset (months from start)."""
    window = int(v.get('horizon_months') or 12)
    raw = qry("""
        SELECT period_month, variant_id, channel, units_forecast
        FROM plan_sales_forecast WHERE plan_version_id=? ORDER BY period_month
    """, (v['id'],))
    start_month = v.get('horizon_start_month') or (raw[0]['period_month'] if raw else None)
    start_idx = _month_index(start_month) if start_month else None
    out = []
    for r in raw:
        off = None
        if start_idx is not None:
            off = _month_index(r['period_month']) - start_idx
            if off < 0 or off >= window:
                continue
        out.append({'period_month': r['period_month'], 'variant_id': r['variant_id'],
                    'channel': r['channel'], 'units': r['units_forecast'] or 0, 'offset': off})
    return start_month, window, out


def _require_editable(version_id):
    """Return the version dict, or raise if missing / approved (immutable)."""
    v = qry1("SELECT * FROM plan_version WHERE id=?", (version_id,))
    if not v:
        raise ValueError(f"Plan version {version_id} not found")
    if v['status'] == 'approved':
        raise ValueError("Approved plan is read-only — clone it to a 'revised' version to edit")
    return v


# ═══════════════════════════════════════════════════════════════════
#  PLAN VERSIONS
# ═══════════════════════════════════════════════════════════════════

def list_plan_versions():
    """All plan versions, newest first, with child-row counts."""
    rows = qry("""
        SELECT v.*,
               (SELECT COUNT(*) FROM plan_sales_forecast f WHERE f.plan_version_id = v.id) AS forecast_rows,
               (SELECT COUNT(*) FROM plan_sales_target   t WHERE t.plan_version_id = v.id) AS target_rows
        FROM plan_version v
        ORDER BY v.created_at DESC, v.id DESC
    """)
    return rows


def get_plan_version(version_id):
    v = qry1("SELECT * FROM plan_version WHERE id=?", (version_id,))
    if not v:
        raise ValueError(f"Plan version {version_id} not found")
    return v


def create_plan_version(data, changed_by):
    """Create a plan version. Required: name, scenario_type. Optional: notes,
    horizon_start_month (YYYY-MM), horizon_months (default 12), parent_version_id."""
    name = (data.get('name') or '').strip()
    scenario_type = (data.get('scenario_type') or 'expected').strip().lower()
    if not name:
        raise ValueError("Plan name is required")
    if scenario_type not in SCENARIO_TYPES:
        raise ValueError(f"scenario_type must be one of {', '.join(SCENARIO_TYPES)}")

    horizon_start = data.get('horizon_start_month')
    horizon_start = _norm_month(horizon_start) if horizon_start else None
    horizon_months = int(data.get('horizon_months') or 12)
    if not (1 <= horizon_months <= 36):
        raise ValueError("horizon_months must be between 1 and 36")

    parent_id = data.get('parent_version_id')
    notes = (data.get('notes') or '').strip() or None
    reason = (data.get('reason') or '').strip() or None

    c = _conn()
    try:
        has_code = any(r[1] == 'plan_code' for r in c.execute("PRAGMA table_info(plan_version)").fetchall())
        if has_code:
            mx = c.execute(
                "SELECT MAX(CAST(SUBSTR(plan_code,6) AS INTEGER)) FROM plan_version WHERE plan_code LIKE 'PLAN-%'"
            ).fetchone()[0] or 0
            plan_code = f"PLAN-{mx + 1:03d}"
            c.execute("""
                INSERT INTO plan_version
                    (plan_code, name, scenario_type, status, parent_version_id, notes,
                     horizon_start_month, horizon_months, created_by, updated_by)
                VALUES (?,?,?,'draft',?,?,?,?,?,?)
            """, (plan_code, name, scenario_type, parent_id, notes, horizon_start, horizon_months,
                  changed_by, changed_by))
        else:
            c.execute("""
                INSERT INTO plan_version
                    (name, scenario_type, status, parent_version_id, notes,
                     horizon_start_month, horizon_months, created_by, updated_by)
                VALUES (?,?,'draft',?,?,?,?,?,?)
            """, (name, scenario_type, parent_id, notes, horizon_start, horizon_months,
                  changed_by, changed_by))
        vid = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        _log_change(c, vid, 'INSERT',
                    {'name': name, 'scenario_type': scenario_type, 'horizon_months': horizon_months},
                    changed_by, reason)
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return get_plan_version(vid)


def update_plan_version(version_id, data, changed_by):
    """Edit name / scenario_type / notes / horizon of a non-approved version.
    status changes (approve/unapprove) are separate actions."""
    v = _require_editable(version_id)
    sets, vals = [], []
    if 'name' in data and str(data['name']).strip():
        sets.append("name=?"); vals.append(str(data['name']).strip())
    if 'scenario_type' in data and str(data.get('scenario_type') or '').strip():
        st = str(data['scenario_type']).strip().lower()
        if st not in SCENARIO_TYPES:
            raise ValueError(f"scenario_type must be one of {', '.join(SCENARIO_TYPES)}")
        sets.append("scenario_type=?"); vals.append(st)
    if 'notes' in data:
        sets.append("notes=?"); vals.append((str(data.get('notes') or '').strip() or None))
    if 'horizon_start_month' in data:
        hs = data.get('horizon_start_month')
        sets.append("horizon_start_month=?"); vals.append(_norm_month(hs) if hs else None)
    if 'horizon_months' in data:
        hm = int(data.get('horizon_months') or 12)
        if not (1 <= hm <= 36):
            raise ValueError("horizon_months must be between 1 and 36")
        sets.append("horizon_months=?"); vals.append(hm)
    if not sets:
        return v
    sets.append("updated_by=?"); vals.append(changed_by)
    sets.append("updated_at=?"); vals.append(_now())
    reason = (data.get('reason') or '').strip() or None

    c = _conn()
    try:
        c.execute(f"UPDATE plan_version SET {', '.join(sets)} WHERE id=?", vals + [version_id])
        _log_change(c, version_id, 'UPDATE', {k: data[k] for k in data if k != 'reason'},
                    changed_by, reason)
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return get_plan_version(version_id)


def approve_plan_version(version_id, changed_by):
    """Approve a plan (draft → approved). Approved plans become immutable (input edits
    blocked) and eligible for manufacturing release. The one_approved_launch_plan index
    only restricts plans whose scenario_type is literally 'approved'."""
    v = get_plan_version(version_id)
    if v['status'] == 'approved':
        return v
    c = _conn()
    try:
        c.execute("""UPDATE plan_version SET status='approved', approved_by=?, approved_at=?,
                     updated_by=?, updated_at=? WHERE id=?""",
                  (changed_by, _now(), changed_by, _now(), version_id))
        _log_change(c, version_id, 'UPDATE', {'action': 'approve', 'status': 'approved'}, changed_by, None)
        c.commit()
    except Exception as e:
        c.rollback()
        if 'one_approved_launch_plan' in str(e) or 'UNIQUE' in str(e).upper():
            raise ValueError("Another launch plan is already approved — unlock it first.")
        raise
    finally:
        c.close()
    save_db()
    return get_plan_version(version_id)


def unapprove_plan_version(version_id, changed_by):
    """Revert an approved plan back to draft so it can be edited again. (Work orders already
    released to the ERP are unaffected — plan/actual stay separate.)"""
    v = get_plan_version(version_id)
    if v['status'] != 'approved':
        return v
    c = _conn()
    try:
        c.execute("""UPDATE plan_version SET status='draft', approved_by=NULL, approved_at=NULL,
                     updated_by=?, updated_at=? WHERE id=?""",
                  (changed_by, _now(), version_id))
        _log_change(c, version_id, 'UPDATE', {'action': 'unapprove', 'status': 'draft'}, changed_by, None)
        c.commit()
    finally:
        c.close()
    save_db()
    return get_plan_version(version_id)


# ═══════════════════════════════════════════════════════════════════
#  SALES FORECAST
# ═══════════════════════════════════════════════════════════════════

def list_sales_forecast(version_id):
    """Forecast rows for a version, joined to SKU/product names, ordered by month/sku."""
    get_plan_version(version_id)  # 404 if missing
    return qry("""
        SELECT f.*, pv.sku_code, p.name AS product_name
        FROM plan_sales_forecast f
        JOIN product_variants pv ON pv.id = f.variant_id
        JOIN products p          ON p.id  = pv.product_id
        WHERE f.plan_version_id = ?
        ORDER BY f.period_month, pv.sku_code, f.channel
    """, (version_id,))


def upsert_sales_forecast(version_id, data, changed_by):
    """Insert or update a forecast cell (unique by version+month+variant+channel)."""
    _require_editable(version_id)
    variant_id = data.get('variant_id')
    if not variant_id:
        raise ValueError("variant_id is required")
    variant_id = int(variant_id)
    if not qry1("SELECT 1 FROM product_variants WHERE id=? AND active_flag=1", (variant_id,)):
        raise ValueError(f"Active product variant {variant_id} not found")

    month = _norm_month(data.get('period_month'))
    channel = (data.get('channel') or '').strip().lower()
    if channel not in PLAN_CHANNELS:
        raise ValueError(f"channel must be one of {', '.join(PLAN_CHANNELS)}")

    units = float(data.get('units_forecast') or 0)
    if units < 0:
        raise ValueError("units_forecast cannot be negative")
    store_count = data.get('store_count')
    store_count = int(store_count) if store_count not in (None, '') else None
    sell_through = data.get('sell_through_per_store')
    sell_through = float(sell_through) if sell_through not in (None, '') else None
    reason = (data.get('reason') or '').strip() or None

    c = _conn()
    try:
        existing = c.execute(
            """SELECT id FROM plan_sales_forecast
               WHERE plan_version_id=? AND period_month=? AND variant_id=? AND channel=?""",
            (version_id, month, variant_id, channel)
        ).fetchone()
        if existing:
            row_id = existing[0]
            c.execute("""
                UPDATE plan_sales_forecast
                SET units_forecast=?, store_count=?, sell_through_per_store=?,
                    updated_by=?, updated_at=?
                WHERE id=?
            """, (units, store_count, sell_through, changed_by, _now(), row_id))
            action = 'UPDATE'
        else:
            c.execute("""
                INSERT INTO plan_sales_forecast
                    (plan_version_id, period_month, variant_id, channel,
                     units_forecast, store_count, sell_through_per_store, created_by, updated_by)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (version_id, month, variant_id, channel, units, store_count, sell_through,
                  changed_by, changed_by))
            row_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
            action = 'INSERT'
        _log_change(c, version_id, action,
                    {'forecast_id': row_id, 'month': month, 'variant_id': variant_id,
                     'channel': channel, 'units': units}, changed_by, reason)
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return {'id': row_id, 'action': action, 'period_month': month,
            'variant_id': variant_id, 'channel': channel, 'units_forecast': units}


def delete_sales_forecast(row_id, changed_by, reason=None):
    row = qry1("SELECT * FROM plan_sales_forecast WHERE id=?", (row_id,))
    if not row:
        raise ValueError(f"Forecast row {row_id} not found")
    _require_editable(row['plan_version_id'])
    c = _conn()
    try:
        c.execute("DELETE FROM plan_sales_forecast WHERE id=?", (row_id,))
        _log_change(c, row['plan_version_id'], 'DELETE', {'forecast_id': row_id}, changed_by, reason)
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return {'deleted': row_id}


# ═══════════════════════════════════════════════════════════════════
#  SALES TARGETS
# ═══════════════════════════════════════════════════════════════════

def list_sales_targets(version_id):
    get_plan_version(version_id)
    return qry("""
        SELECT * FROM plan_sales_target
        WHERE plan_version_id=?
        ORDER BY period_month, channel
    """, (version_id,))


def upsert_sales_target(version_id, data, changed_by):
    """Insert or update a monthly channel target (unique by version+month+channel)."""
    _require_editable(version_id)
    month = _norm_month(data.get('period_month'))
    channel = (data.get('channel') or '').strip().lower()
    if channel not in PLAN_CHANNELS:
        raise ValueError(f"channel must be one of {', '.join(PLAN_CHANNELS)}")
    target_units = data.get('target_units')
    target_units = float(target_units) if target_units not in (None, '') else None
    target_revenue = data.get('target_revenue')
    target_revenue = float(target_revenue) if target_revenue not in (None, '') else None
    if target_units is not None and target_units < 0:
        raise ValueError("target_units cannot be negative")
    if target_revenue is not None and target_revenue < 0:
        raise ValueError("target_revenue cannot be negative")
    reason = (data.get('reason') or '').strip() or None

    c = _conn()
    try:
        existing = c.execute(
            """SELECT id FROM plan_sales_target
               WHERE plan_version_id=? AND period_month=? AND channel=?""",
            (version_id, month, channel)
        ).fetchone()
        if existing:
            row_id = existing[0]
            c.execute("""UPDATE plan_sales_target
                         SET target_units=?, target_revenue=?, updated_by=?, updated_at=?
                         WHERE id=?""",
                      (target_units, target_revenue, changed_by, _now(), row_id))
            action = 'UPDATE'
        else:
            c.execute("""INSERT INTO plan_sales_target
                         (plan_version_id, period_month, channel, target_units, target_revenue,
                          created_by, updated_by)
                         VALUES (?,?,?,?,?,?,?)""",
                      (version_id, month, channel, target_units, target_revenue,
                       changed_by, changed_by))
            row_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
            action = 'INSERT'
        _log_change(c, version_id, action,
                    {'target_id': row_id, 'month': month, 'channel': channel,
                     'target_units': target_units, 'target_revenue': target_revenue},
                    changed_by, reason)
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return {'id': row_id, 'action': action, 'period_month': month, 'channel': channel}


def delete_sales_target(row_id, changed_by, reason=None):
    row = qry1("SELECT * FROM plan_sales_target WHERE id=?", (row_id,))
    if not row:
        raise ValueError(f"Target row {row_id} not found")
    _require_editable(row['plan_version_id'])
    c = _conn()
    try:
        c.execute("DELETE FROM plan_sales_target WHERE id=?", (row_id,))
        _log_change(c, row['plan_version_id'], 'DELETE', {'target_id': row_id}, changed_by, reason)
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return {'deleted': row_id}


# ═══════════════════════════════════════════════════════════════════
#  PROJECTED SALES  (calculation — never stored)
# ═══════════════════════════════════════════════════════════════════

def _scenario_prices(version_id):
    """{variant_id: wholesale_price} from plan_pricing, if that table exists (M2+)."""
    if not _table_exists('plan_pricing'):
        return {}
    rows = qry("SELECT variant_id, wholesale_price FROM plan_pricing WHERE plan_version_id=?",
               (version_id,))
    return {r['variant_id']: r['wholesale_price'] for r in rows
            if r['wholesale_price'] is not None}


def projected_sales(version_id, months=None):
    """Projected sales for a plan version over a window.

    Window = `months` if given, else the version's horizon_months (default 12),
    anchored at horizon_start_month (or the earliest forecast month if unset).
    Returns units always; revenue only when scenario prices exist (plan_pricing, M2+)
    — until then revenue fields are null (volume-first, money added in M2).

    Output: {version_id, window_months, start_month, end_month,
             totals:{units,revenue}, by_channel:{...}, by_month:[...],
             windows:{m3,m6,m12}, has_pricing}
    """
    v = get_plan_version(version_id)
    window = int(months) if months else int(v.get('horizon_months') or 12)
    if window < 1:
        raise ValueError("months must be >= 1")

    rows = qry("""
        SELECT period_month, variant_id, channel, units_forecast
        FROM plan_sales_forecast
        WHERE plan_version_id=?
        ORDER BY period_month
    """, (version_id,))

    prices = _scenario_prices(version_id)
    has_pricing = bool(prices)

    # Anchor: explicit horizon start, else earliest forecast month.
    start_month = v.get('horizon_start_month')
    if not start_month:
        start_month = rows[0]['period_month'] if rows else None
    start_idx = _month_index(start_month) if start_month else None

    def _rev(variant_id, units):
        p = prices.get(variant_id)
        return (units * p) if p is not None else None

    by_channel, by_month_map = {}, {}
    tot_units, tot_rev = 0.0, 0.0
    win_units = {3: 0.0, 6: 0.0, 12: 0.0}
    win_rev   = {3: 0.0, 6: 0.0, 12: 0.0}

    for r in rows:
        if start_idx is not None:
            offset = _month_index(r['period_month']) - start_idx
            if offset < 0 or offset >= window:
                continue
        units = r['units_forecast'] or 0
        rev = _rev(r['variant_id'], units)

        ch = by_channel.setdefault(r['channel'], {'units': 0.0, 'revenue': 0.0 if has_pricing else None})
        ch['units'] += units
        if has_pricing and rev is not None:
            ch['revenue'] += rev

        bm = by_month_map.setdefault(r['period_month'], {'units': 0.0, 'revenue': 0.0 if has_pricing else None})
        bm['units'] += units
        if has_pricing and rev is not None:
            bm['revenue'] += rev

        tot_units += units
        if has_pricing and rev is not None:
            tot_rev += rev

        if start_idx is not None:
            for w in (3, 6, 12):
                if offset < w:
                    win_units[w] += units
                    if has_pricing and rev is not None:
                        win_rev[w] += rev

    by_month = [{'period_month': m, **vals} for m, vals in sorted(by_month_map.items())]
    end_month = by_month[-1]['period_month'] if by_month else None

    windows = {}
    for w in (3, 6, 12):
        windows[f"m{w}"] = {
            'units': win_units[w],
            'revenue': win_rev[w] if has_pricing else None,
        }

    return {
        'version_id': version_id,
        'window_months': window,
        'start_month': start_month,
        'end_month': end_month,
        'has_pricing': has_pricing,
        'totals': {'units': tot_units, 'revenue': tot_rev if has_pricing else None},
        'by_channel': by_channel,
        'by_month': by_month,
        'windows': windows,
    }


# ═══════════════════════════════════════════════════════════════════
#  M2 — MANUFACTURING (manufacturers are global; capacity is per-version)
# ═══════════════════════════════════════════════════════════════════

def list_manufacturers():
    return qry("SELECT * FROM plan_manufacturer ORDER BY is_backup, name")


def create_manufacturer(data, changed_by):
    name = (data.get('name') or '').strip()
    if not name:
        raise ValueError("Manufacturer name is required")
    is_backup = 1 if data.get('is_backup') else 0
    reason = (data.get('reason') or '').strip() or None
    c = _conn()
    try:
        c.execute("INSERT INTO plan_manufacturer (name, is_backup, created_by, updated_by) VALUES (?,?,?,?)",
                  (name, is_backup, changed_by, changed_by))
        mid = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        _log_change(c, mid, 'INSERT', {'name': name, 'is_backup': is_backup}, changed_by, reason,
                    table='plan_manufacturer')
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return qry1("SELECT * FROM plan_manufacturer WHERE id=?", (mid,))


def list_manufacturing(version_id):
    get_plan_version(version_id)
    return qry("""
        SELECT m.*, mf.name AS manufacturer_name, mf.is_backup
        FROM plan_manufacturing m
        JOIN plan_manufacturer mf ON mf.id = m.manufacturer_id
        WHERE m.plan_version_id=?
        ORDER BY mf.is_backup, mf.name
    """, (version_id,))


def upsert_manufacturing(version_id, data, changed_by):
    """Insert/update a manufacturer's capacity for a version (unique by version+manufacturer)."""
    _require_editable(version_id)
    manufacturer_id = data.get('manufacturer_id')
    if not manufacturer_id:
        raise ValueError("manufacturer_id is required")
    manufacturer_id = int(manufacturer_id)
    if not qry1("SELECT 1 FROM plan_manufacturer WHERE id=?", (manufacturer_id,)):
        raise ValueError(f"Manufacturer {manufacturer_id} not found")

    def _num(key):
        val = data.get(key)
        return float(val) if val not in (None, '') else None
    cap = float(data.get('monthly_capacity_units') or 0)
    if cap < 0:
        raise ValueError("monthly_capacity_units cannot be negative")
    lead = data.get('lead_time_days')
    lead = int(lead) if lead not in (None, '') else None
    bottleneck = (data.get('bottleneck_process') or '').strip() or None
    reason = (data.get('reason') or '').strip() or None

    c = _conn()
    try:
        existing = c.execute("SELECT id FROM plan_manufacturing WHERE plan_version_id=? AND manufacturer_id=?",
                             (version_id, manufacturer_id)).fetchone()
        vals = (cap, _num('batch_size'), _num('moq'), lead, _num('packaging_capacity_units'),
                bottleneck, _num('cost_per_run'))
        if existing:
            row_id = existing[0]
            c.execute("""UPDATE plan_manufacturing SET monthly_capacity_units=?, batch_size=?, moq=?,
                         lead_time_days=?, packaging_capacity_units=?, bottleneck_process=?, cost_per_run=?,
                         updated_by=?, updated_at=? WHERE id=?""",
                      vals + (changed_by, _now(), row_id))
            action = 'UPDATE'
        else:
            c.execute("""INSERT INTO plan_manufacturing (plan_version_id, manufacturer_id,
                         monthly_capacity_units, batch_size, moq, lead_time_days,
                         packaging_capacity_units, bottleneck_process, cost_per_run, created_by, updated_by)
                         VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                      (version_id, manufacturer_id) + vals + (changed_by, changed_by))
            row_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
            action = 'INSERT'
        _log_change(c, version_id, action,
                    {'manufacturing_id': row_id, 'manufacturer_id': manufacturer_id, 'capacity': cap},
                    changed_by, reason)
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return {'id': row_id, 'action': action, 'manufacturer_id': manufacturer_id}


def delete_manufacturing(row_id, changed_by, reason=None):
    row = qry1("SELECT * FROM plan_manufacturing WHERE id=?", (row_id,))
    if not row:
        raise ValueError(f"Manufacturing row {row_id} not found")
    _require_editable(row['plan_version_id'])
    c = _conn()
    try:
        c.execute("DELETE FROM plan_manufacturing WHERE id=?", (row_id,))
        _log_change(c, row['plan_version_id'], 'DELETE', {'manufacturing_id': row_id}, changed_by, reason)
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return {'deleted': row_id}


# ═══════════════════════════════════════════════════════════════════
#  M2 — FINANCIAL (one row per version)
# ═══════════════════════════════════════════════════════════════════

def get_financial(version_id):
    get_plan_version(version_id)
    return qry1("SELECT * FROM plan_financial WHERE plan_version_id=?", (version_id,)) or {
        'plan_version_id': version_id, 'beginning_cash': None, 'marketing_budget': None,
        'payroll_budget': None, 'freight_cost_per_unit': None, 'other_opex_monthly': None,
        'minimum_cash_threshold': None,
    }


def upsert_financial(version_id, data, changed_by):
    """Insert/update the single financial-assumptions row for a version.
    Budgets (marketing/payroll/other_opex) are treated as MONTHLY recurring amounts."""
    _require_editable(version_id)

    def _num(key):
        val = data.get(key)
        return float(val) if val not in (None, '') else None
    fields = ['beginning_cash', 'marketing_budget', 'payroll_budget',
              'freight_cost_per_unit', 'other_opex_monthly', 'minimum_cash_threshold']
    vals = {f: _num(f) for f in fields}
    reason = (data.get('reason') or '').strip() or None

    c = _conn()
    try:
        exists = c.execute("SELECT 1 FROM plan_financial WHERE plan_version_id=?", (version_id,)).fetchone()
        if exists:
            c.execute(f"""UPDATE plan_financial SET {', '.join(f+'=?' for f in fields)},
                         updated_by=?, updated_at=? WHERE plan_version_id=?""",
                      [vals[f] for f in fields] + [changed_by, _now(), version_id])
            action = 'UPDATE'
        else:
            c.execute(f"""INSERT INTO plan_financial (plan_version_id, {', '.join(fields)}, created_by, updated_by)
                         VALUES ({','.join(['?']*(len(fields)+3))})""",
                      [version_id] + [vals[f] for f in fields] + [changed_by, changed_by])
            action = 'INSERT'
        _log_change(c, version_id, action, {'financial': vals}, changed_by, reason)
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return get_financial(version_id)


# ═══════════════════════════════════════════════════════════════════
#  M2 — SCENARIO PRICING (money — admin-gated at API)
# ═══════════════════════════════════════════════════════════════════

def list_pricing(version_id):
    get_plan_version(version_id)
    return qry("""
        SELECT pp.*, pv.sku_code, p.name AS product_name
        FROM plan_pricing pp
        JOIN product_variants pv ON pv.id = pp.variant_id
        JOIN products p          ON p.id  = pv.product_id
        WHERE pp.plan_version_id=?
        ORDER BY pv.sku_code
    """, (version_id,))


def upsert_pricing(version_id, data, changed_by):
    """Insert/update scenario pricing for a variant (unique by version+variant)."""
    _require_editable(version_id)
    variant_id = data.get('variant_id')
    if not variant_id:
        raise ValueError("variant_id is required")
    variant_id = int(variant_id)
    if not qry1("SELECT 1 FROM product_variants WHERE id=?", (variant_id,)):
        raise ValueError(f"Product variant {variant_id} not found")

    def _num(key):
        val = data.get(key)
        return float(val) if val not in (None, '') else None
    cost, whole, retail = _num('product_cost'), _num('wholesale_price'), _num('retail_price')
    for label, val in (('product_cost', cost), ('wholesale_price', whole), ('retail_price', retail)):
        if val is not None and val < 0:
            raise ValueError(f"{label} cannot be negative")
    reason = (data.get('reason') or '').strip() or None

    c = _conn()
    try:
        existing = c.execute("SELECT id FROM plan_pricing WHERE plan_version_id=? AND variant_id=?",
                             (version_id, variant_id)).fetchone()
        if existing:
            row_id = existing[0]
            c.execute("""UPDATE plan_pricing SET product_cost=?, wholesale_price=?, retail_price=?,
                         updated_by=?, updated_at=? WHERE id=?""",
                      (cost, whole, retail, changed_by, _now(), row_id))
            action = 'UPDATE'
        else:
            c.execute("""INSERT INTO plan_pricing (plan_version_id, variant_id, product_cost,
                         wholesale_price, retail_price, created_by, updated_by) VALUES (?,?,?,?,?,?,?)""",
                      (version_id, variant_id, cost, whole, retail, changed_by, changed_by))
            row_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
            action = 'INSERT'
        _log_change(c, version_id, action,
                    {'pricing_id': row_id, 'variant_id': variant_id,
                     'wholesale_price': whole, 'product_cost': cost}, changed_by, reason)
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return {'id': row_id, 'action': action, 'variant_id': variant_id}


def delete_pricing(row_id, changed_by, reason=None):
    row = qry1("SELECT * FROM plan_pricing WHERE id=?", (row_id,))
    if not row:
        raise ValueError(f"Pricing row {row_id} not found")
    _require_editable(row['plan_version_id'])
    c = _conn()
    try:
        c.execute("DELETE FROM plan_pricing WHERE id=?", (row_id,))
        _log_change(c, row['plan_version_id'], 'DELETE', {'pricing_id': row_id}, changed_by, reason)
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return {'deleted': row_id}


# ═══════════════════════════════════════════════════════════════════
#  M2 — OUTPUTS (calculations — never stored)
# ═══════════════════════════════════════════════════════════════════

import math


def _demand_by_month(rows):
    """{period_month: total_units} from windowed forecast rows."""
    out = {}
    for r in rows:
        out[r['period_month']] = out.get(r['period_month'], 0) + (r['units'] or 0)
    return dict(sorted(out.items()))


def capacity_vs_demand(version_id):
    """Per-month demand vs base (non-backup) monthly manufacturing capacity.
    Backup capacity is reported separately (contingency, not counted in base)."""
    v = get_plan_version(version_id)
    _, window, rows = _window_forecast(v)
    demand = _demand_by_month(rows)

    caps = qry("""SELECT m.monthly_capacity_units, mf.is_backup
                  FROM plan_manufacturing m JOIN plan_manufacturer mf ON mf.id=m.manufacturer_id
                  WHERE m.plan_version_id=?""", (version_id,))
    base_cap = sum((c['monthly_capacity_units'] or 0) for c in caps if not c['is_backup'])
    backup_cap = sum((c['monthly_capacity_units'] or 0) for c in caps if c['is_backup'])

    months, worst_deficit, peak_demand, can_meet = [], 0.0, 0.0, True
    for m, d in demand.items():
        surplus = base_cap - d
        if surplus < 0:
            can_meet = False
            worst_deficit = min(worst_deficit, surplus)
        peak_demand = max(peak_demand, d)
        months.append({'period_month': m, 'demand': d, 'capacity': base_cap,
                       'surplus': surplus, 'deficit': (-surplus if surplus < 0 else 0)})
    return {
        'version_id': version_id, 'window_months': window,
        'base_monthly_capacity': base_cap, 'backup_monthly_capacity': backup_cap,
        'can_meet': can_meet, 'peak_demand': peak_demand,
        'worst_deficit': (-worst_deficit if worst_deficit < 0 else 0),
        'has_capacity': bool(caps), 'months': months,
    }


def production_required(version_id):
    """Per-month production required to meet demand. Net of finished-goods on-hand
    only when plan_inventory exists (M4); otherwise gross. Rounded to the primary
    (first non-backup) manufacturer's batch_size / MOQ when provided."""
    v = get_plan_version(version_id)
    _, window, rows = _window_forecast(v)
    demand = _demand_by_month(rows)

    prim = qry1("""SELECT m.batch_size, m.moq FROM plan_manufacturing m
                   JOIN plan_manufacturer mf ON mf.id=m.manufacturer_id
                   WHERE m.plan_version_id=? AND mf.is_backup=0
                   ORDER BY m.id LIMIT 1""", (version_id,))
    batch = (prim or {}).get('batch_size') or None
    moq = (prim or {}).get('moq') or None

    # Finished-goods on-hand snapshot (M4, optional/forward-compatible)
    on_hand = {}
    if _table_exists('plan_inventory'):
        for r in qry("""SELECT item_id, on_hand_qty FROM plan_inventory
                        WHERE plan_version_id=? AND item_type='finished_good'""", (version_id,)):
            on_hand[r['item_id']] = r['on_hand_qty'] or 0
    total_on_hand = sum(on_hand.values()) if on_hand else 0

    months, total_required = [], 0.0
    remaining_oh = total_on_hand
    for m, d in demand.items():
        net = d - remaining_oh
        remaining_oh = max(0, remaining_oh - d)
        net = max(0, net)
        rounded = net
        if rounded > 0 and batch:
            rounded = math.ceil(rounded / batch) * batch
        if rounded > 0 and moq and rounded < moq:
            rounded = moq
        total_required += rounded
        months.append({'period_month': m, 'demand': d,
                       'required_raw': net, 'required_rounded': rounded})
    return {
        'version_id': version_id, 'window_months': window,
        'rounding': {'batch_size': batch, 'moq': moq},
        'fg_on_hand_applied': total_on_hand, 'uses_inventory_snapshot': bool(on_hand),
        'total_required': total_required, 'months': months,
    }


def cash_flow(version_id):
    """Monthly cash-flow projection. MONEY — admin only.

    cash_in  = Σ(units × wholesale_price)
    cash_out = Σ(units × product_cost)  [COGS]
             + marketing_budget + payroll_budget + other_opex_monthly  [monthly]
             + total_units × freight_cost_per_unit
    running_balance starts at beginning_cash. Flags months below minimum_cash_threshold.
    Requires scenario pricing (plan_pricing) for revenue/COGS; financial row for budgets.
    """
    v = get_plan_version(version_id)
    _, window, rows = _window_forecast(v)

    prices = {}
    if _table_exists('plan_pricing'):
        for r in qry("SELECT variant_id, product_cost, wholesale_price FROM plan_pricing WHERE plan_version_id=?",
                     (version_id,)):
            prices[r['variant_id']] = r
    has_pricing = bool(prices)
    fin = get_financial(version_id)
    has_financial = bool(qry1("SELECT 1 FROM plan_financial WHERE plan_version_id=?", (version_id,)))

    beginning_cash = fin.get('beginning_cash') or 0
    marketing = fin.get('marketing_budget') or 0
    payroll = fin.get('payroll_budget') or 0
    other_opex = fin.get('other_opex_monthly') or 0
    freight_pu = fin.get('freight_cost_per_unit') or 0
    threshold = fin.get('minimum_cash_threshold')

    # aggregate per month
    agg = {}
    for r in rows:
        a = agg.setdefault(r['period_month'], {'units': 0.0, 'rev': 0.0, 'cogs': 0.0})
        units = r['units'] or 0
        a['units'] += units
        p = prices.get(r['variant_id'])
        if p:
            a['rev'] += units * (p['wholesale_price'] or 0)
            a['cogs'] += units * (p['product_cost'] or 0)

    months, running = [], beginning_cash
    min_running = beginning_cash
    breaches = False
    for m in sorted(agg.keys()):
        a = agg[m]
        cash_in = a['rev']
        cash_out = a['cogs'] + marketing + payroll + other_opex + a['units'] * freight_pu
        net = cash_in - cash_out
        running += net
        min_running = min(min_running, running)
        below = (threshold is not None and running < threshold)
        if below:
            breaches = True
        months.append({'period_month': m, 'cash_in': cash_in, 'cash_out': cash_out,
                       'net': net, 'running_balance': running, 'below_threshold': below})
    return {
        'version_id': version_id, 'window_months': window,
        'has_pricing': has_pricing, 'has_financial': has_financial,
        'beginning_cash': beginning_cash, 'minimum_cash_threshold': threshold,
        'ending_balance': running, 'min_running_balance': min_running,
        'breaches_threshold': breaches,
        'assumptions_note': 'budgets are monthly recurring; freight = units × freight_cost_per_unit',
        'months': months,
    }


# ═══════════════════════════════════════════════════════════════════
#  M3 — RISK ASSESSMENT + SCENARIO COMPARISON  (calculation — never stored)
# ═══════════════════════════════════════════════════════════════════

_RISK_ORDER = {'green': 0, 'yellow': 1, 'red': 2}  # not_assessed excluded from "overall"


def _risk_config():
    """Thresholds — overridable via costing_config keys (planning_*), else defaults.
    Keeps them admin-tunable without hardcoding."""
    cfg = {}
    if _table_exists('costing_config'):
        for r in qry("SELECT key, value FROM costing_config WHERE key LIKE 'planning_%'"):
            cfg[r['key']] = r['value']

    def f(key, default):
        try:
            return float(cfg.get(key, default))
        except (TypeError, ValueError):
            return default
    return {
        'cash_warn_pct': f('planning_cash_warn_pct', 0.10),      # within 10% of threshold → yellow
        'capacity_warn_util': f('planning_capacity_warn_util', 0.85),  # >85% utilization → yellow
    }


def risk_assessment(version_id):
    """Red/Yellow/Green risk for one plan version across categories.
    Overall = worst assessed category. Categories that lack inputs are 'not_assessed'.
    (Stockout needs the M4 finished-goods snapshot — reported not_assessed until then.)"""
    v = get_plan_version(version_id)
    cfg = _risk_config()
    cats = {}

    # ── Capacity ──
    cap = capacity_vs_demand(version_id)
    if not cap['has_capacity']:
        cats['capacity'] = {'level': 'not_assessed', 'detail': 'No manufacturing capacity entered'}
    else:
        base = cap['base_monthly_capacity']
        if not cap['can_meet']:
            cats['capacity'] = {'level': 'red',
                                'detail': f"Capacity deficit — worst {cap['worst_deficit']:.0f} units/mo short"}
        elif base > 0 and cap['peak_demand'] > base * cfg['capacity_warn_util']:
            cats['capacity'] = {'level': 'yellow',
                                'detail': f"Peak demand {cap['peak_demand']:.0f} > {cfg['capacity_warn_util']*100:.0f}% of capacity {base:.0f}"}
        else:
            cats['capacity'] = {'level': 'green', 'detail': 'Capacity covers demand'}

    # ── Cash ──
    cf = cash_flow(version_id)
    if not (cf['has_pricing'] and cf['has_financial']):
        cats['cash'] = {'level': 'not_assessed', 'detail': 'Needs scenario pricing + financial inputs'}
    else:
        thr = cf['minimum_cash_threshold']
        minb = cf['min_running_balance']
        if cf['breaches_threshold'] or (thr is None and minb < 0):
            d = f"Cash dips to {minb:.0f}" + (f", below threshold {thr:.0f}" if thr is not None else " (goes negative)")
            cats['cash'] = {'level': 'red', 'detail': d}
        elif thr is not None and minb < thr * (1 + cfg['cash_warn_pct']):
            cats['cash'] = {'level': 'yellow',
                            'detail': f"Min cash {minb:.0f} within {cfg['cash_warn_pct']*100:.0f}% of threshold {thr:.0f}"}
        else:
            cats['cash'] = {'level': 'green', 'detail': f"Min cash {minb:.0f} clears the threshold"}

    # ── Supply resilience (single point of failure) ──
    mfrs = list_manufacturing(version_id)
    non_backup = [m for m in mfrs if not m['is_backup']]
    backup = [m for m in mfrs if m['is_backup']]
    if not mfrs:
        cats['supply'] = {'level': 'not_assessed', 'detail': 'No manufacturer assigned'}
    elif not non_backup:
        cats['supply'] = {'level': 'red', 'detail': 'No primary (non-backup) manufacturer'}
    elif not backup:
        cats['supply'] = {'level': 'yellow', 'detail': 'Single point of failure — no backup manufacturer'}
    else:
        cats['supply'] = {'level': 'green', 'detail': 'Primary + backup manufacturer in place'}

    # ── Stockout (deferred to M4 inventory snapshot) ──
    cats['stockout'] = {'level': 'not_assessed', 'detail': 'Finished-goods inventory snapshot (M4) not yet available'}

    assessed = [c['level'] for c in cats.values() if c['level'] in _RISK_ORDER]
    overall = max(assessed, key=lambda l: _RISK_ORDER[l]) if assessed else 'not_assessed'
    return {'version_id': version_id, 'name': v['name'], 'scenario_type': v['scenario_type'],
            'overall': overall, 'categories': cats}


def compare_scenarios(version_ids):
    """Side-by-side metrics for N versions + a 'safest' ranking.
    Safest = lowest overall risk, then not breaching cash, then highest min cash, then can-meet."""
    if not version_ids:
        raise ValueError("Provide at least one version id to compare")
    rows = []
    for vid in version_ids:
        v = get_plan_version(vid)
        ps = projected_sales(vid)
        cap = capacity_vs_demand(vid)
        cf = cash_flow(vid)
        risk = risk_assessment(vid)
        rows.append({
            'version_id': vid, 'name': v['name'], 'scenario_type': v['scenario_type'],
            'total_units': ps['totals']['units'], 'total_revenue': ps['totals']['revenue'],
            'can_meet': cap['can_meet'] if cap['has_capacity'] else None,
            'worst_deficit': cap['worst_deficit'] if cap['has_capacity'] else None,
            'min_running_balance': cf['min_running_balance'] if cf['has_financial'] else None,
            'breaches_threshold': cf['breaches_threshold'] if cf['has_financial'] else None,
            'overall_risk': risk['overall'],
        })

    rank_order = {'green': 0, 'yellow': 1, 'not_assessed': 2, 'red': 3}

    def safety_key(r):
        mb = r['min_running_balance']
        return (
            rank_order.get(r['overall_risk'], 2),
            1 if r['breaches_threshold'] else 0,
            -(mb if mb is not None else float('-inf')),
            0 if r['can_meet'] else 1,
        )
    ranked = sorted(rows, key=safety_key)
    return {
        'scenarios': rows,
        'ranked': [r['version_id'] for r in ranked],
        'safest_version_id': ranked[0]['version_id'] if ranked else None,
    }


# ═══════════════════════════════════════════════════════════════════
#  INGREDIENTS TO BUY  (reuses the existing BOM engine — no recipe stored here)
# ═══════════════════════════════════════════════════════════════════

def ingredient_requirements(version_id):
    """Consolidated ingredient purchase list for a plan, from its forecast.

    Pipeline: forecast units per SKU (within horizon) → existing
    bom_calculate_ingredients() per SKU → aggregate NEEDED kg per ingredient
    across SKUs → net against current stock ONCE (stock is shared, not summed) →
    cost from the ingredient master. Codes only; the legend stays off-system.
    SKUs without an active BOM are listed separately (not silently skipped)."""
    from modules.purchasing import bom_calculate_ingredients
    v = get_plan_version(version_id)
    _, window, rows = _window_forecast(v)

    units_by_variant = {}
    for r in rows:
        units_by_variant[r['variant_id']] = units_by_variant.get(r['variant_id'], 0) + (r['units'] or 0)

    agg = {}          # ing_id -> {ing_code, needed_kg, available_kg}
    no_bom = []
    for vid_, units in units_by_variant.items():
        if units <= 0:
            continue
        try:
            res = bom_calculate_ingredients(vid_, units)
        except ValueError:
            no_bom.append(vid_)
            continue
        for ing in res['ingredients']:
            a = agg.setdefault(ing['ingId'], {'ing_code': ing['ingCode'], 'needed_kg': 0.0,
                                              'available_kg': ing['availableKg']})
            a['needed_kg'] += ing['neededKg']
            a['available_kg'] = ing['availableKg']   # shared stock — overwrite, never sum

    costmap = {}
    if agg:
        ids = list(agg.keys())
        ph = ",".join("?" * len(ids))
        for r in qry(f"SELECT id, COALESCE(cost_per_kg,0) AS cost FROM ingredients WHERE id IN ({ph})", tuple(ids)):
            costmap[r['id']] = r['cost']

    items, total_cost = [], 0.0
    for ing_id, a in agg.items():
        to_order = max(0.0, round(a['needed_kg'] - a['available_kg'], 3))
        cpk = costmap.get(ing_id, 0) or 0
        cost = round(to_order * cpk, 2)
        total_cost += cost
        items.append({
            'ingredient_id': ing_id, 'ing_code': a['ing_code'],
            'needed_kg': round(a['needed_kg'], 3), 'available_kg': round(a['available_kg'], 3),
            'to_order_kg': to_order, 'cost_per_kg': cpk, 'estimated_cost': cost,
            'sufficient': to_order < 0.001,
        })
    items.sort(key=lambda x: (-x['to_order_kg'], x['ing_code']))

    nobom_labels = []
    for vid_ in no_bom:
        r = qry1("""SELECT pv.sku_code, p.name FROM product_variants pv
                    JOIN products p ON p.id=pv.product_id WHERE pv.id=?""", (vid_,))
        nobom_labels.append((f"{r['name']} {r['sku_code']}" if r else str(vid_)))

    return {
        'version_id': version_id, 'window_months': window,
        'ingredients': items,
        'total_estimated_cost': round(total_cost, 2),
        'variants_without_bom': nobom_labels,
        'has_data': bool(items),
        'note': 'Quantities from forecast production volume, run through the live BOM; '
                'netted against current ingredient stock. Costs from the ingredient master.',
    }


# ═══════════════════════════════════════════════════════════════════
#  PLAN → MANUFACTURING HANDOFF  (in-house: release plan-month as Work Orders)
# ═══════════════════════════════════════════════════════════════════

def list_releases(version_id):
    """Releases already pushed from this plan to manufacturing (drives released-state)."""
    get_plan_version(version_id)
    if not _table_exists('plan_release'):
        return []
    return qry("""
        SELECT r.*, pv.sku_code, p.name AS product_name, w.wo_number, w.status AS wo_status
        FROM plan_release r
        JOIN product_variants pv ON pv.id = r.variant_id
        JOIN products p          ON p.id  = pv.product_id
        LEFT JOIN work_orders w  ON w.id  = r.work_order_id
        WHERE r.plan_version_id = ?
        ORDER BY r.period_month, pv.sku_code
    """, (version_id,))


def release_to_manufacturing(version_id, period_month, changed_by):
    """Release one plan-month's per-SKU production into the ERP as standalone Work Orders.

    In-house model: each SKU's forecast units for the month are rounded up to the
    primary line's batch size (and floored at its minimum run), then a standalone WO is
    created via the ERP's own create_work_order (which generates IDs + checks feasibility).
    The team converts each WO → Batch through the normal two-step flow; inventory moves
    only there. Approved plans only. Idempotent per (plan, month, variant) via plan_release.
    """
    from modules.production import create_work_order
    v = get_plan_version(version_id)
    if v.get('status') != 'approved':
        raise ValueError("Only an approved plan can release to manufacturing")
    month = _norm_month(period_month)

    _, _window, rows = _window_forecast(v)
    units_by_variant = {}
    for r in rows:
        if r['period_month'] == month:
            units_by_variant[r['variant_id']] = units_by_variant.get(r['variant_id'], 0) + (r['units'] or 0)
    if not units_by_variant:
        raise ValueError(f"No forecast for {month[:7]} to release")

    prim = qry1("""SELECT m.batch_size, m.moq FROM plan_manufacturing m
                   JOIN plan_manufacturer mf ON mf.id=m.manufacturer_id
                   WHERE m.plan_version_id=? AND mf.is_backup=0
                   ORDER BY m.id LIMIT 1""", (version_id,))
    batch = (prim or {}).get('batch_size') or None
    moq = (prim or {}).get('moq') or None

    released = set()
    if _table_exists('plan_release'):
        for r in qry("SELECT variant_id FROM plan_release WHERE plan_version_id=? AND period_month=?",
                     (version_id, month)):
            released.add(r['variant_id'])

    plan_code = v.get('plan_code') or f"PLAN-{version_id}"
    label = month[:7]
    created, skipped = [], []
    for vid_, units in units_by_variant.items():
        if units <= 0:
            continue
        if vid_ in released:
            skipped.append({'variant_id': vid_, 'reason': 'already released'})
            continue
        qty = units
        if batch:
            qty = math.ceil(qty / batch) * batch
        if moq and qty < moq:
            qty = moq
        qty = int(math.ceil(qty))
        if qty <= 0:
            continue
        wo = create_work_order({'productVariantId': vid_, 'qtyUnits': qty,
                                'targetDate': month, 'notes': f'Plan {plan_code} · {label}'})
        created.append({'variant_id': vid_, 'qty': qty,
                        'work_order_id': wo['id'], 'wo_number': wo['woNumber']})

    if created:
        c = _conn()
        try:
            for item in created:
                c.execute("""INSERT INTO plan_release
                             (plan_version_id, period_month, variant_id, work_order_id, released_by)
                             VALUES (?,?,?,?,?)""",
                          (version_id, month, item['variant_id'], item['work_order_id'], changed_by))
            _log_change(c, version_id, 'INSERT',
                        {'action': 'release_to_manufacturing', 'month': month, 'created': created}, changed_by, None)
            c.commit()
        except Exception:
            c.rollback(); raise
        finally:
            c.close()
        save_db()

    return {'version_id': version_id, 'period_month': month,
            'created': created, 'skipped': skipped,
            'created_count': len(created), 'skipped_count': len(skipped)}
