"""
modules/migrations.py — All startup schema migrations and table-creation functions.

Extracted from server.py (Sprint 8). Overrides originals via bottom-import pattern:
    from modules.migrations import *   # just before __main__ in server.py

All functions are idempotent — safe to run on every startup.
No _refresh_ref needed: migrations never call load_ref().
"""

from modules.db import _conn, save_db

__all__ = [
    'ensure_system_settings_schema',
    '_migrate_invoice_items_line_total',
    'ensure_full_schema',
    'ensure_work_orders_table',
    'ensure_customer_orders_schema',
    'ensure_review_queue_schema',
    '_migrate_supplier_bills_void',
    '_migrate_change_log_void_action',
    '_migrate_customer_type_wholesale',
    '_ensure_b2b_order_columns',
    'ensure_supplier_bills_schema',
    'ensure_purchase_orders_schema',
    'ensure_batch_cost_column',
    'ensure_master_schema',
    'ensure_costing_config',
    'ensure_price_types_sprint6',
    'ensure_price_history_extended',
    'ensure_margin_alerts_table',
    'ensure_field_otp_table',
    'ensure_ingredient_price_volatile',
    'ensure_ingredient_unit_kg',
    'ensure_clean_product_codes',
    'ensure_web_price_type',
    'ensure_25g_pack_and_spgm25',
    'ensure_web_prices',
    'ensure_variant_show_online',
    'ensure_recipe_tables',
    'ensure_change_log_reason',
    'ensure_planning_foundations',
    'ensure_plan_version_horizon',
    'ensure_plan_sales_tables',
    'ensure_plan_m2_tables',
    'ensure_plan_code',
    'ensure_plan_release',
    'ensure_scenario_type_cleanup',
    'ensure_plan_forecast_zone',
    'ensure_operating_costs',
    'ensure_deactivate_spring_catalog',
    'ensure_rep_zones',
    'ensure_dedup_seed_suppliers',
    'ensure_cost_lines',
    'ensure_wo_produced_units',
    'ensure_ingredient_target_grams',
    'ensure_batch_stages',
    'ensure_rep_app_access',
    'ensure_customer_gst',
    'ensure_drop_qty_in_production',
    'ensure_bill_vendor_capture',
    'ensure_ledger_po_link',
    'ensure_purchase_in_po_trigger',
    'ensure_po_line_types',
    'ensure_shop_geo',
]


# ═══════════════════════════════════════════════════════════════════
#  SYSTEM SETTINGS
# ═══════════════════════════════════════════════════════════════════

def ensure_system_settings_schema():
    """Create system_settings key-value table if not present."""
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS system_settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            )
        """)
        c.commit()
    finally:
        c.close()


# ═══════════════════════════════════════════════════════════════════
#  INVOICE ITEMS
# ═══════════════════════════════════════════════════════════════════

def _migrate_invoice_items_line_total():
    """
    Migration: if invoice_items has a 'total' column (old schema) but no 'line_total',
    recreate the table with 'line_total'. Fixes 'no such column: line_total' on Railway
    instances created before the column was renamed. Idempotent — safe to run every startup.
    """
    c = _conn()
    try:
        cols = [r[1] for r in c.execute("PRAGMA table_info(invoice_items)").fetchall()]
        if 'line_total' not in cols and 'total' in cols:
            print("  ⚙ Migrating invoice_items: renaming 'total' → 'line_total'")
            c.execute("PRAGMA foreign_keys=OFF")
            c.execute("""
                CREATE TABLE invoice_items_new (
                    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                    invoice_id         INTEGER NOT NULL REFERENCES invoices(id),
                    product_variant_id INTEGER REFERENCES product_variants(id),
                    product_code       TEXT NOT NULL,
                    product_name       TEXT NOT NULL,
                    pack_size          TEXT NOT NULL,
                    quantity           INTEGER NOT NULL,
                    unit_price         REAL NOT NULL,
                    line_total         REAL NOT NULL,
                    sale_id            TEXT
                )
            """)
            c.execute("""
                INSERT INTO invoice_items_new
                    (id, invoice_id, product_variant_id, product_code, product_name,
                     pack_size, quantity, unit_price, line_total, sale_id)
                SELECT
                    id, invoice_id, product_variant_id, product_code, product_name,
                    pack_size, quantity, unit_price, total, sale_id
                FROM invoice_items
            """)
            c.execute("DROP TABLE invoice_items")
            c.execute("ALTER TABLE invoice_items_new RENAME TO invoice_items")
            c.execute("PRAGMA foreign_keys=ON")
            c.commit()
            print("  ✓ invoice_items migrated — 'line_total' column now in place")
        else:
            print("  ✓ invoice_items schema OK — 'line_total' column present")
    except Exception as e:
        print(f"  ⚠ invoice_items migration skipped: {e}")
        c.rollback()
    finally:
        c.close()


# ═══════════════════════════════════════════════════════════════════
#  CORE SCHEMA — ALL TABLES
# ═══════════════════════════════════════════════════════════════════

def ensure_full_schema():
    """
    Create ALL core tables on a fresh database (idempotent — safe to run on existing DBs).
    Must be called immediately after bootstrap_db(), before any other ensure_* functions.
    """
    c = _conn()
    try:
        stmts = [
            # ── Core counters ────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS id_counters (
                entity    TEXT PRIMARY KEY,
                last_num  INTEGER DEFAULT 0
            )""",
            # ── Reference / lookup tables ────────────────────────────
            """CREATE TABLE IF NOT EXISTS pack_sizes (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                label  TEXT NOT NULL UNIQUE,
                grams  INTEGER NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS price_types (
                id    INTEGER PRIMARY KEY AUTOINCREMENT,
                code  TEXT NOT NULL UNIQUE,
                label TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS zones (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT NOT NULL,
                city       TEXT NOT NULL DEFAULT 'Karachi',
                active     INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now'))
            )""",
            # ── Core entities ────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS products (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                code       TEXT NOT NULL UNIQUE,
                name       TEXT NOT NULL,
                name_urdu  TEXT DEFAULT '',
                blend_code TEXT DEFAULT '',
                active     INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (date('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS product_variants (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                sku_code     TEXT NOT NULL UNIQUE,
                product_id   INTEGER NOT NULL REFERENCES products(id),
                pack_size_id INTEGER NOT NULL REFERENCES pack_sizes(id),
                active_flag  INTEGER DEFAULT 1,
                UNIQUE (product_id, pack_size_id)
            )""",
            """CREATE TABLE IF NOT EXISTS product_prices (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                product_variant_id INTEGER NOT NULL REFERENCES product_variants(id),
                price_type_id      INTEGER NOT NULL REFERENCES price_types(id),
                price              REAL NOT NULL,
                effective_from     TEXT NOT NULL,
                active_flag        INTEGER DEFAULT 1
            )""",
            """CREATE TABLE IF NOT EXISTS customers (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                code               TEXT NOT NULL UNIQUE,
                account_number     TEXT DEFAULT NULL,
                name               TEXT NOT NULL,
                customer_type      TEXT NOT NULL DEFAULT 'RETAIL'
                                   CHECK(customer_type IN ('RETAIL','DIRECT')),
                category           TEXT DEFAULT '',
                city               TEXT DEFAULT '',
                address            TEXT DEFAULT '',
                phone              TEXT DEFAULT '',
                email              TEXT DEFAULT '',
                default_pack       TEXT DEFAULT '50g',
                payment_terms_days INTEGER DEFAULT 30,
                credit_limit       REAL DEFAULT 0,
                active             INTEGER DEFAULT 1,
                created_at         TEXT DEFAULT (date('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS suppliers (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                code        TEXT NOT NULL UNIQUE,
                name        TEXT NOT NULL,
                contact     TEXT DEFAULT '',
                phone       TEXT DEFAULT '',
                email       TEXT DEFAULT '',
                city        TEXT DEFAULT '',
                address     TEXT DEFAULT '',
                active_flag INTEGER DEFAULT 1,
                zone_id     INTEGER REFERENCES zones(id),
                created_at  TEXT DEFAULT (date('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS ingredients (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                code          TEXT NOT NULL UNIQUE,
                name          TEXT NOT NULL DEFAULT '',
                opening_grams REAL DEFAULT 0,
                reorder_level REAL DEFAULT 0,
                cost_per_kg   REAL NOT NULL DEFAULT 0,
                active        INTEGER NOT NULL DEFAULT 1,
                created_at    TEXT DEFAULT (date('now'))
            )""",
            # ── BOM ──────────────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS bom_versions (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id       INTEGER NOT NULL REFERENCES products(id),
                version_no       INTEGER NOT NULL DEFAULT 1,
                batch_size_grams REAL NOT NULL DEFAULT 1000,
                effective_from   TEXT NOT NULL,
                active_flag      INTEGER DEFAULT 1,
                notes            TEXT DEFAULT '',
                UNIQUE (product_id, version_no)
            )""",
            """CREATE TABLE IF NOT EXISTS bom_items (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                bom_version_id INTEGER NOT NULL REFERENCES bom_versions(id),
                ingredient_id  INTEGER NOT NULL REFERENCES ingredients(id),
                quantity_grams REAL NOT NULL,
                UNIQUE (bom_version_id, ingredient_id)
            )""",
            # ── Inventory ────────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS inventory_ledger (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                ingredient_id INTEGER NOT NULL REFERENCES ingredients(id),
                movement_type TEXT NOT NULL
                              CHECK(movement_type IN ('OPENING','PURCHASE_IN','PRODUCTION_USE','ADJUSTMENT')),
                qty_grams     REAL NOT NULL,
                reference_id  TEXT DEFAULT '',
                notes         TEXT DEFAULT '',
                created_at    TEXT DEFAULT (datetime('now'))
            )""",
            # ── Production ───────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS production_batches (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id              TEXT NOT NULL UNIQUE,
                batch_date            TEXT NOT NULL,
                product_id            INTEGER NOT NULL REFERENCES products(id),
                product_variant_id    INTEGER REFERENCES product_variants(id),
                bom_version_id        INTEGER REFERENCES bom_versions(id),
                qty_grams             REAL NOT NULL,
                qty_units             INTEGER DEFAULT 0,
                pack_size             TEXT DEFAULT '',
                mfg_date              TEXT DEFAULT '',
                best_before           TEXT DEFAULT '',
                notes                 TEXT DEFAULT '',
                unit_cost_at_posting  REAL DEFAULT 0,
                created_at            TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS production_consumption (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id      INTEGER NOT NULL REFERENCES production_batches(id),
                ingredient_id INTEGER NOT NULL REFERENCES ingredients(id),
                qty_grams     REAL NOT NULL
            )""",
            # ── Sales & AR ───────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS invoices (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_number   TEXT NOT NULL UNIQUE,
                customer_id      INTEGER NOT NULL REFERENCES customers(id),
                invoice_date     TEXT NOT NULL,
                due_date         TEXT NOT NULL,
                status           TEXT DEFAULT 'UNPAID'
                                 CHECK(status IN ('DRAFT','UNPAID','PARTIAL','PAID','VOID')),
                notes            TEXT DEFAULT '',
                customer_order_id INTEGER REFERENCES customer_orders(id),
                created_at       TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS invoice_items (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_id         INTEGER NOT NULL REFERENCES invoices(id),
                product_variant_id INTEGER REFERENCES product_variants(id),
                product_code       TEXT NOT NULL,
                product_name       TEXT NOT NULL,
                pack_size          TEXT NOT NULL,
                quantity           INTEGER NOT NULL,
                unit_price         REAL NOT NULL,
                line_total         REAL NOT NULL,
                sale_id            TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS sales (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                sale_id            TEXT NOT NULL UNIQUE,
                sale_date          TEXT NOT NULL,
                customer_id        INTEGER REFERENCES customers(id),
                cust_code          TEXT NOT NULL,
                cust_name          TEXT NOT NULL,
                customer_type      TEXT DEFAULT 'RETAIL',
                product_variant_id INTEGER REFERENCES product_variants(id),
                product_code       TEXT NOT NULL,
                product_name       TEXT NOT NULL,
                pack_size          TEXT NOT NULL,
                qty                INTEGER NOT NULL,
                unit_price         REAL NOT NULL,
                total              REAL NOT NULL,
                cogs               REAL DEFAULT 0,
                gross_profit       REAL DEFAULT 0,
                invoice_id         INTEGER REFERENCES invoices(id),
                notes              TEXT DEFAULT '',
                created_at         TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS customer_payments (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                payment_ref  TEXT NOT NULL UNIQUE,
                customer_id  INTEGER NOT NULL REFERENCES customers(id),
                payment_date TEXT NOT NULL,
                amount       REAL NOT NULL,
                payment_mode TEXT DEFAULT 'CASH'
                             CHECK(payment_mode IN ('CASH','BANK_TRANSFER','CHEQUE','OTHER')),
                notes        TEXT DEFAULT '',
                created_at   TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS payment_allocations (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                payment_id       INTEGER NOT NULL REFERENCES customer_payments(id),
                invoice_id       INTEGER NOT NULL REFERENCES invoices(id),
                allocated_amount REAL NOT NULL,
                UNIQUE (payment_id, invoice_id)
            )""",
            # ── Purchasing & AP ──────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS supplier_bills (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                bill_number  TEXT NOT NULL UNIQUE,
                supplier_id  INTEGER NOT NULL REFERENCES suppliers(id),
                bill_date    TEXT NOT NULL,
                due_date     TEXT NOT NULL,
                status       TEXT DEFAULT 'UNPAID'
                             CHECK(status IN ('UNPAID','PARTIAL','PAID','VOID')),
                notes        TEXT DEFAULT '',
                total_amount REAL DEFAULT 0,
                supplier_ref TEXT DEFAULT '',
                created_at   TEXT DEFAULT (datetime('now')),
                voided_at    TEXT DEFAULT NULL,
                voided_by    TEXT DEFAULT NULL,
                void_note    TEXT DEFAULT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS supplier_bill_items (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                bill_id       INTEGER NOT NULL REFERENCES supplier_bills(id),
                ingredient_id INTEGER NOT NULL REFERENCES ingredients(id),
                quantity_kg   REAL NOT NULL,
                unit_cost_kg  REAL NOT NULL,
                line_total    REAL NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS supplier_payments (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                payment_ref  TEXT NOT NULL UNIQUE,
                supplier_id  INTEGER NOT NULL REFERENCES suppliers(id),
                payment_date TEXT NOT NULL,
                amount       REAL NOT NULL,
                payment_mode TEXT DEFAULT 'BANK_TRANSFER'
                             CHECK(payment_mode IN ('CASH','BANK_TRANSFER','CHEQUE','OTHER')),
                notes        TEXT DEFAULT '',
                created_at   TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS supplier_payment_allocations (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                payment_id       INTEGER NOT NULL REFERENCES supplier_payments(id),
                bill_id          INTEGER NOT NULL REFERENCES supplier_bills(id),
                allocated_amount REAL NOT NULL,
                UNIQUE (payment_id, bill_id)
            )""",
            # ── Sales reps & field ops ───────────────────────────────
            """CREATE TABLE IF NOT EXISTS sales_reps (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_id       TEXT UNIQUE NOT NULL,
                name              TEXT NOT NULL,
                phone             TEXT UNIQUE NOT NULL,
                pin_hash          TEXT NOT NULL,
                cnic              TEXT DEFAULT '',
                address           TEXT DEFAULT '',
                emergency_contact TEXT DEFAULT '',
                designation       TEXT NOT NULL DEFAULT 'SR',
                joining_date      TEXT DEFAULT '',
                reporting_to      INTEGER,
                primary_zone_id   INTEGER,
                status            TEXT NOT NULL DEFAULT 'active',
                pin_attempts      INTEGER NOT NULL DEFAULT 0,
                pin_locked        INTEGER NOT NULL DEFAULT 0,
                last_field_login  TEXT DEFAULT '',
                email             TEXT DEFAULT '',
                notes             TEXT DEFAULT '',
                whatsapp_apikey   TEXT DEFAULT '',
                created_at        TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS routes (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                zone_id    INTEGER NOT NULL,
                name       TEXT NOT NULL,
                visit_days TEXT DEFAULT '',
                active     INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS rep_routes (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id        INTEGER NOT NULL,
                route_id      INTEGER NOT NULL,
                assigned_from TEXT NOT NULL,
                assigned_to   TEXT DEFAULT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS route_customers (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                route_id      INTEGER NOT NULL,
                customer_id   INTEGER NOT NULL,
                stop_sequence INTEGER NOT NULL DEFAULT 0,
                UNIQUE(route_id, customer_id)
            )""",
            """CREATE TABLE IF NOT EXISTS beat_visits (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id            INTEGER NOT NULL,
                customer_id       INTEGER NOT NULL,
                route_id          INTEGER NOT NULL,
                visit_date        TEXT NOT NULL,
                outcome           TEXT NOT NULL DEFAULT 'visited',
                payment_collected REAL NOT NULL DEFAULT 0,
                notes             TEXT DEFAULT '',
                created_at        TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS field_orders (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                order_ref            TEXT UNIQUE NOT NULL,
                rep_id               INTEGER NOT NULL,
                customer_id          INTEGER NOT NULL,
                visit_id             INTEGER,
                order_date           TEXT NOT NULL,
                status               TEXT NOT NULL DEFAULT 'pending',
                notes                TEXT DEFAULT '',
                invoice_id           INTEGER,
                route_id             INTEGER,
                cash_collected       REAL DEFAULT 0,
                confirmed_invoice_id INTEGER,
                created_at           TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS field_order_items (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id           INTEGER NOT NULL,
                product_variant_id INTEGER NOT NULL,
                quantity           INTEGER NOT NULL DEFAULT 0,
                unit_price         REAL NOT NULL DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS rep_salary_components (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id           INTEGER NOT NULL,
                basic_salary     REAL NOT NULL DEFAULT 0,
                fuel_allowance   REAL NOT NULL DEFAULT 0,
                mobile_allowance REAL NOT NULL DEFAULT 0,
                other_allowance  REAL NOT NULL DEFAULT 0,
                effective_from   TEXT NOT NULL,
                active           INTEGER NOT NULL DEFAULT 1
            )""",
            """CREATE TABLE IF NOT EXISTS rep_commission_rules (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id                INTEGER NOT NULL,
                base_commission_pct   REAL NOT NULL DEFAULT 0,
                accelerator_pct       REAL NOT NULL DEFAULT 0,
                target_bonus          REAL NOT NULL DEFAULT 0,
                effective_from        TEXT NOT NULL,
                active                INTEGER NOT NULL DEFAULT 1
            )""",
            """CREATE TABLE IF NOT EXISTS rep_targets (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id         INTEGER NOT NULL,
                month          TEXT NOT NULL,
                visit_target   INTEGER NOT NULL DEFAULT 0,
                revenue_target REAL NOT NULL DEFAULT 0,
                UNIQUE(rep_id, month)
            )""",
            """CREATE TABLE IF NOT EXISTS rep_advances (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id           INTEGER NOT NULL,
                advance_date     TEXT NOT NULL,
                amount           REAL NOT NULL,
                monthly_recovery REAL NOT NULL DEFAULT 0,
                outstanding      REAL NOT NULL,
                notes            TEXT DEFAULT '',
                approved_by      TEXT DEFAULT '',
                recovered        INTEGER DEFAULT 0,
                created_at       TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS rep_attendance (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id   INTEGER NOT NULL,
                att_date TEXT NOT NULL,
                status   TEXT NOT NULL DEFAULT 'present',
                notes    TEXT DEFAULT '',
                UNIQUE(rep_id, att_date)
            )""",
            """CREATE TABLE IF NOT EXISTS payroll_runs (
                id                     INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id                 INTEGER NOT NULL,
                month                  TEXT NOT NULL,
                basic_salary           REAL NOT NULL DEFAULT 0,
                fuel_allowance         REAL NOT NULL DEFAULT 0,
                mobile_allowance       REAL NOT NULL DEFAULT 0,
                other_allowance        REAL NOT NULL DEFAULT 0,
                commission             REAL NOT NULL DEFAULT 0,
                accelerator_commission REAL NOT NULL DEFAULT 0,
                target_bonus           REAL NOT NULL DEFAULT 0,
                gross_pay              REAL NOT NULL DEFAULT 0,
                advance_recovery       REAL NOT NULL DEFAULT 0,
                absent_deduction       REAL NOT NULL DEFAULT 0,
                other_deductions       REAL NOT NULL DEFAULT 0,
                net_pay                REAL NOT NULL DEFAULT 0,
                sales_achieved         REAL NOT NULL DEFAULT 0,
                visits_done            INTEGER NOT NULL DEFAULT 0,
                status                 TEXT NOT NULL DEFAULT 'draft',
                notes                  TEXT DEFAULT '',
                period                 TEXT DEFAULT '',
                base_salary            REAL DEFAULT 0,
                actual_sales           REAL DEFAULT 0,
                target_amount          REAL DEFAULT 0,
                base_commission        REAL DEFAULT 0,
                accelerator_bonus      REAL DEFAULT 0,
                total_commission       REAL DEFAULT 0,
                total_advances         REAL DEFAULT 0,
                run_at                 TEXT,
                created_at             TEXT DEFAULT (datetime('now')),
                UNIQUE(rep_id, month)
            )""",
            # ── Audit ────────────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS change_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                record_id  TEXT NOT NULL,
                action     TEXT NOT NULL CHECK(action IN ('INSERT','UPDATE','DELETE','VOID')),
                old_value  TEXT DEFAULT NULL,
                new_value  TEXT DEFAULT NULL,
                changed_by TEXT DEFAULT 'system',
                timestamp  TEXT DEFAULT (datetime('now'))
            )""",
        ]

        for sql in stmts:
            c.execute(sql)
        c.commit()

        # Seed id_counters rows
        for entity in ('work_order', 'customer_order', 'purchase_order', 'sku', 'ingredient'):
            exists = c.execute("SELECT 1 FROM id_counters WHERE entity=?", (entity,)).fetchone()
            if not exists:
                c.execute("INSERT INTO id_counters (entity, last_num) VALUES (?,0)", (entity,))
        c.commit()

        # Seed price_types (required for pricing and production cost features)
        for code, label in [
            ('mfg_cost',    'Manufacturing Cost'),
            ('ex_factory',  'Ex-Factory Price'),
            ('distributor', 'Distributor Price'),
            ('retail_mrp',  'Retail MRP'),
        ]:
            c.execute("INSERT OR IGNORE INTO price_types (code, label) VALUES (?,?)", (code, label))
        c.commit()

        print("  ✓ Full schema: all tables verified / created")
    finally:
        c.close()


# ═══════════════════════════════════════════════════════════════════
#  WORK ORDERS & CUSTOMER ORDERS
# ═══════════════════════════════════════════════════════════════════

def ensure_work_orders_table():
    """Create work_orders table if not exists. Also seed id_counters row."""
    c = _conn()
    try:
        # Ensure id_counters table exists (needed for fresh databases)
        c.execute("""
            CREATE TABLE IF NOT EXISTS id_counters (
                entity    TEXT PRIMARY KEY,
                last_num  INTEGER NOT NULL DEFAULT 0
            )
        """)
        c.commit()
        c.execute("""
            CREATE TABLE IF NOT EXISTS work_orders (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                wo_number           TEXT    NOT NULL UNIQUE,
                product_variant_id  INTEGER NOT NULL,
                qty_units           INTEGER NOT NULL,
                target_date         TEXT,
                status              TEXT    NOT NULL DEFAULT 'planned',
                notes               TEXT    DEFAULT '',
                feasibility_ok      INTEGER DEFAULT 0,
                batch_id            TEXT    DEFAULT NULL,
                created_at          TEXT    DEFAULT (datetime('now')),
                updated_at          TEXT    DEFAULT (datetime('now')),
                FOREIGN KEY (product_variant_id) REFERENCES product_variants(id)
            )
        """)
        # Ensure id_counters has a row for work_order
        existing = c.execute("SELECT 1 FROM id_counters WHERE entity='work_order'").fetchone()
        if not existing:
            c.execute("INSERT INTO id_counters (entity, last_num) VALUES ('work_order', 0)")
        c.commit()
        print("  ✓ Work Orders: table ready")
    finally:
        c.close()
    save_db()


def ensure_customer_orders_schema():
    """Create customer_orders + customer_order_items tables and add FK columns to work_orders/invoices."""
    c = _conn()
    try:
        # ── New tables ────────────────────────────────────────────
        c.execute("""
            CREATE TABLE IF NOT EXISTS customer_orders (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                order_number    TEXT    NOT NULL UNIQUE,
                customer_id     INTEGER NOT NULL REFERENCES customers(id),
                order_date      TEXT    NOT NULL,
                required_date   TEXT,
                status          TEXT    NOT NULL DEFAULT 'draft',
                notes           TEXT    DEFAULT '',
                created_at      TEXT    DEFAULT (datetime('now')),
                updated_at      TEXT    DEFAULT (datetime('now'))
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS customer_order_items (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id            INTEGER NOT NULL REFERENCES customer_orders(id),
                product_variant_id  INTEGER NOT NULL REFERENCES product_variants(id),
                qty_ordered         INTEGER NOT NULL,
                unit_price          REAL    NOT NULL DEFAULT 0,
                line_total          REAL    NOT NULL DEFAULT 0,
                qty_in_production   INTEGER NOT NULL DEFAULT 0,
                qty_invoiced        INTEGER NOT NULL DEFAULT 0
            )
        """)

        # ── Add FK columns to existing tables (idempotent) ───────
        for sql in [
            "ALTER TABLE work_orders    ADD COLUMN customer_order_id      INTEGER REFERENCES customer_orders(id)",
            "ALTER TABLE work_orders    ADD COLUMN customer_order_item_id  INTEGER REFERENCES customer_order_items(id)",
            "ALTER TABLE invoices       ADD COLUMN customer_order_id      INTEGER REFERENCES customer_orders(id)",
            "ALTER TABLE invoice_items  ADD COLUMN sale_id TEXT",
        ]:
            try:
                c.execute(sql)
            except Exception:
                pass   # column already exists

        # ── Seed id_counters ──────────────────────────────────────
        existing = c.execute("SELECT 1 FROM id_counters WHERE entity='customer_order'").fetchone()
        if not existing:
            c.execute("INSERT INTO id_counters (entity, last_num) VALUES ('customer_order', 0)")

        c.commit()
        print("  ✓ Customer Orders: schema ready")
    finally:
        c.close()
    save_db()


def ensure_review_queue_schema():
    """
    Phase 3 — Review Queue & Soft Hold schema (idempotent).
    Adds order_source, approval columns to customer_orders;
    qty_soft_hold to customer_order_items;
    creates order_hold_expiry and order_approval_rules tables.
    """
    c = _conn()
    try:
        # ── New columns on customer_orders ────────────────────────
        for sql in [
            "ALTER TABLE customer_orders ADD COLUMN order_source        TEXT    DEFAULT 'internal'",
            "ALTER TABLE customer_orders ADD COLUMN approval_method     TEXT    DEFAULT 'manual'",
            "ALTER TABLE customer_orders ADD COLUMN approval_timestamp  TEXT    DEFAULT NULL",
            "ALTER TABLE customer_orders ADD COLUMN approval_note       TEXT    DEFAULT ''",
            "ALTER TABLE customer_orders ADD COLUMN rejection_reason    TEXT    DEFAULT ''",
        ]:
            try:
                c.execute(sql); c.commit()
            except Exception:
                pass   # column already exists

        # ── Soft hold quantity on order items ─────────────────────
        try:
            c.execute("ALTER TABLE customer_order_items ADD COLUMN qty_soft_hold INTEGER DEFAULT 0")
            c.commit()
        except Exception:
            pass   # already exists

        # ── Hold tracking table ───────────────────────────────────
        c.execute("""
            CREATE TABLE IF NOT EXISTS order_hold_expiry (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id          INTEGER NOT NULL UNIQUE REFERENCES customer_orders(id),
                hold_placed_at    TEXT    NOT NULL,
                hold_expires_at   TEXT    NOT NULL,
                is_expired        INTEGER NOT NULL DEFAULT 0,
                notification_sent INTEGER NOT NULL DEFAULT 0,
                expired_at        TEXT    DEFAULT NULL
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_hold_expiry ON order_hold_expiry(hold_expires_at, is_expired)")

        # ── Auto-approval rules registry (infrastructure; all disabled by default) ──
        c.execute("""
            CREATE TABLE IF NOT EXISTS order_approval_rules (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_name   TEXT    NOT NULL UNIQUE,
                rule_code   TEXT    NOT NULL,
                enabled     INTEGER NOT NULL DEFAULT 0,
                created_at  TEXT    DEFAULT (datetime('now')),
                notes       TEXT    DEFAULT ''
            )
        """)

        # ── WhatsApp notification columns (Phase 4 — idempotent) ──────
        for sql in [
            "ALTER TABLE users        ADD COLUMN whatsapp_phone  TEXT DEFAULT ''",
            "ALTER TABLE users        ADD COLUMN whatsapp_apikey TEXT DEFAULT ''",
            "ALTER TABLE sales_reps   ADD COLUMN whatsapp_apikey TEXT DEFAULT ''",
            "ALTER TABLE customer_orders ADD COLUMN created_by_rep_id INTEGER DEFAULT NULL",
            "ALTER TABLE order_hold_expiry ADD COLUMN expiry_warning_sent INTEGER DEFAULT 0",
            "ALTER TABLE ingredients ADD COLUMN unit TEXT DEFAULT 'kg'",
            "ALTER TABLE ingredients ADD COLUMN updated_at TEXT DEFAULT NULL",
            "ALTER TABLE supplier_bills ADD COLUMN po_id INTEGER DEFAULT NULL REFERENCES purchase_orders(id)",
        ]:
            try:
                c.execute(sql); c.commit()
            except Exception:
                pass   # column already exists

        c.commit()
        print("  ✓ Review Queue: schema ready (soft hold, hold expiry, approval rules, WA columns)")
    finally:
        c.close()
    save_db()


# ═══════════════════════════════════════════════════════════════════
#  SUPPLIER BILLS VOID MIGRATION
# ═══════════════════════════════════════════════════════════════════

def _migrate_supplier_bills_void():
    """Migration: add voided_at/voided_by/void_note columns and VOID status to supplier_bills.
    SQLite doesn't allow ALTER TABLE to change a CHECK constraint, so we recreate the table."""
    c = _conn()
    try:
        c.execute("PRAGMA foreign_keys=OFF")
        # Check if VOID is already in the constraint
        tbl = c.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='supplier_bills'"
        ).fetchone()
        if not tbl:
            return  # table doesn't exist yet — schema creation handles it
        tbl_sql = tbl[0] or ''
        cols = [row[1] for row in c.execute("PRAGMA table_info(supplier_bills)")]

        needs_rebuild    = "'VOID'" not in tbl_sql and '"VOID"' not in tbl_sql
        needs_voided_cols = 'voided_at' not in cols

        if needs_rebuild:
            # Recreate table with VOID in CHECK constraint + voided columns
            c.execute("""CREATE TABLE IF NOT EXISTS supplier_bills_new (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                bill_number  TEXT NOT NULL UNIQUE,
                supplier_id  INTEGER NOT NULL REFERENCES suppliers(id),
                bill_date    TEXT NOT NULL,
                due_date     TEXT NOT NULL,
                status       TEXT DEFAULT 'UNPAID'
                             CHECK(status IN ('UNPAID','PARTIAL','PAID','VOID')),
                notes        TEXT DEFAULT '',
                total_amount REAL DEFAULT 0,
                supplier_ref TEXT DEFAULT '',
                created_at   TEXT DEFAULT (datetime('now')),
                voided_at    TEXT DEFAULT NULL,
                voided_by    TEXT DEFAULT NULL,
                void_note    TEXT DEFAULT NULL
            )""")
            existing_cols = [row[1] for row in c.execute("PRAGMA table_info(supplier_bills)")]
            c.execute(f"""INSERT INTO supplier_bills_new
                SELECT id, bill_number, supplier_id, bill_date, due_date, status,
                       notes, total_amount, COALESCE(supplier_ref,''), created_at,
                       {'voided_at' if 'voided_at' in existing_cols else 'NULL'},
                       {'voided_by' if 'voided_by' in existing_cols else 'NULL'},
                       {'void_note' if 'void_note' in existing_cols else 'NULL'}
                FROM supplier_bills""")
            c.execute("DROP TABLE supplier_bills")
            c.execute("ALTER TABLE supplier_bills_new RENAME TO supplier_bills")
            print("  ✓ supplier_bills: migrated — added VOID status + voided columns")
        elif needs_voided_cols:
            # Table has VOID but missing the voided columns — just add them
            for col in ['voided_at', 'voided_by', 'void_note']:
                if col not in cols:
                    try:
                        c.execute(f"ALTER TABLE supplier_bills ADD COLUMN {col} TEXT DEFAULT NULL")
                    except Exception:
                        pass
            print("  ✓ supplier_bills: added voided_at/voided_by/void_note columns")
        c.commit()
    except Exception as e:
        print(f"  ⚠ supplier_bills migration error: {e}")
        try: c.rollback()
        except: pass
    finally:
        try: c.execute("PRAGMA foreign_keys=ON")
        except: pass
        c.close()


# ═══════════════════════════════════════════════════════════════════
#  CHANGE LOG VOID ACTION
# ═══════════════════════════════════════════════════════════════════

def _migrate_change_log_void_action():
    """Migration: widen change_log CHECK constraint to include 'VOID'.
    SQLite doesn't allow ALTER TABLE to change a CHECK constraint, so we recreate the table."""
    c = _conn()
    try:
        c.execute("PRAGMA foreign_keys=OFF")
        tbl = c.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='change_log'"
        ).fetchone()
        if not tbl:
            return  # table doesn't exist yet — schema creation handles it
        tbl_sql = tbl[0] or ''
        if "'VOID'" in tbl_sql or '"VOID"' in tbl_sql:
            return  # already has VOID — nothing to do
        # Recreate with the wider constraint
        c.execute("""CREATE TABLE IF NOT EXISTS change_log_new (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            record_id  TEXT NOT NULL,
            action     TEXT NOT NULL CHECK(action IN ('INSERT','UPDATE','DELETE','VOID')),
            old_value  TEXT DEFAULT NULL,
            new_value  TEXT DEFAULT NULL,
            changed_by TEXT DEFAULT 'system',
            timestamp  TEXT DEFAULT (datetime('now'))
        )""")
        c.execute("""INSERT INTO change_log_new
            SELECT id, table_name, record_id, action, old_value, new_value, changed_by, timestamp
            FROM change_log""")
        c.execute("DROP TABLE change_log")
        c.execute("ALTER TABLE change_log_new RENAME TO change_log")
        c.commit()
        print("  ✓ change_log: widened CHECK constraint to include VOID action")
    except Exception as e:
        print(f"  ⚠ change_log migration error: {e}")
        try: c.rollback()
        except: pass
    finally:
        try: c.execute("PRAGMA foreign_keys=ON")
        except: pass
        c.close()


# ═══════════════════════════════════════════════════════════════════
#  CUSTOMER TYPE WHOLESALE
# ═══════════════════════════════════════════════════════════════════

def _migrate_customer_type_wholesale():
    """Migration: add WHOLESALE to customer_type CHECK constraint (idempotent).
    SQLite doesn't allow ALTER COLUMN — recreates the customers table with updated CHECK."""
    c = _conn()
    try:
        schema = c.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='customers'"
        ).fetchone()
        if schema and 'WHOLESALE' in schema[0]:
            return  # already done
        c.execute("PRAGMA foreign_keys=OFF")
        c.execute("""
            CREATE TABLE IF NOT EXISTS customers_new (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                code               TEXT NOT NULL UNIQUE,
                account_number     TEXT DEFAULT NULL,
                name               TEXT NOT NULL,
                customer_type      TEXT NOT NULL DEFAULT 'RETAIL'
                                   CHECK(customer_type IN ('RETAIL','DIRECT','WHOLESALE')),
                category           TEXT DEFAULT '',
                city               TEXT DEFAULT '',
                address            TEXT DEFAULT '',
                phone              TEXT DEFAULT '',
                email              TEXT DEFAULT '',
                default_pack       TEXT DEFAULT '50g',
                payment_terms_days INTEGER DEFAULT 30,
                credit_limit       REAL DEFAULT 0,
                active             INTEGER DEFAULT 1,
                created_at         TEXT DEFAULT (date('now'))
            )
        """)
        c.execute("""
            INSERT INTO customers_new
            SELECT id, code, account_number, name,
                   CASE WHEN customer_type IN ('RETAIL','DIRECT','WHOLESALE')
                        THEN customer_type ELSE 'RETAIL' END,
                   category, city, address, phone, email, default_pack,
                   payment_terms_days, credit_limit, active, created_at
            FROM customers
        """)
        c.execute("DROP TABLE customers")
        c.execute("ALTER TABLE customers_new RENAME TO customers")
        c.commit()
        print("  ✓ Migration: customer_type CHECK updated — WHOLESALE added")
    except Exception as e:
        print(f"  ⚠ _migrate_customer_type_wholesale error: {e}")
        try: c.rollback()
        except: pass
    finally:
        try: c.execute("PRAGMA foreign_keys=ON")
        except: pass
        c.close()
    save_db()


# ═══════════════════════════════════════════════════════════════════
#  B2B ORDER COLUMNS
# ═══════════════════════════════════════════════════════════════════

def _ensure_b2b_order_columns():
    """Idempotent: add out_of_route + idempotency_key columns to customer_orders for B2B portal."""
    c = _conn()
    try:
        for sql in [
            "ALTER TABLE customer_orders ADD COLUMN out_of_route    INTEGER DEFAULT 0",
            "ALTER TABLE customer_orders ADD COLUMN idempotency_key TEXT    DEFAULT NULL",
        ]:
            try:
                c.execute(sql)
            except Exception:
                pass  # column already exists
        c.commit()
        print("  ✓ B2B columns: out_of_route + idempotency_key ready")
    finally:
        c.close()
    save_db()


# ═══════════════════════════════════════════════════════════════════
#  SUPPLIER BILLS & PURCHASE ORDERS SCHEMA
# ═══════════════════════════════════════════════════════════════════

def ensure_supplier_bills_schema():
    """Add total_amount and supplier_ref columns to supplier_bills (idempotent migration)."""
    c = _conn()
    try:
        # Add stored total_amount column — used as authoritative total when items have zero costs
        try:
            c.execute("ALTER TABLE supplier_bills ADD COLUMN total_amount REAL DEFAULT 0")
            c.commit()
            print("  ✓ Supplier Bills: added total_amount column")
        except Exception:
            pass  # column already exists

        # Add supplier_ref column — supplier's own invoice/reference number (prevents duplicates)
        try:
            c.execute("ALTER TABLE supplier_bills ADD COLUMN supplier_ref TEXT DEFAULT ''")
            c.commit()
            print("  ✓ Supplier Bills: added supplier_ref column")
        except Exception:
            pass  # column already exists

        # Back-fill total_amount from existing bill items for any bills where it's still NULL/0
        bills_to_fix = c.execute("""
            SELECT sb.id, COALESCE(SUM(sbi.line_total),0) as items_sum
            FROM supplier_bills sb
            LEFT JOIN supplier_bill_items sbi ON sbi.bill_id = sb.id
            WHERE sb.total_amount IS NULL OR sb.total_amount = 0
            GROUP BY sb.id
        """).fetchall()
        fixed = 0
        for row in bills_to_fix:
            if row[1] > 0:
                c.execute("UPDATE supplier_bills SET total_amount=? WHERE id=?", (row[1], row[0]))
                fixed += 1
        if fixed:
            c.commit()
            print(f"  ✓ Supplier Bills: back-filled total_amount for {fixed} bill(s)")
        else:
            c.commit()
    finally:
        c.close()
    save_db()


def ensure_purchase_orders_schema():
    """Create purchase_orders and po_items tables if not exists."""
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS purchase_orders (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                po_number      TEXT    NOT NULL UNIQUE,
                supplier_id    INTEGER NOT NULL REFERENCES suppliers(id),
                po_date        TEXT    NOT NULL,
                expected_date  TEXT,
                status         TEXT    NOT NULL DEFAULT 'draft',
                notes          TEXT    DEFAULT '',
                payment_terms  TEXT    NOT NULL DEFAULT 'CREDIT',
                bill_id        INTEGER REFERENCES supplier_bills(id),
                created_at     TEXT    DEFAULT (datetime('now')),
                updated_at     TEXT    DEFAULT (datetime('now'))
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS po_items (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                po_id          INTEGER NOT NULL REFERENCES purchase_orders(id),
                ingredient_id  INTEGER NOT NULL REFERENCES ingredients(id),
                quantity_kg    REAL    NOT NULL,
                received_kg    REAL    NOT NULL DEFAULT 0,
                unit_cost_kg   REAL    NOT NULL DEFAULT 0,
                notes          TEXT    DEFAULT ''
            )
        """)
        # id_counters row for purchase_order
        existing = c.execute("SELECT 1 FROM id_counters WHERE entity='purchase_order'").fetchone()
        if not existing:
            c.execute("INSERT INTO id_counters (entity, last_num) VALUES ('purchase_order', 0)")
        c.commit()
        print("  ✓ Purchase Orders: tables ready")
    finally:
        c.close()
    save_db()


# ═══════════════════════════════════════════════════════════════════
#  PRODUCTION BATCH COST COLUMN
# ═══════════════════════════════════════════════════════════════════

def ensure_batch_cost_column():
    """
    Add unit_cost_at_posting to production_batches (idempotent migration).
    This column freezes the ingredient cost at the moment a batch is posted —
    so historical COGS are never affected by future price changes.
    """
    c = _conn()
    try:
        try:
            c.execute("ALTER TABLE production_batches ADD COLUMN unit_cost_at_posting REAL DEFAULT 0")
            c.commit()
            print("  ✓ Production Batches: added unit_cost_at_posting column")
        except Exception:
            pass  # column already exists
    finally:
        c.close()


# ═══════════════════════════════════════════════════════════════════
#  MASTER SCHEMA ADDITIONS
# ═══════════════════════════════════════════════════════════════════

def ensure_master_schema():
    """Add cost_per_kg, active to ingredients; credit_limit to customers; create price_history table."""
    c = _conn()
    try:
        # ingredients.cost_per_kg (original)
        try:
            c.execute("ALTER TABLE ingredients ADD COLUMN cost_per_kg REAL NOT NULL DEFAULT 0")
            c.commit()
            print("  ✓ Masters: added cost_per_kg column to ingredients")
        except Exception:
            pass  # column already exists
        # ingredients.active (new)
        try:
            c.execute("ALTER TABLE ingredients ADD COLUMN active INTEGER NOT NULL DEFAULT 1")
            c.commit()
            print("  ✓ Masters: added active column to ingredients")
        except Exception:
            pass
        # customers.credit_limit (new)
        try:
            c.execute("ALTER TABLE customers ADD COLUMN credit_limit REAL DEFAULT 0")
            c.commit()
            print("  ✓ Masters: added credit_limit column to customers")
        except Exception:
            pass
        # customers.account_number (new)
        try:
            c.execute("ALTER TABLE customers ADD COLUMN account_number TEXT DEFAULT NULL")
            c.commit()
            print("  ✓ Masters: added account_number column to customers")
        except Exception:
            pass
        # customers.address (new)
        try:
            c.execute("ALTER TABLE customers ADD COLUMN address TEXT DEFAULT ''")
            c.commit()
            print("  ✓ Masters: added address column to customers")
        except Exception:
            pass

        # customers.zone_id — territory zone for sales rep out-of-zone detection
        try:
            c.execute("ALTER TABLE customers ADD COLUMN zone_id INTEGER DEFAULT NULL")
            c.commit()
            print("  ✓ Masters: added zone_id column to customers")
        except Exception:
            pass  # column already exists

        # ── P2.5 void/cancel migrations ─────────────────────────────────
        # invoices: add voided_at / voided_by / void_note columns
        for col, dflt in [('voided_at', 'NULL'), ('voided_by', "''"), ('void_note', "''")]:
            try:
                c.execute(f"ALTER TABLE invoices ADD COLUMN {col} TEXT DEFAULT {dflt}")
                c.commit()
                print(f"  ✓ Masters: added invoices.{col}")
            except Exception:
                pass

        # sales: add voided flag so voided invoices restore finished-goods stock
        try:
            c.execute("ALTER TABLE sales ADD COLUMN voided INTEGER DEFAULT 0")
            c.commit()
            print("  ✓ Masters: added sales.voided column")
        except Exception:
            pass

        # supplier_bills: add VOID to the status CHECK constraint via writable_schema
        try:
            sb_schema = c.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='supplier_bills'"
            ).fetchone()
            if sb_schema and 'VOID' not in (sb_schema[0] or ''):
                new_sql = (sb_schema[0] or '').replace(
                    "CHECK(status IN ('UNPAID','PARTIAL','PAID'))",
                    "CHECK(status IN ('UNPAID','PARTIAL','PAID','VOID'))"
                )
                c.execute("PRAGMA writable_schema = ON")
                c.execute(
                    "UPDATE sqlite_master SET sql=? WHERE type='table' AND name='supplier_bills'",
                    (new_sql,)
                )
                c.execute("PRAGMA writable_schema = OFF")
                c.commit()
                print("  ✓ Masters: added VOID to supplier_bills.status CHECK constraint")
        except Exception as e:
            print(f"  ⚠ supplier_bills VOID constraint migration: {e}")
        for col, dflt in [('voided_at', 'NULL'), ('voided_by', "''"), ('void_note', "''")]:
            try:
                c.execute(f"ALTER TABLE supplier_bills ADD COLUMN {col} TEXT DEFAULT {dflt}")
                c.commit()
                print(f"  ✓ Masters: added supplier_bills.{col}")
            except Exception:
                pass

        c.execute("""
            CREATE TABLE IF NOT EXISTS ingredient_price_history (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                ingredient_id   INTEGER NOT NULL,
                old_cost_per_kg REAL,
                new_cost_per_kg REAL NOT NULL,
                pct_change      REAL,
                changed_at      TEXT DEFAULT (datetime('now')),
                source          TEXT DEFAULT 'master_sync',
                FOREIGN KEY (ingredient_id) REFERENCES ingredients(id)
            )
        """)
        c.commit()

        # ── P1 Sprint: add ADJUSTMENT to payment_mode CHECK constraints ──────
        for tbl in ('customer_payments', 'supplier_payments'):
            try:
                tbl_schema = c.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
                ).fetchone()
                if tbl_schema and 'ADJUSTMENT' not in (tbl_schema[0] or ''):
                    old_check = "CHECK(payment_mode IN ('CASH','BANK_TRANSFER','CHEQUE','OTHER'))"
                    new_check = "CHECK(payment_mode IN ('CASH','BANK_TRANSFER','CHEQUE','OTHER','ADJUSTMENT'))"
                    new_sql   = (tbl_schema[0] or '').replace(old_check, new_check)
                    if new_sql != tbl_schema[0]:
                        c.execute("PRAGMA writable_schema = ON")
                        c.execute(
                            "UPDATE sqlite_master SET sql=? WHERE type='table' AND name=?",
                            (new_sql, tbl)
                        )
                        c.execute("PRAGMA writable_schema = OFF")
                        c.commit()
                        print(f"  ✓ Migration: added ADJUSTMENT to {tbl}.payment_mode CHECK constraint")
            except Exception as e:
                print(f"  ⚠ {tbl} ADJUSTMENT constraint migration: {e}")

    finally:
        c.close()
    save_db()


# ═══════════════════════════════════════════════════════════════════
#  COSTING CONFIG
# ═══════════════════════════════════════════════════════════════════

def ensure_costing_config():
    """Create costing_config + costing_config_history tables and seed defaults. Idempotent."""
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS costing_config (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL,
                label      TEXT,
                updated_at TEXT,
                updated_by TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS costing_config_history (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                config_key TEXT NOT NULL,
                old_value  TEXT,
                new_value  TEXT NOT NULL,
                pct_change REAL,
                changed_by TEXT,
                changed_at TEXT DEFAULT (datetime('now')),
                note       TEXT
            )
        """)
        defaults = [
            ('packaging_cost_per_unit', '15.00', 'Packaging Cost per Unit (Rs)'),
            ('overhead_pct',            '0.10',  'Overhead % of RM Cost'),
            ('margin_mfr',              '1.30',  'Direct Sale Margin Multiplier'),
            ('margin_dist',             '1.10',  'Distributor Margin Multiplier'),
            ('margin_mrp',              '1.22',  'MRP Margin Multiplier'),
            ('margin_floor_pct',        '30.00', 'Minimum Profit Margin % (alert threshold)'),
            ('labour_cost_per_unit',    '5.00',  'Labour Cost per Unit (Rs)'),
            # ── Itemized cost lines (₨/pack). Category totals feed compute_standard_cost.
            #    Seeds carry the legacy lumped values: packaging 15 → pouch; labour 5 → labour.
            #    Overhead seeded fixed-₨ at 0 (set Transport/Salaries/Admin in Cost Parameters).
            ('pkg_pouch',        '15.00', 'Packaging — Pouch (Rs/pack)'),
            ('pkg_label',        '0.00',  'Packaging — Adhesive Label (Rs/pack)'),
            ('pkg_carton',       '0.00',  'Packaging — Master Carton share (Rs/pack)'),
            ('conv_labour',      '5.00',  'Conversion — Labour (Rs/pack)'),
            ('conv_electricity', '0.00',  'Conversion — Electricity (Rs/pack)'),
            ('conv_gas',         '0.00',  'Conversion — Gas (Rs/pack)'),
            ('conv_rent',        '0.00',  'Conversion — Rent share (Rs/pack)'),
            ('ovh_transport',    '0.00',  'Overhead — Transport (Rs/pack)'),
            ('ovh_salaries',     '0.00',  'Overhead — Salaries share (Rs/pack)'),
            ('ovh_admin',        '0.00',  'Overhead — Admin share (Rs/pack)'),
            # ── Two-engine model (2026-07-03): FIXED costs entered as MONTHLY totals,
            #    absorbed in compute_standard_cost as (sum ÷ normal_monthly_volume) per pack.
            #    Distinct from the per-pack VARIABLE lines above (pkg_*, conv_labour/gas/electricity).
            ('fix_salaries',     '0.00',  'Fixed — Salaries, permanent staff (Rs/MONTH)'),
            ('fix_rent',         '0.00',  'Fixed — Rent (Rs/MONTH)'),
            ('fix_transport',    '0.00',  'Fixed — Transport, own vehicle (Rs/MONTH)'),
            ('fix_admin',        '0.00',  'Fixed — Admin (Rs/MONTH)'),
            # Dashboard-only Rs→US$ display rate (owner-set). Never used in costing/invoices.
            ('usd_rate',         '280',   'Rs per US$ (dashboard $ display; owner-set)'),
        ]
        for key, value, label in defaults:
            c.execute(
                "INSERT OR IGNORE INTO costing_config (key, value, label) VALUES (?,?,?)",
                (key, value, label)
            )
        # Update overhead if still at old placeholder 0.29
        c.execute("UPDATE costing_config SET value='0.10' WHERE key='overhead_pct' AND value='0.29'")
        # Update stale labels
        c.execute("UPDATE costing_config SET label='Direct Sale Margin Multiplier' WHERE key='margin_mfr'")
        c.execute("UPDATE costing_config SET label='Minimum Profit Margin % (alert threshold)' WHERE key='margin_floor_pct'")
        c.commit()
        print("  ✓ Costing config table ready")
    finally:
        c.close()


def ensure_operating_costs():
    """Monthly operating-costs log (salaries, utilities, rent, …) + the normal-volume
    denominator used to derive per-pack labour/utilities/overhead. Idempotent."""
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS monthly_operating_costs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                month      TEXT NOT NULL,            -- 'YYYY-MM'
                category   TEXT NOT NULL,            -- salaries | electricity | gas | rent | transport | admin
                amount     REAL NOT NULL DEFAULT 0,
                note       TEXT,
                updated_at TEXT DEFAULT (datetime('now')),
                updated_by TEXT,
                UNIQUE(month, category)
            )
        """)
        # Normal monthly volume = the stable denominator (packs/month) for fixed-cost absorption.
        c.execute("""
            INSERT OR IGNORE INTO costing_config (key, value, label)
            VALUES ('normal_monthly_volume', '600', 'Normal monthly volume (packs) — fixed-cost denominator')
        """)
        # Bump the old default (1000) to the soft-launch volume (600). Only touches the
        # untouched default — if AK set any other value it won't be 1000, so it's left alone. Idempotent.
        c.execute("UPDATE costing_config SET value='600' WHERE key='normal_monthly_volume' AND value='1000'")
        c.commit()
        print("  ✓ Operating costs table ready")
    finally:
        c.close()


def ensure_wo_produced_units():
    """Add produced_units to work_orders so ONE work order can be made in several batches
    (e.g. 250/week toward a 1000 WO). The WO stays in_progress until produced reaches the
    target, then completes. Idempotent."""
    c = _conn()
    try:
        cols = [r[1] for r in c.execute("PRAGMA table_info(work_orders)").fetchall()]
        if cols and 'produced_units' not in cols:
            c.execute("ALTER TABLE work_orders ADD COLUMN produced_units INTEGER NOT NULL DEFAULT 0")
            c.commit()
        print("  ✓ work_orders.produced_units ready")
    except Exception as e:
        print(f"  ⚠ ensure_wo_produced_units: {e}")
    finally:
        c.close()


def ensure_ingredient_target_grams():
    """Add ingredients.target_grams — the 'full tank' / par stock level per ingredient (grams).
    Powers the fuel-gauge inventory view + the bulk stock loader. Idempotent."""
    c = _conn()
    try:
        cols = [r[1] for r in c.execute("PRAGMA table_info(ingredients)").fetchall()]
        if cols and 'target_grams' not in cols:
            c.execute("ALTER TABLE ingredients ADD COLUMN target_grams REAL NOT NULL DEFAULT 0")
            c.commit()
        print("  ✓ ingredients.target_grams ready")
    except Exception as e:
        print(f"  ⚠ ensure_ingredient_target_grams: {e}")
    finally:
        c.close()


def ensure_batch_stages():
    """Granular batch execution.

    A *batch run* is the staged making of (part of) a work order. Raw material is
    consumed when the run starts (first stage); the run is advanced stage-by-stage
    with a timestamped event log; on owner verification at the final stage it is
    finalised into a production_batch (finished goods) at the ACTUAL yield — which
    is what catches wastage (planned 500, got 485).

    - batch_stage_defs : the process stages, seeded with a default 7, fully editable
      (add/rename/reorder) without a code change.
    - batch_runs       : one staged run (links to a work order + the FG batch it becomes).
    - batch_run_events : the per-stage timeline (entered / note / cancelled).

    Idempotent — creates tables if missing, seeds default stages only when empty.
    """
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS batch_stage_defs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT NOT NULL,
                sort_order INTEGER NOT NULL DEFAULT 0,
                active     INTEGER NOT NULL DEFAULT 1
            )
        """)
        if c.execute("SELECT COUNT(*) FROM batch_stage_defs").fetchone()[0] == 0:
            for nm, so in (('Received', 10), ('Cleaning / prep', 20), ('Roasting', 30),
                           ('Cooling', 40), ('Blending', 50), ('Packaging', 60), ('Done', 70)):
                c.execute("INSERT INTO batch_stage_defs (name, sort_order, active) VALUES (?,?,1)", (nm, so))

        c.execute("""
            CREATE TABLE IF NOT EXISTS batch_runs (
                id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                run_code                 TEXT UNIQUE,
                wo_id                    INTEGER,
                product_id               INTEGER,
                product_variant_id       INTEGER,
                bom_version_id           INTEGER,
                pack_size                TEXT,
                qty_units                INTEGER NOT NULL,
                qty_grams                REAL,
                planned_ingredient_cost  REAL DEFAULT 0,
                planned_unit_cost        REAL DEFAULT 0,
                current_stage_id         INTEGER,
                status                   TEXT NOT NULL DEFAULT 'in_progress',
                actual_qty_units         INTEGER,
                batch_id                 TEXT,
                notes                    TEXT,
                started_by               TEXT,
                verified_by              TEXT,
                started_at               TEXT DEFAULT (datetime('now')),
                finished_at              TEXT
            )
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS batch_run_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id      INTEGER NOT NULL,
                stage_id    INTEGER,
                stage_name  TEXT,
                event       TEXT,
                note        TEXT,
                by_user     TEXT,
                at          TEXT DEFAULT (datetime('now'))
            )
        """)
        c.commit()
        print("  ✓ batch_stages / batch_runs ready")
    except Exception as e:
        print(f"  ⚠ ensure_batch_stages: {e}")
    finally:
        c.close()


def ensure_rep_app_access():
    """Per-app grant flags on a rep's phone identity. One phone login (phone + PIN) fronts both
    phone apps; these flags gate which sections a person gets on the unified launcher:
      - app_field defaults 1 (existing reps keep Sales),
      - app_batch defaults 0 (owner grants Production to the floor, e.g. FK + spouse).
    Idempotent."""
    c = _conn()
    try:
        cols = [r[1] for r in c.execute("PRAGMA table_info(sales_reps)").fetchall()]
        if cols and 'app_batch' not in cols:
            c.execute("ALTER TABLE sales_reps ADD COLUMN app_batch INTEGER NOT NULL DEFAULT 0")
        if cols and 'app_field' not in cols:
            c.execute("ALTER TABLE sales_reps ADD COLUMN app_field INTEGER NOT NULL DEFAULT 1")
        c.commit()
        print("  ✓ sales_reps.app_field / app_batch ready")
    except Exception as e:
        print(f"  ⚠ ensure_rep_app_access: {e}")
    finally:
        c.close()


def ensure_customer_gst():
    """Configurable per-customer GST.
      - customers.gst_applicable (0/1): who is charged GST (default 0 → GST off until flagged).
      - costing_config 'gst_rate' (default 18): the global rate, editable as needed.
      - invoices.gst_rate: the rate SNAPSHOTTED onto each invoice at creation (0 for a
        non-GST customer) so historical invoices stay stable if the flag/rate later change.
    Idempotent."""
    c = _conn()
    try:
        cc = [r[1] for r in c.execute("PRAGMA table_info(customers)").fetchall()]
        if cc and 'gst_applicable' not in cc:
            c.execute("ALTER TABLE customers ADD COLUMN gst_applicable INTEGER NOT NULL DEFAULT 0")
        ic = [r[1] for r in c.execute("PRAGMA table_info(invoices)").fetchall()]
        if ic and 'gst_rate' not in ic:
            c.execute("ALTER TABLE invoices ADD COLUMN gst_rate REAL NOT NULL DEFAULT 0")
        row = c.execute("SELECT 1 FROM costing_config WHERE key='gst_rate'").fetchone()
        if not row:
            c.execute("INSERT INTO costing_config (key, value, label) VALUES (?,?,?)",
                      ('gst_rate', '18', 'GST Rate (%)'))
        c.commit()
        print("  ✓ customer GST (gst_applicable / invoices.gst_rate / config gst_rate) ready")
    except Exception as e:
        print(f"  ⚠ ensure_customer_gst: {e}")
    finally:
        c.close()


def ensure_bill_vendor_capture():
    """Vendor-bill capture on supplier_bills so AP is backed by the REAL vendor invoice:
      - expected_amount   : the system estimate at creation (from PO receipt / bill items),
                            kept so we can flag variance against the actual vendor bill.
      - attachment_filename: the scanned/photographed vendor invoice (stored on the volume).
      - vendor_confirmed   : 1 once the real vendor bill has been captured & confirmed.
    (supplier_ref already holds the vendor's invoice number.) Backfills expected_amount to the
    current total_amount for existing bills (→ zero variance until a real bill is captured).
    Idempotent."""
    c = _conn()
    try:
        cols = [r[1] for r in c.execute("PRAGMA table_info(supplier_bills)").fetchall()]
        if cols and 'expected_amount' not in cols:
            c.execute("ALTER TABLE supplier_bills ADD COLUMN expected_amount REAL")
            c.execute("UPDATE supplier_bills SET expected_amount = total_amount WHERE expected_amount IS NULL")
        if cols and 'attachment_filename' not in cols:
            c.execute("ALTER TABLE supplier_bills ADD COLUMN attachment_filename TEXT DEFAULT ''")
        if cols and 'vendor_confirmed' not in cols:
            c.execute("ALTER TABLE supplier_bills ADD COLUMN vendor_confirmed INTEGER NOT NULL DEFAULT 0")
        c.commit()
        print("  ✓ supplier_bills vendor capture (expected_amount / attachment / vendor_confirmed) ready")
    except Exception as e:
        print(f"  ⚠ ensure_bill_vendor_capture: {e}")
    finally:
        c.close()


def ensure_ledger_po_link():
    """Procurement funnel foundation: link every raw-material PURCHASE_IN to its Purchase Order.
    Adds inventory_ledger.po_id (nullable) and backfills it from the free-text reference_id where
    it matches a known po_number. ADJUSTMENT / OPENING / PRODUCTION_USE keep po_id NULL — they are
    deliberately outside the funnel (corrections, opening balances, production usage are not
    purchases). Additive ADD COLUMN (movement_type CHECK unchanged, no table rebuild). Idempotent."""
    c = _conn()
    try:
        cols = [r[1] for r in c.execute("PRAGMA table_info(inventory_ledger)").fetchall()]
        if cols and 'po_id' not in cols:
            c.execute("ALTER TABLE inventory_ledger ADD COLUMN po_id INTEGER")
            c.execute("""
                UPDATE inventory_ledger
                   SET po_id = (SELECT po.id FROM purchase_orders po
                                WHERE po.po_number = inventory_ledger.reference_id)
                 WHERE movement_type='PURCHASE_IN'
                   AND po_id IS NULL
                   AND reference_id IN (SELECT po_number FROM purchase_orders)
            """)
        c.commit()
        print("  ✓ inventory_ledger.po_id (procurement funnel PO link) ready")
    except Exception as e:
        print(f"  ⚠ ensure_ledger_po_link: {e}")
    finally:
        c.close()


def ensure_purchase_in_po_trigger():
    """Enforce the procurement-funnel invariant at the DATABASE level: a PURCHASE_IN stock
    movement must carry a po_id. The single `inventory.post_movement()` emitter already enforces
    this in Python; this additive trigger makes it impossible to bypass — any INSERT of a
    PURCHASE_IN row with a NULL po_id is rejected by SQLite itself. ADJUSTMENT / OPENING /
    PRODUCTION_USE are deliberately unaffected (they live outside the funnel). Fires only on NEW
    inserts, so historical grandfathered rows (null po_id) are untouched. A CHECK would need a
    full table rebuild; a BEFORE-INSERT trigger is additive + idempotent (DROP + re-CREATE)."""
    c = _conn()
    try:
        cols = [r[1] for r in c.execute("PRAGMA table_info(inventory_ledger)").fetchall()]
        if cols and 'po_id' in cols:   # requires the column from ensure_ledger_po_link
            c.execute("DROP TRIGGER IF EXISTS trg_purchase_in_requires_po")
            c.execute("""
                CREATE TRIGGER trg_purchase_in_requires_po
                BEFORE INSERT ON inventory_ledger
                FOR EACH ROW
                WHEN NEW.movement_type = 'PURCHASE_IN' AND NEW.po_id IS NULL
                BEGIN
                    SELECT RAISE(ABORT, 'PURCHASE_IN stock movement requires a purchase order (po_id)');
                END
            """)
            c.commit()
            print("  ✓ inventory_ledger PURCHASE_IN→po_id trigger ready")
    except Exception as e:
        print(f"  ⚠ ensure_purchase_in_po_trigger: {e}")
    finally:
        c.close()


def ensure_po_line_types():
    """Generalize purchasing beyond ingredients. A PO/bill line is now either a STOCK line
    (ingredient — receiving posts PURCHASE_IN to inventory, exactly as before) or a NON-STOCK
    line (equipment / supplies / services / labour — never touches inventory; flows to AP + P&L).
    Adds line_type ('ingredient'|'other'), category, description to po_items and
    supplier_bill_items, and makes ingredient_id NULLABLE (non-stock lines have no ingredient).
    SQLite can't drop NOT NULL via ALTER, so each table is rebuilt once (FK off, like the
    supplier_bills VOID migration). quantity_kg / unit_cost_kg are REUSED as generic qty ×
    unit-price for non-stock lines, so the existing line-total maths (quantity_kg*unit_cost_kg)
    is unchanged. Idempotent: skips if line_type already present. Existing rows → 'ingredient'."""
    c = _conn()
    try:
        c.execute("PRAGMA foreign_keys=OFF")
        # ── po_items ──
        pcols = [r[1] for r in c.execute("PRAGMA table_info(po_items)").fetchall()]
        if pcols and 'line_type' not in pcols:
            c.execute("""CREATE TABLE po_items_new (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                po_id         INTEGER NOT NULL REFERENCES purchase_orders(id),
                ingredient_id INTEGER REFERENCES ingredients(id),
                quantity_kg   REAL    NOT NULL,
                received_kg   REAL    NOT NULL DEFAULT 0,
                unit_cost_kg  REAL    NOT NULL DEFAULT 0,
                notes         TEXT    DEFAULT '',
                line_type     TEXT    NOT NULL DEFAULT 'ingredient',
                category      TEXT    DEFAULT '',
                description   TEXT    DEFAULT ''
            )""")
            c.execute("""INSERT INTO po_items_new
                (id, po_id, ingredient_id, quantity_kg, received_kg, unit_cost_kg, notes)
                SELECT id, po_id, ingredient_id, quantity_kg, received_kg, unit_cost_kg,
                       COALESCE(notes,'') FROM po_items""")
            c.execute("DROP TABLE po_items")
            c.execute("ALTER TABLE po_items_new RENAME TO po_items")
            print("  ✓ po_items: +line_type/category/description, ingredient_id now nullable")
        # ── supplier_bill_items ──
        bcols = [r[1] for r in c.execute("PRAGMA table_info(supplier_bill_items)").fetchall()]
        if bcols and 'line_type' not in bcols:
            c.execute("""CREATE TABLE supplier_bill_items_new (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                bill_id       INTEGER NOT NULL REFERENCES supplier_bills(id),
                ingredient_id INTEGER REFERENCES ingredients(id),
                quantity_kg   REAL    NOT NULL,
                unit_cost_kg  REAL    NOT NULL,
                line_total    REAL    NOT NULL,
                line_type     TEXT    NOT NULL DEFAULT 'ingredient',
                category      TEXT    DEFAULT '',
                description   TEXT    DEFAULT ''
            )""")
            c.execute("""INSERT INTO supplier_bill_items_new
                (id, bill_id, ingredient_id, quantity_kg, unit_cost_kg, line_total)
                SELECT id, bill_id, ingredient_id, quantity_kg, unit_cost_kg, line_total
                FROM supplier_bill_items""")
            c.execute("DROP TABLE supplier_bill_items")
            c.execute("ALTER TABLE supplier_bill_items_new RENAME TO supplier_bill_items")
            print("  ✓ supplier_bill_items: +line_type/category/description, ingredient_id now nullable")
        c.commit()
    except Exception as e:
        print(f"  ⚠ ensure_po_line_types: {e}")
        try: c.rollback()
        except Exception: pass
    finally:
        try: c.execute("PRAGMA foreign_keys=ON")
        except Exception: pass
        c.close()
    save_db()


def ensure_shop_geo():
    """Shop-location capture from day one (Track B / B1). Adds nullable geo columns so
    every shop can carry a coordinate and every visit/order can be geo-stamped — the base
    the future Rep Day Log (B3.5) clusters into visits. No geocoding service: the rep's
    phone captures the coordinate in the shop (one browser permission prompt).
      - customers.lat/lng/geo_accuracy_m/geo_captured_at → the durable shop location.
      - beat_visits.lat/lng/geo_accuracy_m               → where a visit was logged.
      - customer_orders.lat/lng/geo_accuracy_m           → where an order was taken.
    All nullable, additive, PRAGMA-guarded, idempotent (adds only missing columns)."""
    c = _conn()
    try:
        specs = {
            'customers':       [('lat', 'REAL'), ('lng', 'REAL'),
                                ('geo_accuracy_m', 'REAL'), ('geo_captured_at', 'TEXT')],
            'beat_visits':     [('lat', 'REAL'), ('lng', 'REAL'), ('geo_accuracy_m', 'REAL')],
            'customer_orders': [('lat', 'REAL'), ('lng', 'REAL'), ('geo_accuracy_m', 'REAL')],
        }
        for table, cols in specs.items():
            existing = [r[1] for r in c.execute(f"PRAGMA table_info({table})").fetchall()]
            if not existing:
                continue  # table not present on this DB — skip
            for name, typ in cols:
                if name not in existing:
                    c.execute(f"ALTER TABLE {table} ADD COLUMN {name} {typ}")
        c.commit()
        print("  ✓ shop geo (customers/beat_visits/customer_orders lat/lng/accuracy) ready")
    except Exception as e:
        print(f"  ⚠ ensure_shop_geo: {e}")
    finally:
        c.close()
    save_db()


def ensure_drop_qty_in_production():
    """Remove the drifting cached customer_order_items.qty_in_production column.
    It was only ever incremented (never decremented) → drifted high. The live truth is
    orders._item_in_production() (active WOs net of produced_units), used everywhere it's read.
    DROP COLUMN needs SQLite 3.35+; if unsupported this is a safe no-op (the column just sits
    unused at its default, and nothing reads it). Idempotent."""
    c = _conn()
    try:
        cols = [r[1] for r in c.execute("PRAGMA table_info(customer_order_items)").fetchall()]
        if cols and 'qty_in_production' in cols:
            try:
                c.execute("ALTER TABLE customer_order_items DROP COLUMN qty_in_production")
                c.commit()
                print("  ✓ dropped customer_order_items.qty_in_production (drift cache retired)")
            except Exception as e:
                print(f"  ⚠ qty_in_production drop skipped (SQLite < 3.35?): {e}")
        else:
            print("  ✓ qty_in_production already absent")
    except Exception as e:
        print(f"  ⚠ ensure_drop_qty_in_production: {e}")
    finally:
        c.close()


def ensure_cost_lines():
    """User-managed cost lines (two-engine model). One row per cost line the owner tracks.
    bucket ∈ {'variable','fixed','tracking'}:
      - variable  → standard_value is ₨/pack, flows into product cost-to-make.
      - fixed     → standard_value is ₨/MONTH, absorbed ÷ normal volume into cost-to-make.
      - tracking  → monthly spend only (e.g. Raw materials, Marketing) — NOT in product cost.
    Monthly actuals are logged per line NAME in monthly_operating_costs. Seeds once (from any
    existing costing_config standards, for continuity) then is fully user-editable. Idempotent."""
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS cost_lines (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                name           TEXT NOT NULL,
                bucket         TEXT NOT NULL DEFAULT 'variable',
                standard_value REAL NOT NULL DEFAULT 0,
                sort_order     INTEGER NOT NULL DEFAULT 0,
                active         INTEGER NOT NULL DEFAULT 1,
                created_at     TEXT DEFAULT (datetime('now'))
            )
        """)
        n = c.execute("SELECT COUNT(*) FROM cost_lines").fetchone()[0]
        if n == 0:
            def _cv(key, d=0.0):
                r = c.execute("SELECT value FROM costing_config WHERE key=?", (key,)).fetchone()
                try:
                    return float(r[0]) if r and r[0] is not None else d
                except (TypeError, ValueError):
                    return d
            pkg = _cv('pkg_pouch', 15) + _cv('pkg_label') + _cv('pkg_carton')
            seed = [
                ('Raw materials',   'tracking', 0.0,                  10),
                ('Packaging',       'variable', pkg,                  20),
                ('Labour (temp)',   'variable', _cv('conv_labour', 5), 30),
                ('Gas',             'variable', _cv('conv_gas'),       40),
                ('Electricity',     'variable', _cv('conv_electricity'), 50),
                ('Misc. variable',  'variable', 0.0,                  60),
                ('Salaries',        'fixed',    _cv('fix_salaries'),   70),
                ('Rent',            'fixed',    _cv('fix_rent'),       80),
                ('Transport',       'fixed',    _cv('fix_transport'),  90),
                ('Admin',           'fixed',    _cv('fix_admin'),     100),
                ('Misc. fixed',     'fixed',    0.0,                 110),
            ]
            for name, bucket, val, so in seed:
                c.execute("INSERT INTO cost_lines (name, bucket, standard_value, sort_order) VALUES (?,?,?,?)",
                          (name, bucket, val, so))
        c.commit()
        print("  ✓ cost_lines ready")
    except Exception as e:
        print(f"  ⚠ ensure_cost_lines: {e}")
    finally:
        c.close()


# ═══════════════════════════════════════════════════════════════════
#  PRICE TYPES & PRICE HISTORY
# ═══════════════════════════════════════════════════════════════════

def ensure_price_types_sprint6():
    """Update price_type labels for Sprint 6 terminology + add bulk. Idempotent."""
    c = _conn()
    try:
        updates = [
            ('mfg_cost',    'Cost to Make'),
            ('ex_factory',  'Direct Sale'),
            ('retail_mrp',  'MRP'),
            ('distributor', 'Distributor'),
        ]
        for code, label in updates:
            c.execute("UPDATE price_types SET label=? WHERE code=?", (label, code))
        c.execute("INSERT OR IGNORE INTO price_types (code, label) VALUES ('bulk', 'Bulk')")
        c.commit()
        print("  ✓ price_types: Sprint 6 labels updated + bulk added")
    finally:
        c.close()


def ensure_price_history_extended():
    """Add change_type, config_key, changed_by, note columns to ingredient_price_history. Idempotent."""
    c = _conn()
    try:
        existing = {r[1] for r in c.execute("PRAGMA table_info(ingredient_price_history)").fetchall()}
        additions = [
            ('change_type', "TEXT DEFAULT 'ingredient'"),
            ('config_key',  'TEXT'),
            ('changed_by',  "TEXT DEFAULT 'system'"),
            ('note',        'TEXT'),
        ]
        for col, typedef in additions:
            if col not in existing:
                c.execute(f"ALTER TABLE ingredient_price_history ADD COLUMN {col} {typedef}")
                print(f"  ✓ price_history: added {col}")
        c.commit()
    finally:
        c.close()


# ═══════════════════════════════════════════════════════════════════
#  MARGIN ALERTS
# ═══════════════════════════════════════════════════════════════════

def ensure_margin_alerts_table():
    """Create margin_alerts table if not present (idempotent)."""
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS margin_alerts (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                product_code TEXT NOT NULL,
                pack_size    TEXT NOT NULL,
                sku_code     TEXT,
                margin_pct   REAL NOT NULL,
                floor_pct    REAL NOT NULL,
                detected_at  TEXT DEFAULT (datetime('now')),
                dismissed_at TEXT,
                dismissed_by TEXT,
                email_sent   INTEGER DEFAULT 0
            )
        """)
        c.commit()
    finally:
        c.close()


# ═══════════════════════════════════════════════════════════════════
#  FIELD OTP
# ═══════════════════════════════════════════════════════════════════

def ensure_clean_product_codes():
    """Normalize product codes to SPGM/SPCM and rebuild variant SKU codes.

    Identifies products by name (Garam Masala → SPGM, Chaat Masala → SPCM).
    Also updates product_variants.sku_code, sales.product_code,
    invoice_items.product_code, and production_batches references.
    Safe to run on both DEV and PROD — idempotent (skips if already correct).
    """
    # Match by name keywords OR by code containing GM/CM fragments
    def _canonical(pcode, pname):
        pcode_up = pcode.upper()
        pname_lo = pname.lower()
        if pcode_up == 'SPGM' or pcode_up == 'SPCM':
            return pcode_up  # already canonical
        if 'garam' in pname_lo or 'GM' in pcode_up:
            return 'SPGM'
        if 'chaat' in pname_lo or 'CM' in pcode_up:
            return 'SPCM'
        return None

    try:
        c = _conn()
        try:
            products = c.execute("SELECT id, code, name FROM products").fetchall()

            # Group by canonical code — PROD may have multiple rows for same product
            # e.g. SPCM-50, SPCM-100, SPCM-1000 as separate product rows
            from collections import defaultdict as _dd
            groups = _dd(list)
            for prod in products:
                can = _canonical(prod['code'], prod['name'] or '')
                if can:
                    groups[can].append(prod)

            for canonical, prods in groups.items():
                if not prods:
                    continue

                # Sort so the one with the most variants becomes the "keeper"
                # (or just take the first one alphabetically)
                prods_with_counts = []
                for p in prods:
                    vc = c.execute("SELECT COUNT(*) FROM product_variants WHERE product_id=?", (p['id'],)).fetchone()[0]
                    prods_with_counts.append((vc, p))
                prods_with_counts.sort(key=lambda x: -x[0])

                keeper = prods_with_counts[0][1]  # keep the one with most variants
                keeper_id = keeper['id']

                # Rename keeper to canonical if needed
                if keeper['code'] != canonical:
                    print(f"  → Renaming product '{keeper['code']}' → '{canonical}'")
                    c.execute("UPDATE products SET code=? WHERE id=?", (canonical, keeper_id))
                    c.execute("UPDATE sales         SET product_code=? WHERE product_code=?", (canonical, keeper['code']))
                    c.execute("UPDATE invoice_items SET product_code=? WHERE product_code=?", (canonical, keeper['code']))

                # Handle duplicates: re-parent their variants to keeper, then deactivate
                for _, dup in prods_with_counts[1:]:
                    dup_id = dup['id']
                    print(f"  → Merging duplicate product '{dup['code']}' (id={dup_id}) into '{canonical}' (id={keeper_id})")
                    c.execute("UPDATE product_variants SET product_id=? WHERE product_id=?", (keeper_id, dup_id))
                    c.execute("UPDATE sales         SET product_code=? WHERE product_code=?", (canonical, dup['code']))
                    c.execute("UPDATE invoice_items SET product_code=? WHERE product_code=?", (canonical, dup['code']))
                    c.execute("UPDATE production_batches SET product_id=? WHERE product_id=?", (keeper_id, dup_id))
                    c.execute("UPDATE products SET active=0 WHERE id=?", (dup_id,))

                # Rebuild variant SKU codes for keeper: canonical + '-' + pack_grams
                variants = c.execute("""
                    SELECT pv.id, pv.sku_code, ps.grams
                    FROM product_variants pv
                    LEFT JOIN pack_sizes ps ON ps.id = pv.pack_size_id
                    WHERE pv.product_id=?
                """, (keeper_id,)).fetchall()

                for v in variants:
                    grams = int(v['grams']) if v['grams'] else 0
                    new_sku = f"{canonical}-{grams}" if grams else canonical
                    old_sku = v['sku_code']
                    if old_sku == new_sku:
                        continue
                    print(f"    SKU: '{old_sku}' → '{new_sku}'")
                    c.execute("UPDATE product_variants SET sku_code=? WHERE id=?", (new_sku, v['id']))
                    c.execute("UPDATE sales          SET product_code=?, sku_code=? WHERE sku_code=?", (canonical, new_sku, old_sku))
                    c.execute("UPDATE invoice_items  SET product_code=? WHERE sku_code=?", (canonical, old_sku))

            c.commit()
            print("  ✓ ensure_clean_product_codes done")
        except Exception as e:
            c.rollback()
            print(f"  ⚠ ensure_clean_product_codes error (non-fatal): {e}")
        finally:
            c.close()
    except Exception as e:
        print(f"  ⚠ ensure_clean_product_codes outer error (non-fatal): {e}")


def ensure_ingredient_price_volatile():
    """Add price_volatile column to ingredients (idempotent)."""
    c = _conn()
    try:
        cols = [r['name'] for r in c.execute("PRAGMA table_info(ingredients)").fetchall()]
        if 'price_volatile' not in cols:
            c.execute("ALTER TABLE ingredients ADD COLUMN price_volatile INTEGER DEFAULT 0")
            c.commit()
            print("  ✓ ingredients.price_volatile column added")
        else:
            print("  ✓ ingredients.price_volatile already exists")
    finally:
        c.close()


def ensure_ingredient_unit_kg():
    """Normalize ingredients.unit to 'kg' (idempotent). Costing always treats
    cost_per_kg as a per-KILOGRAM rate and BOM quantities as grams — the `unit`
    column is decorative and was a footgun (a 'g'/'gram' label could trick a
    per-gram cost entry that the engine then reads as per-kg → ~1000x error).
    Standardize the stored label so display, export, and reconcile all agree."""
    c = _conn()
    try:
        cols = [r['name'] for r in c.execute("PRAGMA table_info(ingredients)").fetchall()]
        if 'unit' not in cols:
            print("  ✓ ingredients.unit absent — nothing to normalize")
            return
        n = c.execute("UPDATE ingredients SET unit='kg' WHERE unit IS NULL OR unit<>'kg'").rowcount
        c.commit()
        print(f"  ✓ ingredients.unit normalized to 'kg' ({n} row(s) updated)")
    except Exception as e:
        print(f"  ⚠ ensure_ingredient_unit_kg skipped: {e}")
    finally:
        c.close()


def ensure_field_otp_table():
    """Create field_otp table for WhatsApp OTP login (idempotent)."""
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS field_otp (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                phone      TEXT NOT NULL,
                code       TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                used       INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_field_otp_phone ON field_otp(phone)")
        c.commit()
        print("  ✓ field_otp table ready")
    finally:
        c.close()


# ═══════════════════════════════════════════════════════════════════
#  CONSUMER WEBSITE (chachamasala.com)
# ═══════════════════════════════════════════════════════════════════

def ensure_web_price_type():
    """Add 'web' price type for consumer website pricing. Idempotent."""
    c = _conn()
    try:
        c.execute("INSERT OR IGNORE INTO price_types (code, label) VALUES ('web', 'Web / Online')")
        c.commit()
        print("  ✓ price_types: web / Web Online added")
    except Exception as e:
        print(f"  ✗ ensure_web_price_type error: {e}")
    finally:
        c.close()


def ensure_25g_pack_and_spgm25():
    """Add 25g pack size and SPGM-25 product variant for consumer website. Idempotent."""
    c = _conn()
    try:
        # 1. Add 25g pack size
        c.execute("INSERT OR IGNORE INTO pack_sizes (label, grams) VALUES ('25g', 25)")

        # 2. Get pack size id
        ps = c.execute("SELECT id FROM pack_sizes WHERE label='25g'").fetchone()
        if not ps:
            print("  ✗ 25g pack size not found after insert")
            return
        ps_id = ps[0]

        # 3. Get SPGM product
        prod = c.execute("SELECT id FROM products WHERE code='SPGM' AND active=1").fetchone()
        if not prod:
            print("  ✗ SPGM product not found — skipping SPGM-25 variant")
            return
        prod_id = prod[0]

        # 4. Inherit wastage_pct from existing SPGM variant
        existing = c.execute(
            "SELECT wastage_pct FROM product_variants WHERE product_id=? AND active_flag=1 LIMIT 1",
            (prod_id,)
        ).fetchone()
        wastage = existing[0] if existing else 0.0

        # 5. Insert SPGM-25 only if not already present
        existing_var = c.execute("SELECT id FROM product_variants WHERE sku_code='SPGM-25'").fetchone()
        if not existing_var:
            c.execute("""
                INSERT INTO product_variants (product_id, pack_size_id, sku_code, active_flag, wastage_pct)
                VALUES (?, ?, 'SPGM-25', 1, ?)
            """, (prod_id, ps_id, wastage))
            print("  ✓ SPGM-25 variant created")
        else:
            print("  ✓ SPGM-25 variant already exists")

        c.commit()
    except Exception as e:
        print(f"  ✗ ensure_25g_pack_and_spgm25 error: {e}")
    finally:
        c.close()


def ensure_web_prices():
    """Seed initial web prices for consumer website SKUs. Only seeds if no active web price exists — never overwrites ERP-managed prices. Idempotent."""
    WEB_PRICES = {
        'SPCM-50':   100.0,
        'SPCM-100':  195.0,
        'SPCM-1000': 1600.0,
        'SPGM-25':   165.0,
        'SPGM-50':   300.0,
    }
    c = _conn()
    try:
        pt = c.execute("SELECT id FROM price_types WHERE code='web'").fetchone()
        if not pt:
            print("  ✗ web price type not found — run ensure_web_price_type first")
            return
        pt_id = pt[0]

        for sku, price in WEB_PRICES.items():
            pv = c.execute("SELECT id FROM product_variants WHERE sku_code=?", (sku,)).fetchone()
            if not pv:
                print(f"  ✗ variant {sku} not found — skipping web price seed")
                continue
            pv_id = pv[0]

            # Only seed if no active web price exists (respect ERP-managed updates)
            existing = c.execute(
                "SELECT id FROM product_prices WHERE product_variant_id=? AND price_type_id=? AND active_flag=1",
                (pv_id, pt_id)
            ).fetchone()
            if not existing:
                c.execute("""
                    INSERT INTO product_prices (product_variant_id, price_type_id, price, effective_from, active_flag)
                    VALUES (?, ?, ?, date('now'), 1)
                """, (pv_id, pt_id, price))
                print(f"  ✓ web price seeded: {sku} = PKR {price}")
            else:
                print(f"  · web price already set: {sku}")

        c.commit()
    except Exception as e:
        print(f"  ✗ ensure_web_prices error: {e}")
    finally:
        c.close()



# ═══════════════════════════════════════════════════════════════════
#  PLANNING + RECIPE + VARIANT (moved from server.py — S0 safe slice)
#  All idempotent; use only _conn from modules.db.
# ═══════════════════════════════════════════════════════════════════

def ensure_variant_show_online():
    """Add show_online column to product_variants. Idempotent."""
    try:
        c = _conn()
        try:
            existing = {r[1] for r in c.execute("PRAGMA table_info(product_variants)").fetchall()}
            if 'show_online' not in existing:
                c.execute("ALTER TABLE product_variants ADD COLUMN show_online INTEGER DEFAULT 0")
                c.commit()
                print("  ✓ product_variants: added show_online column")
        finally:
            c.close()
    except Exception as e:
        print(f"  ⚠ ensure_variant_show_online: {e}")


def ensure_recipe_tables():
    """Create recipes, recipe_steps, recipe_ingredients tables. Idempotent."""
    try:
        c = _conn()
        try:
            c.execute("""
                CREATE TABLE IF NOT EXISTS recipes (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    title       TEXT    NOT NULL,
                    slug        TEXT    NOT NULL UNIQUE,
                    masala_code TEXT    NOT NULL,
                    description TEXT,
                    prep_mins   INTEGER DEFAULT 0,
                    cook_mins   INTEGER DEFAULT 0,
                    serves      INTEGER DEFAULT 4,
                    image_path  TEXT,
                    active      INTEGER DEFAULT 1,
                    sort_order  INTEGER DEFAULT 0,
                    created_at  TEXT    DEFAULT (datetime('now'))
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS recipe_steps (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    recipe_id   INTEGER NOT NULL REFERENCES recipes(id),
                    step_number INTEGER NOT NULL,
                    instruction TEXT    NOT NULL
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS recipe_ingredients (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    recipe_id   INTEGER NOT NULL REFERENCES recipes(id),
                    sort_order  INTEGER DEFAULT 0,
                    item        TEXT    NOT NULL
                )
            """)
            c.commit()
            print("  ✓ recipe tables ready")
        finally:
            c.close()
    except Exception as e:
        print(f"  ⚠ ensure_recipe_tables: {e}")


def ensure_change_log_reason():
    """Add nullable `reason` column to change_log so planning edits can record WHY.

    Reuses the existing system-wide audit table instead of a parallel one.
    Nullable + app-enforced: mandatory only for edits to approved/revised plan
    versions; NULL for drafts and all existing non-planning audit rows. Idempotent.
    NOTE: if change_log is ever rebuilt (see _migrate_change_log_void_action's
    change_log_new copy pattern), carry the `reason` column forward.
    """
    try:
        c = _conn()
        try:
            existing = {r[1] for r in c.execute("PRAGMA table_info(change_log)").fetchall()}
            if 'reason' not in existing:
                c.execute("ALTER TABLE change_log ADD COLUMN reason TEXT")
                print("  ✓ change_log: added reason column")
            c.commit()
        finally:
            c.close()
    except Exception as e:
        print(f"  ⚠ ensure_change_log_reason: {e}")


def ensure_planning_foundations():
    """Create the plan_version spine (Planning Input Module, M0). Idempotent.

    All planning input tables (M1+) FK to plan_version(id). Scenarios, drafts,
    approvals and revisions are all plan_version rows. Money/approve gating is
    enforced in the app layer (cost permission), not here.
    """
    try:
        c = _conn()
        try:
            c.execute("""
                CREATE TABLE IF NOT EXISTS plan_version (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    name              TEXT    NOT NULL,
                    scenario_type     TEXT    NOT NULL
                                        CHECK (scenario_type IN
                                          ('draft','approved','conservative',
                                           'expected','aggressive','revised')),
                    status            TEXT    NOT NULL DEFAULT 'draft'
                                        CHECK (status IN ('draft','approved','archived')),
                    parent_version_id INTEGER REFERENCES plan_version(id),
                    notes             TEXT,
                    created_by        TEXT,
                    created_at        TEXT    DEFAULT (datetime('now')),
                    approved_by       TEXT,
                    approved_at       TEXT,
                    updated_by        TEXT,
                    updated_at        TEXT    DEFAULT (datetime('now'))
                )
            """)
            # At most one ACTIVE approved launch plan at a time.
            c.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS one_approved_launch_plan
                ON plan_version (scenario_type)
                WHERE scenario_type = 'approved' AND status = 'approved'
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_plan_version_status ON plan_version (status)")
            c.commit()
            print("  ✓ planning: plan_version spine ready")
        finally:
            c.close()
    except Exception as e:
        print(f"  ⚠ ensure_planning_foundations: {e}")


def ensure_plan_version_horizon():
    """Add per-version planning horizon columns to plan_version (M1). Idempotent.

    horizon_start_month: anchor month 'YYYY-MM-01' (NULL = infer from earliest forecast).
    horizon_months:      window length, default 12. Per-version so scenarios can differ.
    """
    try:
        c = _conn()
        try:
            existing = {r[1] for r in c.execute("PRAGMA table_info(plan_version)").fetchall()}
            if 'horizon_start_month' not in existing:
                c.execute("ALTER TABLE plan_version ADD COLUMN horizon_start_month TEXT")
                print("  ✓ plan_version: added horizon_start_month")
            if 'horizon_months' not in existing:
                c.execute("ALTER TABLE plan_version ADD COLUMN horizon_months INTEGER DEFAULT 12")
                print("  ✓ plan_version: added horizon_months")
            c.commit()
        finally:
            c.close()
    except Exception as e:
        print(f"  ⚠ ensure_plan_version_horizon: {e}")


def ensure_plan_sales_tables():
    """Create plan_sales_forecast + plan_sales_target (M1). Idempotent.

    Both FK to plan_version. Grain: month × variant × channel (forecast),
    month × channel (target). Monthly atomic grain; window is per-version (horizon).
    """
    try:
        c = _conn()
        try:
            c.execute("""
                CREATE TABLE IF NOT EXISTS plan_sales_forecast (
                    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
                    plan_version_id        INTEGER NOT NULL REFERENCES plan_version(id) ON DELETE CASCADE,
                    period_month           TEXT    NOT NULL,
                    variant_id             INTEGER NOT NULL REFERENCES product_variants(id),
                    channel                TEXT    NOT NULL
                                             CHECK (channel IN ('retail','distributor','ecommerce','other')),
                    units_forecast         REAL    NOT NULL DEFAULT 0,
                    store_count            INTEGER,
                    sell_through_per_store REAL,
                    created_by             TEXT,
                    created_at             TEXT    DEFAULT (datetime('now')),
                    updated_by             TEXT,
                    updated_at             TEXT    DEFAULT (datetime('now')),
                    UNIQUE (plan_version_id, period_month, variant_id, channel)
                )
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_plan_forecast_version ON plan_sales_forecast (plan_version_id)")
            c.execute("""
                CREATE TABLE IF NOT EXISTS plan_sales_target (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    plan_version_id INTEGER NOT NULL REFERENCES plan_version(id) ON DELETE CASCADE,
                    period_month    TEXT    NOT NULL,
                    channel         TEXT    NOT NULL
                                      CHECK (channel IN ('retail','distributor','ecommerce','other')),
                    target_units    REAL,
                    target_revenue  REAL,
                    created_by      TEXT,
                    created_at      TEXT    DEFAULT (datetime('now')),
                    updated_by      TEXT,
                    updated_at      TEXT    DEFAULT (datetime('now')),
                    UNIQUE (plan_version_id, period_month, channel)
                )
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_plan_target_version ON plan_sales_target (plan_version_id)")
            c.commit()
            print("  ✓ planning: sales forecast + target tables ready")
        finally:
            c.close()
    except Exception as e:
        print(f"  ⚠ ensure_plan_sales_tables: {e}")


def ensure_plan_m2_tables():
    """Create M2 planning tables: manufacturer, manufacturing capacity, financial,
    scenario pricing. Idempotent.

    plan_manufacturer is global (reused across versions); the rest FK to plan_version.
    plan_pricing is scenario pricing — separate from the live product_prices book —
    and is money-sensitive (admin-gated at the API).
    """
    try:
        c = _conn()
        try:
            c.execute("""
                CREATE TABLE IF NOT EXISTS plan_manufacturer (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    name       TEXT    NOT NULL,
                    is_backup  INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT,
                    created_at TEXT    DEFAULT (datetime('now')),
                    updated_by TEXT,
                    updated_at TEXT    DEFAULT (datetime('now'))
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS plan_manufacturing (
                    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
                    plan_version_id          INTEGER NOT NULL REFERENCES plan_version(id) ON DELETE CASCADE,
                    manufacturer_id          INTEGER NOT NULL REFERENCES plan_manufacturer(id),
                    monthly_capacity_units   REAL    NOT NULL DEFAULT 0,
                    batch_size               REAL,
                    moq                      REAL,
                    lead_time_days           INTEGER,
                    packaging_capacity_units REAL,
                    bottleneck_process       TEXT,
                    cost_per_run             REAL,
                    created_by               TEXT,
                    created_at               TEXT    DEFAULT (datetime('now')),
                    updated_by               TEXT,
                    updated_at               TEXT    DEFAULT (datetime('now')),
                    UNIQUE (plan_version_id, manufacturer_id)
                )
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_plan_mfg_version ON plan_manufacturing (plan_version_id)")
            c.execute("""
                CREATE TABLE IF NOT EXISTS plan_financial (
                    plan_version_id        INTEGER PRIMARY KEY REFERENCES plan_version(id) ON DELETE CASCADE,
                    beginning_cash         REAL,
                    marketing_budget       REAL,
                    payroll_budget         REAL,
                    freight_cost_per_unit  REAL,
                    other_opex_monthly     REAL,
                    minimum_cash_threshold REAL,
                    created_by             TEXT,
                    created_at             TEXT    DEFAULT (datetime('now')),
                    updated_by             TEXT,
                    updated_at             TEXT    DEFAULT (datetime('now'))
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS plan_pricing (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    plan_version_id INTEGER NOT NULL REFERENCES plan_version(id) ON DELETE CASCADE,
                    variant_id      INTEGER NOT NULL REFERENCES product_variants(id),
                    product_cost    REAL,
                    wholesale_price REAL,
                    retail_price    REAL,
                    created_by      TEXT,
                    created_at      TEXT    DEFAULT (datetime('now')),
                    updated_by      TEXT,
                    updated_at      TEXT    DEFAULT (datetime('now')),
                    UNIQUE (plan_version_id, variant_id)
                )
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_plan_pricing_version ON plan_pricing (plan_version_id)")
            c.commit()
            print("  ✓ planning: M2 tables (manufacturing, financial, pricing) ready")
        finally:
            c.close()
    except Exception as e:
        print(f"  ⚠ ensure_plan_m2_tables: {e}")


def ensure_plan_code():
    """Add a human-friendly PLAN-### code to plan_version. Idempotent.

    Adds plan_code TEXT + a unique index. Backfills any rows missing a code in
    creation order (id ASC) so the oldest plan becomes PLAN-001. New plans get
    their code generated at create time in modules/planning.create_plan_version.
    """
    try:
        c = _conn()
        try:
            cols = {r[1] for r in c.execute("PRAGMA table_info(plan_version)").fetchall()}
            if 'plan_code' not in cols:
                c.execute("ALTER TABLE plan_version ADD COLUMN plan_code TEXT")
                print("  ✓ plan_version: added plan_code")
            missing = c.execute(
                "SELECT id FROM plan_version WHERE plan_code IS NULL OR plan_code='' ORDER BY id ASC"
            ).fetchall()
            if missing:
                mx = c.execute(
                    "SELECT MAX(CAST(SUBSTR(plan_code,6) AS INTEGER)) FROM plan_version WHERE plan_code LIKE 'PLAN-%'"
                ).fetchone()[0] or 0
                n = mx
                for (rid,) in missing:
                    n += 1
                    c.execute("UPDATE plan_version SET plan_code=? WHERE id=?", (f"PLAN-{n:03d}", rid))
                print(f"  ✓ plan_version: backfilled {len(missing)} plan_code(s)")
            c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_plan_code ON plan_version(plan_code)")
            c.commit()
        finally:
            c.close()
    except Exception as e:
        print(f"  ⚠ ensure_plan_code: {e}")


def ensure_plan_release():
    """Create plan_release — the log of plan-month production released to the ERP as
    Work Orders (in-house handoff). Idempotent.

    UNIQUE(plan_version_id, period_month, variant_id) makes re-releasing a SKU-month a
    no-op (prevents duplicate WOs) and is the traceability + released-state record.
    """
    try:
        c = _conn()
        try:
            c.execute("""
                CREATE TABLE IF NOT EXISTS plan_release (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    plan_version_id  INTEGER NOT NULL REFERENCES plan_version(id) ON DELETE CASCADE,
                    period_month     TEXT    NOT NULL,
                    variant_id       INTEGER NOT NULL REFERENCES product_variants(id),
                    work_order_id    INTEGER,
                    released_by      TEXT,
                    released_at      TEXT    DEFAULT (datetime('now')),
                    UNIQUE (plan_version_id, period_month, variant_id)
                )
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_plan_release_version ON plan_release (plan_version_id)")
            c.commit()
            print("  ✓ planning: plan_release (manufacturing handoff log) ready")
        finally:
            c.close()
    except Exception as e:
        print(f"  ⚠ ensure_plan_release: {e}")


def ensure_scenario_type_cleanup():
    """Separate scenario TYPE from plan STATUS. Idempotent.

    Earlier the scenario_type column mixed forecast shapes (expected/conservative/
    aggressive) with lifecycle values (draft/approved/revised). Lifecycle is the
    separate `status` column, so remap any non-type value to 'expected'. No data is
    lost: those rows already carry their real state in `status`.
    """
    try:
        c = _conn()
        try:
            cols = {r[1] for r in c.execute("PRAGMA table_info(plan_version)").fetchall()}
            if 'scenario_type' not in cols:
                return
            n = c.execute(
                "UPDATE plan_version SET scenario_type='expected' "
                "WHERE LOWER(COALESCE(scenario_type,'')) NOT IN ('expected','conservative','aggressive')"
            ).rowcount
            c.commit()
            if n:
                print(f"  ✓ plan_version: remapped {n} scenario_type(s) to 'expected'")
        finally:
            c.close()
    except Exception as e:
        print(f"  ⚠ ensure_scenario_type_cleanup: {e}")


def ensure_plan_forecast_zone():
    """Add zone_id to plan_sales_forecast so a forecast line can be assigned to a delivery
    zone (maps to ERP zones → reps). Idempotent.

    Rebuilds the table because the grain's UNIQUE is an inline constraint SQLite can't ALTER.
    zone_id=0 = 'All zones / unassigned' (a sentinel, NOT NULL — keeps the per-cell upsert
    idempotent; a nullable zone would be DISTINCT in the UNIQUE and allow duplicates).
    Existing rows are preserved and copied as zone 0. Nothing FK-references this table, so the
    drop/rename is safe.
    """
    try:
        c = _conn()
        try:
            cols = {r[1] for r in c.execute("PRAGMA table_info(plan_sales_forecast)").fetchall()}
            if not cols:
                return  # table not created yet (ensure_plan_sales_tables runs first)
            if 'zone_id' in cols:
                return  # already migrated
            c.execute("PRAGMA foreign_keys=OFF")
            c.execute("""
                CREATE TABLE plan_sales_forecast_new (
                    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
                    plan_version_id        INTEGER NOT NULL REFERENCES plan_version(id) ON DELETE CASCADE,
                    period_month           TEXT    NOT NULL,
                    variant_id             INTEGER NOT NULL REFERENCES product_variants(id),
                    channel                TEXT    NOT NULL
                                             CHECK (channel IN ('retail','distributor','ecommerce','other')),
                    zone_id                INTEGER NOT NULL DEFAULT 0,
                    units_forecast         REAL    NOT NULL DEFAULT 0,
                    store_count            INTEGER,
                    sell_through_per_store REAL,
                    created_by             TEXT,
                    created_at             TEXT    DEFAULT (datetime('now')),
                    updated_by             TEXT,
                    updated_at             TEXT    DEFAULT (datetime('now')),
                    UNIQUE (plan_version_id, period_month, variant_id, channel, zone_id)
                )
            """)
            c.execute("""
                INSERT INTO plan_sales_forecast_new
                    (id, plan_version_id, period_month, variant_id, channel, zone_id,
                     units_forecast, store_count, sell_through_per_store,
                     created_by, created_at, updated_by, updated_at)
                SELECT id, plan_version_id, period_month, variant_id, channel, 0,
                     units_forecast, store_count, sell_through_per_store,
                     created_by, created_at, updated_by, updated_at
                FROM plan_sales_forecast
            """)
            c.execute("DROP TABLE plan_sales_forecast")
            c.execute("ALTER TABLE plan_sales_forecast_new RENAME TO plan_sales_forecast")
            c.execute("CREATE INDEX IF NOT EXISTS idx_plan_forecast_version ON plan_sales_forecast (plan_version_id)")
            c.execute("PRAGMA foreign_keys=ON")
            c.commit()
            print("  ✓ plan_sales_forecast: added zone_id (rebuilt, rows preserved as zone 0)")
        finally:
            c.close()
    except Exception as e:
        print(f"  ⚠ ensure_plan_forecast_zone: {e}")


def ensure_deactivate_spring_catalog():
    """Retire the orphaned SP-ING### ingredient catalog (idempotent).

    Spicetopia carried TWO overlapping ingredient code schemes: the 15 `ING-###SP`
    codes that the live SPGM/SPCM BOMs, the costing engine, the Planning buy-list,
    and the ingredient-code generator (`next_ingredient_code`) all use — and 29
    `SP-ING###` codes that are a parallel, UNUSED catalog (referenced by ZERO
    bom_items). Keeping both is a footgun: duplicate names and conflicting per-kg
    prices for the same spice. This deactivates the SP-ING catalog so only the
    in-use scheme stays active.

    Reversible — sets active=0, never deletes; the rows (and their prices) remain
    as a reference and can be re-activated. SAFETY: the query explicitly refuses to
    deactivate any ingredient referenced by a BOM line, so it can never break a
    recipe even if BOM links change later. Idempotent — re-running touches 0 rows."""
    c = _conn()
    try:
        cols = [r['name'] for r in c.execute("PRAGMA table_info(ingredients)").fetchall()]
        if 'active' not in cols or 'code' not in cols:
            print("  ✓ ingredients.active/code absent — skipping SP-ING retire")
            return
        n = c.execute(
            "UPDATE ingredients SET active=0 "
            "WHERE code LIKE 'SP-ING%' AND active=1 "
            "AND id NOT IN (SELECT DISTINCT ingredient_id FROM bom_items)"
        ).rowcount
        c.commit()
        print(f"  ✓ SP-ING catalog retired (deactivated {n} orphaned ingredient(s))")
    except Exception as e:
        print(f"  ⚠ ensure_deactivate_spring_catalog skipped: {e}")
    finally:
        c.close()


def ensure_rep_zones():
    """Many-to-many rep↔zone assignment so a sales rep can cover MULTIPLE zones
    (e.g. a Karachi zone + Hyderabad). Backfills each rep's existing single
    primary_zone_id as their first assigned zone. Idempotent."""
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS rep_zones (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id  INTEGER NOT NULL,
                zone_id INTEGER NOT NULL,
                UNIQUE(rep_id, zone_id)
            )
        """)
        cols = [r['name'] for r in c.execute("PRAGMA table_info(sales_reps)").fetchall()]
        if 'primary_zone_id' in cols:
            c.execute("""
                INSERT OR IGNORE INTO rep_zones (rep_id, zone_id)
                SELECT id, primary_zone_id FROM sales_reps WHERE primary_zone_id IS NOT NULL
            """)
        c.commit()
        print("  ✓ rep_zones ready (multi-zone rep assignment)")
    except Exception as e:
        print(f"  ⚠ ensure_rep_zones skipped: {e}")
    finally:
        c.close()


def ensure_dedup_seed_suppliers():
    """One-time cleanup: remove the accumulated duplicate SEED suppliers
    ('Spice World Ltd', 'Lahore Spices Co') that the old startup master-CSV sync kept
    re-creating (sync re-inserted them, then the code-normalizer renamed them, freeing
    the code for the next deploy to re-insert — +2 per deploy). Only deletes rows NOT
    referenced by any bill / PO / supplier payment, so a genuinely-used supplier of the
    same name is never harmed. Idempotent (re-run deletes 0)."""
    c = _conn()
    try:
        c.execute("PRAGMA foreign_keys=OFF")
        n = c.execute("""
            DELETE FROM suppliers
            WHERE name IN ('Spice World Ltd', 'Lahore Spices Co')
              AND id NOT IN (SELECT supplier_id FROM supplier_bills    WHERE supplier_id IS NOT NULL)
              AND id NOT IN (SELECT supplier_id FROM purchase_orders   WHERE supplier_id IS NOT NULL)
              AND id NOT IN (SELECT supplier_id FROM supplier_payments WHERE supplier_id IS NOT NULL)
        """).rowcount
        c.execute("PRAGMA foreign_keys=ON")
        c.commit()
        print(f"  ✓ removed {n} duplicate seed supplier(s)")
    except Exception as e:
        print(f"  ⚠ ensure_dedup_seed_suppliers skipped: {e}")
    finally:
        c.close()
