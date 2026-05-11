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
    'ensure_clean_product_codes',
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
