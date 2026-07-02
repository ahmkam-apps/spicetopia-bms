#!/usr/bin/env python3
"""
cleanup_for_launch.py — one-time pre-launch data cleanup for Spicetopia BMS.

Purges ALL transactional/test data, keeps master data, zeroes stock,
resets document counters. Run against a DB FILE (never the live server):

    python3 cleanup_for_launch.py path/to/backup.db            # DRY RUN (default)
    python3 cleanup_for_launch.py path/to/backup.db --execute  # actually clean

Intended flow:
  1. Admin -> Backup -> "Download Backup" (fresh PROD snapshot)
  2. Dry-run this script on it, review the printout
  3. --execute (a .pre-cleanup.bak copy is made first)
  4. Verify counts, then Admin -> DB restore (upload) on PROD, restart
  5. Restore the same cleaned file to DEV so both match

KEEPS: ingredients (codes/names/costs untouched — reconciliation is a
separate task), products, variants, pack sizes, price types, product
prices, costing config, BOMs, users, recipes (website), system settings,
8 real customers (SP-CUST001-008), 11 real zones (KHI-Z*/HYD-Z*) + their
routes, 1x Spice World Ltd + 1x Lahore Spices Co.

DELETES: all orders/invoices/payments/sales/WOs/batches/consumption/
POs/bills/supplier payments/field data/plans/change log/sessions/price
history, test customers, test+legacy zones and their routes, duplicate
suppliers, all sales reps. Zeroes ingredient opening_grams. Resets
document counters (WO/batch/order/invoice/PO/bill/payments/sale) to 0.
acct_* counters are left untouched (kept customers keep their account
numbers; no collision risk).

Decisions per AK 2026-07-01. Idempotent — safe to run twice.
"""
import sqlite3, sys, shutil, os, datetime

# ── config ──────────────────────────────────────────────────────────
KEEP_CUSTOMER_CODES = ['SP-CUST001','SP-CUST002','SP-CUST003','SP-CUST004',
                       'SP-CUST005','SP-CUST006','SP-CUST007','SP-CUST008',
                       # real customers added on PROD (confirmed AK 2026-07-01):
                       'SP-CUST-0011','SP-CUST-0012','SP-CUST-0013']
KEEP_ZONE_PREFIXES  = ('KHI-Z', 'HYD-Z')
# Real suppliers (confirmed AK 2026-07-01). Seed dupes "Spice World Ltd" /
# "Lahore Spices Co" are all deleted.
KEEP_SUPPLIER_NAMES = ['Subhan Allah Store', 'Hussain Spice Traders',
                       'Punjab Agri Wholesale', 'Sindh Farm Direct',
                       'Al-Madina Trading Co.']  # keep 1 each (min id)

# Fully purged tables, FK-safe order (children before parents).
# Missing tables are skipped (PROD schema may differ from local).
PURGE_TABLES = [
    # sessions / security / noise
    'sessions', 'login_rate_limits', 'margin_alerts', 'change_log',
    # sales side
    'payment_allocations', 'customer_payments',
    'invoice_items', 'invoices',
    'order_hold_expiry', 'customer_order_items', 'customer_orders',
    'sales',
    # field
    'field_otp', 'field_order_items', 'field_orders', 'beat_visits',
    # production
    'production_consumption', 'production_batches', 'work_orders',
    # purchasing
    'supplier_payment_allocations', 'supplier_payments',
    'supplier_bill_items', 'supplier_bills',
    'po_items', 'purchase_orders',
    # inventory + history
    'inventory_ledger', 'ingredient_price_history', 'costing_config_history',
    # planning (test plans; PLAN-001 real gets created after cleanup)
    'plan_release', 'plan_sales_forecast', 'plan_sales_target',
    'plan_pricing', 'plan_manufacturing', 'plan_financial',
    'plan_version', 'plan_manufacturer',
    # reps / payroll
    'rep_advances', 'rep_attendance', 'rep_targets', 'rep_routes',
    'route_customers', 'payroll_runs',
]

# id_counters entities reset to 0 (document numbering restarts at 0001)
RESET_COUNTERS = ['sale','batch','invoice','payment','bill','spay',
                  'work_order','customer_order','purchase_order','field_order','rep']

# ── helpers ─────────────────────────────────────────────────────────
def table_exists(db, t):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone() is not None

def count(db, t, where='', args=()):
    return db.execute(f"SELECT COUNT(*) FROM '{t}' {where}", args).fetchone()[0]

def main():
    if len(sys.argv) < 2:
        print(__doc__); sys.exit(1)
    path = sys.argv[1]
    execute = '--execute' in sys.argv
    if not os.path.exists(path):
        print(f"ERROR: {path} not found"); sys.exit(1)

    if execute:
        bak = path + '.pre-cleanup.bak'
        shutil.copy2(path, bak)
        print(f"Backup copy: {bak}")

    db = sqlite3.connect(path)
    db.execute("PRAGMA foreign_keys=OFF")   # we manage order ourselves
    mode = "EXECUTE" if execute else "DRY RUN"
    print(f"\n=== Spicetopia pre-launch cleanup — {mode} — {datetime.datetime.now():%Y-%m-%d %H:%M} ===\n")

    actions = []   # (description, sql, args)

    # 1. full purges
    for t in PURGE_TABLES:
        if not table_exists(db, t):
            print(f"  (skip — no table '{t}')"); continue
        n = count(db, t)
        if n: actions.append((f"purge {t:<32} {n:>6} rows", f"DELETE FROM '{t}'", ()))

    # 2. test customers
    if table_exists(db, 'customers'):
        ph = ','.join('?'*len(KEEP_CUSTOMER_CODES))
        n = count(db, 'customers', f"WHERE code NOT IN ({ph})", KEEP_CUSTOMER_CODES)
        kept = count(db, 'customers', f"WHERE code IN ({ph})", KEEP_CUSTOMER_CODES)
        actions.append((f"delete test customers                {n:>6} rows (keep {kept} real)",
                        f"DELETE FROM customers WHERE code NOT IN ({ph})", tuple(KEEP_CUSTOMER_CODES)))

    # 3. zones: keep KHI-Z*/HYD-Z* only; routes under deleted zones go too
    if table_exists(db, 'zones'):
        keep_zone_ids = [r[0] for r in db.execute(
            "SELECT id FROM zones WHERE " + " OR ".join("name LIKE ?||'%'" for _ in KEEP_ZONE_PREFIXES),
            KEEP_ZONE_PREFIXES)]
        if not keep_zone_ids:
            print("  !! No KHI-Z/HYD-Z zones found — zone cleanup SKIPPED (check live data)")
        else:
            zph = ','.join('?'*len(keep_zone_ids))
            nz = count(db, 'zones', f"WHERE id NOT IN ({zph})", keep_zone_ids)
            actions.append((f"delete legacy/test zones             {nz:>6} rows (keep {len(keep_zone_ids)})",
                            f"DELETE FROM zones WHERE id NOT IN ({zph})", tuple(keep_zone_ids)))
            if table_exists(db, 'routes'):
                nr = count(db, 'routes', f"WHERE zone_id NOT IN ({zph}) OR zone_id IS NULL", keep_zone_ids)
                actions.append((f"delete routes of deleted zones       {nr:>6} rows",
                                f"DELETE FROM routes WHERE zone_id NOT IN ({zph}) OR zone_id IS NULL",
                                tuple(keep_zone_ids)))

    # 4. suppliers: keep min-id row per kept name, delete everything else
    if table_exists(db, 'suppliers'):
        keep_sup_ids = []
        for nm in KEEP_SUPPLIER_NAMES:
            r = db.execute("SELECT MIN(id) FROM suppliers WHERE name=?", (nm,)).fetchone()
            if r and r[0] is not None: keep_sup_ids.append(r[0])
        if keep_sup_ids:
            sph = ','.join('?'*len(keep_sup_ids))
            ns = count(db, 'suppliers', f"WHERE id NOT IN ({sph})", keep_sup_ids)
            actions.append((f"dedupe suppliers                     {ns:>6} rows (keep {len(keep_sup_ids)})",
                            f"DELETE FROM suppliers WHERE id NOT IN ({sph})", tuple(keep_sup_ids)))
        else:
            print("  !! Kept supplier names not found — supplier cleanup SKIPPED (check live data)")

    # 5. sales reps: all test
    if table_exists(db, 'sales_reps'):
        n = count(db, 'sales_reps')
        if n: actions.append((f"delete sales reps (test)             {n:>6} rows",
                              "DELETE FROM sales_reps", ()))

    # 6. zero RM stock
    if table_exists(db, 'ingredients'):
        n = count(db, 'ingredients', "WHERE COALESCE(opening_grams,0) <> 0")
        actions.append((f"zero ingredient opening_grams        {n:>6} rows affected",
                        "UPDATE ingredients SET opening_grams=0", ()))

    # 7. counters
    if table_exists(db, 'id_counters'):
        ph = ','.join('?'*len(RESET_COUNTERS))
        n = count(db, 'id_counters', f"WHERE entity IN ({ph}) AND last_num<>0", RESET_COUNTERS)
        actions.append((f"reset document counters to 0         {n:>6} counters",
                        f"UPDATE id_counters SET last_num=0 WHERE entity IN ({ph})", tuple(RESET_COUNTERS)))

    # print + maybe run
    for desc, sql, args in actions:
        print("  " + desc)
        if execute:
            db.execute(sql, args)

    if execute:
        # reset AUTOINCREMENT for purged tables
        if table_exists(db, 'sqlite_sequence'):
            for t in PURGE_TABLES:
                db.execute("DELETE FROM sqlite_sequence WHERE name=?", (t,))
        db.commit()
        fk = db.execute("PRAGMA foreign_key_check").fetchall()
        if fk:
            print(f"\n!! foreign_key_check reported {len(fk)} issue(s):")
            for row in fk[:20]: print("   ", row)
        else:
            print("\nforeign_key_check: clean")
        print("integrity_check:", db.execute("PRAGMA integrity_check").fetchone()[0])
        db.execute("VACUUM")

        print("\n=== POST-CLEANUP STATE ===")
        for t in [r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]:
            n = count(db, t)
            if n: print(f"  {n:>6}  {t}")
        print("\nDone. Upload this file via Admin -> DB restore, then restart the service.")
    else:
        print("\nDry run only — nothing changed. Re-run with --execute to apply.")
    db.close()

if __name__ == '__main__':
    main()
