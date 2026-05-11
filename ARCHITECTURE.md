# Spicetopia BMS — Architecture Reference

## Overview

Spicetopia BMS is a lightweight ERP for a spice manufacturing and distribution business
(Karachi / Hyderabad, Pakistan). It runs as a single Python process with a SQLite database,
served over HTTP on port 3001. The frontend is a single-file vanilla JS SPA.

**Currency:** PKR (₨)  
**Products:** Chaat Masala (SPCM) + Garam Masala (SPGM) in 50g / 100g / 1000g packs  
**SKU format:** `SPGM-50`, `SPCM-100`, etc.

---

## File Structure

```
spicetopia-erp-v2/
├── server.py            # HTTP server + all route handlers (~13,800 lines)
├── modules/             # Business logic package (extracted from server.py)
│   ├── __init__.py
│   ├── utils.py         # Pure stateless utilities (r2, today, validation, RBAC)
│   ├── db.py            # SQLite connection and query helpers
│   ├── id_gen.py        # ID and code generation (SP-CUST-NNN, SUP-NNN, etc.)
│   ├── auth.py          # Login, sessions, rate limiting, Argon2id hashing
│   ├── users.py         # User CRUD (6 roles)
│   ├── customers.py     # Customer CRUD, route assignment, field app helpers
│   ├── suppliers.py     # Supplier CRUD
│   ├── products.py      # Product + variant CRUD, BOM import
│   ├── inventory.py     # Raw material stock, FG stock, ingredient CRUD
│   ├── migrations.py    # All startup schema migrations (idempotent)
│   ├── orders.py        # Customer orders, soft holds, WA notifications
│   ├── invoices.py      # AR lifecycle, payments, aging, PDF generation
│   ├── purchasing.py    # AP lifecycle, bills, POs, GRN, PDF generation
│   ├── production.py    # Work orders, production batches, BOM management
│   ├── costing.py       # Standard costs, prices, margin alerts, batch variances
│   ├── reports.py       # Dashboard, P&L, margin report, rep performance
│   └── field.py         # Zones, routes, reps, payroll, field orders, direct sales
├── public/
│   └── index.html       # Frontend: single-page app (~12,000 lines, vanilla JS + CSS)
│   └── order.html       # B2B portal PWA (7-screen flow for external orders)
├── tests/               # API test suite (157 tests, 14 files)
│   ├── run_all.py       # Orchestrator: --baseline, --compare, --module flags
│   ├── base.py          # Shared helpers, auth, HTTP utils
│   └── test_*.py        # Per-module test files
├── overseer.py          # Pre-deploy health checker (code + live endpoints + git)
├── deploy.py            # Deployment helper (wraps overseer)
├── Dockerfile           # Docker build for Railway
└── railway.toml         # Railway config (builder = "dockerfile")
```

---

## The Bottom-Override Pattern

`server.py` is a ~13,800-line monolith that contains both HTTP route handlers and
business logic. The `modules/` package was extracted without rewriting the handler layer.

**How it works:**

1. All original functions stay in `server.py` as-written (safe fallback).
2. At the bottom of `server.py`, just before `if __name__ == '__main__':`, each module
   is imported with `*`:

```python
from modules.utils      import *
from modules.db         import *
from modules.id_gen     import *
from modules.auth       import *
# ... all 16 modules ...
from modules.reports    import *
from modules.field      import *
```

3. Because Python resolves names at call time, the module versions **win** — they
   override the identically-named originals in server.py's global namespace.

**Why this approach:**
- Zero handler rewrites required — the ~400 handler methods in server.py call the
  same function names they always did, but now get the module implementations.
- Rollback is trivial: remove the import lines and the originals take over.
- The test suite (`tests/`) validates every sprint against a locked baseline.

---

## Module Dependency Graph

```
utils  ←─── (no deps — safe to import anywhere)
  │
db  ←────── utils
  │
id_gen ←─── utils, db
  │
auth ←────── utils, db, id_gen
  │
users ←───── utils, db, auth
customers ←── utils, db, id_gen, auth
suppliers ←── utils, db, id_gen, auth
products ←─── utils, db, id_gen, auth
migrations ←── utils, db, id_gen, auth
inventory ←─── utils, db, id_gen, auth
orders ←────── utils, db, id_gen, auth   [lazy: inventory, production]
invoices ←───── utils, db, id_gen, auth  [lazy: orders]
purchasing ←──── utils, db, id_gen, auth
production ←───── utils, db, id_gen      [lazy: inventory]
costing ←──────── utils, db
reports ←────────── utils, db            [lazy: invoices, purchasing, inventory]
field ←────────────── utils, db, id_gen  [lazy: customers, invoices, orders, inventory]
```

**Lazy imports** (inside function bodies) are used wherever a circular dependency
would otherwise result — e.g. `reports.py` calling `compute_invoice_balance` from
`invoices.py`. These are marked with `# lazy import` comments in the code.

---

## Callback Hooks (Startup Wiring)

Some modules need to call functions that live in other modules loaded later, or in
server.py itself. These are wired as callback attributes at startup:

```python
# In server.py _apply_startup_config():
import modules.orders as _ord_mod
_ord_mod._refresh_ref               = load_ref
_ord_mod._is_out_of_route_fn        = _is_out_of_route        # → field.py
_ord_mod._wa_notify_out_of_route_fn = _wa_notify_out_of_route # → field.py
_ord_mod._check_wo_feasibility_fn   = check_wo_feasibility    # → production.py

import modules.invoices as _inv_mod
_inv_mod._order_status_fn           = _order_status           # → orders.py

import modules.customers as _cust_mod
_cust_mod._refresh_ref              = load_ref

# ... same pattern for suppliers, products, inventory
```

Each callback attribute is initialized to `None` (or a no-op lambda) at the top of
its module, so calls before startup wiring silently do nothing rather than crashing.

---

## Database

- **Engine:** SQLite 3 in WAL mode with `PRAGMA foreign_keys=ON`
- **Live path:** `/tmp/spicetopia_v3_live.db` (in-memory copy for speed)
- **Persisted path:** Railway persistent volume → `DB_SRC` (set by `bootstrap_db()`)
- **Save pattern:** Every write calls `save_db()` which `shutil.copy2`s the temp DB
  back to the persistent volume. This makes writes slightly slower but crash-safe.
- **Backups:** Auto-backup on startup + daily background thread + manual trigger
  via `POST /api/admin/backup`

### Critical WAL Rule

**Never call `next_id()` inside an open `_conn()` transaction.** `next_id()` opens
its own connection internally; nesting connections in WAL mode causes a deadlock.
Always generate all IDs before opening the main transaction:

```python
# CORRECT
sale_id = next_id('sale', 'SALE')      # generates ID (own connection)
inv_num = next_id('invoice', 'INV')    # generates ID (own connection)
c = _conn()                             # open transaction
try:
    c.execute("INSERT INTO invoices ...", (inv_num, ...))
    c.execute("INSERT INTO sales ...", (sale_id, ...))
    c.commit()
finally:
    c.close()

# WRONG — deadlock
c = _conn()
sale_id = next_id('sale', 'SALE')  # ← opens second connection inside first = DEADLOCK
```

---

## Auth & Session Model

- **Password hashing:** Argon2id (preferred) or SHA-256 (legacy, auto-upgraded on login)
- **Sessions:** Stored in `sessions` table with `expires_at` + sliding `last_seen_at`
- **Auth header:** `Authorization: Bearer <token>`
- **Rate limiting:** `login_rate_limits` table (DB-backed, survives restarts)
- **Roles:** `admin` / `sales` / `warehouse` / `accountant` / `field_rep` / `user`
- **Field rep login:** `POST /api/field/auth` with `{phone, pin}` — separate from user login

---

## ID / Code Formats

| Entity       | Format       | Example         | Generator            |
|--------------|--------------|-----------------|----------------------|
| Customer     | SP-CUST-NNNN | SP-CUST-0042    | `next_id('customer')`|
| Supplier     | SUP-NNN      | SUP-007         | `next_id('supplier')` + reformat |
| Invoice      | SP-INV-NNNN  | SP-INV-0015     | `next_id('invoice')` |
| Order        | SP-ORD-NNNN  | SP-ORD-0033     | `next_id('order')`   |
| PO           | SP-PO-NNNN   | SP-PO-0008      | `next_id('po')`      |
| Bill         | auto / free  | from supplier   | user-entered         |
| Sale         | SP-SALE-NNNN | SP-SALE-0101    | `next_id('sale')`    |
| Ingredient   | ING-NNNsp    | ING-007SP       | `next_ingredient_code()` |
| Account #    | KHI-R001     | HYD-D004        | `generate_account_number()` |

**Supplier code quirk:** `next_id()` always prepends `SP-`, so supplier codes must be
post-processed: strip `SP-`, then reformat the numeric part as `SUP-{num:03d}`.

---

## Inventory Rules (Inviolable)

1. **Raw material stock** changes only via `inventory_ledger` movement records — never
   a direct `UPDATE` to a stock column.
2. **Finished goods stock** is derived by summing `production_batches` minus `sales`
   (no separate FG ledger table).
3. **Production is two-step:** Work Order (`work_orders`) → Batch (`production_batches`).
   Skipping the WO step is blocked by the server.
4. **No recipes/formulas stored** — BOM versions track ingredient quantities only;
   actual blend ratios are kept offline.
5. **Negative finished goods inventory is blocked** at invoice/sale creation time.

---

## Startup Migration Order

All migrations run on every startup and are idempotent (safe to re-run). Order matters:

```python
_migrate_supplier_bills_void()          # VOID status + voided columns
_migrate_change_log_void_action()       # widens CHECK constraint
_ensure_supplier_zone_col()             # zone_id on suppliers
ensure_batch_cost_column()              # unit_cost_at_posting on batches
ensure_review_queue_schema()            # soft holds + order approval
ensure_rate_limit_table()
ensure_costing_config()                 # BEFORE ensure_master_schema
ensure_full_schema()                    # ensure_master_schema equivalent
ensure_price_history_extended()         # AFTER ensure_master_schema
ensure_margin_alerts_table()            # AFTER ensure_price_history_extended
ensure_variant_gtin()                   # gtin column on product_variants
ensure_clean_customer_codes()           # fix malformed SP-CUST-* codes
ensure_clean_supplier_codes()           # normalize any SP-SUP-* → SUP-NNN
```

---

## Key Business Rules

| Rule | Where enforced |
|---|---|
| Credit limit check on invoice/sale | `orders._enforce_credit_limit()` |
| Soft hold expiry (48h) | `_hold_expiry_thread` background thread |
| Out-of-route detection | `field._is_out_of_route()` wired as callback to `orders.py` |
| Invoice/bill status sync | `invoices._sync_invoice_status()`, `purchasing._sync_bill_status()` |
| Margin floor breach | `costing.send_margin_alert_email()` on price update |
| WO feasibility check | `production.check_wo_feasibility()` wired as callback to `orders.py` |

---

## B2B Portal (order.spicetopia.food)

`order.html` is a 7-screen PWA served when `X-Forwarded-Host` matches `order.spicetopia.food`.
Orders arrive via `POST /api/orders/external` with an idempotency key (replay-safe: returns
200 on replay, 201 on new). They land in a review queue (`GET /api/review-queue`) where
staff can approve, reject, or reopen them. Soft holds are placed on FG stock while orders
await review.

---

## Test Suite

```bash
# Run full suite against DEV
BMS_URL=https://dev-spicetopia-bms-production.up.railway.app \
BMS_PASS='...' \
python3 tests/run_all.py

# Compare against locked baseline (157 passed, 7 by-design skips)
python3 tests/run_all.py --compare

# Run a single module
python3 tests/run_all.py --module auth
```

Baseline is locked in `tests/baseline.json`. Run `--compare` after any refactor to
verify zero regressions before pushing to PROD.

---

## Deployment

```bash
# Deploy to DEV (Railway service: DEV-SPICETOPIA-BMS)
cd spicetopia-erp-v2
railway up --detach

# Push to PROD (triggers Railway auto-deploy via git)
git add . && git commit -m "..." && git push origin master
```

The pre-push git hook runs `overseer.py` automatically. The push is blocked if any
check fails. Railway builds from the `Dockerfile` (~60–90s build time).

**Hosting:**
- PROD: https://spicetopia-bms-production.up.railway.app
- DEV:  https://dev-spicetopia-bms-production.up.railway.app
