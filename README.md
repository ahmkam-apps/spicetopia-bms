# Spicetopia BMS — Developer Guide

## Requirements

- **Python 3.10+** — check with `python3 --version`
- No other dependencies (SQLite is built into Python)
- Optional: `argon2-cffi` for Argon2id password hashing (`pip install argon2-cffi`)

---

## Starting the Server

```bash
cd spicetopia-bms
python3 server.py
```

The server starts on **http://localhost:3001** (Railway) or **http://localhost:8765** (local).
Open that URL in any browser.

**Default login:**

| Field    | Value       |
|----------|-------------|
| Username | `admin`     |
| Password | `spice2024` |

> Change this after first login via Settings → Users.

---

## Project Structure

```
spicetopia-bms/
├── server.py            # HTTP server + all route handlers (~13,800 lines)
├── modules/             # Business logic package (16 modules)
│   ├── utils.py         # Pure utilities: r2, today, validation, RBAC
│   ├── db.py            # SQLite helpers: _conn, qry, qry1, run, save_db
│   ├── id_gen.py        # ID/code generation (SP-CUST-NNN, SUP-NNN, etc.)
│   ├── auth.py          # Login, sessions, Argon2id hashing, rate limiting
│   ├── users.py         # User CRUD (6 roles)
│   ├── customers.py     # Customer CRUD, route assignment
│   ├── suppliers.py     # Supplier CRUD
│   ├── products.py      # Product + variant CRUD
│   ├── inventory.py     # Raw material + FG stock, ingredient CRUD
│   ├── migrations.py    # Startup schema migrations (all idempotent)
│   ├── orders.py        # Customer orders, soft holds, WA notifications
│   ├── invoices.py      # AR lifecycle, payments, PDF generation
│   ├── purchasing.py    # AP lifecycle, bills, POs, GRN
│   ├── production.py    # Work orders, production batches, BOM
│   ├── costing.py       # Standard costs, prices, margin alerts
│   ├── reports.py       # Dashboard, P&L, margin report, rep performance
│   └── field.py         # Zones, routes, reps, payroll, field orders
├── public/
│   ├── index.html       # Frontend SPA (~12,000 lines, vanilla JS + CSS)
│   └── order.html       # B2B portal PWA (order.spicetopia.food)
├── tests/               # API test suite (157 tests across 14 modules)
│   ├── run_all.py       # Test orchestrator
│   └── ...
├── ARCHITECTURE.md      # In-depth architecture reference ← read this
├── overseer.py          # Pre-deploy health checker
├── Dockerfile
└── railway.toml
```

For a full explanation of the architecture, module dependencies, startup wiring,
and inviolable system rules, read **[ARCHITECTURE.md](ARCHITECTURE.md)**.

---

## Running Tests

```bash
# Requires a running server (DEV or local)
BMS_URL=https://dev-spicetopia-bms-production.up.railway.app \
BMS_PASS='your-password' \
python3 tests/run_all.py

# Compare against locked baseline (must stay at 157 passed, 0 failed)
python3 tests/run_all.py --compare

# Run one module only
python3 tests/run_all.py --module customers
```

**Baseline:** 157/164 passed, 0 failed, 7 skipped (all skips are by design).
The baseline is locked in `tests/baseline.json`. Always run `--compare` before
pushing to PROD.

---

## Database

- **Engine:** SQLite 3, WAL mode, FK enforcement on
- **Live path:** `/tmp/spicetopia_v3_live.db` (temp copy, fast I/O)
- **Persisted path:** Railway volume (written back by `save_db()` after every write)
- **Backups:** auto-created at startup + daily background thread + manual via Admin UI

### Critical Rule

**Never call `next_id()` inside an open `_conn()` transaction** — it opens a second
connection and causes a WAL deadlock. Generate all IDs *before* opening the transaction.
See [ARCHITECTURE.md](ARCHITECTURE.md) for the correct pattern.

---

## Module System

The `modules/` package uses a **bottom-override pattern**:

1. All original functions stay in `server.py` (safe fallback).
2. At the bottom of `server.py`, each module is imported with `from modules.X import *`.
3. Module versions win over the originals at runtime (Python resolves names at call time).

This means the 400+ route handlers in `server.py` were never rewritten — they call
the same function names but now get the module implementations. See
[ARCHITECTURE.md](ARCHITECTURE.md) for the full dependency graph and startup wiring.

---

## Deployment

```bash
# DEV (Railway — test before pushing to PROD)
cd spicetopia-bms
railway up --detach

# PROD (git push triggers Railway auto-deploy)
git add . && git commit -m "your message"
git push origin master   # pre-push hook runs overseer automatically
```

**Hosting:**
- PROD: https://spicetopia-bms-production.up.railway.app
- DEV:  https://dev-spicetopia-bms-production.up.railway.app

Railway build time: ~60–90 seconds after push.

---

## System Rules (Never Break These)

1. Inventory changes only via ledger movement records — never a direct `UPDATE` to stock
2. Production is two-step: Work Order → Batch — skipping is blocked by the server
3. No recipe/formula stored in ERP — blend ratios are kept offline
4. `next_id()` must never be called inside an open `_conn()` transaction (WAL deadlock)
5. Negative finished goods inventory is blocked at invoice/sale creation time

---

## Common Issues

| Problem | Solution |
|---------|----------|
| Server won't start (lock file) | Delete `data/spicetopia.lock` and restart |
| "Port already in use" | Kill with `kill $(lsof -t -i:3001)` |
| DB appears empty after restart | Check `data/spicetopia.db` is non-zero size |
| `railway up` silent ("No changes") | Dashboard → service Settings → Watch Paths → clear the field |
| `git push` password prompt | `git remote set-url origin https://<PAT>@github.com/...` |
| Git index.lock on server | Run `rm .git/index.lock` from Mac terminal |
