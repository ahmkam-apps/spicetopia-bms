# Spicetopia ERP v2 — Startup Guide

## Requirements

- **Python 3.10+** — check with `python3 --version`
- No other dependencies (SQLite is built into Python)

---

## Starting the Server

### macOS / Linux

Double-click **`start.command`** in Finder
— or run from Terminal:

```bash
cd "/path/to/spicetopia-erp-v2"
python3 server.py
```

The server starts on **http://localhost:8765**. Open that URL in any browser.

### Windows

Open **`spicetopia-erp-windows/`** and run **`start.bat`** (double-click).
The server starts on **http://localhost:8765**.

---

## Default Login

| Field    | Value       |
|----------|-------------|
| Username | `admin`     |
| Password | `spice2024` |

> Change the password after first login via Settings → Users.

---

## Database

- **Live database:** `data/spicetopia.db` (SQLite, persists between sessions)
- **Backups:** `data/backups/` — auto-created each session start
- The server copies `data/spicetopia.db` into `/tmp/` while running, then writes changes back on save. This prevents corruption if the server crashes.

---

## Running Tests

```bash
cd "/path/to/spicetopia-erp-v2"
python3 test_erp.py
```

— or double-click **`run_tests.command`** on macOS.

All tests should report `72 passed, 0 failed` (1 expected skip).

---

## Key Modules & Navigation

| Section         | Purpose                                      |
|-----------------|----------------------------------------------|
| Dashboard       | Today's sales, MTD vs last month, AR/AP, stock |
| Customers       | Customer master, statements, quick payment   |
| Invoices        | Sales invoices, payment allocation, AR       |
| AR Aging        | Receivables aging with overdue flags         |
| Suppliers       | Supplier master                              |
| Purchase Orders | Create POs, receive goods (creates bills)    |
| Supplier Bills  | AP bills with supplier reference tracking   |
| AP Aging        | Payables aging with overdue flags            |
| Inventory       | Raw material stock levels and movements      |
| Products        | Finished goods SKUs and pricing              |
| Work Orders     | Production planning and execution            |
| Sales Reps      | Field rep management and payroll             |

---

## System Rules (Do Not Override)

1. **Inventory** only changes via movement records — never edit stock directly
2. **Production** is two-step: Work Order completion (deducts raw materials) → Batch release (adds finished goods)
3. **Recipes/formulas** are NOT stored — use abstract blend codes
4. **Pricing** enforces one active price per product
5. **Orders** are warehouse-specific
6. **Negative inventory** is blocked by the system

---

## Common Issues

| Problem | Solution |
|---------|----------|
| Server won't start (lock file error) | Delete `data/spicetopia.lock` and restart |
| "Port already in use" | Another instance is running — kill it with `kill $(lsof -t -i:8765)` |
| DB appears empty after restart | Check `data/spicetopia.db` exists and has non-zero size |
| Tests fail with auth error | Server must be running (`python3 server.py`) before running tests |

---

## File Structure

```
spicetopia-erp-v2/
├── server.py          # Backend: HTTP server + all business logic
├── public/
│   └── index.html     # Frontend: Single-page app (all JS + CSS)
├── data/
│   ├── spicetopia.db  # Persistent SQLite database
│   └── backups/       # Auto-backups (timestamped)
├── test_erp.py        # Integration test suite
├── start.command      # macOS launcher
└── README.md          # This file
```

---

*For architecture decisions and implementation log, see the project notes in Claude.*
