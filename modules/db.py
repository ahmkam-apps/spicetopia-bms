"""
modules/db.py — Database helpers for Spicetopia BMS.

Module-level variables (DB_TMP, DB_SRC, MAX_BACKUPS) must be set by server.py
at startup before any of these functions are called.  server.py does this inside
bootstrap_db() immediately after resolving the DB path.

Pattern used in server.py:
    import modules.db as _db_mod
    _db_mod.DB_SRC = DB_SRC

Exported via `from modules.db import *` in server.py.
"""

import shutil
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path

__all__ = [
    '_conn', 'qry', 'qry1', 'run', 'run_many', 'save_db',
    'audit_log',
]

# ── Module-level state (set by server.py at startup) ─────────────────────────
# DB_TMP is the same temp path that server.py uses — initialized identically.
DB_TMP      = Path(tempfile.gettempdir()) / 'spicetopia_v3_live.db'
DB_SRC      = None   # set by server.py: import modules.db as _db; _db.DB_SRC = DB_SRC
MAX_BACKUPS = 5      # keep the last N rolling backups alongside DB_SRC


# ── Connection ────────────────────────────────────────────────────────────────

def _conn():
    # timeout=30: SQLite will retry for up to 30 s on SQLITE_BUSY instead of
    # immediately raising "database is locked".  Needed because ThreadingHTTPServer
    # spawns one thread per request, so concurrent write requests can collide.
    c = sqlite3.connect(str(DB_TMP), timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    return c


# ── Query helpers ─────────────────────────────────────────────────────────────

def qry(sql, params=()):
    c = _conn()
    try:
        return [dict(r) for r in c.execute(sql, params).fetchall()]
    finally:
        c.close()


def qry1(sql, params=()):
    rows = qry(sql, params)
    return rows[0] if rows else None


# ── Write helpers (auto-commit + persist) ─────────────────────────────────────

def run(sql, params=()):
    c = _conn()
    try:
        c.execute(sql, params)
        c.commit()
    finally:
        c.close()
    save_db()


def run_many(ops):
    """ops = list of (sql, params). All in one transaction."""
    c = _conn()
    try:
        for sql, params in ops:
            c.execute(sql, params)
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()


# ── Persistence ───────────────────────────────────────────────────────────────

def save_db():
    """Copy working DB back to persistent storage, keeping a rolling backup of
    the last MAX_BACKUPS versions alongside DB_SRC."""
    if DB_SRC is None:
        return  # DB_SRC not yet set (early startup) — skip silently

    # ── Safety check: never write if the working copy is empty/corrupt ──
    if not DB_TMP.exists() or DB_TMP.stat().st_size < 512:
        print(f"  ✗ save_db aborted — working copy is empty or missing: {DB_TMP}")
        return

    # ── 1. Rotate backups before overwriting ─────────────────────
    if DB_SRC.exists() and DB_SRC.stat().st_size >= 512:
        backup_dir = DB_SRC.parent / 'spicetopia_backups'
        backup_dir.mkdir(exist_ok=True)
        ts          = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = backup_dir / f"{DB_SRC.stem}_{ts}.db"
        shutil.copy2(str(DB_SRC), str(backup_path))
        # Prune — keep only the MAX_BACKUPS most recent
        backups = sorted(
            backup_dir.glob(f"{DB_SRC.stem}_*.db"),
            key=lambda p: p.stat().st_mtime
        )
        for old in backups[:-MAX_BACKUPS]:
            try:
                old.unlink()
            except Exception:
                pass

    # ── 2. Save current working copy ─────────────────────────────
    shutil.copy2(str(DB_TMP), str(DB_SRC))


# ── Audit helpers ─────────────────────────────────────────────────────────────

def audit_log(ops, table, record_id, action, old_val=None, new_val=None):
    """Append an audit entry to the ops list (for run_many transactions)."""
    import json
    ops.append((
        """INSERT INTO change_log (table_name, record_id, action, old_value, new_value)
           VALUES (?,?,?,?,?)""",
        (table, str(record_id), action,
         json.dumps(old_val) if old_val else None,
         json.dumps(new_val) if new_val else None)
    ))
