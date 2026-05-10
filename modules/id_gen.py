"""
modules/id_gen.py — ID / code generation for Spicetopia BMS.

All functions that mint new primary codes (customer codes, supplier codes,
ingredient codes, account numbers, blend codes) live here.  They share a
common counter table in SQLite (id_counters) and always call save_db() after
incrementing so the counter survives a process restart.

Depends on:
  modules.db    — _conn(), save_db()
  modules.utils — _city_to_code()

Exported via `from modules.id_gen import *` in server.py.
"""

from modules.db    import _conn, save_db
from modules.utils import _city_to_code

__all__ = [
    '_sync_counter_to_max',
    'next_id',
    '_sync_ingredient_counter',
    'next_ingredient_code',
    'peek_next_ingredient_code',
    'generate_account_number',
    'backfill_customer_account_numbers',
    'next_blend_code',
    'peek_next_blend_code',
]


# ── Counter sync helpers ───────────────────────────────────────────────────────

def _sync_counter_to_max(entity, table, code_col, full_prefix):
    """Ensure id_counters[entity] >= the highest number already used in table.code_col.
    Prevents UNIQUE constraint failures when the counter was freshly seeded at 0
    but rows with higher numbers already exist (e.g. after a Railway redeploy).
    full_prefix: the full code prefix string, e.g. 'SP-SUP-' or 'SP-SP-CUST-'."""
    c = _conn()
    try:
        rows = c.execute(f"SELECT {code_col} FROM {table}").fetchall()
        max_num = 0
        for (code,) in rows:
            if code and code.startswith(full_prefix):
                try:
                    max_num = max(max_num, int(code[len(full_prefix):]))
                except ValueError:
                    pass
        # Seed the row if missing, then bump if behind
        c.execute("INSERT OR IGNORE INTO id_counters (entity, last_num) VALUES (?,?)",
                  (entity, max_num))
        c.execute(
            "UPDATE id_counters SET last_num=? WHERE entity=? AND last_num<?",
            (max_num, entity, max_num)
        )
        c.commit()
    finally:
        c.close()


def _sync_ingredient_counter(c=None):
    """Sync id_counters[ingredient] to the highest ING-NNNSP number already in the DB.
    Accepts an open connection or opens its own. Always safe to call multiple times."""
    owned = c is None
    if owned:
        c = _conn()
    try:
        rows = c.execute("SELECT code FROM ingredients").fetchall()
        max_num = 0
        for (code,) in rows:
            if code and code.startswith('ING-') and code.endswith('SP'):
                try:
                    max_num = max(max_num, int(code[4:-2]))
                except ValueError:
                    pass
        c.execute(
            "INSERT OR IGNORE INTO id_counters (entity, last_num) VALUES ('ingredient', ?)",
            (max_num,)
        )
        c.execute(
            "UPDATE id_counters SET last_num=? WHERE entity='ingredient' AND last_num<?",
            (max_num, max_num)
        )
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()


# ── ID generators ─────────────────────────────────────────────────────────────

def next_id(entity, prefix, width=4, conn=None):
    """Atomically increment counter and return formatted SP-PREFIX-XXXX.
    If conn is provided, reuse it (no separate commit/close) so that callers
    inside an open write transaction don't open a second conflicting connection.

    CRITICAL: Never call next_id() inside an open _conn() transaction —
    it opens its own connection (WAL deadlock).  Generate all IDs before
    opening the main transaction.
    """
    owned = conn is None
    c = _conn() if owned else conn
    try:
        c.execute("INSERT OR IGNORE INTO id_counters (entity, last_num) VALUES (?,0)", (entity,))
        c.execute("UPDATE id_counters SET last_num=last_num+1 WHERE entity=?", (entity,))
        num = c.execute("SELECT last_num FROM id_counters WHERE entity=?", (entity,)).fetchone()[0]
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()
    if owned:
        save_db()
    return f"SP-{prefix}-{num:0{width}d}"


def next_ingredient_code():
    """Generate next coded ingredient ID: ING-001SP, ING-002SP, ...
    The 'SP' suffix ties the code to Spicetopia without revealing what the
    ingredient is.  The real ingredient name is kept in a physical, off-system
    legend only."""
    c = _conn()
    try:
        _sync_ingredient_counter(c)
        c.execute("UPDATE id_counters SET last_num=last_num+1 WHERE entity='ingredient'")
        num = c.execute(
            "SELECT last_num FROM id_counters WHERE entity='ingredient'"
        ).fetchone()[0]
        c.commit()
    finally:
        c.close()
    save_db()
    return f"ING-{num:03d}SP"


def peek_next_ingredient_code():
    """Return what the NEXT ingredient code would be, without incrementing the counter."""
    _sync_ingredient_counter()  # ensure counter reflects any existing codes
    from modules.db import qry1
    row = qry1("SELECT last_num FROM id_counters WHERE entity='ingredient'", ())
    num = ((row['last_num'] if row else 0) + 1)
    return f"ING-{num:03d}SP"


def next_blend_code(prefix: str) -> str:
    """Generate next blend code for a given product-category prefix.
    Format: {PREFIX}-BC-{NNN}  e.g. GM-BC-001, CM-BC-001, RCP-BC-001
    Each prefix has its own independent counter so series don't clash.
    The code carries no ingredient or ratio information — IP safe."""
    prefix = prefix.strip().upper()
    if not prefix:
        raise ValueError("Blend code prefix is required (e.g. GM, CM, RCP)")
    entity = f"blend_{prefix}"
    c = _conn()
    try:
        c.execute("INSERT OR IGNORE INTO id_counters (entity, last_num) VALUES (?,0)", (entity,))
        c.execute("UPDATE id_counters SET last_num=last_num+1 WHERE entity=?", (entity,))
        num = c.execute(
            "SELECT last_num FROM id_counters WHERE entity=?", (entity,)
        ).fetchone()[0]
        c.commit()
    finally:
        c.close()
    save_db()
    return f"{prefix}-BC-{num:03d}"


def peek_next_blend_code(prefix: str) -> str:
    """Return what the NEXT blend code would be for a prefix, without incrementing."""
    prefix = prefix.strip().upper()
    entity = f"blend_{prefix}"
    from modules.db import qry1
    row = qry1("SELECT last_num FROM id_counters WHERE entity=?", (entity,))
    num = ((row['last_num'] if row else 0) + 1)
    return f"{prefix}-BC-{num:03d}"


# ── Account number generation ─────────────────────────────────────────────────

def generate_account_number(city, customer_type='RETAIL'):
    """Generate {CITY3}-{TYPE}{NNN} account number (e.g. KHI-R001, KHI-D004, ISB-W001).
    Each city+type combination has its own independent counter in id_counters.
    Type codes: RETAIL→R, DIRECT→D, WHOLESALE→W.
    Existing customers with legacy SPKHI-NNN format are unaffected."""
    type_code = {'RETAIL': 'R', 'DIRECT': 'D', 'WHOLESALE': 'W'}.get(
        (customer_type or 'RETAIL').upper(), 'R')
    city3  = _city_to_code(city)
    entity = f'acct_{city3}_{type_code}'
    c = _conn()
    try:
        c.execute("INSERT OR IGNORE INTO id_counters (entity, last_num) VALUES (?,0)", (entity,))
        c.execute("UPDATE id_counters SET last_num=last_num+1 WHERE entity=?", (entity,))
        num = c.execute(
            "SELECT last_num FROM id_counters WHERE entity=?", (entity,)
        ).fetchone()[0]
        c.commit()
    finally:
        c.close()
    save_db()
    return f"{city3}-{type_code}{num:03d}"


def backfill_customer_account_numbers():
    """
    One-time startup task (idempotent):
      1. Delete test customers (SP-CUST-0001 … SP-CUST-0010) if they have no FK deps.
      2. Assign account_number to real customers that still have NULL.
    """
    c = _conn()
    try:
        # ── Delete test customers ─────────────────────────────────
        test_codes = [f'SP-CUST-{n:04d}' for n in range(1, 11)]
        for code in test_codes:
            row = c.execute("SELECT id FROM customers WHERE code=?", (code,)).fetchone()
            if not row:
                continue
            cid = row[0]
            has_deps = (
                c.execute("SELECT 1 FROM customer_orders   WHERE customer_id=? LIMIT 1", (cid,)).fetchone() or
                c.execute("SELECT 1 FROM invoices          WHERE customer_id=? LIMIT 1", (cid,)).fetchone() or
                c.execute("SELECT 1 FROM customer_payments WHERE customer_id=? LIMIT 1", (cid,)).fetchone() or
                c.execute("SELECT 1 FROM sales             WHERE customer_id=? LIMIT 1", (cid,)).fetchone()
            )
            if not has_deps:
                c.execute("DELETE FROM customers WHERE id=?", (cid,))
                print(f"  ✓ Backfill: deleted test customer {code}")
            else:
                print(f"  ⚠ Backfill: {code} has data, skipped deletion")

        # ── Assign account_number to real customers without one ───
        unassigned = c.execute(
            "SELECT id, name, city, customer_type FROM customers WHERE account_number IS NULL"
        ).fetchall()
        for (cid, name, city, ctype) in unassigned:
            city3     = _city_to_code(city or '')
            type_code = {'RETAIL': 'R', 'DIRECT': 'D', 'WHOLESALE': 'W'}.get(
                (ctype or 'RETAIL').upper(), 'R')
            entity = f'acct_{city3}_{type_code}'
            c.execute("INSERT OR IGNORE INTO id_counters (entity, last_num) VALUES (?,0)", (entity,))
            c.execute("UPDATE id_counters SET last_num=last_num+1 WHERE entity=?", (entity,))
            num = c.execute(
                "SELECT last_num FROM id_counters WHERE entity=?", (entity,)
            ).fetchone()[0]
            acc_num = f"{city3}-{type_code}{num:03d}"
            c.execute("UPDATE customers SET account_number=? WHERE id=?", (acc_num, cid))
            print(f"  ✓ Backfill: assigned {acc_num} to '{name}' (id={cid})")

        c.commit()
    finally:
        c.close()
    save_db()
