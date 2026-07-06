#!/usr/bin/env python3
"""
RESERVATION TESTS — P0-5b: get_wo_reserved_stock_map must reserve RM only for
units that still need it, not the full work-order quantity.

Unlike the other modules, this is an IN-PROCESS unit test: it builds a tiny
throwaway SQLite fixture, points modules.db at it, and calls the real
inventory.get_wo_reserved_stock_map() directly. That makes the arithmetic
deterministic and independent of whatever BOM/RM the shared test DB happens to
have (the HTTP suite skips these paths when the DB has no active BOMs).

Fixture arithmetic (fixed so numbers are easy to reason about):
    pack = 50 g, BOM = 200 g of ingredient #100 per 1000 g batch
    → reserved_grams(ingredient 100) = 200 * (reserved_units * 50) / 1000
                                     = 10 * reserved_units
where reserved_units = MAX(qty_units - produced_units - active_run_units, 0).
"""
import os
import sys
import sqlite3
import tempfile

sys.path.insert(0, os.path.dirname(__file__))                       # tests/  (for base)
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))      # repo root (for modules.*)
from base import _pass, _fail, _section, summary, reset_results

ING = 100          # ingredient id under test
GRAMS_PER_UNIT = 10  # from the fixture arithmetic above


def _build_fixture(path):
    """Create the minimal schema the reservation query touches, one WO for 1000 units."""
    c = sqlite3.connect(path)
    c.executescript("""
        CREATE TABLE pack_sizes       (id INTEGER PRIMARY KEY, grams REAL);
        CREATE TABLE product_variants (id INTEGER PRIMARY KEY, product_id INTEGER, pack_size_id INTEGER);
        CREATE TABLE bom_versions     (id INTEGER PRIMARY KEY, product_id INTEGER, active_flag INTEGER, batch_size_grams REAL);
        CREATE TABLE bom_items        (id INTEGER PRIMARY KEY, bom_version_id INTEGER, ingredient_id INTEGER, quantity_grams REAL);
        CREATE TABLE work_orders      (id INTEGER PRIMARY KEY, product_variant_id INTEGER,
                                       qty_units INTEGER, produced_units INTEGER DEFAULT 0, status TEXT);
        CREATE TABLE batch_runs       (id INTEGER PRIMARY KEY, wo_id INTEGER, qty_units INTEGER, status TEXT);

        INSERT INTO pack_sizes        (id, grams) VALUES (1, 50);
        INSERT INTO product_variants  (id, product_id, pack_size_id) VALUES (1, 1, 1);
        INSERT INTO bom_versions      (id, product_id, active_flag, batch_size_grams) VALUES (1, 1, 1, 1000);
        INSERT INTO bom_items         (id, bom_version_id, ingredient_id, quantity_grams) VALUES (1, 1, 100, 200);
        INSERT INTO work_orders       (id, product_variant_id, qty_units, produced_units, status)
                                      VALUES (1, 1, 1000, 0, 'in_progress');
    """)
    c.commit()
    c.close()


def _set_state(path, produced_units, run_units, wo_status='in_progress'):
    """Reset the WO to a given produced/active-run state for one scenario."""
    c = sqlite3.connect(path)
    c.execute("UPDATE work_orders SET produced_units=?, status=? WHERE id=1",
              (produced_units, wo_status))
    c.execute("DELETE FROM batch_runs")
    if run_units:
        c.execute("INSERT INTO batch_runs (id, wo_id, qty_units, status) VALUES (1, 1, ?, 'in_progress')",
                  (run_units,))
    c.commit()
    c.close()


def run():
    _section("RESERVATION — P0-5b: reserve only units still needing RM")
    reset_results()

    tmp = tempfile.mkdtemp(prefix="resv_test_")
    db_path = os.path.join(tmp, "resv.db")
    try:
        _build_fixture(db_path)

        # Point modules.db at the fixture, then import the real function.
        import modules.db as _db
        _db.DB_TMP = db_path
        _db.DB_SRC = None
        from modules.inventory import get_wo_reserved_stock_map

        def _check(label, produced, run_units, expected_units, exclude=None, wo_status='in_progress'):
            _set_state(db_path, produced, run_units, wo_status)
            got = get_wo_reserved_stock_map(exclude_wo_id=exclude).get(ING, 0)
            want = expected_units * GRAMS_PER_UNIT
            if abs(got - want) < 0.001:
                _pass(f"{label}: reserved {got:.0f}g (= {expected_units} units)")
            else:
                _fail(label, f"expected {want:.0f}g ({expected_units} units), got {got:.0f}g")

        # 1. Nothing produced, no live run → full quantity reserved (baseline behaviour).
        _check("no production reserves full WO", produced=0, run_units=0, expected_units=1000)

        # 2. Partial produced (earlier verified batch) → remainder only (the multi-batch case).
        _check("partial produced reserves remainder", produced=250, run_units=0, expected_units=750)

        # 3. Live batch run consumed RM but produced_units not yet bumped → the P0-5b core:
        #    the run's units must NOT be reserved again (on-hand already dropped).
        _check("active batch run not double-reserved", produced=250, run_units=250, expected_units=500)

        # 4. Whole WO committed to a live run → reserves nothing (no false shortfall).
        _check("fully committed run reserves nothing", produced=0, run_units=1000, expected_units=0)

        # 5. Over-committed (produced + run exceed target) → clamped at 0, never negative.
        _check("over-committed clamps at zero", produced=800, run_units=300, expected_units=0)

        # 6. exclude_wo_id drops the WO entirely (self-feasibility check must not self-block).
        _set_state(db_path, 0, 0)
        excluded = get_wo_reserved_stock_map(exclude_wo_id=1).get(ING, 0)
        _pass("exclude_wo_id removes the WO's reservation") if excluded == 0 \
            else _fail("exclude_wo_id", f"expected 0g, got {excluded:.0f}g")

    except Exception as e:
        import traceback
        traceback.print_exc()
        _fail("reservation test harness", str(e))
    finally:
        try:
            import shutil
            shutil.rmtree(tmp, ignore_errors=True)
        except Exception:
            pass

    return summary()


if __name__ == "__main__":
    run()
    from base import print_summary
    print_summary()
