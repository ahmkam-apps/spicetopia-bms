#!/usr/bin/env python3
"""
INVENTORY MODULE TESTS
Covers: ingredient CRUD, stock map, ledger, code format, export
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(__file__))
from base import *

def run():
    _section("INVENTORY — Ingredient CRUD / Stock Map / Ledger / Code Format")
    reset_results()
    tok = login()
    if not tok:
        _fail("Login", "Cannot proceed"); return summary()

    # ── Ingredient list ────────────────────────────────────────────────────
    r = GET("/api/ingredients", token=tok)
    assert_status(r, 200, "GET /api/ingredients returns 200")
    ings = r.json() if r.status_code == 200 else []
    if isinstance(ings, list):
        _pass(f"Ingredient list is array ({len(ings)} ingredients)")

    # ── Code format: ING-NNNSP ─────────────────────────────────────────────
    if ings:
        for ing in ings[:3]:
            code = ing.get("code", "")
            if code.startswith("ING-") and code.endswith("SP"):
                _pass(f"Ingredient code format correct: {code}")
            else:
                _fail("Ingredient code format (ING-NNNSP)", f"got: {code}")
            break

    # ── Get next code from server ──────────────────────────────────────────
    r_code = GET("/api/ingredients/next-code", token=tok)
    next_code = None
    if r_code.status_code == 200:
        next_code = r_code.json().get("code")
        _pass(f"GET /api/ingredients/next-code returns code: {next_code}")
    else:
        _fail("GET /api/ingredients/next-code", f"got {r_code.status_code}")

    # ── Create ingredient (code required by API) ────────────────────────────
    ts = int(time.time())
    if next_code:
        r2 = POST("/api/ingredients", {
            "code": next_code,
            "name": f"Test Spice {ts}",
            "costPerKg": 500,
            "reorderLevel": 1000,
            "openingGrams": 5000
        }, token=tok)
        assert_status(r2, 201, "POST /api/ingredients creates ingredient")
        ing_code = None
        if r2.status_code == 201:
            d = r2.json()
            ing_code = d.get("code")
            if ing_code and ing_code.startswith("ING-") and ing_code.endswith("SP"):
                _pass(f"New ingredient code format correct: {ing_code}")
            else:
                _fail("New ingredient code format (ING-NNNSP)", f"got: {ing_code}")
    else:
        _skip("POST /api/ingredients test", "could not get next code")
        ing_code = None

    # ── Update ingredient ──────────────────────────────────────────────────
    if ing_code:
        r3 = PUT(f"/api/ingredients/{ing_code}", {
            "name": f"Updated Spice {ts}",
            "costPerKg": 600,
            "reorderLevel": 1500
        }, token=tok)
        assert_status(r3, 200, f"PUT /api/ingredients/{ing_code} updates ingredient")

    # ── Stock info in ref ──────────────────────────────────────────────────
    r4 = GET("/api/ref", token=tok)
    if r4.status_code == 200:
        ref = r4.json()
        if "ingredients" in ref:
            _pass("Reference data includes ingredient stock info")
        else:
            _skip("Ingredient stock in ref", "key not found")

    # ── Inventory ledger ───────────────────────────────────────────────────
    r5 = GET("/api/inventory/ledger", token=tok)
    if r5.status_code == 200:
        _pass("GET /api/inventory/ledger returns 200")
        ledger = r5.json()
        if isinstance(ledger, list):
            _pass(f"Ledger is an array ({len(ledger)} entries)")
    else:
        _fail("GET /api/inventory/ledger", f"got {r5.status_code}")

    # ── Export ─────────────────────────────────────────────────────────────
    r6 = GET("/api/ingredients/export", token=tok)
    if r6.status_code == 200:
        _pass("GET /api/ingredients/export returns 200")
    else:
        _fail("GET /api/ingredients/export", f"got {r6.status_code}")

    # ── Deactivate ─────────────────────────────────────────────────────────
    if ing_code:
        r7 = DELETE(f"/api/ingredients/{ing_code}", token=tok)
        if r7.status_code in (200, 204):
            _pass(f"DELETE /api/ingredients/{ing_code} deactivates ingredient")
        else:
            _fail(f"DELETE /api/ingredients/{ing_code}", f"got {r7.status_code}")

    logout(tok)
    print_summary()
    return summary()

if __name__ == "__main__":
    run()
