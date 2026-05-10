#!/usr/bin/env python3
"""
PRODUCTS MODULE TESTS
Covers: product list, variants, SKU format, GTIN validation, prices
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from base import *

EXPECTED_SKUS = {"SPGM-50","SPGM-100","SPGM-1000","SPCM-50","SPCM-100","SPCM-1000"}

def run():
    _section("PRODUCTS — List / Variants / SKU Format / GTIN / Prices")
    reset_results()
    tok = login()
    if not tok:
        _fail("Login", "Cannot proceed"); return summary()

    # ── Product list ───────────────────────────────────────────────────────
    r = GET("/api/products", token=tok)
    assert_status(r, 200, "GET /api/products returns 200")
    products = r.json() if r.status_code == 200 else []
    if isinstance(products, list) and len(products) >= 2:
        _pass(f"At least 2 products exist ({len(products)} found)")
    else:
        _fail("At least 2 products exist (SPGM + SPCM)", f"got {len(products) if isinstance(products,list) else products}")

    # ── Product codes ──────────────────────────────────────────────────────
    codes = {p.get("code") for p in products if isinstance(products, list)}
    for expected in ["SPGM", "SPCM"]:
        if expected in codes:
            _pass(f"Product code {expected} exists")
        else:
            _fail(f"Product code {expected} exists", f"found: {codes}")

    # ── Variants / SKUs ────────────────────────────────────────────────────
    r2 = GET("/api/ref", token=tok)
    assert_status(r2, 200, "GET /api/ref returns reference data")
    ref = r2.json() if r2.status_code == 200 else {}
    variants = ref.get("variants", [])
    if variants:
        _pass(f"Reference data contains variants ({len(variants)} found)")
        found_skus = {v.get("sku_code") for v in variants}
        for sku in EXPECTED_SKUS:
            if sku in found_skus:
                _pass(f"SKU {sku} exists")
            else:
                _fail(f"SKU {sku} exists", f"found: {found_skus}")
    else:
        _skip("Variant SKU check", "no variants in ref data")

    # ── GTIN validation ────────────────────────────────────────────────────
    if variants:
        variant_id = variants[0].get("id")
        if variant_id:
            # Valid GTIN (13 digits)
            r3 = PUT(f"/api/products/variants/{variant_id}/gtin",
                     {"gtin": "1234567890123"}, token=tok)
            if r3.status_code in (200, 204):
                _pass("Valid 13-digit GTIN accepted")
            else:
                _fail("Valid 13-digit GTIN accepted", f"got {r3.status_code}: {r3.text[:100]}")

            # Invalid GTIN — too short
            r4 = PUT(f"/api/products/variants/{variant_id}/gtin",
                     {"gtin": "123"}, token=tok)
            if r4.status_code in (400, 422):
                _pass("GTIN shorter than 8 digits rejected")
            else:
                _fail("GTIN shorter than 8 digits rejected", f"got {r4.status_code}")

            # Invalid GTIN — too long
            r5 = PUT(f"/api/products/variants/{variant_id}/gtin",
                     {"gtin": "123456789012345"}, token=tok)
            if r5.status_code in (400, 422):
                _pass("GTIN longer than 14 digits rejected")
            else:
                _fail("GTIN longer than 14 digits rejected", f"got {r5.status_code}")

            # Invalid GTIN — non-numeric
            r6 = PUT(f"/api/products/variants/{variant_id}/gtin",
                     {"gtin": "ABCDEFGHIJKLM"}, token=tok)
            if r6.status_code in (400, 422):
                _pass("Non-numeric GTIN rejected")
            else:
                _fail("Non-numeric GTIN rejected", f"got {r6.status_code}")

    # ── Prices ─────────────────────────────────────────────────────────────
    r7 = GET("/api/prices", token=tok)
    assert_status(r7, 200, "GET /api/prices returns 200")
    prices = r7.json() if r7.status_code == 200 else []
    if isinstance(prices, list) and len(prices) > 0:
        _pass(f"Price records exist ({len(prices)} found)")
    else:
        _skip("Price records exist", "no prices configured yet")

    logout(tok)
    print_summary()
    return summary()

if __name__ == "__main__":
    run()
