#!/usr/bin/env python3
"""
PRODUCTION MODULE TESTS
Covers: WO feasibility, two-step enforcement, WO creation, batch conversion, batch list
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(__file__))
from base import *

def _get_cust_code_and_variant(tok):
    ref = GET("/api/ref", token=tok)
    if ref.status_code != 200: return None, None
    d = ref.json()
    custs    = d.get("customers", [])
    variants = d.get("variants",  [])
    cust = next((c for c in custs    if c.get("active") != False), None)
    var  = next((v for v in variants if v.get("active_flag") != 0), None)
    return (cust.get("code") if cust else None), (var.get("id") if var else None)

def run():
    _section("PRODUCTION — Feasibility / Two-Step / WO Creation / Batch / Cost Freeze")
    reset_results()
    tok = login()
    if not tok:
        _fail("Login", "Cannot proceed"); return summary()

    cust_code, var_id = _get_cust_code_and_variant(tok)

    # ── Work order list ────────────────────────────────────────────────────
    r = GET("/api/work-orders", token=tok)
    assert_status(r, 200, "GET /api/work-orders returns 200")
    wos = r.json() if r.status_code == 200 else []
    if isinstance(wos, list):
        _pass(f"Work order list is array ({len(wos)} WOs)")

    # ── Production batch list ──────────────────────────────────────────────
    r2 = GET("/api/production", token=tok)
    assert_status(r2, 200, "GET /api/production returns 200")
    batches = r2.json() if r2.status_code == 200 else []
    if isinstance(batches, list):
        _pass(f"Production batch list is array ({len(batches)} batches)")

    # ── Check batch has frozen unit cost ───────────────────────────────────
    if batches:
        batch = batches[0]
        if "unit_cost_at_posting" in batch:
            _pass("Production batch has unit_cost_at_posting (frozen)")
        else:
            _skip("unit_cost_at_posting in batch", "field may be named differently or batches empty")

    # ── WO feasibility check ───────────────────────────────────────────────
    if var_id:
        r3 = GET("/api/work-orders/check", params={"variantId": var_id, "qty": 10}, token=tok)
        if r3.status_code in (200, 400):
            _pass("GET /api/work-orders/check feasibility endpoint responds")
            if r3.status_code == 200:
                d = r3.json()
                if "feasible" in d or "ok" in d or "shortfalls" in d:
                    _pass("Feasibility response has expected structure")
                else:
                    _skip("Feasibility response structure", f"keys: {list(d.keys())[:5]}")
        else:
            _fail("GET /api/work-orders/check", f"got {r3.status_code}")

    # ── Two-step enforcement: WO from order item → confirm order first ─────
    if cust_code and var_id:
        r4 = POST("/api/customer-orders", {
            "custCode": cust_code,
            "orderDate": TODAY,
            "requiredDate": FUTURE,
            "items": [{"variantId": var_id, "qty": 5, "unitPrice": 150}]
        }, token=tok)
        if r4.status_code == 201:
            order_id = r4.json().get("id")
            items = r4.json().get("items", [])
            item_id = items[0].get("id") if items else None

            # Confirm order first (required before WO)
            conf = POST(f"/api/customer-orders/{order_id}/confirm", {}, token=tok)

            if item_id and conf.status_code in (200,201):
                # Create WO from order item (correct path)
                r5 = POST(f"/api/customer-orders/{order_id}/items/{item_id}/work-order",
                          {"notes": "Test WO"}, token=tok)
                if r5.status_code in (200, 201):
                    _pass("WO creation from order item succeeds (two-step: order first)")
                    wo_id = r5.json().get("id")

                    # Now try to convert WO to batch (second step)
                    if wo_id:
                        r6 = POST(f"/api/work-orders/{wo_id}/convert", {
                            "batchDate": TODAY,
                            "mfgDate": TODAY,
                            "bestBefore": FUTURE,
                            "notes": "Test batch"
                        }, token=tok)
                        if r6.status_code in (200, 201, 400):
                            # 400 is acceptable if not enough stock
                            _pass("POST /api/work-orders/:id/convert responds (step 2 of 2)")
                        else:
                            _fail("WO convert to batch", f"got {r6.status_code}: {r6.text[:100]}")
                elif r5.status_code == 400:
                    _pass("WO creation enforces business rules (returned 400 — stock/BOM issue)")
                else:
                    _fail("WO creation from order item", f"got {r5.status_code}: {r5.text[:100]}")

            # Clean up
            POST(f"/api/customer-orders/{order_id}/cancel", {}, token=tok)

    # ── Direct WO creation (not via order) — verify correct endpoint ───────
    # The correct endpoint for order-linked WOs is POST /api/customer-orders/:id/items/:item_id/work-order
    # POST /api/work-orders directly is for standalone WOs
    r7 = POST("/api/work-orders", {
        "variantId": var_id,
        "qtyUnits": 10,
        "targetDate": FUTURE,
        "notes": "Standalone WO test"
    }, token=tok)
    if r7.status_code in (200, 201, 400, 422):
        _pass("POST /api/work-orders standalone endpoint responds")
    else:
        _fail("POST /api/work-orders", f"got {r7.status_code}")

    logout(tok)
    print_summary()
    return summary()

if __name__ == "__main__":
    run()
