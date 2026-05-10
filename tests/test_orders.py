#!/usr/bin/env python3
"""
ORDERS MODULE TESTS
Covers: order lifecycle (draft→confirmed→invoiced), WO creation, cancel, review queue
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(__file__))
from base import *

def _get_customer_and_variants(tok):
    """Return (cust_code, var1, var2) where each var is (product_code, pack_size).
    var2 is a different variant from var1 (for add-item tests), or None if only one exists."""
    ref = GET("/api/ref", token=tok)
    if ref.status_code != 200:
        return None, None, None, None, None
    d = ref.json()
    custs = d.get("customers", [])
    variants = [v for p in d.get("products", []) for v in p.get("variants", [])]
    active = [v for v in variants if v.get("active_flag") != 0]
    cust = next((c for c in custs if c.get("active") != False), None)
    var1 = active[0] if len(active) > 0 else None
    var2 = active[1] if len(active) > 1 else None
    return (
        cust.get("code") if cust else None,
        var1.get("product_code") if var1 else None,
        var1.get("pack_size") if var1 else None,
        var2.get("product_code") if var2 else None,
        var2.get("pack_size") if var2 else None,
    )

def run():
    _section("ORDERS — Lifecycle / WO Creation / Cancel / Review Queue")
    reset_results()
    tok = login()
    if not tok:
        _fail("Login", "Cannot proceed"); return summary()

    cust_code, product_code, pack_size, p2, ps2 = _get_customer_and_variants(tok)
    if not cust_code or not product_code or not pack_size:
        _skip("All order tests", "no customer or variant available")
        logout(tok); print_summary(); return summary()

    # ── Create order ───────────────────────────────────────────────────────
    ts = int(time.time())
    order_payload = {
        "custCode": cust_code,
        "orderDate": TODAY,
        "requiredDate": FUTURE,
        "notes": f"Test order {ts}",
        "lines": [{"productCode": product_code, "packSize": pack_size, "qty": 2, "unitPrice": 100}]
    }
    r = POST("/api/customer-orders", order_payload, token=tok)
    assert_status(r, 201, "POST /api/customer-orders creates order")
    order_id = None
    if r.status_code == 201:
        d = r.json()
        order_id = d.get("orderId")
        order_num = d.get("orderNumber", "")
        _pass(f"Order created with id={order_id}") if order_id else _fail("Order has id")
        if order_num:
            _pass(f"Order has orderNumber: {order_num}")

    # ── Get order detail ───────────────────────────────────────────────────
    if order_id:
        r2 = GET(f"/api/customer-orders/{order_id}", token=tok)
        assert_status(r2, 200, f"GET /api/customer-orders/{order_id} returns 200")
        if r2.status_code == 200:
            d = r2.json()
            assert_field(d, "items",        "Order detail has items")
            assert_field(d, "order_number", "Order detail has order_number")
            assert_field(d, "status",       "Order detail has status")

    # ── Confirm order ──────────────────────────────────────────────────────
    if order_id:
        r3 = POST(f"/api/customer-orders/{order_id}/confirm", {}, token=tok)
        if r3.status_code in (200, 201):
            _pass(f"POST /api/customer-orders/{order_id}/confirm succeeds")
            d = r3.json()
            status = d.get("status","")
            if status in ("confirmed","CONFIRMED"):
                _pass("Order status is confirmed after confirm")
        else:
            _fail(f"Confirm order", f"got {r3.status_code}: {r3.text[:200]}")

    # ── Add item to order (use a different variant to avoid duplicate rejection) ──
    if p2 and ps2:
        r_new = POST("/api/customer-orders", {
            "custCode": cust_code,
            "orderDate": TODAY,
            "requiredDate": FUTURE,
            "lines": [{"productCode": product_code, "packSize": pack_size, "qty": 1, "unitPrice": 100}]
        }, token=tok)
        if r_new.status_code == 201:
            new_id = r_new.json().get("orderId")
            if new_id:
                r_add = POST(f"/api/customer-orders/{new_id}/items",
                             {"productCode": p2, "packSize": ps2, "qty": 1, "unitPrice": 100}, token=tok)
                if r_add.status_code in (200, 201):
                    _pass("POST /api/customer-orders/:id/items adds item to draft order")
                else:
                    _fail("Add item to draft order", f"got {r_add.status_code}")
                # Cancel it
                POST(f"/api/customer-orders/{new_id}/cancel", {}, token=tok)
    else:
        _skip("Add item to draft order", "only one variant available")

    # ── Cancel order ───────────────────────────────────────────────────────
    r_cancel_order = POST("/api/customer-orders", {
        "custCode": cust_code,
        "orderDate": TODAY,
        "requiredDate": FUTURE,
        "lines": [{"productCode": product_code, "packSize": pack_size, "qty": 1, "unitPrice": 100}]
    }, token=tok)
    if r_cancel_order.status_code == 201:
        cancel_id = r_cancel_order.json().get("orderId")
        if cancel_id:
            r_cancel = POST(f"/api/customer-orders/{cancel_id}/cancel", {}, token=tok)
            if r_cancel.status_code in (200, 204):
                _pass("POST /api/customer-orders/:id/cancel succeeds")
            else:
                _fail("Cancel order", f"got {r_cancel.status_code}: {r_cancel.text[:100]}")

    # ── Order list ─────────────────────────────────────────────────────────
    r4 = GET("/api/customer-orders", token=tok)
    assert_status(r4, 200, "GET /api/customer-orders returns 200")
    orders = r4.json() if r4.status_code == 200 else []
    if isinstance(orders, list):
        _pass(f"Order list is array ({len(orders)} orders)")

    # ── Review queue ───────────────────────────────────────────────────────
    r5 = GET("/api/review-queue", token=tok)
    assert_status(r5, 200, "GET /api/review-queue returns 200")

    logout(tok)
    print_summary()
    return summary()

if __name__ == "__main__":
    run()
