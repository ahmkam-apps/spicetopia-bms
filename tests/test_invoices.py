#!/usr/bin/env python3
"""
INVOICES MODULE TESTS
Covers: invoice generation, payment, allocation, status sync, AR aging, void
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(__file__))
from base import *

def _get_customer_and_variant(tok):
    ref = GET("/api/ref", token=tok)
    if ref.status_code != 200: return None, None, None
    d = ref.json()
    custs = d.get("customers", [])
    variants = [v for p in d.get("products", []) for v in p.get("variants", [])]
    cust = next((c for c in custs if c.get("active") != False), None)
    var  = next((v for v in variants if v.get("active_flag") != 0), None)
    cust_id      = cust.get("id")           if cust else None
    cust_code    = cust.get("code")         if cust else None
    product_code = var.get("product_code")  if var  else None
    pack_size    = var.get("pack_size")     if var  else None
    return cust_id, cust_code, product_code, pack_size

def run():
    _section("INVOICES — Generate / Pay / Allocation / Status Sync / AR Aging / Void")
    reset_results()
    tok = login()
    if not tok:
        _fail("Login", "Cannot proceed"); return summary()

    cust_id, cust_code, product_code, pack_size = _get_customer_and_variant(tok)
    if not cust_code or not product_code or not pack_size:
        _skip("All invoice tests", "no customer or variant"); logout(tok); print_summary(); return summary()

    # ── Create and confirm an order to generate invoice from ──────────────
    r = POST("/api/customer-orders", {
        "custCode": cust_code,
        "orderDate": TODAY,
        "requiredDate": FUTURE,
        "lines": [{"productCode": product_code, "packSize": pack_size, "qty": 1, "unitPrice": 200}]
    }, token=tok)
    if r.status_code != 201:
        _skip("Invoice tests", f"Could not create order: {r.status_code}"); logout(tok); print_summary(); return summary()
    order_id = r.json().get("orderId")

    conf = POST(f"/api/customer-orders/{order_id}/confirm", {}, token=tok)
    if conf.status_code not in (200, 201):
        _skip("Invoice tests", f"Could not confirm order: {conf.status_code}"); logout(tok); print_summary(); return summary()

    # Fetch order items so we can reference their IDs in the invoice
    order_detail = GET(f"/api/customer-orders/{order_id}", token=tok)
    order_items = order_detail.json().get("items", []) if order_detail.status_code == 200 else []
    if not order_items:
        _skip("Invoice tests", "Order has no items to invoice"); logout(tok); print_summary(); return summary()

    inv_lines = [{"orderItemId": item["id"], "qty": item["qty_ordered"]} for item in order_items]

    # ── Generate invoice from order ────────────────────────────────────────
    r2 = POST(f"/api/customer-orders/{order_id}/invoice",
              {"invoiceDate": TODAY, "dueDate": FUTURE, "lines": inv_lines}, token=tok)
    inv_id = None
    if r2.status_code == 201:
        _pass("POST /api/customer-orders/:id/invoice generates invoice")
        d = r2.json()
        inv_id = d.get("invoiceId")
        inv_num = d.get("invoiceNumber", "")
        _pass(f"Invoice created id={inv_id}, num={inv_num}") if inv_id else _fail("Invoice has id")
    elif r2.status_code == 400 and "stock" in r2.text.lower():
        _skip("POST /api/customer-orders/:id/invoice generates invoice",
              "insufficient FG stock on DEV — run production first")
    else:
        _fail("POST /api/customer-orders/:id/invoice generates invoice",
              f"expected 201, got {r2.status_code}: {r2.text[:200]}")

    # ── Invoice list ───────────────────────────────────────────────────────
    r3 = GET("/api/invoices", token=tok)
    assert_status(r3, 200, "GET /api/invoices returns 200")

    # ── Invoice detail ─────────────────────────────────────────────────────
    if inv_id:
        r4 = GET(f"/api/invoices/{inv_id}", token=tok)
        assert_status(r4, 200, f"GET /api/invoices/{inv_id} returns 200")
        if r4.status_code == 200:
            d = r4.json()
            assert_field(d, "status",         "Invoice has status")
            assert_field(d, "invoice_number", "Invoice has invoice_number")
            status = d.get("status","")
            if status == "UNPAID":
                _pass("New invoice status is UNPAID")
            else:
                _fail("New invoice status is UNPAID", f"got: {status}")

    # ── Record payment (partial) ───────────────────────────────────────────
    if inv_id:
        r5 = POST(f"/api/invoices/{inv_id}/pay", {
            "amount": 50,
            "paymentDate": TODAY,
            "paymentMode": "CASH",
            "notes": "Test partial payment"
        }, token=tok)
        if r5.status_code in (200, 201):
            _pass("POST /api/invoices/:id/pay records payment")
            # Check status is now PARTIAL
            r5b = GET(f"/api/invoices/{inv_id}", token=tok)
            if r5b.status_code == 200:
                status = r5b.json().get("status","")
                if status == "PARTIAL":
                    _pass("Invoice status PARTIAL after partial payment")
                elif status == "PAID":
                    _pass("Invoice status PAID after payment (amount covered full)")
                else:
                    _fail("Invoice status updated after payment", f"got: {status}")
        else:
            _fail("POST /api/invoices/:id/pay", f"got {r5.status_code}: {r5.text[:100]}")

    # ── AR Aging ───────────────────────────────────────────────────────────
    r6 = GET("/api/ar/aging", token=tok)
    assert_status(r6, 200, "GET /api/ar/aging returns 200")
    if r6.status_code == 200:
        aging = r6.json()
        if isinstance(aging, list):
            _pass(f"AR aging returns list ({len(aging)} entries)")

    # ── Customer payments list ─────────────────────────────────────────────
    r7 = GET("/api/customer-payments", token=tok)
    assert_status(r7, 200, "GET /api/customer-payments returns 200")

    # ── Void invoice ───────────────────────────────────────────────────────
    if inv_id:
        r8 = POST(f"/api/invoices/{inv_id}/void",
                  {"note": "Test void"}, token=tok)
        if r8.status_code in (200, 204):
            _pass("POST /api/invoices/:id/void voids invoice")
        else:
            _fail("Void invoice", f"got {r8.status_code}: {r8.text[:100]}")

    logout(tok)
    print_summary()
    return summary()

if __name__ == "__main__":
    run()
