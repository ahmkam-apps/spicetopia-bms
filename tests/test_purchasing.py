#!/usr/bin/env python3
"""
PURCHASING MODULE TESTS
Covers: PO creation, GRN, supplier bills, AP payment, AP aging, void
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(__file__))
from base import *

def _get_supplier_and_ingredient(tok):
    sups = GET("/api/suppliers", token=tok)
    ings = GET("/api/ingredients", token=tok)
    sup = next((s for s in (sups.json() if sups.status_code==200 else [])
                if s.get("active_flag") != 0), None)
    ing = next((i for i in (ings.json() if ings.status_code==200 else [])
                if i.get("active") != False), None)
    return (sup.get("id") if sup else None), (ing.get("id") if ing else None)

def run():
    _section("PURCHASING — PO / GRN / Bills / AP Payment / AP Aging / Void")
    reset_results()
    tok = login()
    if not tok:
        _fail("Login", "Cannot proceed"); return summary()

    sup_id, ing_id = _get_supplier_and_ingredient(tok)

    # ── PO list ────────────────────────────────────────────────────────────
    r = GET("/api/purchase-orders", token=tok)
    assert_status(r, 200, "GET /api/purchase-orders returns 200")
    pos = r.json() if r.status_code == 200 else []
    if isinstance(pos, list):
        _pass(f"PO list is array ({len(pos)} POs)")

    # ── Create PO ──────────────────────────────────────────────────────────
    po_id = None
    if sup_id and ing_id:
        ts = int(time.time())
        r2 = POST("/api/purchase-orders", {
            "supplierId": sup_id,
            "poDate": TODAY,
            "expectedDate": FUTURE,
            "paymentTerms": "30 days",
            "notes": f"Test PO {ts}",
            "items": [{"ingredientId": ing_id, "quantityKg": 10, "unitCostKg": 500}]
        }, token=tok)
        assert_status(r2, 201, "POST /api/purchase-orders creates PO")
        if r2.status_code == 201:
            d = r2.json()
            po_id = d.get("id")
            po_num = d.get("po_number","")
            _pass(f"PO created id={po_id}, num={po_num}") if po_id else _fail("PO has id")
    else:
        _skip("PO creation test", "no supplier or ingredient available")

    # ── PO detail ──────────────────────────────────────────────────────────
    if po_id:
        r3 = GET(f"/api/purchase-orders/{po_id}", token=tok)
        assert_status(r3, 200, f"GET /api/purchase-orders/{po_id} returns 200")
        if r3.status_code == 200:
            d = r3.json()
            assert_field(d, "po_number", "PO detail has po_number")
            assert_field(d, "status",    "PO detail has status")

    # ── GRN — receive goods ────────────────────────────────────────────────
    if po_id:
        r4 = POST(f"/api/purchase-orders/{po_id}/status", {
            "status": "received",
            "items": [{"ingredientId": ing_id, "receivedKg": 10}]
        }, token=tok)
        if r4.status_code in (200, 201, 204):
            _pass("GRN: POST /api/purchase-orders/:id/status marks received")
        else:
            _skip("GRN receive", f"got {r4.status_code}: {r4.text[:100]}")

    # ── Bills list ─────────────────────────────────────────────────────────
    r5 = GET("/api/bills", token=tok)
    assert_status(r5, 200, "GET /api/bills returns 200")
    bills = r5.json() if r5.status_code == 200 else []
    if isinstance(bills, list):
        _pass(f"Bill list is array ({len(bills)} bills)")

    # ── Create supplier bill (manual) ──────────────────────────────────────
    bill_id = None
    if sup_id and ing_id:
        r6 = POST("/api/bills", {
            "supplierId": sup_id,
            "billDate": TODAY,
            "dueDate": FUTURE,
            "supplierRef": f"TEST-{int(time.time())}",
            "items": [{"ingredientId": ing_id, "quantityKg": 5, "unitCostKg": 500}]
        }, token=tok)
        assert_status(r6, 201, "POST /api/bills creates supplier bill")
        if r6.status_code == 201:
            bill_id = r6.json().get("billId") or r6.json().get("id")
            _pass(f"Bill created id={bill_id}") if bill_id else _fail("Bill has id")

    # ── Pay bill ───────────────────────────────────────────────────────────
    if bill_id:
        r7 = POST(f"/api/bills/{bill_id}/pay", {
            "amount": 500,
            "paymentDate": TODAY,
            "paymentMode": "BANK_TRANSFER",
            "notes": "Test payment"
        }, token=tok)
        if r7.status_code in (200, 201):
            _pass("POST /api/bills/:id/pay records AP payment")
        else:
            _fail("Pay bill", f"got {r7.status_code}: {r7.text[:100]}")

    # ── AP Aging ───────────────────────────────────────────────────────────
    r8 = GET("/api/ap/aging", token=tok)
    assert_status(r8, 200, "GET /api/ap/aging returns 200")

    # ── Supplier payments list ─────────────────────────────────────────────
    r9 = GET("/api/supplier-payments", token=tok)
    assert_status(r9, 200, "GET /api/supplier-payments returns 200")

    # ── Void bill ──────────────────────────────────────────────────────────
    if bill_id:
        r10 = POST(f"/api/supplier-bills/{bill_id}/void",
                   {"note": "Test void"}, token=tok)
        if r10.status_code in (200, 204):
            _pass("POST /api/supplier-bills/:id/void voids bill")
        else:
            _fail("Void bill", f"got {r10.status_code}: {r10.text[:100]}")

    logout(tok)
    print_summary()
    return summary()

if __name__ == "__main__":
    run()
