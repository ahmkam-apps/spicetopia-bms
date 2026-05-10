#!/usr/bin/env python3
"""
CUSTOMERS MODULE TESTS
Covers: CRUD, code format, account number format, credit limit, customer types
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(__file__))
from base import *

def run():
    _section("CUSTOMERS — CRUD / Code Format / Account Numbers / Credit Limits")
    reset_results()
    tok = login()
    if not tok:
        _fail("Login", "Cannot proceed"); return summary()

    # ── List ───────────────────────────────────────────────────────────────
    r = GET("/api/customers", token=tok)
    assert_status(r, 200, "GET /api/customers returns 200")
    custs = r.json() if r.status_code == 200 else []
    if isinstance(custs, list):
        _pass(f"Customer list is an array ({len(custs)} customers)")

    # ── Create ─────────────────────────────────────────────────────────────
    ts = int(time.time())
    new_cust = {
        "name": f"Test Customer {ts}",
        "customerType": "RETAIL",
        "city": "Karachi",
        "phone": "03001234567",
        "creditLimit": 50000,
        "paymentTermsDays": 30
    }
    r2 = POST("/api/customers", new_cust, token=tok)
    assert_status(r2, 201, "POST /api/customers creates new customer")
    cust_id = None
    if r2.status_code == 201:
        d = r2.json()
        cust_id = d.get("id")
        code = d.get("code", "")
        account = d.get("account_number", "")
        _pass(f"New customer has id={cust_id}") if cust_id else _fail("New customer has id")
        # Correct format after fix: SP-CUST-NNNN
        if code.startswith("SP-CUST-") and not code.startswith("SP-SP-CUST-"):
            _pass(f"Customer code format correct: {code}")
        elif code.startswith("SP-SP-CUST-"):
            _fail("Customer code format correct (SP-CUST-NNN)", f"double prefix detected: {code} — server bug")
        else:
            _fail("Customer code format correct (SP-CUST-NNN)", f"got: {code}")
        if account and "-" in account:
            _pass(f"Account number assigned: {account}")
        else:
            _fail("Account number assigned (KHI-R001 format)", f"got: {account}")

    # ── Customer types ─────────────────────────────────────────────────────
    for ctype in ["RETAIL", "DIRECT", "WHOLESALE"]:
        rc = POST("/api/customers", {
            "name": f"Test {ctype} {ts}",
            "customerType": ctype,
            "city": "Karachi"
        }, token=tok)
        if rc.status_code == 201:
            _pass(f"Customer type {ctype} accepted")
        else:
            _fail(f"Customer type {ctype} accepted", f"got {rc.status_code}: {rc.text[:100]}")

    # ── Update ─────────────────────────────────────────────────────────────
    if cust_id:
        r4 = PUT(f"/api/customers/{cust_id}", {
            "name": f"Updated Customer {ts}",
            "customerType": "DIRECT",
            "city": "Karachi",
            "creditLimit": 100000,
            "paymentTermsDays": 15
        }, token=tok)
        assert_status(r4, 200, f"PUT /api/customers/{cust_id} updates customer")

    # ── Deactivate ─────────────────────────────────────────────────────────
    if cust_id:
        r5 = DELETE(f"/api/customers/{cust_id}", token=tok)
        if r5.status_code in (200, 204):
            _pass("DELETE /api/customers/:id deactivates customer")
        else:
            _fail("DELETE /api/customers/:id deactivates customer", f"got {r5.status_code}")

    # ── Invalid customer type ──────────────────────────────────────────────
    rb = POST("/api/customers", {
        "name": f"Bad Type {ts}",
        "customerType": "INVALID_TYPE",
        "city": "Karachi"
    }, token=tok)
    if rb.status_code in (400, 422):
        _pass("Invalid customerType rejected with 400/422")
    else:
        _fail("Invalid customerType rejected", f"got {rb.status_code}")

    # ── Export ─────────────────────────────────────────────────────────────
    r6 = GET("/api/customers/export", token=tok)
    if r6.status_code == 200:
        _pass("GET /api/customers/export returns 200")
    else:
        _fail("GET /api/customers/export", f"got {r6.status_code}")

    logout(tok)
    print_summary()
    return summary()

if __name__ == "__main__":
    run()
