#!/usr/bin/env python3
"""
Spicetopia ERP — Comprehensive Test Suite
==========================================
Tests all modules end-to-end against a running server on PORT 3001.

Usage:
    python3 test_erp.py                          # run all tests (12 modules)
    python3 test_erp.py --module sales           # run specific module
    python3 test_erp.py --module integration     # run cross-module flow only
    python3 test_erp.py --report-only            # print recommendations only

Modules: connectivity, dashboard, reference, inventory, sales, bills,
         production, prices, reports, field, payroll, orders, integration

Reports saved to: test_reports/  (timestamped .txt + .json, plus latest.txt / latest.json)

Requirements:
    pip install requests  --break-system-packages
    Server must be running:  python3 server.py
"""

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any, Optional

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package not installed.")
    print("Run:  pip install requests --break-system-packages")
    sys.exit(1)

# ── Config ─────────────────────────────────────────────────────────────────────
BASE_URL  = "http://localhost:3001"
USERNAME  = "admin"
PASSWORD  = "admin123"
TODAY     = date.today().isoformat()
TOMORROW  = (date.today() + timedelta(days=30)).isoformat()

# ── Global state ───────────────────────────────────────────────────────────────
_token: Optional[str] = None
_results: list[dict]  = []

# ── Colour output ──────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def _pass(msg: str):
    print(f"  {GREEN}✓ PASS{RESET}  {msg}")
    _results.append({"status": "PASS", "test": msg})

def _fail(msg: str, detail: str = ""):
    detail_str = f"  → {detail}" if detail else ""
    print(f"  {RED}✗ FAIL{RESET}  {msg}{detail_str}")
    _results.append({"status": "FAIL", "test": msg, "detail": detail})

def _skip(msg: str, reason: str = ""):
    print(f"  {YELLOW}⊘ SKIP{RESET}  {msg}" + (f"  [{reason}]" if reason else ""))
    _results.append({"status": "SKIP", "test": msg, "reason": reason})

def _section(title: str):
    bar = "─" * 60
    print(f"\n{BOLD}{CYAN}{bar}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{bar}{RESET}")

# ── HTTP helpers ───────────────────────────────────────────────────────────────
def _headers() -> dict:
    return {"Authorization": f"Bearer {_token}", "Content-Type": "application/json"}

def GET(path: str, params: dict = None) -> Any:
    r = requests.get(f"{BASE_URL}{path}", headers=_headers(), params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def POST(path: str, body: dict = None) -> Any:
    r = requests.post(f"{BASE_URL}{path}", headers=_headers(), json=body or {}, timeout=10)
    r.raise_for_status()
    return r.json()

def PUT(path: str, body: dict = None) -> Any:
    r = requests.put(f"{BASE_URL}{path}", headers=_headers(), json=body or {}, timeout=10)
    r.raise_for_status()
    return r.json()

def DELETE(path: str) -> Any:
    r = requests.delete(f"{BASE_URL}{path}", headers=_headers(), timeout=10)
    r.raise_for_status()
    return r.json()

def try_post(path: str, body: dict) -> tuple[bool, Any]:
    """POST that returns (success, data) without raising."""
    try:
        r = requests.post(f"{BASE_URL}{path}", headers=_headers(), json=body, timeout=10)
        return r.ok, r.json() if r.content else {}
    except Exception as e:
        return False, str(e)

def try_delete(path: str) -> tuple[bool, Any]:
    try:
        r = requests.delete(f"{BASE_URL}{path}", headers=_headers(), timeout=10)
        return r.ok, r.json() if r.content else {}
    except Exception as e:
        return False, str(e)

# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 0 — Server Connectivity & Authentication
# ═══════════════════════════════════════════════════════════════════════════════
def test_connectivity():
    _section("MODULE 0 — Server Connectivity & Authentication")
    global _token

    # 0.1 Server is up
    try:
        r = requests.get(f"{BASE_URL}/", timeout=5)
        _pass("Server is reachable on port 3001")
    except Exception as e:
        _fail("Server is reachable on port 3001", str(e))
        print(f"\n{RED}FATAL: Server not running. Start it with: python3 server.py{RESET}")
        sys.exit(1)

    # 0.2 Login
    try:
        r = requests.post(f"{BASE_URL}/api/auth/login",
                          json={"username": USERNAME, "password": PASSWORD}, timeout=10)
        data = r.json()
        if r.ok and data.get("token"):
            _token = data["token"]
            _pass(f"Login as '{USERNAME}' returns auth token")
        else:
            _fail("Login returns auth token", data.get("error", "no token"))
            sys.exit(1)
    except Exception as e:
        _fail("Login endpoint responds", str(e))
        sys.exit(1)

    # 0.3 Authenticated /api/auth/me
    try:
        me = GET("/api/auth/me")
        if me.get("username") == USERNAME:
            _pass("/api/auth/me returns current user")
        else:
            _fail("/api/auth/me returns current user", str(me))
    except Exception as e:
        _fail("/api/auth/me returns current user", str(e))

    # 0.4 Reject bad token — /api/auth/me returns {authenticated:false} for unknown tokens
    try:
        r = requests.get(f"{BASE_URL}/api/auth/me",
                         headers={"Authorization": "Bearer BADTOKEN"}, timeout=5)
        data = r.json() if r.content else {}
        if r.status_code == 401:
            _pass("Invalid token correctly rejected (401)")
        elif r.ok and data.get("authenticated") is False:
            _pass("Invalid token correctly rejected (authenticated:false)")
        else:
            _fail("Invalid token correctly rejected",
                  f"status={r.status_code} body={data}")
    except Exception as e:
        _fail("Invalid token correctly rejected", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 1 — Dashboard
# ═══════════════════════════════════════════════════════════════════════════════
def test_dashboard():
    _section("MODULE 1 — Dashboard")

    try:
        dash = GET("/api/dashboard")
        # Actual field names returned by server (camelCase nested)
        required_keys = ["salesToday", "salesMonth", "cashPosition", "ar", "ap",
                         "lowStockAlerts", "finishedGoods"]
        missing = [k for k in required_keys if k not in dash]
        if not missing:
            _pass("Dashboard returns all required KPI fields")
        else:
            _fail("Dashboard returns all required KPI fields", f"missing: {missing}")
    except Exception as e:
        _fail("Dashboard endpoint responds", str(e))

    # Numeric sanity on nested AR/AP
    try:
        dash = GET("/api/dashboard")
        ar = float((dash.get("ar") or {}).get("outstanding", -1))
        ap = float((dash.get("ap") or {}).get("outstanding", -1))
        if ar >= 0 and ap >= 0:
            _pass(f"Dashboard AR={ar:.2f}  AP={ap:.2f}  (non-negative)")
        else:
            _fail("Dashboard AR/AP are non-negative numbers", f"AR={ar} AP={ap}")
    except Exception as e:
        _fail("Dashboard AR/AP values are numeric", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 2 — Reference Data (Products, Ingredients, Suppliers, Customers)
# ═══════════════════════════════════════════════════════════════════════════════
def test_reference_data():
    _section("MODULE 2 — Reference Data")
    global _test_customer_code, _test_supplier_id, _test_product_code, _test_ingredient_id
    global _test_variant_id, _test_pack_size, _test_price_type_id

    # Products list
    try:
        products = GET("/api/products")
        if isinstance(products, list):
            _pass(f"Products list returns {len(products)} product(s)")
            _test_product_code = products[0]["code"] if products else None
        else:
            _fail("Products list returns a list", str(products))
            _test_product_code = None
    except Exception as e:
        _fail("Products endpoint responds", str(e))
        _test_product_code = None

    # Look up a variant_id and price_type_id for use in later tests
    if _test_product_code:
        try:
            prices = GET(f"/api/products/{_test_product_code}/prices")
            if prices:
                _test_variant_id    = prices[0].get("product_variant_id")
                _test_pack_size     = prices[0].get("pack_size")
                _test_price_type_id = prices[0].get("price_type_id")
                _pass(f"Resolved variant_id={_test_variant_id}  pack_size={_test_pack_size}  "
                      f"price_type_id={_test_price_type_id} for '{_test_product_code}'")
            else:
                _skip("Variant/price-type lookup", "No active prices for test product")
        except Exception as e:
            _skip("Variant/price-type lookup", str(e))

    # Ingredients list
    try:
        ingredients = GET("/api/ingredients")
        if isinstance(ingredients, list):
            _pass(f"Ingredients list returns {len(ingredients)} ingredient(s)")
            _test_ingredient_id = ingredients[0]["id"] if ingredients else None
        else:
            _fail("Ingredients list returns a list", str(ingredients))
            _test_ingredient_id = None
    except Exception as e:
        _fail("Ingredients endpoint responds", str(e))
        _test_ingredient_id = None

    # Customers — create new
    try:
        cust_code = f"TEST-{int(time.time()) % 100000}"
        r = POST("/api/customers", {
            "code": cust_code, "name": "Test Customer (Auto)",
            "category": "Retailer", "credit_limit": 50000,
            "payment_terms": 30, "address": "123 Test St",
            "phone": "0300-0000000", "email": "test@spicetopia.test"
        })
        if r.get("code") or r.get("customer_code"):
            _test_customer_code = r.get("code") or r.get("customer_code") or cust_code
            _pass(f"Create new customer → code: {_test_customer_code}")
        else:
            _test_customer_code = cust_code
            _pass(f"Create customer endpoint responded (code: {cust_code})")
    except Exception as e:
        _fail("Create new customer", str(e))
        _test_customer_code = None

    # Customers — edit
    if _test_customer_code:
        try:
            r = PUT(f"/api/customers/{_test_customer_code}", {
                "name": "Test Customer (Edited)", "category": "Wholesaler",
                "credit_limit": 100000, "payment_terms": 45,
                "address": "456 Edited Ave", "phone": "0300-1111111",
                "email": "edited@spicetopia.test"
            })
            _pass(f"Edit customer {_test_customer_code}")
        except Exception as e:
            _fail(f"Edit customer {_test_customer_code}", str(e))

    # Suppliers — create
    try:
        r = POST("/api/suppliers", {
            "name": "Test Supplier (Auto)", "contact": "Test Contact",
            "phone": "0300-9999999", "email": "supplier@spicetopia.test",
            "address": "789 Supplier Rd", "payment_terms": 30
        })
        _test_supplier_id = r.get("id") or r.get("supplier_id")
        _pass(f"Create new supplier → id: {_test_supplier_id}")
    except Exception as e:
        _fail("Create new supplier", str(e))
        _test_supplier_id = None

    # Customers list
    try:
        customers = GET("/api/customers")
        if isinstance(customers, list) and len(customers) > 0:
            _pass(f"Customers list returns {len(customers)} customer(s)")
        else:
            _fail("Customers list returns non-empty list", str(customers))
    except Exception as e:
        _fail("Customers list endpoint responds", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 3 — Inventory
# ═══════════════════════════════════════════════════════════════════════════════
def test_inventory():
    _section("MODULE 3 — Inventory")

    # Inventory ledger
    try:
        ledger = GET("/api/inventory/ledger")
        if isinstance(ledger, list):
            _pass(f"Inventory ledger returns {len(ledger)} movement(s)")
        else:
            _fail("Inventory ledger returns a list", str(ledger))
    except Exception as e:
        _fail("Inventory ledger endpoint responds", str(e))

    # Inventory adjustment (add stock)
    # Server expects: {ingredientId, qtyGrams, notes}
    if _test_ingredient_id:
        try:
            r = POST("/api/inventory/adjustment", {
                "ingredientId": _test_ingredient_id,
                "qtyGrams": 100000,   # 100 kg in grams
                "notes": "Test stock addition (automated test)"
            })
            _pass(f"Inventory adjustment (add 100kg to ingredient {_test_ingredient_id})")
        except Exception as e:
            _fail("Inventory adjustment (stock addition)", str(e))

        # Negative inventory guard — try to remove more than exists
        try:
            ledger = GET("/api/inventory/ledger")
            current = sum(
                m.get("qty_grams", 0) for m in ledger
                if m.get("ingredient_id") == _test_ingredient_id
            )
            over_qty = current + 99999999
            r = requests.post(
                f"{BASE_URL}/api/inventory/adjustment",
                headers=_headers(),
                json={"ingredientId": _test_ingredient_id, "qtyGrams": -over_qty,
                      "notes": "Test — should fail"},
                timeout=10
            )
            if not r.ok:
                _pass("Negative inventory guard: server rejects stock below zero")
            else:
                _fail("Negative inventory guard: server rejects stock below zero",
                      "Server accepted an adjustment that would push inventory negative")
        except Exception as e:
            _skip("Negative inventory guard", f"Could not calculate current stock: {e}")
    else:
        _skip("Inventory adjustment test", "No ingredient available")


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 4 — Sales & Invoicing
# ═══════════════════════════════════════════════════════════════════════════════
_test_invoice_id   = None
_test_invoice_no   = None

def test_sales():
    _section("MODULE 4 — Sales & Invoicing")
    global _test_invoice_id, _test_invoice_no

    if not _test_customer_code or not _test_product_code:
        _skip("All Sales tests", "Missing customer or product from earlier setup")
        return

    # 4.1 Multi-line sale
    # Server expects: {custCode, saleDate, notes, lines:[{productCode, packSize, qty, unitPrice}]}
    # Stock must exist — if no finished goods, skip gracefully
    if not _test_variant_id or not _test_pack_size:
        _skip("Multi-line sale", "No variant/pack_size resolved from reference data")
        return

    # Check finished goods stock before attempting sale (sales runs before production in suite)
    try:
        dash = GET("/api/dashboard")
        fg   = dash.get("finishedGoods", [])
        avail = next(
            (item["units"] for item in fg
             if item.get("skuCode", "").startswith(_test_product_code or "")
             and item.get("packSize") == _test_pack_size),
            0
        )
        if avail < 1:
            _skip("Multi-line sale",
                  f"No finished goods stock for {_test_product_code}/{_test_pack_size} "
                  f"— run production module first or re-run full suite")
            return
    except Exception as e:
        _skip("Multi-line sale", f"Could not check stock: {e}")
        return

    try:
        payload = {
            "custCode":  _test_customer_code,
            "saleDate":  TODAY,
            "notes":     "Automated test invoice",
            "lines": [
                {"productCode": _test_product_code, "packSize": _test_pack_size,
                 "qty": 1, "unitPrice": 200},
            ],
        }
        ok, r = try_post("/api/sales/multi", payload)
        if ok:
            _test_invoice_id = r.get("invoiceId") or r.get("invoice_id")
            _test_invoice_no = r.get("invoiceNumber") or r.get("invoice_number")
            if _test_invoice_id:
                _pass(f"Multi-line sale creates invoice #{_test_invoice_no} (id={_test_invoice_id})")
            else:
                _fail("Multi-line sale returns invoiceId", str(r))
        else:
            detail = r.get("error", str(r)) if isinstance(r, dict) else str(r)
            _fail("Multi-line sale (POST /api/sales/multi)", detail)
            return
    except Exception as e:
        _fail("Multi-line sale (POST /api/sales/multi)", str(e))
        return

    # 4.2 Invoice detail & GST
    if _test_invoice_id:
        try:
            invs = GET("/api/invoices")
            inv = next((i for i in invs if i["id"] == _test_invoice_id), None)
            if inv:
                balance = float(inv.get("balance", 0))
                line_total = 2*500 + 3*300  # = 1900
                expected = round(line_total * 1.18, 2)
                if abs(balance - expected) < 0.02:
                    _pass(f"Invoice balance includes 18% GST correctly ({balance:.2f} = {expected:.2f})")
                else:
                    _fail("Invoice GST calculation", f"Expected ~{expected} got {balance}")
            else:
                _fail("Invoice appears in invoice list after creation")
        except Exception as e:
            _fail("Invoice detail & GST check", str(e))

    # 4.3 Add line item to existing invoice
    if _test_invoice_id:
        try:
            r = POST(f"/api/invoices/{_test_invoice_id}/items", {
                "product_code": _test_product_code,
                "pack_size": "250g", "qty": 1, "unit_price": 150,
                "line_total": 150
            })
            _pass(f"Add line item to invoice {_test_invoice_id}")
        except Exception as e:
            _fail(f"Add line item to invoice {_test_invoice_id}", str(e))

    # 4.4 AR Aging report
    try:
        aging = GET("/api/ar/aging")
        if isinstance(aging, list):
            _pass(f"AR aging report returns {len(aging)} customer row(s)")
        else:
            _fail("AR aging report returns a list", str(aging))
    except Exception as e:
        _fail("AR aging report responds", str(e))

    # 4.5 Invoice payment (direct pay)
    if _test_invoice_id:
        try:
            r = POST(f"/api/invoices/{_test_invoice_id}/pay", {
                "amount": 500,
                "payment_date": TODAY,
                "payment_mode": "Bank Transfer",
                "payment_ref": "TEST-PAY-001",
                "notes": "Partial payment — automated test"
            })
            _pass(f"Record payment against invoice {_test_invoice_id}")

            # Verify balance reduced
            invs = GET("/api/invoices")
            inv = next((i for i in invs if i["id"] == _test_invoice_id), None)
            if inv:
                bal = float(inv.get("balance", 9999))
                if bal < (2*500 + 3*300 + 150) * 1.18 - 0.01:
                    _pass(f"Invoice balance reduced after payment → {bal:.2f}")
                else:
                    _fail("Invoice balance reduced after payment",
                          f"Balance still {bal:.2f}")
        except Exception as e:
            _fail(f"Record payment against invoice {_test_invoice_id}", str(e))

    # 4.6 Customer payments list
    try:
        payments = GET("/api/customer-payments")
        if isinstance(payments, list):
            _pass(f"Customer payments list returns {len(payments)} record(s)")
        else:
            _fail("Customer payments list returns a list", str(payments))
    except Exception as e:
        _fail("Customer payments list endpoint responds", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 5 — Bills & Accounts Payable
# ═══════════════════════════════════════════════════════════════════════════════
_test_bill_id = None

def test_bills():
    _section("MODULE 5 — Bills & Accounts Payable")
    global _test_bill_id

    if not _test_supplier_id or not _test_ingredient_id:
        _skip("All Bills tests", "Missing supplier or ingredient from earlier setup")
        return

    # 5.1 Create multi-ingredient bill
    # Server expects: {supplierId, billDate, dueDate, notes, items:[{ingredientId, quantityKg, unitCostKg}]}
    try:
        r = POST("/api/bills", {
            "supplierId": _test_supplier_id,
            "billDate":   TODAY,
            "dueDate":    TOMORROW,
            "items": [
                {"ingredientId": _test_ingredient_id, "quantityKg": 50, "unitCostKg": 40},
                {"ingredientId": _test_ingredient_id, "quantityKg": 25, "unitCostKg": 40},
            ],
            "notes": "Automated test bill"
        })
        _test_bill_id = r.get("billId") or r.get("id") or r.get("bill_id")
        if _test_bill_id:
            _pass(f"Create multi-ingredient bill → id: {_test_bill_id}")
        else:
            _fail("Create bill returns id", str(r))
    except Exception as e:
        _fail("Create multi-ingredient bill (POST /api/bills)", str(e))
        return

    # 5.2 Bill appears in list
    try:
        bills = GET("/api/bills")
        found = any(b["id"] == _test_bill_id for b in bills)
        if found:
            _pass(f"New bill appears in bills list")
        else:
            _fail("New bill appears in bills list", f"bill_id={_test_bill_id} not found")
    except Exception as e:
        _fail("Bills list endpoint responds", str(e))

    # 5.3 Pay bill directly
    if _test_bill_id:
        try:
            r = POST(f"/api/bills/{_test_bill_id}/pay", {
                "amount": 1000,
                "payment_date": TODAY,
                "payment_mode": "Cheque",
                "payment_ref":  "CHQ-TEST-001",
                "notes": "Partial AP payment — automated test"
            })
            _pass(f"Record payment against bill {_test_bill_id}")

            # Verify AP balance reduced
            bills = GET("/api/bills")
            bill = next((b for b in bills if b["id"] == _test_bill_id), None)
            if bill:
                bal = float(bill.get("balance", 9999))
                if bal < 3000 - 0.01:
                    _pass(f"Bill balance reduced after payment → {bal:.2f}")
                else:
                    _fail("Bill balance reduced after payment", f"Balance still {bal:.2f}")
        except Exception as e:
            _fail(f"Record payment against bill {_test_bill_id}", str(e))

    # 5.4 AP Aging
    try:
        aging = GET("/api/ap/aging")
        if isinstance(aging, list):
            _pass(f"AP aging report returns {len(aging)} supplier row(s)")
        else:
            _fail("AP aging report returns a list", str(aging))
    except Exception as e:
        _fail("AP aging report responds", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 6 — Production (Work Orders → Batches)
# ═══════════════════════════════════════════════════════════════════════════════
_test_wo_id = None

def test_production():
    _section("MODULE 6 — Production (Work Orders → Batches)")
    global _test_wo_id

    # 6.1 Create Work Order
    # Server expects: {productVariantId, qtyUnits, targetDate, notes}
    if not _test_variant_id:
        _skip("All Production tests", "No variant_id resolved from reference data")
        return

    try:
        r = POST("/api/work-orders", {
            "productVariantId": _test_variant_id,
            "qtyUnits":         10,
            "targetDate":       TODAY,
            "notes":            "Automated test work order"
        })
        _test_wo_id = r.get("id") or r.get("work_order_id")
        if _test_wo_id:
            _pass(f"Create work order → id: {_test_wo_id}")
        else:
            _fail("Create work order returns id", str(r))
    except Exception as e:
        _fail("Create work order (POST /api/work-orders)", str(e))
        return

    # 6.2 Work order appears in list
    try:
        wos = GET("/api/work-orders")
        found = any(wo["id"] == _test_wo_id for wo in wos)
        if found:
            _pass("Work order appears in work orders list")
        else:
            _fail("Work order appears in list", f"id={_test_wo_id} not found")
    except Exception as e:
        _fail("Work orders list responds", str(e))

    # 6.3 Update WO status to in_progress (server accepts lowercase: planned, in_progress, cancelled)
    try:
        r = POST(f"/api/work-orders/{_test_wo_id}/status", {"status": "in_progress"})
        _pass(f"Update work order status → in_progress")
    except Exception as e:
        _fail(f"Update work order status (IN_PROGRESS)", str(e))

    # 6.4 Convert WO → Production Batch
    try:
        r = POST(f"/api/work-orders/{_test_wo_id}/convert", {
            "actual_quantity": 9,
            "batch_date": TODAY,
            "notes": "Automated test batch"
        })
        batch_id = r.get("id") or r.get("batch_id") or r.get("production_id")
        if batch_id:
            _pass(f"Convert work order → production batch (id={batch_id})")
        else:
            _pass(f"Convert work order → production batch (response: {r})")
    except Exception as e:
        _fail(f"Convert work order to production batch", str(e))

    # 6.5 Production batch list
    try:
        batches = GET("/api/production")
        if isinstance(batches, list):
            _pass(f"Production batches list returns {len(batches)} batch(es)")
        else:
            _fail("Production batches list returns a list", str(batches))
    except Exception as e:
        _fail("Production batches list responds", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 7 — Prices
# ═══════════════════════════════════════════════════════════════════════════════
def test_prices():
    _section("MODULE 7 — Prices")

    if not _test_product_code:
        _skip("All Prices tests", "No product code available")
        return

    # Get current prices
    try:
        prices = GET("/api/prices")
        if isinstance(prices, list):
            _pass(f"Prices list returns {len(prices)} price record(s)")
        else:
            _fail("Prices list returns a list", str(prices))
    except Exception as e:
        _fail("Prices list endpoint responds", str(e))

    # Create new price
    # Server expects: {productVariantId, priceTypeId, price, effectiveFrom}
    if not _test_variant_id or not _test_price_type_id:
        _skip("Create new price", "No variant_id or price_type_id resolved from reference data")
        return

    try:
        r = POST("/api/prices", {
            "productVariantId": _test_variant_id,
            "priceTypeId":      _test_price_type_id,
            "price":            750,
            "effectiveFrom":    TODAY
        })
        price_id = r.get("id") or r.get("price_id")
        if price_id:
            _pass(f"Create new price → id: {price_id}")

            # One active price per variant+price_type
            prices = GET("/api/prices")
            active_for_variant = [
                p for p in prices
                if p.get("product_code") == _test_product_code
                and p.get("pack_size") == _test_pack_size
                and p.get("active_flag") in (1, True, "1")
                and p.get("price_type_code") == r.get("price_type_code")
            ]
            if len(active_for_variant) <= 1:
                _pass("Only one active price per variant+price_type (no duplicates)")
            else:
                _fail("Only one active price per variant+price_type",
                      f"Found {len(active_for_variant)} active prices")
        else:
            _fail("Create price returns id", str(r))
    except Exception as e:
        _fail("Create new price (POST /api/prices)", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 8 — Reports
# ═══════════════════════════════════════════════════════════════════════════════
def test_reports():
    _section("MODULE 8 — Reports")

    # Margins report
    try:
        r = GET("/api/reports/margins")
        if isinstance(r, list):
            _pass(f"Margins report returns {len(r)} product row(s)")
        else:
            _fail("Margins report returns a list", str(r))
    except Exception as e:
        _fail("Margins report responds", str(e))

    # Rep performance — returns {period, reps:[...]} not a plain list
    try:
        r = GET("/api/reports/rep-performance")
        reps = r.get("reps") if isinstance(r, dict) else r
        if isinstance(reps, list):
            _pass(f"Rep performance report returns {len(reps)} rep row(s)")
        else:
            _fail("Rep performance report returns expected structure", str(r))
    except Exception as e:
        _fail("Rep performance report responds", str(e))

    # Audit log
    try:
        r = GET("/api/audit")
        if isinstance(r, list):
            _pass(f"Audit log returns {len(r)} entries")
        else:
            _fail("Audit log returns a list", str(r))
    except Exception as e:
        _fail("Audit log responds", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 9 — Field Operations (Reps, Routes, Beat Visits, Field Orders)
# ═══════════════════════════════════════════════════════════════════════════════
def test_field_ops():
    _section("MODULE 9 — Field Operations")

    # Zones
    try:
        zones = GET("/api/zones")
        if isinstance(zones, list):
            _pass(f"Zones list returns {len(zones)} zone(s)")
        else:
            _fail("Zones list returns a list", str(zones))
    except Exception as e:
        _fail("Zones list endpoint responds", str(e))

    # Routes
    try:
        routes = GET("/api/routes")
        if isinstance(routes, list):
            _pass(f"Routes list returns {len(routes)} route(s)")
        else:
            _fail("Routes list returns a list", str(routes))
    except Exception as e:
        _fail("Routes list endpoint responds", str(e))

    # Reps
    try:
        reps = GET("/api/reps")
        if isinstance(reps, list):
            _pass(f"Reps list returns {len(reps)} rep(s)")
        else:
            _fail("Reps list returns a list", str(reps))
    except Exception as e:
        _fail("Reps list endpoint responds", str(e))

    # Field orders
    try:
        orders = GET("/api/field-orders")
        if isinstance(orders, list):
            _pass(f"Field orders list returns {len(orders)} order(s)")
        else:
            _fail("Field orders list returns a list", str(orders))
    except Exception as e:
        _fail("Field orders list endpoint responds", str(e))

    # Beat visits
    try:
        visits = GET("/api/beat-visits")
        if isinstance(visits, list):
            _pass(f"Beat visits list returns {len(visits)} visit(s)")
        else:
            _fail("Beat visits list returns a list", str(visits))
    except Exception as e:
        _fail("Beat visits list endpoint responds", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 10 — Payroll
# ═══════════════════════════════════════════════════════════════════════════════
def test_payroll():
    _section("MODULE 10 — Payroll")

    try:
        payroll = GET("/api/payroll")
        if isinstance(payroll, list):
            _pass(f"Payroll list returns {len(payroll)} record(s)")
        else:
            _fail("Payroll list returns a list", str(payroll))
    except Exception as e:
        _fail("Payroll list endpoint responds", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 11 — Customer Orders
# ═══════════════════════════════════════════════════════════════════════════════
def test_customer_orders():
    _section("MODULE 11 — Customer Orders")

    # 11.1  List endpoint returns a list
    try:
        orders = GET("/api/customer-orders")
        if isinstance(orders, list):
            _pass(f"GET /api/customer-orders returns list ({len(orders)} order(s))")
        else:
            _fail("GET /api/customer-orders returns a list", str(orders))
            return
    except Exception as e:
        _fail("GET /api/customer-orders endpoint responds", str(e))
        return

    # 11.2  Status filter works
    for status in ("draft", "confirmed", "invoiced", "cancelled"):
        try:
            filtered = GET("/api/customer-orders", params={"status": status})
            if isinstance(filtered, list):
                _pass(f"Status filter ?status={status} returns list ({len(filtered)} order(s))")
            else:
                _fail(f"Status filter ?status={status} returns a list", str(filtered))
        except Exception as e:
            _fail(f"Status filter ?status={status} responds", str(e))

    # 11.3  Need a valid customer & product to create an order
    if not _test_customer_code or not _test_product_code:
        _skip("Create order — need customer + product from reference data",
              "reference data not available")
        return

    # 11.4  Create a draft order
    # Server expects lines with packSize: {productCode, packSize, qty, unitPrice}
    if not _test_pack_size:
        _skip("Create order and subsequent order tests", "No pack_size resolved from reference data")
        return

    new_order_id = None
    try:
        payload = {
            "custCode":     _test_customer_code,
            "orderDate":    TODAY,
            "requiredDate": TOMORROW,
            "notes":        "Automated test order",
            "lines": [
                {"productCode": _test_product_code, "packSize": _test_pack_size,
                 "qty": 2, "unitPrice": 100.0}
            ],
        }
        res = POST("/api/customer-orders", payload)
        # Server returns {orderId, orderNumber}
        new_order_id = res.get("orderId") or res.get("order_id")
        if new_order_id:
            _pass(f"Create draft order → id={new_order_id}  number={res.get('orderNumber','?')}")
        else:
            _fail("Create draft order returns ok+order_id", str(res))
    except Exception as e:
        _fail("POST /api/customer-orders responds", str(e))

    if not new_order_id:
        _skip("Subsequent order tests — order creation failed", "skipping dependents")
        return

    # 11.5  GET detail for the new order — server returns 'id' not 'order_id'
    try:
        detail = GET(f"/api/customer-orders/{new_order_id}")
        returned_id = detail.get("id") or detail.get("order_id")
        if returned_id == new_order_id:
            _pass(f"GET /api/customer-orders/{new_order_id} returns correct order")
        else:
            _fail(f"GET /api/customer-orders/{new_order_id} returns correct order", str(detail))
    except Exception as e:
        _fail(f"GET /api/customer-orders/{new_order_id} responds", str(e))

    # 11.6  Order starts in draft status
    try:
        detail = GET(f"/api/customer-orders/{new_order_id}")
        status = detail.get("status")
        if status == "draft":
            _pass("New order status is 'draft'")
        else:
            _fail("New order status is 'draft'", f"got '{status}'")
    except Exception as e:
        _fail("New order status check", str(e))

    # 11.7  Items list is non-empty and has correct product
    try:
        detail = GET(f"/api/customer-orders/{new_order_id}")
        items = detail.get("items", [])
        if items and items[0].get("product_code") == _test_product_code:
            _pass(f"Order has {len(items)} line item(s) with correct product code")
        else:
            _fail("Order items populated correctly", str(items))
    except Exception as e:
        _fail("Order items check", str(e))

    # 11.8  Cannot invoice a draft order (must confirm first)
    try:
        # Use actual item id from the order detail fetched above
        _draft_items = GET(f"/api/customer-orders/{new_order_id}").get("items", [])
        _draft_item_id = (_draft_items[0].get("item_id") or _draft_items[0].get("id")) if _draft_items else 1
        ok, res = try_post(f"/api/customer-orders/{new_order_id}/invoice",
                           {"invoiceDate": TODAY, "lines": [
                               {"item_id": _draft_item_id, "qty": 1}
                           ]})
        if not ok:
            _pass("Cannot invoice a draft order (correctly rejected)")
        else:
            _fail("Draft order invoice correctly rejected", f"unexpectedly accepted: {res}")
    except Exception as e:
        _fail("Draft invoice rejection check", str(e))

    # 11.9  Confirm the order — server returns full order object (status=confirmed) not {ok:true}
    try:
        res = POST(f"/api/customer-orders/{new_order_id}/confirm", {})
        if res.get("ok") or res.get("status") == "confirmed" or res.get("id") == new_order_id:
            _pass(f"Confirm order {new_order_id} → status updated")
        else:
            _fail(f"Confirm order {new_order_id}", str(res))
    except Exception as e:
        _fail(f"POST /api/customer-orders/{new_order_id}/confirm responds", str(e))

    # 11.10  Status is now confirmed
    try:
        detail = GET(f"/api/customer-orders/{new_order_id}")
        status = detail.get("status")
        if status == "confirmed":
            _pass("Order status is 'confirmed' after confirm call")
        else:
            _fail("Order status is 'confirmed' after confirm", f"got '{status}'")
    except Exception as e:
        _fail("Order status after confirm check", str(e))

    # 11.11  Cannot confirm again (idempotency / guard)
    try:
        ok, res = try_post(f"/api/customer-orders/{new_order_id}/confirm", {})
        if not ok:
            _pass("Re-confirming already-confirmed order is rejected")
        else:
            _fail("Re-confirm guard works", f"unexpectedly accepted: {res}")
    except Exception as e:
        _fail("Re-confirm guard check", str(e))

    # 11.12  Cancel the confirmed order (no invoices yet — should succeed)
    try:
        res = POST(f"/api/customer-orders/{new_order_id}/cancel", {})
        if res.get("ok"):
            _pass(f"Cancel confirmed order {new_order_id} → ok")
        else:
            _fail(f"Cancel confirmed order {new_order_id}", str(res))
    except Exception as e:
        _fail(f"POST /api/customer-orders/{new_order_id}/cancel responds", str(e))

    # 11.13  Status is now cancelled
    try:
        detail = GET(f"/api/customer-orders/{new_order_id}")
        status = detail.get("status")
        if status == "cancelled":
            _pass("Order status is 'cancelled' after cancel call")
        else:
            _fail("Order status is 'cancelled' after cancel", f"got '{status}'")
    except Exception as e:
        _fail("Order status after cancel check", str(e))

    # 11.14  Cannot create WO on cancelled order — items use 'id' not 'item_id'
    try:
        detail = GET(f"/api/customer-orders/{new_order_id}")
        items = detail.get("items", [])
        if items:
            item_id = items[0].get("item_id") or items[0].get("id")
            ok, res = try_post(
                f"/api/customer-orders/{new_order_id}/items/{item_id}/work-order",
                {"targetDate": TOMORROW}
            )
            if not ok:
                _pass("Cannot create WO on a cancelled order (correctly rejected)")
            else:
                _fail("WO creation on cancelled order is rejected", f"unexpectedly accepted: {res}")
        else:
            _skip("WO-on-cancelled-order check — no items found", "skipping")
    except Exception as e:
        _fail("WO creation on cancelled order check", str(e))

    # 11.15  Duplicate order — empty lines rejected
    try:
        ok, res = try_post("/api/customer-orders", {
            "custCode":  _test_customer_code,
            "orderDate": TODAY,
            "lines":     [],
        })
        if not ok:
            _pass("Order with empty lines is rejected")
        else:
            _fail("Order with empty lines is rejected", f"unexpectedly accepted: {res}")
    except Exception as e:
        _fail("Empty-lines order rejection check", str(e))

    # 11.16  Order with missing customer rejected
    try:
        ok, res = try_post("/api/customer-orders", {
            "custCode":  "NONEXISTENT_CUST_99",
            "orderDate": TODAY,
            "lines": [{"productCode": _test_product_code, "qty": 1, "unitPrice": 50.0}],
        })
        if not ok:
            _pass("Order with non-existent customer is rejected")
        else:
            _fail("Order with non-existent customer is rejected", f"unexpectedly accepted: {res}")
    except Exception as e:
        _fail("Non-existent customer order rejection check", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE 12 — End-to-End Integration Flow
#
# Walks the full business lifecycle in sequence, asserting that each step's
# data is correctly visible in the next module:
#
#   Customer Order → Confirm → Work Order (visible in Production)
#   → Production Batch (finished stock increases)
#   → Invoice from Order (consumes stock, AR rises)
#   → Payment (AR balance drops, Dashboard reflects it)
# ═══════════════════════════════════════════════════════════════════════════════
def test_integration_flow():
    _section("MODULE 12 — End-to-End Integration Flow")

    if not _test_customer_code or not _test_product_code or not _test_pack_size:
        _skip("All integration flow tests", "Missing customer, product, or pack_size from reference data")
        return

    # ── Step 1: Place a Customer Order ────────────────────────────────────────
    order_id   = None
    order_items = []
    try:
        res = POST("/api/customer-orders", {
            "custCode":     _test_customer_code,
            "orderDate":    TODAY,
            "requiredDate": TOMORROW,
            "notes":        "Integration flow test order",
            "lines": [
                {"productCode": _test_product_code, "packSize": _test_pack_size,
                 "qty": 3, "unitPrice": 200.0}
            ],
        })
        # Server returns {orderId, orderNumber}
        order_id = res.get("orderId") or res.get("order_id")
        if order_id:
            _pass(f"[Step 1] Customer order created → id={order_id}  number={res.get('orderNumber','?')}")
        else:
            _fail("[Step 1] Customer order created", str(res))
            return
    except Exception as e:
        _fail("[Step 1] POST /api/customer-orders", str(e))
        return

    # ── Step 2: Confirm the order — server returns full order object not {ok:true}
    try:
        res = POST(f"/api/customer-orders/{order_id}/confirm", {})
        if res.get("ok") or res.get("status") == "confirmed" or res.get("id") == order_id:
            _pass(f"[Step 2] Order {order_id} confirmed")
        else:
            _fail(f"[Step 2] Confirm order {order_id}", str(res))
            return
    except Exception as e:
        _fail(f"[Step 2] Confirm order {order_id}", str(e))
        return

    # Fetch order detail to get item_id
    try:
        detail = GET(f"/api/customer-orders/{order_id}")
        order_items = detail.get("items", [])
        if order_items:
            _pass(f"[Step 2] Order detail has {len(order_items)} item(s)")
        else:
            _fail("[Step 2] Order detail has items", str(detail))
            return
    except Exception as e:
        _fail(f"[Step 2] GET /api/customer-orders/{order_id}", str(e))
        return

    # ── Step 3: Create Work Order from the order item ─────────────────────────
    # Items use 'id' not 'item_id'
    item_id = order_items[0].get("item_id") or order_items[0].get("id")
    wo_id   = None
    try:
        res = POST(f"/api/customer-orders/{order_id}/items/{item_id}/work-order",
                   {"targetDate": TOMORROW})
        # Server returns {woId, woNumber, qtyPlanned, feasible, shortfalls}
        wo_id = res.get("woId") or res.get("work_order_id") or res.get("id")
        if wo_id:
            _pass(f"[Step 3] Work order created from order item → wo_id={wo_id}")
        else:
            _fail("[Step 3] Work order created from order item", str(res))
    except Exception as e:
        _fail(f"[Step 3] POST /api/customer-orders/{order_id}/items/{item_id}/work-order", str(e))

    # ── Step 4: Work Order is visible in Production module ────────────────────
    if wo_id:
        try:
            wos = GET("/api/work-orders")
            found = any(wo["id"] == wo_id for wo in wos)
            if found:
                _pass(f"[Step 4] Work order {wo_id} appears in Production module list")
            else:
                _fail(f"[Step 4] Work order {wo_id} visible in Production", "not found in list")
        except Exception as e:
            _fail("[Step 4] Work orders list responds", str(e))

        # Check it carries the customer order reference
        try:
            wos = GET("/api/work-orders")
            wo  = next((w for w in wos if w["id"] == wo_id), None)
            if wo and wo.get("customer_order_id") == order_id:
                _pass(f"[Step 4] Work order links back to customer_order_id={order_id}")
            else:
                _fail("[Step 4] Work order has customer_order_id", str(wo))
        except Exception as e:
            _fail("[Step 4] Work order customer_order_id check", str(e))

        # ── Step 5: Convert WO → Production Batch (adds finished stock) ───────
        stock_before = None
        try:
            batches_before = GET("/api/production")
            # Record count before
            cnt_before = len(batches_before)

            # Set WO to in_progress first (server accepts lowercase only)
            POST(f"/api/work-orders/{wo_id}/status", {"status": "in_progress"})

            res = POST(f"/api/work-orders/{wo_id}/convert", {
                "actual_quantity": 5,
                "batch_date": TODAY,
                "notes": "Integration test batch"
            })
            # Server returns {batchId, totalGrams, qtyUnits, ingredients, bomVersion, woNumber}
            batch_id = res.get("batchId") or res.get("id") or res.get("batch_id") or res.get("production_id")
            if batch_id or res.get("ok"):
                _pass(f"[Step 5] Production batch released from work order (batchId={batch_id})")
            else:
                _fail("[Step 5] Production batch released", str(res))
        except Exception as e:
            _fail(f"[Step 5] Convert work order to batch", str(e))

        # Step 5b: Batch count increased in Production list
        try:
            batches_after = GET("/api/production")
            cnt_after = len(batches_after)
            if cnt_after > cnt_before:
                _pass(f"[Step 5b] Production batch list grew from {cnt_before} → {cnt_after}")
            else:
                _fail("[Step 5b] Production batch count increased",
                      f"before={cnt_before} after={cnt_after}")
        except Exception as e:
            _fail("[Step 5b] Production list after batch", str(e))

    # ── Step 6: Generate Invoice from the Order ───────────────────────────────
    invoice_id    = None
    invoice_total = None

    # Re-fetch order to get current item state
    try:
        detail     = GET(f"/api/customer-orders/{order_id}")
        order_items = detail.get("items", [])
    except Exception as e:
        _fail("[Step 6] Re-fetch order detail before invoicing", str(e))
        return

    try:
        # Invoice qty=2 (partial — order was for 3)
        # Server expects lines with key 'orderItemId' not 'item_id'
        _item_id = order_items[0].get("item_id") or order_items[0].get("id")
        inv_lines = [{"orderItemId": _item_id, "qty": 2}]
        ok, res = try_post(f"/api/customer-orders/{order_id}/invoice",
                           {"invoiceDate": TODAY, "lines": inv_lines})
        if not ok:
            detail = res.get("error", str(res)) if isinstance(res, dict) else str(res)
            _fail(f"[Step 6] POST /api/customer-orders/{order_id}/invoice", detail)
            invoice_id = None
        else:
            # Server returns {invoiceNumber, invoiceId, total, orderStatus, saleIds}
            invoice_id = res.get("invoiceId") or res.get("invoice_id")
            if invoice_id:
                _pass(f"[Step 6] Invoice generated from order → invoice_id={invoice_id}")
            else:
                _fail("[Step 6] Invoice generated from order", str(res))
    except Exception as e:
        _fail(f"[Step 6] POST /api/customer-orders/{order_id}/invoice", str(e))

    # ── Step 7: Order status is now partially_invoiced ────────────────────────
    try:
        detail = GET(f"/api/customer-orders/{order_id}")
        status = detail.get("status")
        if status == "partially_invoiced":
            _pass("[Step 7] Order status is 'partially_invoiced' after partial invoice")
        elif status == "invoiced":
            _pass("[Step 7] Order status is 'invoiced' (full qty invoiced)")
        else:
            _fail("[Step 7] Order status updated after invoice",
                  f"expected partially_invoiced or invoiced, got '{status}'")
    except Exception as e:
        _fail("[Step 7] Order status after invoice check", str(e))

    # ── Step 8: Invoice appears in Invoices list with order reference ─────────
    if invoice_id:
        try:
            invs = GET("/api/invoices")
            inv  = next((i for i in invs if i["id"] == invoice_id), None)
            if inv:
                _pass(f"[Step 8] Invoice {invoice_id} appears in Invoices list")
                invoice_total = float(inv.get("total_amount") or inv.get("balance") or 0)
            else:
                _fail(f"[Step 8] Invoice {invoice_id} appears in Invoices list", "not found")
        except Exception as e:
            _fail("[Step 8] Invoices list after order invoice", str(e))

        # Check it carries the customer order reference
        try:
            invs = GET("/api/invoices")
            inv  = next((i for i in invs if i["id"] == invoice_id), None)
            if inv and inv.get("customer_order_id") == order_id:
                _pass(f"[Step 8] Invoice links back to customer_order_id={order_id}")
            else:
                _fail("[Step 8] Invoice has customer_order_id", str(inv))
        except Exception as e:
            _fail("[Step 8] Invoice customer_order_id check", str(e))

    # ── Step 9: AR balance includes the new invoice ───────────────────────────
    try:
        dash = GET("/api/dashboard")
        ar   = float((dash.get("ar") or {}).get("outstanding", 0))
        if ar > 0:
            _pass(f"[Step 9] Dashboard outstanding AR is positive ({ar:.2f}) — invoice reflected")
        else:
            _fail("[Step 9] Dashboard AR reflects new invoice", f"AR={ar}")
    except Exception as e:
        _fail("[Step 9] Dashboard AR check after invoice", str(e))

    # ── Step 10: Record payment and verify AR drops ───────────────────────────
    if invoice_id and invoice_total and invoice_total > 0:
        try:
            ar_before = float((GET("/api/dashboard").get("ar") or {}).get("outstanding", 0))

            POST(f"/api/invoices/{invoice_id}/pay", {
                "amount":       invoice_total,
                "payment_date": TODAY,
                "payment_mode": "Bank Transfer",
                "payment_ref":  "INT-TEST-PAY-001",
                "notes":        "Integration test — full payment"
            })
            _pass(f"[Step 10] Payment of {invoice_total:.2f} recorded against invoice {invoice_id}")

            ar_after = float((GET("/api/dashboard").get("ar") or {}).get("outstanding", 0))
            if ar_after < ar_before - 0.01:
                _pass(f"[Step 10] Dashboard AR dropped after payment ({ar_before:.2f} → {ar_after:.2f})")
            else:
                _fail("[Step 10] Dashboard AR dropped after payment",
                      f"before={ar_before:.2f} after={ar_after:.2f}")
        except Exception as e:
            _fail(f"[Step 10] Payment and AR verification", str(e))
    else:
        _skip("[Step 10] Payment + AR drop", "invoice_id or invoice_total not available")

    # ── Step 11: Invoice balance is now zero (or near zero) ───────────────────
    if invoice_id:
        try:
            invs    = GET("/api/invoices")
            inv     = next((i for i in invs if i["id"] == invoice_id), None)
            balance = float(inv.get("balance", 9999)) if inv else 9999
            if balance < 0.01:
                _pass(f"[Step 11] Invoice balance is zero after full payment ({balance:.2f})")
            else:
                _fail("[Step 11] Invoice balance is zero after full payment",
                      f"balance still {balance:.2f}")
        except Exception as e:
            _fail("[Step 11] Invoice balance after payment", str(e))


# ═══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
def print_summary():
    passed = sum(1 for r in _results if r["status"] == "PASS")
    failed = sum(1 for r in _results if r["status"] == "FAIL")
    skipped = sum(1 for r in _results if r["status"] == "SKIP")
    total  = len(_results)

    bar = "═" * 60
    print(f"\n{BOLD}{bar}{RESET}")
    print(f"{BOLD}  TEST SUMMARY{RESET}")
    print(f"{BOLD}{bar}{RESET}")
    print(f"  {GREEN}Passed : {passed}{RESET}")
    print(f"  {RED}Failed : {failed}{RESET}")
    print(f"  {YELLOW}Skipped: {skipped}{RESET}")
    print(f"  Total  : {total}")
    print(f"{BOLD}{bar}{RESET}")

    if failed:
        print(f"\n{RED}{BOLD}FAILED TESTS:{RESET}")
        for r in _results:
            if r["status"] == "FAIL":
                detail = f" → {r['detail']}" if r.get("detail") else ""
                print(f"  {RED}✗{RESET} {r['test']}{detail}")

    pct = int(passed / max(total - skipped, 1) * 100)
    if pct == 100:
        print(f"\n{GREEN}{BOLD}  ALL TESTS PASSED ({pct}%) ✓{RESET}")
    elif pct >= 80:
        print(f"\n{YELLOW}{BOLD}  {pct}% tests passing — review failures above{RESET}")
    else:
        print(f"\n{RED}{BOLD}  {pct}% tests passing — significant issues found{RESET}")

    # ── Write report files ─────────────────────────────────────────────────────
    _write_reports(passed, failed, skipped, total, pct)


def _write_reports(passed: int, failed: int, skipped: int, total: int, pct: int):
    """Write timestamped JSON log and human-readable TXT report to test_reports/."""
    reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_reports")
    os.makedirs(reports_dir, exist_ok=True)

    ts      = datetime.now()
    ts_file = ts.strftime("%Y%m%d_%H%M%S")
    ts_disp = ts.strftime("%Y-%m-%d %H:%M:%S")

    # ── 1. JSON log ────────────────────────────────────────────────────────────
    json_path = os.path.join(reports_dir, f"test_log_{ts_file}.json")
    payload = {
        "run_at":    ts_disp,
        "server":    BASE_URL,
        "summary": {
            "total":   total,
            "passed":  passed,
            "failed":  failed,
            "skipped": skipped,
            "pass_pct": pct,
            "result":  "PASS" if failed == 0 else "FAIL",
        },
        "tests": _results,
    }
    with open(json_path, "w") as f:
        json.dump(payload, f, indent=2)

    # ── 2. Human-readable TXT report ───────────────────────────────────────────
    txt_path = os.path.join(reports_dir, f"test_report_{ts_file}.txt")
    sep  = "═" * 62
    sep2 = "─" * 62
    lines = [
        sep,
        "  SPICETOPIA ERP — TEST REPORT",
        f"  Run at : {ts_disp}",
        f"  Server : {BASE_URL}",
        sep,
        f"  Passed  : {passed}",
        f"  Failed  : {failed}",
        f"  Skipped : {skipped}",
        f"  Total   : {total}",
        f"  Result  : {'ALL PASSED ✓' if failed == 0 else f'FAILURES FOUND ({pct}% passing)'}",
        sep,
        "",
    ]

    # Group by module section
    current_section = None
    for r in _results:
        name = r["test"]
        status = r["status"]
        icon = "✓" if status == "PASS" else ("✗" if status == "FAIL" else "⊘")
        detail = f"  → {r['detail']}" if r.get("detail") else ""
        reason = f"  [{r['reason']}]" if r.get("reason") else ""
        lines.append(f"  {icon} [{status:4s}]  {name}{detail}{reason}")

    lines += [
        "",
        sep2,
        "  FAILED TESTS DETAIL",
        sep2,
    ]
    failures = [r for r in _results if r["status"] == "FAIL"]
    if failures:
        for r in failures:
            lines.append(f"  ✗ {r['test']}")
            if r.get("detail"):
                lines.append(f"      Detail: {r['detail']}")
    else:
        lines.append("  (none)")

    lines += ["", sep, "  END OF REPORT", sep]

    with open(txt_path, "w") as f:
        f.write("\n".join(lines) + "\n")

    # ── 3. Rolling latest symlink / copy ───────────────────────────────────────
    latest_json = os.path.join(reports_dir, "latest.json")
    latest_txt  = os.path.join(reports_dir, "latest.txt")
    for src, dst in [(json_path, latest_json), (txt_path, latest_txt)]:
        with open(src, "rb") as fin:
            data = fin.read()
        with open(dst, "wb") as fout:
            fout.write(data)

    print(f"\n  📄 Report saved → test_reports/test_report_{ts_file}.txt")
    print(f"  🗂  JSON log    → test_reports/test_log_{ts_file}.json")
    print(f"  🔗 Latest copy → test_reports/latest.txt  /  latest.json")


# ═══════════════════════════════════════════════════════════════════════════════
# RECOMMENDATIONS — Professional-Grade ERP Gaps
# ═══════════════════════════════════════════════════════════════════════════════
RECOMMENDATIONS = """
╔══════════════════════════════════════════════════════════════╗
║   SPICETOPIA ERP — PROFESSIONAL-GRADE RECOMMENDATIONS       ║
╚══════════════════════════════════════════════════════════════╝

These are gaps between the current system and a production-grade
ERP. They are prioritised by business impact.

──────────────────────────────────────────────────────────────
PRIORITY 1 — CRITICAL (Data Integrity & Security)
──────────────────────────────────────────────────────────────

1.1  ROLE-BASED ACCESS CONTROL (RBAC)
     Current:  Single admin role. All authenticated users see everything.
     Required: Define roles (Admin, Sales, Warehouse, Accountant, Field Rep)
               and enforce per-endpoint. E.g. Field Reps should not access
               payroll or AP data.
     Impact:   Prevents accidental or malicious data access/modification.

1.2  PASSWORD SECURITY
     Current:  SHA-256 hashed passwords (no salt, no iterations).
     Required: Use bcrypt or argon2 with per-user salt and ≥10 rounds.
     Impact:   SHA-256 is breakable with rainbow tables in seconds.

1.3  HTTPS / TLS
     Current:  Plain HTTP on port 3001.
     Required: Run behind nginx/caddy with a TLS certificate, even on LAN.
     Impact:   Login credentials and auth tokens transmitted in clear text.

1.4  SESSION EXPIRY
     Current:  Tokens never expire (in-memory dict).
     Required: Add expiry timestamps to sessions; auto-expire after N hours.
               Also invalidate token on logout server-side.
     Impact:   A stolen token never becomes invalid.

1.5  INPUT VALIDATION
     Current:  Minimal server-side validation (some endpoints accept any JSON).
     Required: Validate all inputs: required fields, numeric ranges, date
               formats, string lengths. Return 422 with field-level errors.
     Impact:   Prevents corrupted data from entering the database.

──────────────────────────────────────────────────────────────
PRIORITY 2 — HIGH (Operations & Reliability)
──────────────────────────────────────────────────────────────

2.1  AUTOMATED DATABASE BACKUP
     Current:  No backup mechanism. Data lives in a single SQLite file.
     Required: Daily automated SQLite backup to a separate folder/drive.
               SQLite's `.backup` API or scheduled `sqlite3 db .dump`.
     Impact:   Any disk failure = total data loss.

2.2  CREDIT LIMIT ENFORCEMENT
     Current:  Credit limit is stored but never checked when creating a sale.
     Required: Before creating a sale, check:
               customer.outstanding_AR + new_invoice_total ≤ credit_limit
               Return a blocking error (with override option for managers).
     Impact:   Customers can exceed credit limits silently.

2.3  DUPLICATE INVOICE / BILL PREVENTION
     Current:  Nothing prevents recording the same supplier bill twice.
     Required: Add a unique constraint on (supplier_id, bill_ref_number).
               Add supplier bill reference number field.
     Impact:   Double payments to suppliers.

2.4  STOCK RESERVATION (WIP)
     Current:  Inventory is only deducted when a Work Order is converted.
               Nothing reserves stock between WO creation and completion.
     Required: When a WO is created/IN_PROGRESS, reduce "available" quantity
               (not physical — a reserved quantity column or view).
     Impact:   Two WOs can be created for the same stock; one will fail.

2.5  TRANSACTION CANCELLATION / VOID
     Current:  No way to cancel an invoice, bill, or payment.
     Required: Add VOID status to invoices/bills. Voiding reverses all
               ledger entries via compensating transactions (not deletion).
     Impact:   Errors cannot be corrected without direct DB manipulation.

──────────────────────────────────────────────────────────────
PRIORITY 3 — MEDIUM (Reporting & Usability)
──────────────────────────────────────────────────────────────

3.1  FINANCIAL STATEMENTS
     Current:  Margins report and rep performance exist but no P&L,
               Balance Sheet, or Cash Flow statement.
     Required: Build a period-selectable P&L: Revenue - COGS - Expenses.
               At minimum: monthly sales vs. purchase cost summary.
     Impact:   No financial visibility for management decisions.

3.2  PURCHASE ORDER (PO) MODULE
     Current:  Bills are recorded after the fact (no PO before goods receipt).
     Required: Add PO → Goods Receipt → Bill matching workflow (3-way match).
     Impact:   Paying for goods not yet received; no procurement approval.

3.3  PAYMENT TERMS ALERTS
     Current:  Due dates stored but no overdue alerts on dashboard.
     Required: Dashboard should surface:  "X invoices overdue (PKR Y)",
               "X bills due in 7 days (PKR Z)" with drill-down.
     Impact:   Overdue AR/AP go unnoticed without manual checking.

3.4  EXPORT TO PDF / EXCEL
     Current:  No print or export functionality.
     Required: Invoice PDF (with GST breakdown, company letterhead),
               Statement of Account per customer, Excel export for reports.
     Impact:   Invoices must be re-created manually; reports can't be shared.

3.5  SEARCH & FILTERING
     Current:  API supports basic filtering; UI shows full lists.
     Required: UI-side date range, customer, status filters on every list.
               Server-side pagination for lists > 100 rows.
     Impact:   Unusable for businesses with > 6 months of data.

──────────────────────────────────────────────────────────────
PRIORITY 4 — LOW (Future-Proofing)
──────────────────────────────────────────────────────────────

4.1  MULTI-WAREHOUSE INVENTORY
     Current:  Single implicit warehouse.
     Required: Warehouse dimension on inventory movements; transfer between
               warehouses via warehouse transfer records.

4.2  MULTI-CURRENCY
     Current:  All amounts assumed PKR.
     Required: Currency field on invoices/bills; exchange rate table;
               reporting in base currency (PKR).

4.3  API VERSIONING
     Current:  /api/... with no version.
     Required: /api/v1/... so breaking changes don't break field apps.

4.4  ERROR LOGGING
     Current:  Exceptions printed to console only.
     Required: Structured log file (rotating) with stack traces, request IDs,
               and timestamps. Consider Sentry or a log aggregator.

4.5  AUTOMATED TEST CI
     Current:  This script must be run manually.
     Required: Run this test suite on every code change (GitHub Actions or
               a simple pre-commit hook).

──────────────────────────────────────────────────────────────
QUICK WINS (Can be done in < 1 day each)
──────────────────────────────────────────────────────────────

★ Add overdue flag (days overdue) to AR/AP aging UI
★ Dashboard: show total sales THIS MONTH vs LAST MONTH
★ Customer statement page (all invoices + payments for a customer)
★ Confirm dialog before voiding / deleting anything
★ Require due_date on all invoices and bills (currently optional)
★ Supplier bill reference number field (prevents duplicate entry)
★ Print-friendly CSS class on invoices / reports pages
★ README.md with startup instructions for new team members

══════════════════════════════════════════════════════════════
"""

# ═══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════
# Module-level test references (populated by test_reference_data)
_test_customer_code  = None
_test_supplier_id    = None
_test_product_code   = None
_test_ingredient_id  = None
_test_variant_id     = None   # product_variant_id (int)
_test_pack_size      = None   # e.g. "50g"
_test_price_type_id  = None   # price_types.id (int)

def main():
    parser = argparse.ArgumentParser(description="Spicetopia ERP Test Suite")
    parser.add_argument("--module", help="Run a specific module (e.g. sales, bills, production)")
    parser.add_argument("--report-only", action="store_true", help="Print recommendations only")
    args = parser.parse_args()

    if args.report_only:
        print(RECOMMENDATIONS)
        return

    MODULE_MAP = {
        "connectivity": test_connectivity,
        "dashboard":    test_dashboard,
        "reference":    test_reference_data,
        "inventory":    test_inventory,
        "sales":        test_sales,
        "bills":        test_bills,
        "production":   test_production,
        "prices":       test_prices,
        "reports":      test_reports,
        "field":        test_field_ops,
        "payroll":      test_payroll,
        "orders":       test_customer_orders,
        "integration":  test_integration_flow,
    }

    if args.module:
        mod = args.module.lower()
        if mod not in MODULE_MAP:
            print(f"Unknown module '{mod}'. Available: {', '.join(MODULE_MAP.keys())}")
            sys.exit(1)
        # Always run connectivity first
        test_connectivity()
        if mod != "connectivity":
            test_reference_data()   # most tests depend on ref data
            MODULE_MAP[mod]()
    else:
        # Full suite
        print(f"\n{BOLD}{'═'*60}")
        print("  SPICETOPIA ERP — FULL TEST SUITE")
        print(f"  Server: {BASE_URL}")
        print(f"  Date:   {TODAY}")
        print(f"{'═'*60}{RESET}")

        test_connectivity()
        test_dashboard()
        test_reference_data()
        test_inventory()
        test_sales()
        test_bills()
        test_production()
        test_prices()
        test_reports()
        test_field_ops()
        test_payroll()
        test_customer_orders()
        test_integration_flow()

    print_summary()
    print(RECOMMENDATIONS)

    # Exit with non-zero if any failures
    failed = sum(1 for r in _results if r["status"] == "FAIL")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
