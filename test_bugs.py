"""
Spicetopia ERP — Targeted Bug Regression Tests (BUG-001 to BUG-008)
Run against local server on port 8770.
"""
import requests, sys, json, io, csv

BASE  = "http://localhost:8770"
TOKEN = None
PASS  = 0
FAIL  = 0
results = []

def hdr():
    return {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def GET(path, params=None):
    return requests.get(f"{BASE}{path}", headers=hdr(), params=params, timeout=10)

def POST(path, body=None):
    return requests.post(f"{BASE}{path}", headers=hdr(), json=body or {}, timeout=10)

def PUT(path, body=None):
    return requests.put(f"{BASE}{path}", headers=hdr(), json=body or {}, timeout=10)

def check(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        print(f"  \033[92m✓ PASS\033[0m  {name}")
        PASS += 1; results.append(("PASS", name))
    else:
        print(f"  \033[91m✗ FAIL\033[0m  {name}  → {detail}")
        FAIL += 1; results.append(("FAIL", name, detail))

def section(title):
    print(f"\n\033[96m{'─'*60}\033[0m")
    print(f"\033[96m  {title}\033[0m")
    print(f"\033[96m{'─'*60}\033[0m")

def make_csv(fieldnames, rows):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames)
    w.writeheader(); w.writerows(rows)
    return buf.getvalue().encode()

# ── SETUP ─────────────────────────────────────────────────
section("SETUP — Login & Seed Reference Data")

r = requests.post(f"{BASE}/api/auth/login", json={"username":"admin","password":"admin123"})
assert r.status_code == 200, f"Login failed: {r.text}"
TOKEN = r.json()["token"]
check("Login as admin", True)

# Dev reset for clean slate
r = POST("/api/dev/reset-all")
check("Dev reset — clean DB", r.status_code == 200, r.text[:200])

# Seed products + pack sizes via CSV import (pack_size must be label like "50g")
prod_csv = make_csv(
    ["product_code","product_name","sku_code","pack_size"],
    [
        {"product_code":"SPCM","product_name":"Chaat Masala","sku_code":"SPCM-50","pack_size":"50g"},
        {"product_code":"SPGM","product_name":"Garam Masala","sku_code":"SPGM-50","pack_size":"50g"},
    ]
)
r = requests.post(f"{BASE}/api/admin/masters/upload/products",
    headers={"Authorization": f"Bearer {TOKEN}"},
    files={"file": ("products.csv", prod_csv, "text/csv")})
check("Seed products via CSV import", r.status_code == 200 and r.json().get("ok"), r.text[:200])

# Seed ingredient
r = POST("/api/ingredients", {"code":"ING-TEST","name":"Test Spice","cost_per_kg":500,"unit":"kg"})
ING_ID = r.json().get("id") if r.status_code == 201 else None
check("Seed ingredient ING-TEST", ING_ID is not None, r.text[:200])

# Seed supplier (correct field names)
r = POST("/api/suppliers", {"name":"Test Supplier","contact":"Ali Khan","phone":"0300-0000000","city":"Karachi"})
SUP = r.json() if r.status_code == 201 else None
SUP_ID = SUP["id"] if SUP else None
check("Seed supplier", SUP_ID is not None, r.text[:200])

# Seed customer
r = POST("/api/customers", {"name":"Test Customer","customerType":"RETAIL","city":"Karachi","address":"123 Test St","phone":"0300-1111111"})
CUST = r.json()
CUST_CODE = CUST.get("code"); CUST_ID = CUST.get("id")
check("Seed customer", CUST_CODE is not None, r.text[:200])

# Get reference data (pack sizes, price types, variants)
ref = GET("/api/ref").json()
pack_sizes  = ref.get("packSizes", [])
price_types = ref.get("priceTypes", [])
PS_50       = next((p for p in pack_sizes if str(p.get("grams")) == "50"), None)
PT          = price_types[0] if price_types else None
check("Pack size 50g in ref data", PS_50 is not None, f"pack_sizes={pack_sizes}")
check("Price type seeded", PT is not None, f"price_types={price_types}")

# Get variant ID for SPCM-50
prods = GET("/api/products").json() if GET("/api/products").status_code == 200 else []
# Also try ref
ref_prods = ref.get("products", [])
VARIANT_ID = None
PROD_ID    = None
# Find from full product list with variants
for p in (prods if prods else []):
    if p.get("code") == "SPCM":
        PROD_ID = p.get("id")
        for v in p.get("variants", []):
            if str(v.get("pack_grams","")) == "50" or v.get("pack_size") == "50g" or v.get("sku_code") == "SPCM-50":
                VARIANT_ID = v.get("id")
# Try variants from ref
if not VARIANT_ID:
    variants = ref.get("variants", [])
    for v in variants:
        if v.get("sku_code") == "SPCM-50" or (v.get("product_code") == "SPCM" and str(v.get("pack_grams","")) == "50"):
            VARIANT_ID = v.get("id")
check("Variant SPCM-50 found", VARIANT_ID is not None, f"prods={[p.get('code') for p in prods]}, ref_variants={[v.get('sku_code') for v in ref.get('variants',[])]}")

# Seed price for SPCM-50 — API needs productVariantId (int) and priceTypeId (int)
PRICE_SET = False
if PT and VARIANT_ID:
    r = POST("/api/prices", {
        "productVariantId": VARIANT_ID,
        "priceTypeId":      PT["id"],
        "price":            250.00,
        "effectiveFrom":    "2026-01-01"
    })
    PRICE_SET = r.status_code in (200, 201)
check("Seed price SPCM-50g = PKR 250", PRICE_SET, r.text[:200] if not PRICE_SET else "")

# Seed finished goods stock via dev endpoint (bypasses BOM — test only)
FG_STOCK = False
r = POST("/api/dev/seed-fg-stock", {"productCode":"SPCM","packSize":"50g","qtyUnits":100,"batchDate":"2026-04-11"})
FG_STOCK = r.status_code == 200 and r.json().get("ok")
check("Seed 100 units FG stock (SPCM-50g)", FG_STOCK, r.text[:200] if not FG_STOCK else "")

# ── BUG-001 ───────────────────────────────────────────────
section("BUG-001 — Invoice generation (no SQL column crash)")

ORDER_ID = INVOICE_ID = None
if CUST_CODE and FG_STOCK:
    r = POST("/api/customer-orders", {
        "custCode": CUST_CODE,
        "orderDate": "2026-04-11",
        "dueDate": "2026-04-30",
        "lines": [{"productCode":"SPCM","packSize":"50g","qty":2,"unitPrice":250.00}]
    })
    if r.status_code == 201:
        ORDER_ID = r.json().get("orderId") or r.json().get("id")
        check("BUG-001: Create order with lines", ORDER_ID is not None, r.text[:200])
        r3 = POST(f"/api/customer-orders/{ORDER_ID}/confirm")
        check("BUG-001: Confirm order", r3.status_code == 200, r3.text[:200])
        # Get order items to build invoice lines
        order_detail = GET(f"/api/customer-orders/{ORDER_ID}").json()
        order_items  = order_detail.get("items", [])
        inv_lines    = [{"orderItemId": it["id"], "qty": it.get("qty_ordered", it.get("qty", 1))}
                        for it in order_items]
        r4 = POST(f"/api/customer-orders/{ORDER_ID}/invoice", {"lines": inv_lines})
        check("BUG-001: Generate invoice (no SQL crash)", r4.status_code == 201, r4.text[:200])
        if r4.status_code == 201:
            inv = r4.json()
            INVOICE_ID = inv.get("invoiceId") or inv.get("id")
            total = inv.get("total")
            check("BUG-001: Invoice total = 500.0 (2 × 250)", float(total or 0) == 500.0, f"total={total}")
    else:
        check("BUG-001: Create order", False, r.text[:200])
else:
    check("BUG-001: Skipped — seed missing", False, f"cust={CUST_CODE} fg={FG_STOCK}")

# ── BUG-002 ───────────────────────────────────────────────
section("BUG-002 — Can add items to a confirmed order")

if CUST_CODE:
    r = POST("/api/customer-orders", {
        "custCode": CUST_CODE,
        "orderDate": "2026-04-11",
        "dueDate": "2026-04-30",
        "lines": [{"productCode":"SPCM","packSize":"50g","qty":1,"unitPrice":250}]
    })
    if r.status_code == 201:
        o2 = r.json().get("orderId") or r.json().get("id")
        POST(f"/api/customer-orders/{o2}/confirm")
        # Add second item (SPGM, different product) AFTER confirming
        r_add = POST(f"/api/customer-orders/{o2}/items", {"productCode":"SPGM","packSize":"50g","qty":3,"unitPrice":300.00})
        check("BUG-002: Add item to confirmed order succeeds", r_add.status_code == 201, r_add.text[:200])
        items = GET(f"/api/customer-orders/{o2}").json().get("items", [])
        check("BUG-002: Order has 2 items after adding to confirmed", len(items) == 2, f"got {len(items)}")
    else:
        check("BUG-002: Create order", False, r.text[:200])
else:
    check("BUG-002: Skipped — seed missing", False, "")

# ── BUG-003 ───────────────────────────────────────────────
section("BUG-003 — PO detail has qty + cost for receipt pre-fill")

PO_ID = None
if SUP_ID and ING_ID:
    r = POST("/api/purchase-orders", {
        "supplierId": SUP_ID,
        "poDate": "2026-04-11",
        "expectedDate": "2026-04-20",
        "notes": "BUG-003 test",
        "items": [{"ingredientId": ING_ID, "quantityKg": 10.0, "unitCostKg": 500.0}]
    })
    if r.status_code == 201:
        PO_ID = r.json()["id"]
        check("BUG-003: Create PO with item", True)
        po = GET(f"/api/purchase-orders/{PO_ID}").json()
        items = po.get("items", [])
        has_qty  = any(i.get("quantity_kg") or i.get("quantityKg") or i.get("qty_kg") for i in items)
        has_cost = any(i.get("unit_cost_kg") or i.get("unitCostKg") or i.get("unit_cost") for i in items)
        check("BUG-003: PO items have quantity field (for pre-fill)", has_qty, f"item keys={list(items[0].keys()) if items else 'no items'}")
        check("BUG-003: PO items have cost field (for pre-fill)", has_cost, f"item keys={list(items[0].keys()) if items else 'no items'}")
    else:
        check("BUG-003: Create PO", False, r.text[:200])
else:
    check("BUG-003: Skipped — seed missing", False, f"sup={SUP_ID} ing={ING_ID}")

# ── BUG-004 ───────────────────────────────────────────────
section("BUG-004 — Supplier outstanding balance pre-fills AP payment")

if SUP_ID and ING_ID:
    r = POST("/api/bills", {
        "supplierId": SUP_ID,
        "billDate": "2026-04-11",
        "dueDate": "2026-04-30",
        "supplierRef": "INV-BUG004",
        "notes": "BUG-004 test",
        "items": [{"ingredientId": ING_ID, "quantityKg": 10.0, "unitCostKg": 500.0}]
    })
    if r.status_code == 201:
        bill_id = r.json().get("billId") or r.json().get("id")
        check("BUG-004: Create unpaid bill (10kg × PKR500 = PKR5000)", bill_id is not None, r.text[:200])
        # Fetch bills for supplier — AP payment form uses this to pre-fill
        r2 = GET("/api/bills", params={"supplierId": SUP_ID})
        bills = r2.json() if r2.status_code == 200 else []
        unpaid = [b for b in bills if b.get("status") in ("UNPAID","PARTIAL")]
        total  = sum(float(b.get("total_amount",0)) - float(b.get("amount_paid",0) or 0) for b in unpaid)
        check("BUG-004: Outstanding balance > 0 for supplier", total > 0, f"outstanding={total}, bills={len(bills)}")
        # Verify bill detail has total_amount
        r3 = GET(f"/api/bills/{bill_id}")
        bill = r3.json()
        check("BUG-004: Bill has total_amount field", bill.get("total_amount") is not None, f"keys={list(bill.keys())}")
        check("BUG-004: Bill total_amount = 5000", float(bill.get("total_amount",0)) == 5000.0, f"got {bill.get('total_amount')}")
    else:
        check("BUG-004: Create bill", False, r.text[:200])
else:
    check("BUG-004: Skipped — seed missing", False, f"sup={SUP_ID} ing={ING_ID}")

# ── BUG-005 ───────────────────────────────────────────────
section("BUG-005 — Ingredient name returned in inventory API")

r = GET("/api/ingredients")
ings = r.json() if r.status_code == 200 else []
ing  = next((i for i in ings if i.get("code") == "ING-TEST"), ings[0] if ings else None)
if ing:
    check("BUG-005: /api/ingredients has 'name' field", "name" in ing, f"keys={list(ing.keys())}")
    check("BUG-005: Name is populated (not blank)", bool(ing.get("name")), f"name='{ing.get('name')}'")
else:
    check("BUG-005: Ingredients returned", False, "empty list")

r2 = GET("/api/admin/ingredients")
ings2 = r2.json() if r2.status_code == 200 else []
ing2  = next((i for i in ings2 if i.get("code") == "ING-TEST"), ings2[0] if ings2 else None)
if ing2:
    check("BUG-005: Admin endpoint also returns name", bool(ing2.get("name")), f"name='{ing2.get('name')}'")

# ── BUG-006 ───────────────────────────────────────────────
section("BUG-006 — Admin can create and edit ingredients")

r = POST("/api/ingredients", {"code":"ING-NEW","name":"New Ingredient","cost_per_kg":750,"unit":"kg"})
check("BUG-006: Create ingredient via POST", r.status_code == 201, r.text[:200])
if r.status_code == 201:
    r2 = PUT("/api/ingredients/ING-NEW", {"cost_per_kg":800,"name":"Updated Ingredient","unit":"kg","reorder_level":500})
    check("BUG-006: Edit ingredient via PUT", r2.status_code == 200, r2.text[:200])
    check("BUG-006: Updated cost = 800", float(r2.json().get("cost_per_kg",0)) == 800.0, f"got {r2.json().get('cost_per_kg')}")

# ── BUG-007 ───────────────────────────────────────────────
section("BUG-007 — Current price retrievable for Update Price pre-fill")

r = GET("/api/prices")
all_prices = r.json() if r.status_code == 200 else []
spcm_prices = [p for p in all_prices if p.get("product_code") == "SPCM"]
check("BUG-007: /api/prices returns results", len(all_prices) > 0, f"count={len(all_prices)}")
if spcm_prices:
    p = spcm_prices[0]
    check("BUG-007: Price has 'price' value (for pre-fill)", p.get("price") is not None, f"keys={list(p.keys())}")
    check("BUG-007: Price has price_type_code (for pre-fill)", bool(p.get("price_type_code") or p.get("price_type")), f"keys={list(p.keys())}")
    check("BUG-007: Price value = 250", float(p.get("price",0)) == 250.0, f"got {p.get('price')}")
else:
    check("BUG-007: SPCM price found", False, f"all_prices={all_prices[:2]}")

# ── BUG-008 ───────────────────────────────────────────────
section("BUG-008 — Ingredient import full-sync (deactivates stale codes)")

batch1 = [
    {"code":"ING-SYNC-A","name":"Sync Spice A","cost_per_kg":100,"unit":"kg"},
    {"code":"ING-SYNC-B","name":"Sync Spice B","cost_per_kg":200,"unit":"kg"},
]
r = requests.post(f"{BASE}/api/admin/masters/upload/ingredients",
    headers={"Authorization": f"Bearer {TOKEN}"},
    files={"file": ("ing.csv", make_csv(["code","name","cost_per_kg","unit"], batch1), "text/csv")})
d = r.json()
check("BUG-008: First upload — 2 imported", d.get("imported") == 2, str(d))

batch2 = [
    {"code":"ING-SYNC-B","name":"Sync Spice B v2","cost_per_kg":250,"unit":"kg"},
    {"code":"ING-SYNC-C","name":"Sync Spice C",  "cost_per_kg":300,"unit":"kg"},
]
r = requests.post(f"{BASE}/api/admin/masters/upload/ingredients",
    headers={"Authorization": f"Bearer {TOKEN}"},
    files={"file": ("ing.csv", make_csv(["code","name","cost_per_kg","unit"], batch2), "text/csv")})
d = r.json()
check("BUG-008: Second upload — 1 updated + 1 added", d.get("updated") == 1 and d.get("imported") == 1, str(d))
check("BUG-008: ING-SYNC-A deactivated", d.get("deactivated", 0) >= 1, str(d))

active = [i["code"] for i in GET("/api/admin/ingredients").json() if i.get("active") != 0]
check("BUG-008: ING-SYNC-A no longer active", "ING-SYNC-A" not in active, f"active={active}")
check("BUG-008: ING-SYNC-B + ING-SYNC-C active", "ING-SYNC-B" in active and "ING-SYNC-C" in active, f"active={active}")

# ══════════════════════════════════════════════════════════
# SPRINT B1 — B2B PORTAL BACKEND TESTS
# ══════════════════════════════════════════════════════════

# ── B1 SETUP: Create sales rep + field login ──────────────
section("B1 SETUP — Sales Rep + Field Token")

REP_ID = None
FIELD_TOKEN = None
REP_PHONE = "03001234567"
REP_PIN   = "1234"

r = POST("/api/reps", {
    "name": "Test Rep B1", "phone": REP_PHONE, "pin": REP_PIN,
    "designation": "Sales Rep", "city": "Karachi"
})
REP_ID = r.json().get("id") if r.status_code in (200, 201) else None
check("B1 SETUP: Create sales rep", REP_ID is not None, r.text[:200])

r = requests.post(f"{BASE}/api/field/auth", json={"phone": REP_PHONE, "pin": REP_PIN}, timeout=10)
FIELD_TOKEN = r.json().get("token") if r.status_code == 200 else None
check("B1 SETUP: Field login with phone+PIN", FIELD_TOKEN is not None, r.text[:200])

def field_hdr():
    return {"Authorization": f"Bearer {FIELD_TOKEN}", "Content-Type": "application/json"}

def FGET(path, params=None):
    return requests.get(f"{BASE}{path}", headers=field_hdr(), params=params, timeout=10)

def FPOST(path, body=None):
    return requests.post(f"{BASE}{path}", headers=field_hdr(), json=body or {}, timeout=10)

# ── B1-001 ────────────────────────────────────────────────
section("B1-001 — Account number format KHI-R001")

r = POST("/api/customers", {
    "name": "B1 Retail KHI", "customerType": "RETAIL",
    "city": "KHI", "phone": "0300-B1KHI1", "address": "Test St"
})
acct = r.json().get("account_number") or r.json().get("code") if r.status_code in (200,201) else None
check("B1-001: Customer created", r.status_code in (200,201), r.text[:200])
check("B1-001: Account number has city prefix KHI", acct and acct.startswith("KHI-"), f"got={acct}")
check("B1-001: Account number has type letter R for RETAIL", acct and "-R" in acct, f"got={acct}")

r2 = POST("/api/customers", {
    "name": "B1 Wholesale LHE", "customerType": "WHOLESALE",
    "city": "LHE", "phone": "0300-B1LHE1", "address": "Test St"
})
acct2 = r2.json().get("account_number") or r2.json().get("code") if r2.status_code in (200,201) else None
check("B1-001: WHOLESALE customer accepted (new type)", r2.status_code in (200,201), r2.text[:200])
check("B1-001: Wholesale account has -W type letter", acct2 and "-W" in acct2, f"got={acct2}")

# ── B1-002 ────────────────────────────────────────────────
section("B1-002 — Field customer lookup endpoint")

if FIELD_TOKEN:
    r = FGET("/api/field/customers/lookup", params={"q": "B1"})
    check("B1-002: Lookup returns 200", r.status_code == 200, r.text[:200])
    results_list = r.json() if r.status_code == 200 else []
    check("B1-002: Returns list", isinstance(results_list, list), f"type={type(results_list)}")
    check("B1-002: Results include account_number field",
          all("account_number" in c or "code" in c for c in results_list) if results_list else True,
          f"sample={results_list[0] if results_list else 'empty'}")
    # Lookup by phone
    r2 = FGET("/api/field/customers/lookup", params={"q": "0300-B1KHI1"})
    hits = r2.json() if r2.status_code == 200 else []
    check("B1-002: Phone search finds customer", len(hits) >= 1, f"hits={len(hits)}")
else:
    check("B1-002: Skipped — no field token", False, "")

# ── B1-003 ────────────────────────────────────────────────
section("B1-003 — Field customer creation")

B1_NEW_CUST_ACCT = None
if FIELD_TOKEN:
    r = FPOST("/api/field/customers", {
        "name": "B1 Portal Customer", "phone": "0300-PORTAL1",
        "city": "KHI", "customerType": "RETAIL"
        # no address — should be optional
    })
    check("B1-003: Create customer via field endpoint", r.status_code in (200,201), r.text[:200])
    B1_NEW_CUST_ACCT = r.json().get("account_number") or r.json().get("code") if r.status_code in (200,201) else None
    check("B1-003: Returns account_number", B1_NEW_CUST_ACCT is not None, r.text[:200])
    check("B1-003: No address required (blank OK)", r.status_code in (200,201), r.text[:200])
else:
    check("B1-003: Skipped — no field token", False, "")

# ── B1-004 ────────────────────────────────────────────────
section("B1-004 — Field products (no cost prices exposed)")

if FIELD_TOKEN:
    r = FGET("/api/field/products", params={"customerType": "RETAIL"})
    check("B1-004: Field products returns 200", r.status_code == 200, r.text[:200])
    prods_list = r.json() if r.status_code == 200 else []
    check("B1-004: Returns product list", isinstance(prods_list, list), f"type={type(prods_list)}")
    # Cost prices must never be in the response
    for prod in prods_list:
        for v in prod.get("variants", [prod]):
            has_cost = any(k in v for k in ("cost_per_unit","cost","unit_cost","cost_price"))
            if has_cost:
                check("B1-004: Cost price NOT exposed to field reps", False, f"found cost in: {v}")
                break
        else:
            continue
        break
    else:
        check("B1-004: Cost price NOT exposed to field reps", True)
    check("B1-004: Products have selling price", all(
        any(k in prod for k in ("price","selling_price","retail_price")) or prod.get("variants")
        for prod in prods_list
    ) if prods_list else True, "no products")
else:
    check("B1-004: Skipped — no field token", False, "")

# ── B1-005 ────────────────────────────────────────────────
section("B1-005 — Rep-assisted order via /api/orders/external")

B1_ORDER_ID = None
if FIELD_TOKEN and CUST_CODE and VARIANT_ID and REP_ID:
    import time as _time
    IDEMPOTENCY_KEY = f"test-idem-{int(_time.time())}"
    r = FPOST("/api/orders/external", {
        "custCode":         CUST_CODE,
        "orderDate":        "2026-04-17",
        "dueDate":          "2026-05-17",
        "order_source":     "rep_assisted",
        "placed_by_rep_id": REP_ID,
        "idempotency_key":  IDEMPOTENCY_KEY,
        "lines": [{"productCode": "SPCM", "packSize": "50g", "qty": 3, "unitPrice": 250}]
    })
    check("B1-005: Rep-assisted order created", r.status_code in (200,201), r.text[:200])
    B1_ORDER_ID = r.json().get("orderId") or r.json().get("id") if r.status_code in (200,201) else None
    # Verify order source stored correctly
    if B1_ORDER_ID:
        od = GET(f"/api/customer-orders/{B1_ORDER_ID}").json()
        check("B1-005: order_source = rep_assisted", od.get("order_source") == "rep_assisted", f"got={od.get('order_source')}")
else:
    check("B1-005: Skipped — missing seed data", False, f"cust={CUST_CODE} var={VARIANT_ID} rep={REP_ID}")

# ── B1-006 ────────────────────────────────────────────────
section("B1-006 — Idempotency key prevents duplicate orders")

if FIELD_TOKEN and CUST_CODE and B1_ORDER_ID:
    # Resubmit using the EXACT same idempotency key from B1-005
    r_idem = FPOST("/api/orders/external", {
        "custCode":         CUST_CODE,
        "orderDate":        "2026-04-17",
        "dueDate":          "2026-05-17",
        "order_source":     "rep_assisted",
        "placed_by_rep_id": REP_ID,
        "idempotency_key":  IDEMPOTENCY_KEY,
        "lines": [{"productCode": "SPCM", "packSize": "50g", "qty": 3, "unitPrice": 250}]
    })
    # Should return existing order (200) not create a new one (201)
    check("B1-006: Duplicate submission returns 200 (not 201)", r_idem.status_code == 200, f"status={r_idem.status_code}")
    dup_id = r_idem.json().get("orderId") or r_idem.json().get("id")
    check("B1-006: Returns same order ID (not a new one)", dup_id == B1_ORDER_ID, f"original={B1_ORDER_ID} dup={dup_id}")
else:
    check("B1-006: Skipped — missing B1-005 order", False, "")

# ── B1-007 ────────────────────────────────────────────────
section("B1-007 — Field orders list includes portal orders")

if B1_ORDER_ID:
    r = GET("/api/field-orders")
    check("B1-007: /api/field-orders returns 200", r.status_code == 200, r.text[:200])
    fo_list = r.json() if r.status_code == 200 else []
    portal_orders = [o for o in fo_list if o.get("_source") == "portal"]
    check("B1-007: Portal orders present with _source='portal'", len(portal_orders) >= 1,
          f"total={len(fo_list)} portal={len(portal_orders)}")
    legacy_orders = [o for o in fo_list if o.get("_source") == "legacy"]
    # Both sources returned (if any legacy exist)
    check("B1-007: _source field present on all orders",
          all("_source" in o for o in fo_list) if fo_list else True,
          f"sample keys={list(fo_list[0].keys()) if fo_list else 'empty'}")
else:
    check("B1-007: Skipped — no portal order from B1-005", False, "")

# ══════════════════════════════════════════════════════════
# SPRINT B2 — POST-LAUNCH FIX TESTS
# ══════════════════════════════════════════════════════════

# ── B2-001 ────────────────────────────────────────────────
section("B2-001 — Customer registration with blank address")

r = POST("/api/customers", {
    "name": "B2 No Address Customer", "customerType": "RETAIL",
    "city": "KHI", "phone": "0300-NOADR01"
    # address intentionally omitted
})
check("B2-001: Customer created without address", r.status_code in (200,201), r.text[:200])
check("B2-001: Returns account_number",
      bool(r.json().get("account_number") or r.json().get("code")) if r.status_code in (200,201) else False,
      r.text[:200])

r2 = POST("/api/customers", {
    "name": "B2 Empty Address Customer", "customerType": "RETAIL",
    "city": "KHI", "phone": "0300-NOADR02", "address": ""
    # address explicitly blank
})
check("B2-001: Customer created with empty-string address", r2.status_code in (200,201), r2.text[:200])

# ── B2-002 ────────────────────────────────────────────────
section("B2-002 — WhatsApp settings endpoints")

r = GET("/api/admin/settings")
check("B2-002: GET /api/admin/settings returns 200", r.status_code == 200, r.text[:200])
settings = r.json() if r.status_code == 200 else {}
check("B2-002: Response has whatsapp_enabled field", "whatsapp_enabled" in settings, f"keys={list(settings.keys())}")
check("B2-002: Response has whatsapp_admin_phone field", "whatsapp_admin_phone" in settings, f"keys={list(settings.keys())}")
check("B2-002: Response has whatsapp_admin_apikey field", "whatsapp_admin_apikey" in settings, f"keys={list(settings.keys())}")

# ── B2-003 ────────────────────────────────────────────────
section("B2-003 — PUT /api/admin/settings saves and hot-reloads")

r = PUT("/api/admin/settings", {
    "whatsapp_enabled":    False,
    "whatsapp_admin_phone": "+1234567890",
    "whatsapp_admin_apikey": "test-key-999"
})
check("B2-003: PUT /api/admin/settings returns 200", r.status_code == 200, r.text[:200])
check("B2-003: Response confirms ok=True", r.json().get("ok") is True if r.status_code == 200 else False, r.text[:200])

# Verify persisted by reading back
r2 = GET("/api/admin/settings")
saved = r2.json() if r2.status_code == 200 else {}
check("B2-003: Phone persisted correctly", saved.get("whatsapp_admin_phone") == "+1234567890",
      f"got={saved.get('whatsapp_admin_phone')}")

# ── B2-004 ────────────────────────────────────────────────
section("B2-004 — Edit sales rep profile")

if REP_ID:
    r = PUT(f"/api/reps/{REP_ID}", {
        "name":            "Test Rep B1 Updated",
        "designation":     "Senior Sales Rep",
        "baseSalary":      50000,
        "fuelAllowance":   5000,
        "mobileAllowance": 2000,
        "commissionPct":   2.5,
        "status":          "active"
    })
    check("B2-004: PUT /api/reps/:id returns 200", r.status_code == 200, r.text[:200])
    # Verify changes persisted
    reps = GET("/api/reps").json() if GET("/api/reps").status_code == 200 else []
    rep = next((rep for rep in reps if rep.get("id") == REP_ID), None)
    check("B2-004: Designation updated", rep and rep.get("designation") == "Senior Sales Rep",
          f"got={rep.get('designation') if rep else 'rep not found'}")
else:
    check("B2-004: Skipped — no rep from B1 setup", False, "")

# ── B2-005 ────────────────────────────────────────────────
section("B2-005 — Confirm portal order uses customer-orders endpoint")

if B1_ORDER_ID:
    # Portal orders must be confirmed via /api/customer-orders/:id/confirm
    # NOT via /api/field-orders/:id/confirm (legacy endpoint)
    r = POST(f"/api/customer-orders/{B1_ORDER_ID}/confirm")
    check("B2-005: Portal order confirmed via customer-orders endpoint", r.status_code == 200, r.text[:200])
    # Verify status changed
    od = GET(f"/api/customer-orders/{B1_ORDER_ID}").json()
    check("B2-005: Order status is now confirmed", od.get("status") == "confirmed",
          f"got={od.get('status')}")
else:
    check("B2-005: Skipped — no portal order from B1-005", False, "")

# ── SUMMARY ───────────────────────────────────────────────
print(f"\n{'═'*60}")
print(f"  REGRESSION TEST SUMMARY")
print(f"{'═'*60}")
print(f"  \033[92mPassed : {PASS}\033[0m")
print(f"  \033[91mFailed : {FAIL}\033[0m")
print(f"  Total  : {PASS+FAIL}")
print(f"{'═'*60}")
if FAIL == 0:
    print(f"\n  \033[92m\033[1m  ALL TESTS PASSED ✓\033[0m\n")
else:
    print(f"\n  \033[91m\033[1m  {FAIL} TEST(S) FAILING\033[0m\n")
    for res in results:
        if res[0] == "FAIL":
            print(f"    ✗ {res[1]}: {res[2] if len(res)>2 else ''}")
sys.exit(0 if FAIL == 0 else 1)
