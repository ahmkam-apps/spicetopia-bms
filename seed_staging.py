#!/usr/bin/env python3
"""
seed_staging.py — Spicetopia ERP Staging Seed Script
=====================================================
Populates a fresh ERP database with ~3 months of realistic
Pakistan-localised dummy data for testing purposes.

Usage:
    1. Start the ERP server once to initialise all tables.
    2. Stop the server.
    3. Run:  python3 seed_staging.py [path/to/spicetopia_v3_live.db]
    4. Restart the server.

The script is idempotent for master data (skips if already present)
but will refuse to run if any invoices already exist, to avoid
double-seeding on a live database.
"""

import sqlite3
import sys
import hashlib
import secrets
from datetime import date, timedelta

# ── Configuration ─────────────────────────────────────────────────────────────

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else None
if not DB_PATH:
    import tempfile, os
    DB_PATH = os.path.join(tempfile.gettempdir(), "spicetopia_v3_live.db")

print(f"Seeding database: {DB_PATH}")

# Date anchors — all relative to today so history stays realistic
TODAY       = date.today()
D           = lambda days: (TODAY + timedelta(days=days)).isoformat()
MONTH_AGO_3 = D(-90)
MONTH_AGO_2 = D(-60)
MONTH_AGO_1 = D(-30)
WEEK_AGO    = D(-7)
TODAY_S     = TODAY.isoformat()

# ── Connection ────────────────────────────────────────────────────────────────

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA foreign_keys = OFF")   # we'll manage FK integrity manually
conn.execute("PRAGMA journal_mode = WAL")
c = conn

# ── Guard: refuse to seed a live database ─────────────────────────────────────

existing_invoices = c.execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
if existing_invoices > 0:
    print(f"\n⛔  Database already contains {existing_invoices} invoice(s).")
    print("   This script will not overwrite live data.")
    print("   To re-seed, delete the database file and restart the server first.\n")
    sys.exit(1)

print("✓  Empty database confirmed — proceeding with seed.\n")

# ── Helpers ───────────────────────────────────────────────────────────────────

def r2(v): return round(v, 2)

def ins(table, row: dict):
    cols = ', '.join(row.keys())
    phs  = ', '.join(['?'] * len(row))
    cur  = c.execute(f"INSERT INTO {table} ({cols}) VALUES ({phs})", list(row.values()))
    return cur.lastrowid

def ins_or_ignore(table, row: dict):
    cols = ', '.join(row.keys())
    phs  = ', '.join(['?'] * len(row))
    cur  = c.execute(f"INSERT OR IGNORE INTO {table} ({cols}) VALUES ({phs})", list(row.values()))
    return cur.lastrowid

def bump_counter(entity, new_val):
    c.execute("INSERT OR IGNORE INTO id_counters (entity, last_num) VALUES (?,0)", (entity,))
    c.execute("UPDATE id_counters SET last_num=MAX(last_num,?) WHERE entity=?", (new_val, entity))

# ─────────────────────────────────────────────────────────────────────────────
# 1. REFERENCE DATA
# ─────────────────────────────────────────────────────────────────────────────

print("1/9  Reference data (pack sizes, price types, zones)...")

# Pack sizes
for label, grams in [('50g',50),('100g',100),('200g',200),('500g',500),('1kg',1000)]:
    ins_or_ignore('pack_sizes', {'label': label, 'grams': grams})

# Price types
for code, label in [('RETAIL','Retail Price'),('DIRECT','Direct / Wholesale Price')]:
    ins_or_ignore('price_types', {'code': code, 'label': label})

# Zones
zone_ids = {}
for name, city in [('Karachi Central','Karachi'),('Karachi West','Karachi'),
                   ('Lahore','Lahore'),('Islamabad','Islamabad')]:
    ins_or_ignore('zones', {'name': name, 'city': city, 'active': 1})

for row in c.execute("SELECT id, name FROM zones"):
    zone_ids[row['name']] = row['id']

c.commit()

# ─────────────────────────────────────────────────────────────────────────────
# 2. INGREDIENTS
# ─────────────────────────────────────────────────────────────────────────────

print("2/9  Ingredients...")

ingredients = [
    # code,       name,                 opening_g,  reorder_g,  cost_per_kg
    ('ING-001SP', 'Lal Mirch (Red Chili)',  50000,  20000,  850.00),
    ('ING-002SP', 'Dhania (Coriander)',     30000,  15000,  320.00),
    ('ING-003SP', 'Zeera (Cumin)',          15000,   8000, 1200.00),
    ('ING-004SP', 'Haldi (Turmeric)',       20000,  10000,  480.00),
    ('ING-005SP', 'Kali Mirch (Black Pepper)', 5000, 3000, 2200.00),
    ('ING-006SP', 'Elaichi (Cardamom)',      3000,   1500, 4500.00),
    ('ING-007SP', 'Laung (Cloves)',          2000,   1000, 3200.00),
    ('ING-008SP', 'Namak (Salt)',           25000,  10000,   45.00),
]

ing_ids = {}
for code, name, opening_g, reorder_g, cost_kg in ingredients:
    ins_or_ignore('ingredients', {
        'code': code, 'name': name,
        'opening_grams': opening_g, 'reorder_level': reorder_g,
        'cost_per_kg': cost_kg, 'active': 1,
        'created_at': MONTH_AGO_3,
    })

for row in c.execute("SELECT id, code FROM ingredients"):
    ing_ids[row['code']] = row['id']

# Opening stock via inventory_ledger
for code, _, opening_g, _, _ in ingredients:
    ins('inventory_ledger', {
        'ingredient_id': ing_ids[code],
        'movement_type': 'OPENING',
        'qty_grams':     opening_g,
        'reference_id':  'SEED-OPEN',
        'notes':         'Opening stock — staging seed',
        'created_at':    MONTH_AGO_3 + 'T08:00:00',
    })

c.commit()
bump_counter('ingredient', 8)

# ─────────────────────────────────────────────────────────────────────────────
# 3. PRODUCTS, VARIANTS, PRICES, BOMs
# ─────────────────────────────────────────────────────────────────────────────

print("3/9  Products, variants, prices and BOMs...")

# Fetch pack_size ids
ps_ids = {r['label']: r['id'] for r in c.execute("SELECT id, label FROM pack_sizes")}
pt_ids = {r['code']:  r['id'] for r in c.execute("SELECT id, code  FROM price_types")}

products = [
    # code,     name,                   urdu_name,          blend_code
    ('SP-LMP', 'Lal Mirch Powder',       'لال مرچ پاؤڈر',    'BLD-LMP-01'),
    ('SP-DHP', 'Dhania Powder',          'دھنیا پاؤڈر',       'BLD-DHP-01'),
    ('SP-HDP', 'Haldi Powder',           'ہلدی پاؤڈر',        'BLD-HDP-01'),
    ('SP-GM',  'Garam Masala',           'گرم مسالہ',         'BLD-GM-01'),
    ('SP-ZP',  'Zeera Powder',           'زیرہ پاؤڈر',        'BLD-ZP-01'),
]

prod_ids = {}
for code, name, urdu, blend in products:
    ins_or_ignore('products', {
        'code': code, 'name': name, 'name_urdu': urdu,
        'blend_code': blend, 'active': 1, 'created_at': MONTH_AGO_3,
    })

for row in c.execute("SELECT id, code FROM products"):
    prod_ids[row['code']] = row['id']

# Variants: each product in 50g, 100g, 200g, 500g, 1kg
# SKU format: SP-SKU-0001 ... (we manually assign to avoid counter conflict)
sku_counter = 1
variant_ids = {}   # (prod_code, pack_label) -> variant_id

for prod_code, _, _, _ in products:
    for pack_label in ['50g','100g','200g','500g','1kg']:
        sku = f"SP-SKU-{sku_counter:04d}"
        sku_counter += 1
        ins_or_ignore('product_variants', {
            'sku_code':     sku,
            'product_id':   prod_ids[prod_code],
            'pack_size_id': ps_ids[pack_label],
            'active_flag':  1,
        })

# Reload variant ids
for row in c.execute("""
    SELECT pv.id, p.code AS prod_code, ps.label AS pack_label
    FROM product_variants pv
    JOIN products p   ON p.id  = pv.product_id
    JOIN pack_sizes ps ON ps.id = pv.pack_size_id
"""):
    variant_ids[(row['prod_code'], row['pack_label'])] = row['id']

bump_counter('sku', sku_counter - 1)

# Prices  (RETAIL / DIRECT per pack, effective from 3 months ago)
# Structure: prod_code -> {pack_label -> (retail_price, direct_price)}
price_table = {
    'SP-LMP': {'50g':(55,45),   '100g':(95,80),    '200g':(180,150),  '500g':(420,360),  '1kg':(800,680)},
    'SP-DHP': {'50g':(40,33),   '100g':(70,58),    '200g':(130,110),  '500g':(310,260),  '1kg':(590,495)},
    'SP-HDP': {'50g':(45,37),   '100g':(80,66),    '200g':(150,125),  '500g':(360,300),  '1kg':(680,570)},
    'SP-GM':  {'50g':(65,54),   '100g':(115,96),   '200g':(220,185),  '500g':(520,440),  '1kg':(980,825)},
    'SP-ZP':  {'50g':(75,62),   '100g':(135,112),  '200g':(255,215),  '500g':(600,510),  '1kg':(1150,975)},
}

for prod_code, packs in price_table.items():
    for pack_label, (retail, direct) in packs.items():
        vid = variant_ids.get((prod_code, pack_label))
        if not vid: continue
        for pt_code, price in [('RETAIL', retail), ('DIRECT', direct)]:
            ins('product_prices', {
                'product_variant_id': vid,
                'price_type_id':      pt_ids[pt_code],
                'price':              price,
                'effective_from':     MONTH_AGO_3,
                'active_flag':        1,
            })

# BOMs — batch_size_grams = 1000g for all products
# bom_items.quantity_grams = grams of ingredient per 1000g of finished product
bom_recipes = {
    'SP-LMP': [('ING-001SP', 950), ('ING-008SP', 50)],
    'SP-DHP': [('ING-002SP', 1000)],
    'SP-HDP': [('ING-004SP', 1000)],
    'SP-GM':  [('ING-001SP', 300), ('ING-002SP', 300), ('ING-003SP', 200),
               ('ING-005SP', 100), ('ING-006SP', 50),  ('ING-007SP', 50)],
    'SP-ZP':  [('ING-003SP', 1000)],
}

bom_ids = {}
for prod_code, items in bom_recipes.items():
    bv_id = ins('bom_versions', {
        'product_id':       prod_ids[prod_code],
        'version_no':       1,
        'batch_size_grams': 1000,
        'effective_from':   MONTH_AGO_3,
        'active_flag':      1,
        'notes':            'Initial version — staging seed',
    })
    bom_ids[prod_code] = bv_id
    for ing_code, qty_g in items:
        ins('bom_items', {
            'bom_version_id': bv_id,
            'ingredient_id':  ing_ids[ing_code],
            'quantity_grams': qty_g,
        })

c.commit()

# ─────────────────────────────────────────────────────────────────────────────
# 4. SUPPLIERS
# ─────────────────────────────────────────────────────────────────────────────

print("4/9  Suppliers...")

suppliers_data = [
    ('SUP-001', 'Hussain Spice Traders',    'Muhammad Hussain',  '0321-2345678', 'hussain@spicetraders.pk', 'Karachi',  'Shop 14, Jodia Bazar, Karachi'),
    ('SUP-002', 'Punjab Agri Wholesale',    'Tariq Mehmood',     '0333-4567890', 'tariq@punjabagri.pk',     'Lahore',   'Plot 7, Sabzi Mandi, Lahore'),
    ('SUP-003', 'Sindh Farm Direct',        'Ali Hassan',        '0300-1234567', '',                        'Hyderabad','Village Nawabshah, Hyderabad'),
    ('SUP-004', 'Al-Madina Trading Co.',    'Arif Khan',         '0312-9876543', 'arif@almadina.pk',        'Karachi',  'Stall 22, Bolton Market, Karachi'),
]

sup_ids = {}
for code, name, contact, phone, email, city, address in suppliers_data:
    ins_or_ignore('suppliers', {
        'code': code, 'name': name, 'contact': contact,
        'phone': phone, 'email': email, 'city': city,
        'address': address, 'active_flag': 1,
        'created_at': MONTH_AGO_3,
    })

for row in c.execute("SELECT id, code FROM suppliers"):
    sup_ids[row['code']] = row['id']

c.commit()

# ─────────────────────────────────────────────────────────────────────────────
# 5. CUSTOMERS
# ─────────────────────────────────────────────────────────────────────────────

print("5/9  Customers...")

customers_data = [
    # code,       acct_no,       name,                    type,     city,          address,                             phone,          credit_limit, terms
    ('CUST-001', 'SPKHI-001', 'Super Mart Karachi',       'DIRECT', 'Karachi',     'Plot 45, PECHS Block 6, Karachi',   '021-34556677', 200000, 30),
    ('CUST-002', 'SPKHI-002', 'Al-Noor General Store',   'RETAIL', 'Karachi',     'Shop 12, Nazimabad No.3, Karachi',  '021-36781234', 50000,  30),
    ('CUST-003', 'SPLHE-001', 'Hameed Traders Lahore',   'RETAIL', 'Lahore',      'Main Market, Gulberg III, Lahore',  '042-35761122', 80000,  45),
    ('CUST-004', 'SPKHI-003', 'City Grocers',            'RETAIL', 'Karachi',     'Block 10, F.B. Area, Karachi',      '021-36912233', 60000,  30),
    ('CUST-005', 'SPLHE-002', 'Metro Wholesale Lahore',  'DIRECT', 'Lahore',      'Scheme Mor, Ferozepur Road, Lahore','042-37889900', 300000, 45),
    ('CUST-006', 'SPISM-001', 'Nasir Brothers',          'RETAIL', 'Islamabad',   'I-8 Markaz, Islamabad',             '051-2855500',  40000,  30),
]

cust_ids   = {}
cust_codes = {}
cust_types = {}
for code, acct, name, ctype, city, address, phone, credit, terms in customers_data:
    ins_or_ignore('customers', {
        'code': code, 'account_number': acct, 'name': name,
        'customer_type': ctype, 'city': city, 'address': address,
        'phone': phone, 'credit_limit': credit,
        'payment_terms_days': terms, 'active': 1,
        'created_at': MONTH_AGO_3,
    })

for row in c.execute("SELECT id, code, name, customer_type FROM customers"):
    cust_ids[row['code']]   = row['id']
    cust_codes[row['code']] = row['code']
    cust_types[row['code']] = row['customer_type']

# Seed account number counters
bump_counter('acct_KHI', 3)
bump_counter('acct_LHE', 2)
bump_counter('acct_ISM', 1)

c.commit()

# ─────────────────────────────────────────────────────────────────────────────
# 6. SUPPLIER BILLS & INVENTORY RECEIPTS
# ─────────────────────────────────────────────────────────────────────────────

print("6/9  Supplier bills and inventory receipts...")

def make_bill(bill_num, sup_code, bill_date, due_date, sup_ref, notes,
              items, status, paid_amount=0, pay_date=None, pay_mode='BANK_TRANSFER'):
    """
    items: list of (ing_code, qty_kg, unit_cost_kg)
    """
    total = r2(sum(qty * cost for _, qty, cost in items))
    bill_id = ins('supplier_bills', {
        'bill_number':  bill_num,
        'supplier_id':  sup_ids[sup_code],
        'bill_date':    bill_date,
        'due_date':     due_date,
        'status':       status,
        'notes':        notes,
        'total_amount': total,
        'supplier_ref': sup_ref,
        'created_at':   bill_date + 'T09:00:00',
    })
    for ing_code, qty_kg, unit_cost in items:
        line = r2(qty_kg * unit_cost)
        ins('supplier_bill_items', {
            'bill_id':      bill_id,
            'ingredient_id': ing_ids[ing_code],
            'quantity_kg':  qty_kg,
            'unit_cost_kg': unit_cost,
            'line_total':   line,
        })
        # Inventory receipt
        ins('inventory_ledger', {
            'ingredient_id': ing_ids[ing_code],
            'movement_type': 'PURCHASE_IN',
            'qty_grams':     r2(qty_kg * 1000),
            'reference_id':  bill_num,
            'notes':         f'Received against {bill_num}',
            'created_at':    bill_date + 'T10:00:00',
        })
    # Payment if any
    if paid_amount and paid_amount > 0:
        pay_ref = bill_num.replace('SP-BILL-', 'SP-SPAY-')
        pay_id  = ins('supplier_payments', {
            'payment_ref':  pay_ref,
            'supplier_id':  sup_ids[sup_code],
            'payment_date': pay_date or bill_date,
            'amount':       paid_amount,
            'payment_mode': pay_mode,
            'notes':        f'Payment for {bill_num}',
            'created_at':   (pay_date or bill_date) + 'T11:00:00',
        })
        ins('supplier_payment_allocations', {
            'payment_id':       pay_id,
            'bill_id':          bill_id,
            'allocated_amount': r2(min(paid_amount, total)),
        })
    return bill_id

# Bill 1 — 3 months ago, Hussain Spice Traders, PAID
make_bill(
    'SP-BILL-0001', 'SUP-001',
    bill_date=MONTH_AGO_3, due_date=D(-60), sup_ref='HST-INV-3841', notes='Bulk purchase Jan',
    items=[('ING-001SP', 100, 850),   # 100kg lal mirch
           ('ING-002SP',  50, 320),   # 50kg dhania
           ('ING-004SP',  40, 480)],  # 40kg haldi
    status='PAID', paid_amount=120200, pay_date=D(-85), pay_mode='BANK_TRANSFER'
)

# Bill 2 — 2 months ago, Punjab Agri, PAID
make_bill(
    'SP-BILL-0002', 'SUP-002',
    bill_date=MONTH_AGO_2, due_date=D(-30), sup_ref='PAW-2024-0219', notes='Feb restock',
    items=[('ING-003SP',  25, 1200),  # 25kg zeera
           ('ING-005SP',  10, 2200),  # 10kg kali mirch
           ('ING-006SP',   5, 4500)], # 5kg elaichi
    status='PAID', paid_amount=74500, pay_date=D(-55), pay_mode='BANK_TRANSFER'
)

# Bill 3 — 1 month ago, Hussain Spice Traders, PARTIAL
make_bill(
    'SP-BILL-0003', 'SUP-001',
    bill_date=MONTH_AGO_1, due_date=D(0), sup_ref='HST-INV-4102', notes='March bulk — partial paid',
    items=[('ING-001SP',  80, 860),   # 80kg lal mirch (price increased)
           ('ING-002SP',  40, 330),   # 40kg dhania
           ('ING-008SP',  50,  45)],  # 50kg namak
    status='PARTIAL', paid_amount=50000, pay_date=D(-25), pay_mode='CHEQUE'
)

# Bill 4 — 1 week ago, Al-Madina Trading, UNPAID (recent delivery)
make_bill(
    'SP-BILL-0004', 'SUP-004',
    bill_date=WEEK_AGO, due_date=D(23), sup_ref='AMT-0088', notes='Premium spice restock',
    items=[('ING-007SP',   8, 3200),  # 8kg laung
           ('ING-005SP',   5, 2250)], # 5kg kali mirch (slight price rise)
    status='UNPAID', paid_amount=0
)

c.commit()
bump_counter('bill', 4)
bump_counter('spay', 3)

# ─────────────────────────────────────────────────────────────────────────────
# 7. PRODUCTION BATCHES
# ─────────────────────────────────────────────────────────────────────────────

print("7/9  Production batches...")

def make_batch(batch_id_str, prod_code, pack_label, batch_date,
               qty_grams, qty_units, mfg_date, best_before, notes, unit_cost):
    """Create a production batch + consumption entries."""
    vid    = variant_ids[(prod_code, pack_label)]
    bv_id  = bom_ids[prod_code]
    pid    = prod_ids[prod_code]

    # Scale factor: qty_grams / 1000 (our BOM is per 1000g)
    scale = qty_grams / 1000.0

    b_id = ins('production_batches', {
        'batch_id':             batch_id_str,
        'batch_date':           batch_date,
        'product_id':           pid,
        'product_variant_id':   vid,
        'bom_version_id':       bv_id,
        'qty_grams':            qty_grams,
        'qty_units':            qty_units,
        'pack_size':            pack_label,
        'mfg_date':             mfg_date,
        'best_before':          best_before,
        'notes':                notes,
        'unit_cost_at_posting': unit_cost,
        'created_at':           batch_date + 'T14:00:00',
    })

    # Consumption from BOM
    bom_items_list = list(c.execute(
        "SELECT ingredient_id, quantity_grams FROM bom_items WHERE bom_version_id=?", (bv_id,)
    ))
    for bi in bom_items_list:
        used_g = r2(bi['quantity_grams'] * scale)
        ins('production_consumption', {
            'batch_id':     b_id,
            'ingredient_id': bi['ingredient_id'],
            'qty_grams':    used_g,
        })
        ins('inventory_ledger', {
            'ingredient_id': bi['ingredient_id'],
            'movement_type': 'PRODUCTION_USE',
            'qty_grams':    -used_g,
            'reference_id':  batch_id_str,
            'notes':         f'Used in batch {batch_id_str}',
            'created_at':    batch_date + 'T14:30:00',
        })
    return b_id

# Batch 1 — Feb, LMP 200g, 50kg batch → 250 units
make_batch('SP-BATCH-0001', 'SP-LMP', '200g',
           batch_date=D(-65), qty_grams=50000, qty_units=250,
           mfg_date=D(-65), best_before=D(300), unit_cost=8.37,
           notes='Lal Mirch 200g batch — Feb run')

# Batch 2 — Feb, Garam Masala 100g, 20kg batch → 200 units
make_batch('SP-BATCH-0002', 'SP-GM', '100g',
           batch_date=D(-63), qty_grams=20000, qty_units=200,
           mfg_date=D(-63), best_before=D(302), unit_cost=9.82,
           notes='Garam Masala 100g batch — Feb run')

# Batch 3 — Mar, Dhania Powder 100g, 40kg → 400 units
make_batch('SP-BATCH-0003', 'SP-DHP', '100g',
           batch_date=D(-35), qty_grams=40000, qty_units=400,
           mfg_date=D(-35), best_before=D(330), unit_cost=3.30,
           notes='Dhania Powder 100g batch — Mar run')

# Batch 4 — Mar, Haldi Powder 200g, 30kg → 150 units
make_batch('SP-BATCH-0004', 'SP-HDP', '200g',
           batch_date=D(-33), qty_grams=30000, qty_units=150,
           mfg_date=D(-33), best_before=D(332), unit_cost=9.60,
           notes='Haldi Powder 200g batch — Mar run')

# Batch 5 — Recent, LMP 50g, 30kg → 600 units
make_batch('SP-BATCH-0005', 'SP-LMP', '50g',
           batch_date=D(-10), qty_grams=30000, qty_units=600,
           mfg_date=D(-10), best_before=D(355), unit_cost=2.08,
           notes='Lal Mirch 50g batch — recent run')

c.commit()
bump_counter('batch', 5)

# ─────────────────────────────────────────────────────────────────────────────
# 8. CUSTOMER INVOICES, SALES & PAYMENTS
# ─────────────────────────────────────────────────────────────────────────────

print("8/9  Customer invoices, sales and payments...")

sale_counter    = 0
invoice_counter = 0
payment_counter = 0

def _next_sale():
    global sale_counter
    sale_counter += 1
    return f"SP-SALE-{sale_counter:04d}"

def _next_inv():
    global invoice_counter
    invoice_counter += 1
    return f"SP-INV-{invoice_counter:04d}"

def _next_pay():
    global payment_counter
    payment_counter += 1
    return f"SP-PAY-{payment_counter:04d}"

def make_invoice(inv_date, due_date, cust_code, status, lines,
                 paid_amount=0, pay_date=None, pay_mode='BANK_TRANSFER', notes=''):
    """
    lines: list of (prod_code, pack_label, qty, unit_price)
    Creates: invoice, invoice_items, sales, optionally payment + allocation.
    """
    inv_num = _next_inv()
    cust_id = cust_ids[cust_code]
    cust_name = c.execute("SELECT name FROM customers WHERE id=?", (cust_id,)).fetchone()[0]
    cust_type = cust_types[cust_code]

    inv_id = ins('invoices', {
        'invoice_number': inv_num,
        'customer_id':    cust_id,
        'invoice_date':   inv_date,
        'due_date':       due_date,
        'status':         status,
        'notes':          notes,
        'created_at':     inv_date + 'T10:00:00',
    })

    for prod_code, pack_label, qty, unit_price in lines:
        vid        = variant_ids[(prod_code, pack_label)]
        pv_row     = c.execute("SELECT p.code, p.name, ps.label FROM product_variants pv JOIN products p ON p.id=pv.product_id JOIN pack_sizes ps ON ps.id=pv.pack_size_id WHERE pv.id=?", (vid,)).fetchone()
        line_total = r2(qty * unit_price)
        sale_id    = _next_sale()

        ins('invoice_items', {
            'invoice_id':         inv_id,
            'product_variant_id': vid,
            'product_code':       pv_row['code'],
            'product_name':       pv_row['name'],
            'pack_size':          pv_row['label'],
            'quantity':           qty,
            'unit_price':         unit_price,
            'line_total':         line_total,
            'sale_id':            sale_id,
        })

        ins('sales', {
            'sale_id':            sale_id,
            'sale_date':          inv_date,
            'customer_id':        cust_id,
            'cust_code':          cust_code,
            'cust_name':          cust_name,
            'customer_type':      cust_type,
            'product_variant_id': vid,
            'product_code':       pv_row['code'],
            'product_name':       pv_row['name'],
            'pack_size':          pv_row['label'],
            'qty':                qty,
            'unit_price':         unit_price,
            'total':              line_total,
            'invoice_id':         inv_id,
            'notes':              f'Seeded — {inv_num}',
            'voided':             0,
            'created_at':         inv_date + 'T10:30:00',
        })

    # Payment
    if paid_amount and paid_amount > 0:
        pay_ref = _next_pay()
        subtotal = sum(r2(q * up) for _, _, q, up in lines)
        total_with_gst = r2(subtotal * 1.18)
        pay_id  = ins('customer_payments', {
            'payment_ref':  pay_ref,
            'customer_id':  cust_id,
            'payment_date': pay_date or inv_date,
            'amount':       paid_amount,
            'payment_mode': pay_mode,
            'notes':        f'Payment for {inv_num}',
            'created_at':   (pay_date or inv_date) + 'T15:00:00',
        })
        ins('payment_allocations', {
            'payment_id':       pay_id,
            'invoice_id':       inv_id,
            'allocated_amount': r2(min(paid_amount, total_with_gst)),
        })
    return inv_id

# ── Invoice 1: Super Mart — Feb, PAID (Direct client, 30 day terms) ──────────
make_invoice(
    inv_date=D(-65), due_date=D(-35), cust_code='CUST-001', status='PAID',
    lines=[
        ('SP-LMP', '200g', 50, 150.00),   # 7,500
        ('SP-GM',  '100g', 30, 110.00),   # 3,300
    ],
    paid_amount=12744,   # (10,800 subtotal × 1.18 GST = 12,744)
    pay_date=D(-55), pay_mode='BANK_TRANSFER',
    notes='Feb bulk order — Super Mart'
)

# ── Invoice 2: Al-Noor General Store — Feb, PAID ──────────────────────────────
make_invoice(
    inv_date=D(-62), due_date=D(-32), cust_code='CUST-002', status='PAID',
    lines=[
        ('SP-LMP', '200g', 20, 180.00),   # 3,600
        ('SP-DHP', '100g', 15,  95.00),   # 1,425
    ],
    paid_amount=6_000,   # approx (5,025 × 1.18 = 5,929.50 → rounded up)
    pay_date=D(-50), pay_mode='CASH',
    notes='Feb retail order — Al-Noor'
)

# ── Invoice 3: City Grocers — Mar, UNPAID (overdue) ──────────────────────────
make_invoice(
    inv_date=D(-35), due_date=D(-5), cust_code='CUST-004', status='UNPAID',
    lines=[
        ('SP-DHP', '100g', 30,  95.00),   # 2,850
        ('SP-HDP', '200g', 20, 160.00),   # 3,200
    ],
    paid_amount=0,
    notes='Mar retail order — City Grocers (overdue)'
)

# ── Invoice 4: Hameed Traders — Mar, PARTIAL ─────────────────────────────────
make_invoice(
    inv_date=D(-32), due_date=D(-2), cust_code='CUST-003', status='PARTIAL',
    lines=[
        ('SP-GM',  '100g', 40, 130.00),   # 5,200
        ('SP-LMP', '200g', 25, 180.00),   # 4,500
    ],
    paid_amount=6_000,
    pay_date=D(-20), pay_mode='CHEQUE',
    notes='Mar Lahore order — partial payment received'
)

# ── Invoice 5: Metro Wholesale — recent, UNPAID (large order) ────────────────
make_invoice(
    inv_date=D(-8), due_date=D(37), cust_code='CUST-005', status='UNPAID',
    lines=[
        ('SP-LMP', '200g', 60, 150.00),   # 9,000
        ('SP-DHP', '100g', 50,  80.00),   # 4,000
        ('SP-HDP', '200g', 30, 135.00),   # 4,050
    ],
    paid_amount=0,
    notes='Large Lahore wholesale order — Apr'
)

# ── Invoice 6: Nasir Brothers — Mar, PAID (small retail) ─────────────────────
make_invoice(
    inv_date=D(-28), due_date=D(2), cust_code='CUST-006', status='PAID',
    lines=[
        ('SP-LMP', '50g',  20, 55.00),    # 1,100
        ('SP-GM',  '100g', 10, 130.00),   # 1,300
    ],
    paid_amount=2_832,   # (2,400 × 1.18 = 2,832)
    pay_date=D(-15), pay_mode='BANK_TRANSFER',
    notes='Islamabad retail order — Nasir Brothers'
)

# ── Invoice 7: Super Mart — recent, UNPAID (repeat order) ────────────────────
make_invoice(
    inv_date=D(-5), due_date=D(25), cust_code='CUST-001', status='UNPAID',
    lines=[
        ('SP-LMP', '200g', 80, 150.00),   # 12,000
        ('SP-GM',  '100g', 50, 110.00),   # 5,500
        ('SP-ZP',  '100g', 20, 112.00),   # 2,240  (Zeera Powder — no stock yet, test scenario)
    ],
    paid_amount=0,
    notes='Apr bulk order — Super Mart (pending dispatch)'
)

c.commit()
bump_counter('invoice', invoice_counter)
bump_counter('sale',    sale_counter)
bump_counter('payment', payment_counter)

# ─────────────────────────────────────────────────────────────────────────────
# 9. FINALISE COUNTERS & VERIFY
# ─────────────────────────────────────────────────────────────────────────────

print("9/9  Finalising counters and verifying...")

# Verify critical table counts
checks = [
    ('ingredients',         8),
    ('products',            5),
    ('product_variants',    25),
    ('customers',           6),
    ('suppliers',           4),
    ('supplier_bills',      4),
    ('production_batches',  5),
    ('invoices',            7),
    ('sales',               None),   # just check > 0
]

all_ok = True
for table, expected in checks:
    count = c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if expected is None:
        ok = count > 0
    else:
        ok = count == expected
    status = '✓' if ok else '✗'
    if not ok:
        all_ok = False
    print(f"   {status}  {table}: {count}" + (f" (expected {expected})" if expected and not ok else ""))

# Show inventory balances
print("\n   Raw material balances after seed:")
rows = c.execute("""
    SELECT i.code, i.name,
           ROUND(SUM(il.qty_grams),0) AS balance_g
    FROM ingredients i
    LEFT JOIN inventory_ledger il ON il.ingredient_id = i.id
    GROUP BY i.id
    ORDER BY i.code
""").fetchall()
for r in rows:
    print(f"   {r['code']}  {r['name']:<28}  {r['balance_g'] or 0:>9,.0f} g")

# Show finished goods
print("\n   Finished goods balances after seed:")
fg_rows = c.execute("""
    SELECT p.code, p.name, ps.label AS pack,
           COALESCE(pb.produced,0) - COALESCE(sl.sold,0) AS available
    FROM products p
    JOIN product_variants pv ON pv.product_id = p.id
    JOIN pack_sizes ps        ON ps.id         = pv.pack_size_id
    LEFT JOIN (
        SELECT product_variant_id, SUM(qty_units) AS produced
        FROM production_batches GROUP BY product_variant_id
    ) pb ON pb.product_variant_id = pv.id
    LEFT JOIN (
        SELECT product_variant_id, SUM(qty) AS sold
        FROM sales WHERE voided=0 GROUP BY product_variant_id
    ) sl ON sl.product_variant_id = pv.id
    WHERE COALESCE(pb.produced,0) > 0
    ORDER BY p.code, ps.grams
""").fetchall()
for r in fg_rows:
    print(f"   {r['code']}  {r['name']:<22}  {r['pack']:<5}  {int(r['available']):>5} units")

c.commit()
c.close()

if all_ok:
    print(f"\n{'='*60}")
    print("  ✅  Seed complete — database is ready for testing.")
    print(f"{'='*60}\n")
else:
    print(f"\n{'='*60}")
    print("  ⚠️   Seed completed with warnings — check counts above.")
    print(f"{'='*60}\n")
