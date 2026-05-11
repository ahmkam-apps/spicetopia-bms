"""
modules/purchasing.py
─────────────────────
AP (accounts payable) lifecycle: supplier bills, payments, allocation,
adjustments, void, AP aging, Purchase Orders (GRN 3-way match), BOM
ingredient calculator, and PO PDF generation.

Extracted from server.py — Sprint 11.
Pattern: bottom-override. Originals remain in server.py as fallback;
these versions win at runtime via `from modules.purchasing import *`.
"""

import json
from datetime import datetime, timedelta, date

from modules.db    import _conn, qry, qry1, run, run_many, save_db, audit_log
from modules.utils import r2, fmtpkr, today, validate_fields, _log

__all__ = [
    # Bill balance / status
    'compute_bill_balance', '_compute_bill_status', '_sync_bill_status',
    # AP aging
    'get_ap_aging',
    # Supplier bills
    'create_supplier_bill', 'update_supplier_bill',
    # AP payments & allocation
    'record_supplier_payment', 'allocate_supplier_payment',
    'pay_bill_direct', 'deallocate_supplier_payment', 'adjust_bill',
    # Void
    'void_supplier_bill',
    # Purchase Orders
    'list_purchase_orders', 'get_purchase_order',
    'create_purchase_order', 'update_purchase_order',
    'update_purchase_order_status',
    # BOM calculator
    'bom_calculate_ingredients',
    # PDF
    'generate_po_pdf',
]


# ═══════════════════════════════════════════════════════════════════
#  BILL BALANCE & STATUS HELPERS
# ═══════════════════════════════════════════════════════════════════

def compute_bill_balance(bill_id):
    """Returns (total, paid, balance).
    Uses the larger of items-sum or stored total_amount, so zero-cost-item bills
    still track their correct total (stored at bill creation).
    """
    items_row = qry1(
        "SELECT COALESCE(SUM(line_total),0) as t FROM supplier_bill_items WHERE bill_id=?",
        (bill_id,)
    )
    items_sum = r2(items_row['t'])

    bill_row = qry1(
        "SELECT COALESCE(total_amount,0) as ta FROM supplier_bills WHERE id=?",
        (bill_id,)
    )
    stored_total = r2(bill_row['ta']) if bill_row else 0

    total = max(items_sum, stored_total)

    paid_row = qry1(
        "SELECT COALESCE(SUM(allocated_amount),0) as p FROM supplier_payment_allocations WHERE bill_id=?",
        (bill_id,)
    )
    paid    = r2(paid_row['p'])
    balance = r2(total - paid)
    return total, paid, balance


def _compute_bill_status(bill_id) -> str:
    """
    Derive the correct supplier bill status purely from the numbers — never from a stored flag.
    Returns 'UNPAID', 'PARTIAL', or 'PAID'.
    """
    total, paid, balance = compute_bill_balance(bill_id)
    if paid <= 0:
        return 'UNPAID'
    if balance > 0.001:
        return 'PARTIAL'
    return 'PAID'


def _sync_bill_status(bill_id) -> str:
    """
    Compute supplier bill status from amounts and write it to the DB.
    Returns the new status string.
    Preserves VOID status — never overwrites a voided bill.
    """
    existing = qry1("SELECT status FROM supplier_bills WHERE id=?", (bill_id,))
    if not existing or existing['status'] == 'VOID':
        return existing['status'] if existing else 'VOID'
    new_status = _compute_bill_status(bill_id)
    run("UPDATE supplier_bills SET status=? WHERE id=?", (new_status, bill_id))
    return new_status


# ═══════════════════════════════════════════════════════════════════
#  AP AGING
# ═══════════════════════════════════════════════════════════════════

def get_ap_aging():
    """Returns aging buckets for all unpaid/partial bills."""
    bills = qry("""
        SELECT sb.id, sb.bill_number, sb.supplier_id, sb.bill_date, sb.due_date,
               sb.status, s.name as supplier_name
        FROM supplier_bills sb JOIN suppliers s ON s.id = sb.supplier_id
        WHERE sb.status IN ('UNPAID','PARTIAL')
        ORDER BY sb.due_date
    """)
    today_dt = date.today()
    result = []
    for bill in bills:
        total, paid, balance = compute_bill_balance(bill['id'])
        if balance <= 0:
            continue
        try:
            due = date.fromisoformat(bill['due_date'])
            days_over = (today_dt - due).days
        except Exception:
            days_over = 0
        bucket = 'current' if days_over <= 0 else (
                 '1_30'   if days_over <= 30  else (
                 '31_60'  if days_over <= 60  else (
                 '61_90'  if days_over <= 90  else '90plus')))
        result.append({**bill, 'total': total, 'paid': paid, 'balance': balance,
                        'days_overdue': days_over, 'aging_bucket': bucket})
    return result


# ═══════════════════════════════════════════════════════════════════
#  SUPPLIER BILLS
# ═══════════════════════════════════════════════════════════════════

def create_supplier_bill(data):
    """
    Create a supplier bill + items + auto inventory_ledger PURCHASE_IN entries.
    data: {supplierId, billDate, dueDate, supplierRef, notes,
           items:[{ingredientId, quantityKg, unitCostKg}]}
    """
    from modules.id_gen import next_id, _sync_counter_to_max

    validate_fields(data, [
        {'field': 'supplierId', 'label': 'Supplier',  'type': 'int', 'min': 1},
        {'field': 'billDate',   'label': 'Bill date', 'type': 'date'},
        {'field': 'dueDate',    'label': 'Due date',  'type': 'date'},
    ])
    sup = qry1("SELECT * FROM suppliers WHERE id=?", (data.get('supplierId'),))
    if not sup:
        raise ValueError("Supplier not found")
    items = data.get('items', [])
    if not items:
        raise ValueError("Bill must have at least one item")

    bill_date    = data.get('billDate', today())
    due_date     = data.get('dueDate', '')
    if not due_date:
        raise ValueError("Due date is required")

    supplier_ref = (data.get('supplierRef') or '').strip()
    if supplier_ref:
        existing = qry1(
            "SELECT bill_number FROM supplier_bills WHERE supplier_id=? AND supplier_ref=?",
            (sup['id'], supplier_ref)
        )
        if existing:
            raise ValueError(f"Duplicate: Supplier ref '{supplier_ref}' already recorded as {existing['bill_number']}")

    _sync_counter_to_max('bill', 'supplier_bills', 'bill_number', 'SP-BILL-')
    bill_num = next_id('bill', 'BILL')

    c = _conn()
    try:
        computed_total = r2(sum(
            r2(float(item.get('quantityKg', 0)) * float(item.get('unitCostKg', 0)))
            for item in items
        ))

        c.execute("""
            INSERT INTO supplier_bills
                (bill_number, supplier_id, bill_date, due_date, status, notes, total_amount, supplier_ref)
            VALUES (?,?,?,?,'UNPAID',?,?,?)
        """, (bill_num, sup['id'], bill_date, due_date,
              data.get('notes', ''), computed_total, supplier_ref))
        bill_db_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        for item in items:
            ing = qry1("SELECT * FROM ingredients WHERE id=?", (item.get('ingredientId'),))
            if not ing:
                raise ValueError(f"Ingredient not found: {item.get('ingredientId')}")
            qty_kg     = r2(item.get('quantityKg', 0))
            unit_cost  = r2(item.get('unitCostKg', 0))
            line_total = r2(qty_kg * unit_cost)
            qty_grams  = r2(qty_kg * 1000)

            c.execute("""
                INSERT INTO supplier_bill_items
                    (bill_id, ingredient_id, quantity_kg, unit_cost_kg, line_total)
                VALUES (?,?,?,?,?)
            """, (bill_db_id, ing['id'], qty_kg, unit_cost, line_total))

            c.execute("""
                INSERT INTO inventory_ledger
                    (ingredient_id, movement_type, qty_grams, reference_id, notes)
                VALUES (?,?,?,?,?)
            """, (ing['id'], 'PURCHASE_IN', qty_grams, bill_num,
                  f"Purchase from {sup['name']}"))

        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('supplier_bills',?,'INSERT',?)
        """, (bill_num, json.dumps({'supplierId': sup['id'], 'items': len(items)})))
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()

    total = r2(qry1(
        "SELECT COALESCE(SUM(line_total),0) as t FROM supplier_bill_items WHERE bill_id=?",
        (bill_db_id,)
    )['t'])
    return {'billNumber': bill_num, 'billId': bill_db_id, 'total': total}


def update_supplier_bill(bill_id, data):
    """Edit supplier bill header fields: due_date, notes, supplier_ref.
    Only allowed on UNPAID or PARTIAL bills."""
    bill = qry1("SELECT * FROM supplier_bills WHERE id=?", (bill_id,))
    if not bill:
        raise ValueError("Supplier bill not found")
    if bill['status'] not in ('UNPAID', 'PARTIAL'):
        raise ValueError(f"Cannot edit a {bill['status']} bill")
    set_parts, vals = [], []
    if 'dueDate' in data:
        set_parts.append("due_date=?"); vals.append(data['dueDate'])
    if 'notes' in data:
        set_parts.append("notes=?"); vals.append(str(data.get('notes', '')).strip())
    if 'supplierRef' in data:
        set_parts.append("supplier_ref=?"); vals.append(str(data.get('supplierRef', '')).strip())
    if not set_parts:
        return bill
    vals.append(bill_id)
    run(f"UPDATE supplier_bills SET {', '.join(set_parts)} WHERE id=?", vals)
    save_db()
    return qry1("SELECT * FROM supplier_bills WHERE id=?", (bill_id,))


# ═══════════════════════════════════════════════════════════════════
#  VOID SUPPLIER BILL
# ═══════════════════════════════════════════════════════════════════

def void_supplier_bill(bill_id: int, note: str, username: str):
    """
    Void a supplier bill.  Rules:
    • Cannot void a PAID bill.
    • Cannot void an already-VOID bill.
    • Marks status = VOID, records voided_at / voided_by / void_note.
    • Creates reversing inventory_ledger entries (positive) for every ingredient
      that was received on this bill, so physical stock is restored.
    • Deletes supplier_payment_allocations for this bill.
    • Writes a change_log audit entry.
    """
    bill = qry1("SELECT * FROM supplier_bills WHERE id=?", (bill_id,))
    if not bill:
        raise ValueError(f"Bill not found: {bill_id}")
    if bill['status'] == 'VOID':
        raise ValueError(f"{bill['bill_number']} is already void")
    if bill['status'] == 'PAID':
        raise ValueError(
            f"{bill['bill_number']} is fully paid — it cannot be voided. "
            "Record a debit note instead."
        )

    now  = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    note = (note or '').strip() or 'No reason given'
    items = qry("SELECT * FROM supplier_bill_items WHERE bill_id=?", (bill_id,))

    c = _conn()
    try:
        c.execute("""
            UPDATE supplier_bills
            SET status='VOID', voided_at=?, voided_by=?, void_note=?
            WHERE id=?
        """, (now, username, note, bill_id))

        for item in items:
            reversal_grams = r2(-item['quantity_kg'] * 1000)
            c.execute("""
                INSERT INTO inventory_ledger
                    (ingredient_id, movement_type, qty_grams, reference_id, notes)
                VALUES (?, 'ADJUSTMENT', ?, ?, ?)
            """, (item['ingredient_id'], reversal_grams, bill['bill_number'],
                  f"Void of bill {bill['bill_number']} — {note}"))

        c.execute("DELETE FROM supplier_payment_allocations WHERE bill_id=?", (bill_id,))

        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('supplier_bills', ?, 'VOID', ?)
        """, (bill['bill_number'], json.dumps({
            'void_note': note, 'voided_by': username,
            'voided_at': now, 'previous_status': bill['status'],
            'items_reversed': len(items)
        })))
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()
    _log('info', 'bill_voided', bill=bill['bill_number'], by=username, status=bill['status'])
    return {'ok': True, 'bill_number': bill['bill_number']}


# ═══════════════════════════════════════════════════════════════════
#  AP PAYMENTS & ALLOCATION
# ═══════════════════════════════════════════════════════════════════

def record_supplier_payment(data):
    """Record a payment to a supplier."""
    from modules.id_gen import next_id, _sync_counter_to_max

    sup = qry1("SELECT * FROM suppliers WHERE id=?", (data.get('supplierId'),))
    if not sup:
        raise ValueError("Supplier not found")
    amount = r2(data.get('amount', 0))
    if amount <= 0:
        raise ValueError("Amount must be positive")

    _sync_counter_to_max('spay', 'supplier_payments', 'payment_ref', 'SP-SPAY-')
    pay_ref = next_id('spay', 'SPAY')
    ops = [("""
        INSERT INTO supplier_payments
            (payment_ref, supplier_id, payment_date, amount, payment_mode, notes)
        VALUES (?,?,?,?,?,?)
    """, (pay_ref, sup['id'], data.get('paymentDate', today()),
          amount, data.get('paymentMode', 'BANK_TRANSFER'), data.get('notes', '')))]
    audit_log(ops, 'supplier_payments', pay_ref, 'INSERT', new_val=data)
    run_many(ops)
    return qry1("SELECT * FROM supplier_payments WHERE payment_ref=?", (pay_ref,))


def allocate_supplier_payment(payment_id, bill_id, amount):
    """Allocate a supplier payment to a bill."""
    pay = qry1("SELECT * FROM supplier_payments WHERE id=?", (payment_id,))
    if not pay:
        raise ValueError("Payment not found")
    bill = qry1("SELECT * FROM supplier_bills WHERE id=?", (bill_id,))
    if not bill:
        raise ValueError("Bill not found")

    already_alloc = r2(qry1(
        "SELECT COALESCE(SUM(allocated_amount),0) as s FROM supplier_payment_allocations WHERE payment_id=?",
        (payment_id,)
    )['s'])
    available = r2(pay['amount'] - already_alloc)
    if amount > available + 0.001:
        raise ValueError(f"Exceeds available payment balance: {available:.2f}")

    bill_total, bill_paid, bill_balance = compute_bill_balance(bill_id)
    if amount > bill_balance + 0.001:
        raise ValueError(f"Exceeds bill balance due: {bill_balance:.2f}")

    amount = r2(min(amount, available, bill_balance))

    ops = [("""
        INSERT INTO supplier_payment_allocations (payment_id, bill_id, allocated_amount)
        VALUES (?,?,?)
        ON CONFLICT(payment_id, bill_id) DO UPDATE SET
            allocated_amount = allocated_amount + excluded.allocated_amount
    """, (payment_id, bill_id, amount))]
    audit_log(ops, 'supplier_payment_allocations', f"{payment_id}-{bill_id}", 'INSERT')
    run_many(ops)

    new_status = _sync_bill_status(bill_id)
    return {'allocated': amount, 'billStatus': new_status}


def pay_bill_direct(bill_id, data):
    """Record a new supplier payment AND immediately allocate it to a specific bill."""
    from modules.id_gen import next_id, _sync_counter_to_max

    bill = qry1("SELECT * FROM supplier_bills WHERE id=?", (bill_id,))
    if not bill:
        raise ValueError("Bill not found")
    if bill['status'] == 'PAID':
        raise ValueError("Bill is already fully paid")

    bill_total, bill_paid, bill_balance = compute_bill_balance(bill_id)
    amount = r2(data.get('amount', bill_balance if bill_balance > 0 else 0))
    if amount <= 0:
        raise ValueError("Payment amount must be positive")

    # If bill has no cost on record (zero-cost PO), update total_amount to this payment
    if bill_total <= 0.001:
        c = _conn()
        try:
            c.execute("UPDATE supplier_bills SET total_amount=? WHERE id=?", (amount, bill_id))
            c.commit()
        finally:
            c.close()
        save_db()
        bill_balance = amount

    _sync_counter_to_max('spay', 'supplier_payments', 'payment_ref', 'SP-SPAY-')
    pay_ref  = next_id('spay', 'SPAY')
    pay_date = data.get('paymentDate', today())
    ops = [("""INSERT INTO supplier_payments
                (payment_ref, supplier_id, payment_date, amount, payment_mode, notes)
               VALUES (?,?,?,?,?,?)""",
            (pay_ref, bill['supplier_id'], pay_date, amount,
             data.get('paymentMode', 'BANK_TRANSFER'), data.get('notes', '')))]
    audit_log(ops, 'supplier_payments', pay_ref, 'INSERT', new_val=data)
    run_many(ops)

    pay = qry1("SELECT * FROM supplier_payments WHERE payment_ref=?", (pay_ref,))
    alloc_amount = r2(min(amount, bill_balance)) if bill_balance > 0 else amount
    result = allocate_supplier_payment(pay['id'], bill_id, alloc_amount)
    return {'paymentRef': pay_ref, 'paymentId': pay['id'],
            'allocated': alloc_amount, 'billStatus': result['billStatus']}


def deallocate_supplier_payment(allocation_id):
    """
    Remove a single AP payment allocation by its ID.
    Restores the payment's unallocated balance and re-syncs the bill status.
    """
    alloc = qry1("SELECT * FROM supplier_payment_allocations WHERE id=?", (allocation_id,))
    if not alloc:
        raise ValueError("Allocation not found")
    bill = qry1("SELECT * FROM supplier_bills WHERE id=?", (alloc['bill_id'],))
    if not bill:
        raise ValueError("Bill not found")
    if bill['status'] == 'VOID':
        raise ValueError("Cannot modify allocations on a VOID bill")
    run("DELETE FROM supplier_payment_allocations WHERE id=?", (allocation_id,))
    audit_log([], 'supplier_payment_allocations', str(allocation_id), 'DELETE',
              old_val={'payment_id': alloc['payment_id'], 'bill_id': alloc['bill_id'],
                       'allocated_amount': alloc['allocated_amount']})
    new_status = _sync_bill_status(alloc['bill_id'])
    save_db()
    return {'ok': True, 'billId': alloc['bill_id'], 'billStatus': new_status,
            'amountRestored': alloc['allocated_amount']}


def adjust_bill(bill_id, data):
    """
    Record a signed payment adjustment against a supplier bill.
    Positive amount  = additional payment made (reduces balance).
    Negative amount  = supplier credit / refund (increases balance).
    Uses payment_mode='ADJUSTMENT' — no new tables needed.
    """
    from modules.id_gen import next_id, _sync_counter_to_max

    bill = qry1("SELECT * FROM supplier_bills WHERE id=?", (bill_id,))
    if not bill:
        raise ValueError("Bill not found")
    if bill['status'] == 'VOID':
        raise ValueError("Cannot adjust a VOID bill")

    amount = r2(data.get('amount', 0))
    if amount == 0:
        raise ValueError("Adjustment amount cannot be zero")

    reason   = (data.get('reason') or 'Manual adjustment').strip()
    adj_date = data.get('date', today())

    _sync_counter_to_max('spay', 'supplier_payments', 'payment_ref', 'SP-SPAY-')
    pay_ref = next_id('spay', 'SPAY')

    ops = [("""
        INSERT INTO supplier_payments
            (payment_ref, supplier_id, payment_date, amount, payment_mode, notes)
        VALUES (?,?,?,?,'ADJUSTMENT',?)
    """, (pay_ref, bill['supplier_id'], adj_date, amount, reason))]
    audit_log(ops, 'supplier_payments', pay_ref, 'INSERT', new_val=data)
    run_many(ops)

    pay = qry1("SELECT * FROM supplier_payments WHERE payment_ref=?", (pay_ref,))

    _, _, bill_balance = compute_bill_balance(bill_id)
    alloc_amount = r2(min(amount, bill_balance)) if amount > 0 else amount

    run("""
        INSERT INTO supplier_payment_allocations (payment_id, bill_id, allocated_amount)
        VALUES (?,?,?)
        ON CONFLICT(payment_id, bill_id) DO UPDATE SET
            allocated_amount = allocated_amount + excluded.allocated_amount
    """, (pay['id'], bill_id, alloc_amount))

    new_status = _sync_bill_status(bill_id)
    save_db()
    return {'paymentRef': pay_ref, 'adjusted': alloc_amount, 'billStatus': new_status}


# ═══════════════════════════════════════════════════════════════════
#  PURCHASE ORDERS
# ═══════════════════════════════════════════════════════════════════

def list_purchase_orders(status_filter=None):
    """Return all POs with supplier name, total cost, and item count.
    Pass status_filter (e.g. 'draft', 'sent', 'received') to narrow results.
    """
    sql = """
        SELECT po.*, s.name as supplier_name,
               COALESCE(SUM(pi.quantity_kg * pi.unit_cost_kg), 0) as total_cost,
               COUNT(pi.id) as item_count
        FROM purchase_orders po
        JOIN suppliers s ON s.id = po.supplier_id
        LEFT JOIN po_items pi ON pi.po_id = po.id
    """
    params = []
    if status_filter:
        sql += " WHERE po.status=?"
        params.append(status_filter)
    sql += " GROUP BY po.id ORDER BY po.id DESC"
    return qry(sql, params)


def get_purchase_order(po_id):
    """Return a single PO with supplier name, line items (ingredient + costs), and linked bill id."""
    po = qry1("""
        SELECT po.*, s.name as supplier_name
        FROM purchase_orders po JOIN suppliers s ON s.id=po.supplier_id
        WHERE po.id=?
    """, (po_id,))
    if not po:
        raise ValueError("Purchase order not found")
    items = qry("""
        SELECT pi.*, i.code as ing_code, i.name as ing_name
        FROM po_items pi JOIN ingredients i ON i.id=pi.ingredient_id
        WHERE pi.po_id=? ORDER BY pi.id
    """, (po_id,))
    po = dict(po)
    po['items'] = [dict(i) for i in items]
    po['total_cost'] = sum(i['quantity_kg'] * i['unit_cost_kg'] for i in po['items'])

    if po.get('bill_id'):
        bill_items = qry("""
            SELECT sbi.ingredient_id,
                   sbi.quantity_kg  as billed_kg,
                   sbi.unit_cost_kg as billed_unit_cost,
                   sbi.line_total   as billed_amount
            FROM supplier_bill_items sbi
            WHERE sbi.bill_id=?
        """, (po['bill_id'],))
        bill_map = {b['ingredient_id']: dict(b) for b in bill_items}
        for item in po['items']:
            brow = bill_map.get(item['ingredient_id'], {})
            item['billed_kg']        = brow.get('billed_kg', 0)
            item['billed_unit_cost'] = brow.get('billed_unit_cost', 0)
            item['billed_amount']    = brow.get('billed_amount', 0)
        bill_row = qry1("SELECT bill_number FROM supplier_bills WHERE id=?", (po['bill_id'],))
        po['bill_number'] = bill_row['bill_number'] if bill_row else None

    return po


def create_purchase_order(data):
    """
    Create a purchase order (draft).
    data: {supplierId, poDate, expectedDate, notes, paymentTerms,
           items:[{ingredientId, quantityKg, unitCostKg}]}
    """
    from modules.id_gen import next_id, _sync_counter_to_max

    sup = qry1("SELECT * FROM suppliers WHERE id=?", (data.get('supplierId'),))
    if not sup:
        raise ValueError("Supplier not found")
    items = data.get('items', [])
    if not items:
        raise ValueError("Purchase order must have at least one item")

    _sync_counter_to_max('purchase_order', 'purchase_orders', 'po_number', 'SP-PO-')
    po_num    = next_id('purchase_order', 'PO')
    po_date   = data.get('poDate', today())
    expected  = data.get('expectedDate', '')
    notes     = data.get('notes', '')
    pay_terms = data.get('paymentTerms', 'CREDIT')

    c = _conn()
    try:
        c.execute("""
            INSERT INTO purchase_orders
                (po_number, supplier_id, po_date, expected_date, status, notes, payment_terms)
            VALUES (?,?,?,?,'draft',?,?)
        """, (po_num, sup['id'], po_date, expected, notes, pay_terms))
        po_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        for item in items:
            ing = qry1("SELECT * FROM ingredients WHERE id=?", (item.get('ingredientId'),))
            if not ing:
                raise ValueError(f"Ingredient not found: {item.get('ingredientId')}")
            qty_kg    = r2(float(item.get('quantityKg', 0)))
            unit_cost = r2(float(item.get('unitCostKg', 0)))
            c.execute("""
                INSERT INTO po_items (po_id, ingredient_id, quantity_kg, unit_cost_kg)
                VALUES (?,?,?,?)
            """, (po_id, ing['id'], qty_kg, unit_cost))

        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('purchase_orders',?,'INSERT',?)
        """, (po_num, json.dumps({'supplierId': sup['id'], 'items': len(items)})))
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()
    return get_purchase_order(po_id)


def update_purchase_order(po_id, data):
    """Edit PO header fields: expected_date, notes, payment_terms.
    Only allowed when PO status is 'draft' or 'sent'."""
    po = qry1("SELECT * FROM purchase_orders WHERE id=?", (po_id,))
    if not po:
        raise ValueError("Purchase order not found")
    if po['status'] in ('received', 'cancelled'):
        raise ValueError(f"Cannot edit a {po['status']} purchase order")
    set_parts, vals = [], []
    if 'expectedDate' in data:
        set_parts.append("expected_date=?"); vals.append(data['expectedDate'] or None)
    if 'notes' in data:
        set_parts.append("notes=?"); vals.append(str(data.get('notes', '')).strip())
    if 'paymentTerms' in data:
        set_parts.append("payment_terms=?"); vals.append(str(data['paymentTerms']).strip())
    if not set_parts:
        return po
    set_parts.append("updated_at=?"); vals.append(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'))
    vals.append(po_id)
    run(f"UPDATE purchase_orders SET {', '.join(set_parts)} WHERE id=?", vals)
    return qry1("""
        SELECT po.*, s.name as supplier_name
        FROM purchase_orders po JOIN suppliers s ON s.id=po.supplier_id
        WHERE po.id=?
    """, (po_id,))


def update_purchase_order_status(po_id, new_status, data=None):
    """
    Update PO status. On 'received'/'partial': update received_kg, update inventory,
    auto-create bill.
    Allowed transitions: draft→sent, sent→received, sent→cancelled, draft→cancelled
    """
    from modules.id_gen import next_id, _sync_counter_to_max

    data = data or {}
    po   = qry1("SELECT * FROM purchase_orders WHERE id=?", (po_id,))
    if not po:
        raise ValueError("Purchase order not found")

    allowed = {
        'draft':   ['sent', 'cancelled'],
        'sent':    ['received', 'partial', 'cancelled'],
        'partial': ['received', 'cancelled'],
    }
    if new_status not in allowed.get(po['status'], []):
        raise ValueError(f"Cannot move from {po['status']} to {new_status}")

    # Sync bill counter BEFORE opening transaction
    if new_status in ('received', 'partial') and not po.get('bill_id'):
        _sync_counter_to_max('bill', 'supplier_bills', 'bill_number', 'SP-BILL-')

    _cod_bill_id = None
    c = _conn()
    try:
        if new_status in ('received', 'partial'):
            received_items = data.get('receivedItems', [])
            for ri in received_items:
                new_cost = ri.get('unitCostKg')
                if new_cost is not None and float(new_cost) > 0:
                    c.execute(
                        "UPDATE po_items SET received_kg=?, unit_cost_kg=? WHERE id=? AND po_id=?",
                        (r2(float(ri.get('receivedKg', 0))), r2(float(new_cost)), ri['id'], po_id))
                else:
                    c.execute(
                        "UPDATE po_items SET received_kg=? WHERE id=? AND po_id=?",
                        (r2(float(ri.get('receivedKg', 0))), ri['id'], po_id))

            items_after = qry("""
                SELECT pi.*, i.code as ing_code
                FROM po_items pi JOIN ingredients i ON i.id=pi.ingredient_id
                WHERE pi.po_id=?
            """, (po_id,))
            fully_received = all(
                r2(float(i['received_kg'])) >= r2(float(i['quantity_kg']))
                for i in items_after
            )
            actual_status = 'received' if fully_received else 'partial'

            for item in items_after:
                newly_received_kg = r2(float(item['received_kg']))
                if newly_received_kg > 0:
                    already_logged = qry1("""
                        SELECT COALESCE(SUM(qty_grams),0) as g FROM inventory_ledger
                        WHERE reference_id=? AND ingredient_id=? AND movement_type='PURCHASE_IN'
                    """, (po['po_number'], item['ingredient_id']))
                    already_kg = r2((already_logged['g'] or 0) / 1000)
                    delta_kg   = r2(newly_received_kg - already_kg)
                    if delta_kg > 0:
                        c.execute("""
                            INSERT INTO inventory_ledger
                                (ingredient_id, movement_type, qty_grams, reference_id, notes)
                            VALUES (?,?,?,?,?)
                        """, (item['ingredient_id'], 'PURCHASE_IN', r2(delta_kg * 1000),
                              po['po_number'], f"Received via {po['po_number']}"))

            bill_id = po.get('bill_id')
            if not bill_id:
                bill_num  = next_id('bill', 'BILL', conn=c)
                bill_date = today()
                pay_terms = po.get('payment_terms', 'CREDIT')
                if pay_terms == 'COD':
                    due_date = bill_date
                else:
                    due_date = (date.fromisoformat(bill_date) + timedelta(days=30)).isoformat()

                po_bill_total = r2(sum(
                    r2(float(item['received_kg']) * float(item['unit_cost_kg']))
                    for item in items_after
                    if float(item['received_kg']) > 0
                ))

                c.execute("""
                    INSERT INTO supplier_bills
                        (bill_number, supplier_id, bill_date, due_date, status,
                         notes, total_amount, po_id)
                    VALUES (?,?,?,?,'UNPAID',?,?,?)
                """, (bill_num, po['supplier_id'], bill_date, due_date,
                      f"From PO {po['po_number']}", po_bill_total, po_id))
                bill_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

                for item in items_after:
                    rcv = r2(float(item['received_kg']))
                    if rcv > 0:
                        unit_cost  = r2(float(item['unit_cost_kg']))
                        line_total = r2(rcv * unit_cost)
                        c.execute("""
                            INSERT INTO supplier_bill_items
                                (bill_id, ingredient_id, quantity_kg, unit_cost_kg, line_total)
                            VALUES (?,?,?,?,?)
                        """, (bill_id, item['ingredient_id'], rcv, unit_cost, line_total))

                c.execute("UPDATE purchase_orders SET bill_id=? WHERE id=?", (bill_id, po_id))

                if pay_terms == 'COD' and po_bill_total > 0:
                    pay_ref = next_id('spay', 'SPAY', conn=c)
                    c.execute("""
                        INSERT INTO supplier_payments
                            (payment_ref, supplier_id, payment_date, amount,
                             payment_mode, notes)
                        VALUES (?,?,?,?,'CASH','Cash on Delivery - auto from PO')
                    """, (pay_ref, po['supplier_id'], bill_date, po_bill_total))
                    pay_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
                    c.execute("""
                        INSERT INTO supplier_payment_allocations
                            (payment_id, bill_id, allocated_amount)
                        VALUES (?,?,?)
                    """, (pay_id, bill_id, po_bill_total))
                    _cod_bill_id = bill_id

            c.execute("""
                UPDATE purchase_orders SET status=?, updated_at=datetime('now') WHERE id=?
            """, (actual_status, po_id))

        else:
            c.execute("""
                UPDATE purchase_orders SET status=?, updated_at=datetime('now') WHERE id=?
            """, (new_status, po_id))

        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('purchase_orders',?,'UPDATE',?)
        """, (po['po_number'], json.dumps({'status': new_status})))
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()

    # Post-commit: sync COD bill status from actual allocations
    if _cod_bill_id:
        _sync_bill_status(_cod_bill_id)
    save_db()
    return get_purchase_order(po_id)


# ═══════════════════════════════════════════════════════════════════
#  BOM INGREDIENT CALCULATOR
# ═══════════════════════════════════════════════════════════════════

def bom_calculate_ingredients(variant_id, qty_units):
    """
    Given a product variant + qty, return ingredient needs vs current stock.
    Returns list of {ingId, ingCode, neededKg, availableKg, toOrderKg, sufficient}
    NOTE: ingredient names are intentionally excluded for IP protection.
    The physical legend (ingCode → real name) is kept off-system.
    """
    from modules.inventory import get_stock_map

    var = qry1("""
        SELECT pv.*, ps.grams as pack_grams, p.name as product_name, p.code as product_code
        FROM product_variants pv
        LEFT JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        JOIN products p ON p.id = pv.product_id
        WHERE pv.id=?
    """, (variant_id,))
    if not var:
        raise ValueError("Product variant not found")

    bom_ver = qry1("""
        SELECT * FROM bom_versions WHERE product_id=? AND active_flag=1
        ORDER BY version_no DESC LIMIT 1
    """, (var['product_id'],))
    if not bom_ver:
        raise ValueError(
            f"No active BOM for {var['product_name']}. "
            f"Go to Production → BOM Setup → click the red chip for "
            f"{var.get('product_code', var['product_name'])} to define ingredients."
        )

    bom_items_list = qry("""
        SELECT bi.*, i.id as ing_id, i.code as ing_code
        FROM bom_items bi JOIN ingredients i ON i.id=bi.ingredient_id
        WHERE bi.bom_version_id=?
    """, (bom_ver['id'],))

    pack_grams  = float(var.get('pack_grams') or 0)
    total_grams = qty_units * pack_grams
    scale       = total_grams / float(bom_ver['batch_size_grams']) if bom_ver['batch_size_grams'] else 0
    stock_map   = get_stock_map()

    result = []
    for b in bom_items_list:
        needed_g    = r2(b['quantity_grams'] * scale)
        needed_kg   = r2(needed_g / 1000)
        avail_g     = stock_map.get(b['ingredient_id'], 0)
        avail_kg    = r2(avail_g / 1000)
        to_order_kg = r2(max(0.0, needed_kg - avail_kg))
        result.append({
            'ingId':       b['ing_id'],
            'ingCode':     b['ing_code'],
            'neededKg':    needed_kg,
            'availableKg': avail_kg,
            'toOrderKg':   to_order_kg,
            'sufficient':  to_order_kg < 0.001,
        })
    return {
        'productName': var['product_name'],
        'packSize':    var.get('pack_size_label') or f"{pack_grams:.0f}g",
        'qtyUnits':    qty_units,
        'ingredients': result,
        'anyShort':    any(r['toOrderKg'] >= 0.001 for r in result),
    }


# ═══════════════════════════════════════════════════════════════════
#  PO PDF GENERATION
# ═══════════════════════════════════════════════════════════════════

def generate_po_pdf(po_id: int) -> bytes:
    """Generate a professional Purchase Order PDF and return raw bytes."""
    from io import BytesIO
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                    TableStyle, HRFlowable)
    from reportlab.lib import colors as rl_colors
    from modules.invoices import _pdf_colors, _pkr

    po  = get_purchase_order(po_id)
    clr = _pdf_colors()
    buf = BytesIO()
    W, H = A4

    N   = ParagraphStyle('N',   fontName='Helvetica',      fontSize=9,  leading=12)
    NR  = ParagraphStyle('NR',  fontName='Helvetica',      fontSize=9,  leading=12, alignment=TA_RIGHT)
    NB  = ParagraphStyle('NB',  fontName='Helvetica-Bold', fontSize=9,  leading=12)
    NBR = ParagraphStyle('NBR', fontName='Helvetica-Bold', fontSize=9,  leading=12, alignment=TA_RIGHT)
    H1  = ParagraphStyle('H1',  fontName='Helvetica-Bold', fontSize=18, leading=22, textColor=clr['saffron'])
    H2  = ParagraphStyle('H2',  fontName='Helvetica-Bold', fontSize=12, leading=15, textColor=clr['dark'])
    SM  = ParagraphStyle('SM',  fontName='Helvetica',      fontSize=8,  leading=10, textColor=clr['text_sub'])

    doc = SimpleDocTemplate(buf, pagesize=A4,
                            leftMargin=18*mm, rightMargin=18*mm,
                            topMargin=14*mm, bottomMargin=14*mm)
    story = []

    # Header
    header_data = [[
        Paragraph('<b>SPICETOPIA</b>',
                  ParagraphStyle('LG', fontName='Helvetica-Bold', fontSize=20,
                                  leading=24, textColor=clr['saffron'])),
        Paragraph(
            'PURCHASE ORDER<br/><font size="14" color="#E8960A"><b>' + po["po_number"] + '</b></font>',
            ParagraphStyle('RT', fontName='Helvetica-Bold', fontSize=11, leading=16,
                           alignment=TA_RIGHT, textColor=clr['dark'])),
    ]]
    hdr_tbl = Table(header_data, colWidths=[W*0.5 - 18*mm, W*0.5 - 18*mm])
    hdr_tbl.setStyle(TableStyle([
        ('VALIGN',        (0,0), (-1,-1), 'TOP'),
        ('LINEBELOW',     (0,0), (-1,0),  1.5, clr['saffron']),
        ('BOTTOMPADDING', (0,0), (-1,0),  8),
    ]))
    story.append(hdr_tbl)
    story.append(Spacer(1, 10))

    # Supplier & PO meta
    pt = po['payment_terms'] or 'Credit'
    terms_label = 'Cash on Delivery' if pt == 'COD' else str(pt)
    meta_data = [[
        Paragraph('<b>To:</b> ' + po["supplier_name"], NB),
        Paragraph('<b>PO Date:</b> ' + po["po_date"], NBR),
    ],[
        Paragraph(po.get('notes', '') or '', SM),
        Paragraph(
            '<b>Expected:</b> ' + (po["expected_date"] or '—') + '<br/>'
            '<b>Payment Terms:</b> ' + terms_label + '<br/>'
            '<b>Status:</b> ' + po["status"].capitalize(),
            NR),
    ]]
    meta_tbl = Table(meta_data, colWidths=[W*0.5 - 18*mm, W*0.5 - 18*mm])
    meta_tbl.setStyle(TableStyle([
        ('VALIGN',        (0,0), (-1,-1), 'TOP'),
        ('TOPPADDING',    (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
    ]))
    story.append(meta_tbl)
    story.append(Spacer(1, 12))

    # Items table
    col_heads = ['Code', 'Ingredient', 'Ordered (kg)', 'Received (kg)',
                 'Unit Cost (PKR/kg)', 'Line Total (PKR)']
    rows = [col_heads]
    for item in po['items']:
        rcv     = item.get('received_kg', 0) or 0
        rcv_str = f'{float(rcv):.3f}' if po['status'] not in ('draft', 'sent') else '—'
        uc      = item.get('unit_cost_kg', 0) or 0
        lt      = float(item['quantity_kg']) * float(uc) if uc else 0
        rows.append([
            Paragraph(item['ing_code'],
                       ParagraphStyle('M', fontName='Courier', fontSize=8, leading=10)),
            Paragraph(item.get('ing_name', ''), N),
            Paragraph(f'{float(item["quantity_kg"]):.3f}', NR),
            Paragraph(rcv_str, NR),
            Paragraph(f'{float(uc):,.2f}' if uc else '—', NR),
            Paragraph(f'{lt:,.2f}' if uc else '—', NBR),
        ])
    rows.append([
        Paragraph('', N), Paragraph('', N), Paragraph('', N), Paragraph('', N),
        Paragraph('<b>Grand Total</b>', NBR),
        Paragraph('<b>' + _pkr(po["total_cost"]) + '</b>', NBR),
    ])

    cw = [22*mm, 60*mm, 28*mm, 28*mm, 38*mm, 40*mm]
    items_tbl = Table(rows, colWidths=cw, repeatRows=1)
    items_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,0),  clr['saffron']),
        ('TEXTCOLOR',     (0,0),(-1,0),  clr['white']),
        ('FONTNAME',      (0,0),(-1,0),  'Helvetica-Bold'),
        ('FONTSIZE',      (0,0),(-1,0),  8),
        ('ALIGN',         (2,0),(-1,-1), 'RIGHT'),
        ('FONTSIZE',      (0,1),(-1,-1), 8),
        ('ROWBACKGROUNDS',(0,1),(-1,-2), [clr['white'], clr['gray_light']]),
        ('BACKGROUND',    (0,-1),(-1,-1),clr['gray_light']),
        ('GRID',          (0,0),(-1,-1), 0.25, clr['gray_mid']),
        ('TOPPADDING',    (0,0),(-1,-1), 4),
        ('BOTTOMPADDING', (0,0),(-1,-1), 4),
        ('LEFTPADDING',   (0,0),(-1,-1), 5),
        ('RIGHTPADDING',  (0,0),(-1,-1), 5),
    ]))
    story.append(items_tbl)

    # 3-way match section if billed
    if po.get('bill_id'):
        story.append(Spacer(1, 14))
        story.append(Paragraph('3-Way Match Verification', H2))
        story.append(Spacer(1, 4))
        match_heads = ['Ingredient', 'Ordered (kg)', 'Received (kg)',
                       'Billed (kg)', 'Billed Amount (PKR)']
        match_rows = [match_heads]
        for item in po['items']:
            match_rows.append([
                Paragraph(item['ing_code'] + ' – ' + item.get('ing_name', ''), N),
                Paragraph(f'{float(item["quantity_kg"]):.3f}', NR),
                Paragraph(f'{float(item.get("received_kg", 0) or 0):.3f}', NR),
                Paragraph(f'{float(item.get("billed_kg", 0) or 0):.3f}', NR),
                Paragraph(_pkr(item.get('billed_amount', 0)), NBR),
            ])
        mcw = [80*mm, 25*mm, 25*mm, 25*mm, 40*mm]
        mtbl = Table(match_rows, colWidths=mcw, repeatRows=1)
        mtbl.setStyle(TableStyle([
            ('BACKGROUND',    (0,0),(-1,0),  clr['dark']),
            ('TEXTCOLOR',     (0,0),(-1,0),  clr['white']),
            ('FONTNAME',      (0,0),(-1,0),  'Helvetica-Bold'),
            ('FONTSIZE',      (0,0),(-1,-1), 8),
            ('ALIGN',         (1,0),(-1,-1), 'RIGHT'),
            ('ROWBACKGROUNDS',(0,1),(-1,-1), [clr['white'], clr['gray_light']]),
            ('GRID',          (0,0),(-1,-1), 0.25, clr['gray_mid']),
            ('TOPPADDING',    (0,0),(-1,-1), 4),
            ('BOTTOMPADDING', (0,0),(-1,-1), 4),
            ('LEFTPADDING',   (0,0),(-1,-1), 5),
            ('RIGHTPADDING',  (0,0),(-1,-1), 5),
        ]))
        story.append(mtbl)
        bill_num = po.get('bill_number', '')
        story.append(Spacer(1, 4))
        story.append(Paragraph('Linked Supplier Bill: <b>' + bill_num + '</b>', SM))

    # Footer
    story.append(Spacer(1, 20))
    story.append(HRFlowable(width='100%', thickness=0.5, color=clr['gray_mid']))
    story.append(Spacer(1, 6))
    story.append(Paragraph('Spicetopia — Generated by Spicetopia BMS', SM))

    doc.build(story)
    return buf.getvalue()
