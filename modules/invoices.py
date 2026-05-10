"""
modules/invoices.py
───────────────────
AR invoice lifecycle, payment recording, allocation, adjustments,
AR aging, void, and PDF generation (invoice + customer statement).

Extracted from server.py — Sprint 10.
Pattern: bottom-override. Originals remain in server.py as fallback;
these versions win at runtime via `from modules.invoices import *`.
"""

import json
from datetime import datetime, timedelta, date

from modules.db    import _conn, qry, qry1, run, run_many, save_db, audit_log
from modules.utils import r2, fmtpkr, today, _log

__all__ = [
    # PDF helpers
    '_pdf_colors', '_pkr',
    # Balance / status
    'compute_invoice_balance', '_compute_invoice_status', '_sync_invoice_status',
    # AR aging
    'get_ar_aging',
    # Invoice CRUD
    'create_invoice', 'add_invoice_item', 'remove_invoice_item',
    # Payments & allocation
    'record_customer_payment', 'allocate_customer_payment',
    'pay_invoice_direct', 'deallocate_payment', 'adjust_invoice',
    # Void
    'void_invoice',
    # PDFs
    'generate_invoice_pdf', 'generate_statement_pdf',
]

# ── Module-level config (synced from server.py at startup) ─────────
GST_RATE = 0.18   # 18% GST

# ── Callback: _order_status wired after startup ────────────────────
# Used by void_invoice to recompute order status post-commit.
_order_status_fn = lambda order_id: 'confirmed'


# ═══════════════════════════════════════════════════════════════════
#  PDF HELPERS
# ═══════════════════════════════════════════════════════════════════

def _pdf_colors():
    """Return brand colour objects for use in reportlab drawings."""
    from reportlab.lib.colors import HexColor
    return {
        'saffron':   HexColor('#E8960A'),
        'dark':      HexColor('#1a1a2e'),
        'chili':     HexColor('#C0392B'),
        'cardamom':  HexColor('#27AE60'),
        'gray_light':HexColor('#F5F5F5'),
        'gray_mid':  HexColor('#CCCCCC'),
        'text_sub':  HexColor('#666666'),
        'white':     HexColor('#FFFFFF'),
    }


def _pkr(v):
    """Format a number as PKR currency string."""
    try:
        v = float(v or 0)
        sign = '-' if v < 0 else ''
        return f"{sign}PKR {abs(v):,.2f}"
    except Exception:
        return 'PKR 0.00'


# ═══════════════════════════════════════════════════════════════════
#  BALANCE & STATUS HELPERS
# ═══════════════════════════════════════════════════════════════════

def compute_invoice_balance(invoice_id):
    """Returns (subtotal, tax, total, paid, balance)."""
    subtotal_row = qry1(
        "SELECT COALESCE(SUM(line_total),0) as s FROM invoice_items WHERE invoice_id=?",
        (invoice_id,)
    )
    subtotal = r2(subtotal_row['s'])
    tax      = r2(subtotal * GST_RATE)
    total    = r2(subtotal + tax)
    paid_row = qry1(
        "SELECT COALESCE(SUM(allocated_amount),0) as p FROM payment_allocations WHERE invoice_id=?",
        (invoice_id,)
    )
    paid    = r2(paid_row['p'])
    balance = r2(total - paid)
    return subtotal, tax, total, paid, balance


def _compute_invoice_status(invoice_id) -> str:
    """
    Derive the correct invoice status purely from the numbers — never from a stored flag.
    Returns 'UNPAID', 'PARTIAL', or 'PAID'.
    This is the single source of truth for invoice status.
    """
    _, _, total, paid, balance = compute_invoice_balance(invoice_id)
    if paid <= 0:
        return 'UNPAID'
    if balance > 0.001:   # leave a 0.001 rounding tolerance
        return 'PARTIAL'
    return 'PAID'


def _sync_invoice_status(invoice_id) -> str:
    """
    Compute invoice status from amounts and write it to the DB.
    Returns the new status string.
    Call after any payment allocation, item add/remove, or admin reconcile.
    """
    new_status = _compute_invoice_status(invoice_id)
    run("UPDATE invoices SET status=? WHERE id=?", (new_status, invoice_id))
    return new_status


# ═══════════════════════════════════════════════════════════════════
#  AR AGING
# ═══════════════════════════════════════════════════════════════════

def get_ar_aging():
    """Returns aging buckets for all unpaid/partial invoices."""
    invoices = qry("""
        SELECT inv.id, inv.invoice_number, inv.customer_id, inv.invoice_date, inv.due_date,
               inv.status, c.name as customer_name, c.customer_type
        FROM invoices inv JOIN customers c ON c.id = inv.customer_id
        WHERE inv.status IN ('UNPAID','PARTIAL')
        ORDER BY inv.due_date
    """)
    today_dt = date.today()
    result = []
    for inv in invoices:
        _, _, total, paid, balance = compute_invoice_balance(inv['id'])
        if balance <= 0:
            continue
        try:
            due = date.fromisoformat(inv['due_date'])
            days_over = (today_dt - due).days
        except Exception:
            days_over = 0
        bucket = 'current' if days_over <= 0 else (
                 '1_30'   if days_over <= 30  else (
                 '31_60'  if days_over <= 60  else (
                 '61_90'  if days_over <= 90  else '90plus')))
        result.append({**inv, 'total': total, 'paid': paid, 'balance': balance,
                        'days_overdue': days_over, 'aging_bucket': bucket})
    return result


# ═══════════════════════════════════════════════════════════════════
#  INVOICE CRUD
# ═══════════════════════════════════════════════════════════════════

def create_invoice(inv_data):
    """
    Create an invoice directly from structured data.
    inv_data: {custCode, invoiceDate, notes, items: [{skuCode, qty, unitPrice}]}
    Returns: {id, invoiceNumber, total}
    """
    from modules.id_gen import next_id, _sync_counter_to_max

    cust = qry1("SELECT * FROM customers WHERE code=?", (inv_data.get('custCode', ''),))
    if not cust:
        raise ValueError(f"Customer not found: {inv_data.get('custCode')}")
    items = inv_data.get('items', [])
    if not items:
        raise ValueError("Invoice must have at least one item")

    _sync_counter_to_max('invoice', 'invoices', 'invoice_number', 'SP-INV-')
    inv_num  = next_id('invoice', 'INV')
    inv_date = inv_data.get('invoiceDate', str(date.today()))
    terms    = int(cust.get('payment_terms_days', 30))
    try:
        due_date = (date.fromisoformat(inv_date) + timedelta(days=terms)).isoformat()
    except Exception:
        due_date = inv_date

    c = _conn()
    try:
        c.execute("""
            INSERT INTO invoices (invoice_number, customer_id, invoice_date, due_date, status, notes)
            VALUES (?,?,?,?,'UNPAID',?)
        """, (inv_num, cust['id'], inv_date, due_date, inv_data.get('notes', '')))
        c.commit()
        inv_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        subtotal = 0.0
        for item in items:
            sku = item.get('skuCode') or item.get('productCode')
            var = qry1("SELECT * FROM product_variants WHERE sku_code=?", (sku,))
            if not var:
                var = qry1("""
                    SELECT pv.* FROM product_variants pv
                    JOIN products p ON p.id=pv.product_id
                    WHERE pv.sku_code=? OR p.code=?
                """, (sku, sku))
            qty    = float(item.get('qty', item.get('quantity', 0)))
            uprice = float(item.get('unitPrice', 0))
            if qty <= 0:
                continue
            line     = round(qty * uprice, 2)
            subtotal += line
            prod_code = prod_name = pack_size = ''
            if var:
                pv_row = qry1("""
                    SELECT p.code as prod_code, p.name as prod_name, ps.label as pack_size
                    FROM product_variants pv
                    JOIN products p ON p.id=pv.product_id
                    JOIN pack_sizes ps ON ps.id=pv.pack_size_id
                    WHERE pv.id=?
                """, (var['id'],))
                if pv_row:
                    prod_code = pv_row['prod_code']
                    prod_name = pv_row['prod_name']
                    pack_size = pv_row['pack_size']
            c.execute("""
                INSERT INTO invoice_items
                    (invoice_id, product_variant_id, product_code, product_name,
                     pack_size, quantity, unit_price, line_total)
                VALUES (?,?,?,?,?,?,?,?)
            """, (inv_id, var['id'] if var else None, prod_code, prod_name,
                  pack_size, qty, uprice, line))
        gst   = round(subtotal * GST_RATE, 2)
        total = round(subtotal + gst, 2)
        c.commit()
    finally:
        c.close()
    save_db()
    return {'id': inv_id, 'invoiceNumber': inv_num,
            'subtotal': subtotal, 'gst': gst, 'total': total}


def add_invoice_item(invoice_id, data):
    """Add a line item to an existing UNPAID invoice."""
    inv = qry1("SELECT * FROM invoices WHERE id=?", (invoice_id,))
    if not inv:
        raise ValueError("Invoice not found")
    if inv['status'] not in ('UNPAID', 'PARTIAL'):
        raise ValueError(f"Cannot edit a {inv['status']} invoice")

    # DB lookup replaces ref['var_by_sku'] dict
    var = qry1("""
        SELECT pv.*, p.code as product_code, p.name as product_name, ps.label as pack_size
        FROM product_variants pv
        JOIN products p ON p.id = pv.product_id
        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE p.code=? AND ps.label=? AND pv.active_flag=1
    """, (data.get('productCode', ''), data.get('packSize', '')))
    if not var:
        raise ValueError("Product variant not found")

    qty        = int(data.get('qty', 0))
    unit_price = r2(data.get('unitPrice', 0))
    if qty <= 0:
        raise ValueError("Quantity must be positive")
    line_total = r2(qty * unit_price)
    run("""INSERT INTO invoice_items
           (invoice_id, product_variant_id, product_code, product_name, pack_size,
            quantity, unit_price, line_total)
           VALUES (?,?,?,?,?,?,?,?)""",
        (invoice_id, var['id'], var['product_code'], var['product_name'],
         var['pack_size'], qty, unit_price, line_total))
    _sync_invoice_status(invoice_id)
    s, t, total, paid, bal = compute_invoice_balance(invoice_id)
    return {'subtotal': s, 'tax': t, 'total': total, 'paid': paid, 'balance': bal}


def remove_invoice_item(item_id):
    """
    Remove a line item from an invoice.
    Blocked if: invoice is PAID, or any payment has been allocated to it.
    """
    item = qry1("SELECT * FROM invoice_items WHERE id=?", (item_id,))
    if not item:
        raise ValueError("Invoice item not found")
    inv = qry1("SELECT * FROM invoices WHERE id=?", (item['invoice_id'],))
    if not inv:
        raise ValueError("Invoice not found")
    if inv['status'] == 'PAID':
        raise ValueError("Cannot edit a PAID invoice — it is locked once fully paid")
    has_payment = qry1(
        "SELECT id FROM payment_allocations WHERE invoice_id=? LIMIT 1",
        (item['invoice_id'],)
    )
    if has_payment:
        raise ValueError("Cannot remove items from an invoice that has payments recorded — "
                         "reverse the payment first")
    remaining = qry("SELECT id FROM invoice_items WHERE invoice_id=?", (item['invoice_id'],))
    if len(remaining) <= 1:
        raise ValueError("Cannot remove the last line item from an invoice")
    run("DELETE FROM invoice_items WHERE id=?", (item_id,))
    _sync_invoice_status(item['invoice_id'])
    s, t, total, paid, bal = compute_invoice_balance(item['invoice_id'])
    return {'subtotal': s, 'tax': t, 'total': total, 'paid': paid, 'balance': bal}


# ═══════════════════════════════════════════════════════════════════
#  PAYMENTS & ALLOCATION
# ═══════════════════════════════════════════════════════════════════

def record_customer_payment(data):
    """
    Record a payment from a customer. Does NOT auto-allocate.
    data: {customerId, paymentDate, amount, paymentMode, notes}
    """
    from modules.id_gen import next_id, _sync_counter_to_max

    cust = qry1("SELECT * FROM customers WHERE id=?", (data.get('customerId'),))
    if not cust:
        raise ValueError("Customer not found")
    amount = r2(data.get('amount', 0))
    if amount <= 0:
        raise ValueError("Payment amount must be positive")

    _sync_counter_to_max('payment', 'customer_payments', 'payment_ref', 'SP-PAY-')
    pay_ref = next_id('payment', 'PAY')
    ops = [("""
        INSERT INTO customer_payments
            (payment_ref, customer_id, payment_date, amount, payment_mode, notes)
        VALUES (?,?,?,?,?,?)
    """, (pay_ref, cust['id'], data.get('paymentDate', today()),
          amount, data.get('paymentMode', 'CASH'), data.get('notes', '')))]
    audit_log(ops, 'customer_payments', pay_ref, 'INSERT', new_val=data)
    run_many(ops)
    return qry1("SELECT * FROM customer_payments WHERE payment_ref=?", (pay_ref,))


def allocate_customer_payment(payment_id, invoice_id, amount):
    """Allocate a payment amount to a specific invoice."""
    pay = qry1("SELECT * FROM customer_payments WHERE id=?", (payment_id,))
    if not pay:
        raise ValueError("Payment not found")
    inv = qry1("SELECT * FROM invoices WHERE id=?", (invoice_id,))
    if not inv:
        raise ValueError("Invoice not found")

    already_alloc = r2(qry1(
        "SELECT COALESCE(SUM(allocated_amount),0) as s FROM payment_allocations WHERE payment_id=?",
        (payment_id,)
    )['s'])
    available = r2(pay['amount'] - already_alloc)
    if amount > available + 0.001:
        raise ValueError(f"Exceeds available payment balance: {available:.2f}")

    _, _, inv_total, inv_paid, inv_balance = compute_invoice_balance(invoice_id)
    if amount > inv_balance + 0.001:
        raise ValueError(f"Exceeds invoice balance due: {inv_balance:.2f}")

    amount = r2(min(amount, available, inv_balance))

    ops = [("""
        INSERT INTO payment_allocations (payment_id, invoice_id, allocated_amount)
        VALUES (?,?,?)
        ON CONFLICT(payment_id, invoice_id) DO UPDATE SET
            allocated_amount = allocated_amount + excluded.allocated_amount
    """, (payment_id, invoice_id, amount))]
    audit_log(ops, 'payment_allocations', f"{payment_id}-{invoice_id}", 'INSERT')
    run_many(ops)

    new_status = _sync_invoice_status(invoice_id)
    return {'allocated': amount, 'invoiceStatus': new_status}


def pay_invoice_direct(invoice_id, data):
    """
    Record a new payment AND immediately allocate it to a specific invoice in one call.
    data: {amount, paymentDate, paymentMode, notes}
    """
    from modules.id_gen import next_id, _sync_counter_to_max

    inv = qry1("SELECT * FROM invoices WHERE id=?", (invoice_id,))
    if not inv:
        raise ValueError("Invoice not found")
    _, _, inv_total, inv_paid, inv_balance = compute_invoice_balance(invoice_id)
    if inv_balance <= 0:
        raise ValueError("Invoice is already fully paid")

    amount = r2(data.get('amount', inv_balance))
    if amount <= 0:
        raise ValueError("Payment amount must be positive")

    _sync_counter_to_max('payment', 'customer_payments', 'payment_ref', 'SP-PAY-')
    pay_ref  = next_id('payment', 'PAY')
    pay_date = data.get('paymentDate', today())
    ops = [("""INSERT INTO customer_payments
                (payment_ref, customer_id, payment_date, amount, payment_mode, notes)
               VALUES (?,?,?,?,?,?)""",
            (pay_ref, inv['customer_id'], pay_date, amount,
             data.get('paymentMode', 'CASH'), data.get('notes', '')))]
    audit_log(ops, 'customer_payments', pay_ref, 'INSERT', new_val=data)
    run_many(ops)

    pay = qry1("SELECT * FROM customer_payments WHERE payment_ref=?", (pay_ref,))
    alloc_amount = r2(min(amount, inv_balance))
    result = allocate_customer_payment(pay['id'], invoice_id, alloc_amount)
    return {'paymentRef': pay_ref, 'paymentId': pay['id'],
            'allocated': alloc_amount, 'invoiceStatus': result['invoiceStatus']}


def deallocate_payment(allocation_id):
    """
    Remove a single AR payment allocation by its ID.
    Restores the payment's unallocated balance and re-syncs the invoice status.
    Only allowed on non-PAID invoices (if invoice is PAID, use adjust instead).
    """
    alloc = qry1("SELECT * FROM payment_allocations WHERE id=?", (allocation_id,))
    if not alloc:
        raise ValueError("Allocation not found")
    inv = qry1("SELECT * FROM invoices WHERE id=?", (alloc['invoice_id'],))
    if not inv:
        raise ValueError("Invoice not found")
    if inv['status'] == 'VOID':
        raise ValueError("Cannot modify allocations on a VOID invoice")
    run("DELETE FROM payment_allocations WHERE id=?", (allocation_id,))
    audit_log([], 'payment_allocations', str(allocation_id), 'DELETE',
              old_val={'payment_id': alloc['payment_id'], 'invoice_id': alloc['invoice_id'],
                       'allocated_amount': alloc['allocated_amount']})
    new_status = _sync_invoice_status(alloc['invoice_id'])
    save_db()
    return {'ok': True, 'invoiceId': alloc['invoice_id'], 'invoiceStatus': new_status,
            'amountRestored': alloc['allocated_amount']}


def adjust_invoice(invoice_id, data):
    """
    Record a signed payment adjustment against an invoice.
    Positive amount  = additional payment received (reduces balance).
    Negative amount  = refund / credit (increases balance, can reopen a PAID invoice).
    Uses payment_mode='ADJUSTMENT' — no new tables needed.
    """
    from modules.id_gen import next_id, _sync_counter_to_max

    inv = qry1("SELECT * FROM invoices WHERE id=?", (invoice_id,))
    if not inv:
        raise ValueError("Invoice not found")
    if inv['status'] == 'VOID':
        raise ValueError("Cannot adjust a VOID invoice")

    amount = r2(data.get('amount', 0))
    if amount == 0:
        raise ValueError("Adjustment amount cannot be zero")

    reason   = (data.get('reason') or 'Manual adjustment').strip()
    adj_date = data.get('date', today())
    customer = qry1("SELECT * FROM customers WHERE id=?", (inv['customer_id'],))
    if not customer:
        raise ValueError("Customer not found")

    _sync_counter_to_max('pay', 'customer_payments', 'payment_ref', 'SP-PAY-')
    pay_ref = next_id('pay', 'PAY')

    ops = [("""
        INSERT INTO customer_payments
            (payment_ref, customer_id, payment_date, amount, payment_mode, notes)
        VALUES (?,?,?,?,'ADJUSTMENT',?)
    """, (pay_ref, customer['id'], adj_date, amount, reason))]
    audit_log(ops, 'customer_payments', pay_ref, 'INSERT', new_val=data)
    run_many(ops)

    pay = qry1("SELECT * FROM customer_payments WHERE payment_ref=?", (pay_ref,))

    _subtotal, _tax, _total, _paid, inv_balance = compute_invoice_balance(invoice_id)
    alloc_amount = r2(min(amount, inv_balance)) if amount > 0 else amount

    run("""
        INSERT INTO payment_allocations (payment_id, invoice_id, allocated_amount)
        VALUES (?,?,?)
        ON CONFLICT(payment_id, invoice_id) DO UPDATE SET
            allocated_amount = allocated_amount + excluded.allocated_amount
    """, (pay['id'], invoice_id, alloc_amount))

    new_status = _sync_invoice_status(invoice_id)
    save_db()
    return {'paymentRef': pay_ref, 'adjusted': alloc_amount,
            'invoiceStatus': new_status}


# ═══════════════════════════════════════════════════════════════════
#  VOID INVOICE
# ═══════════════════════════════════════════════════════════════════

def void_invoice(invoice_id: int, note: str, username: str):
    """
    Void an invoice.  Rules:
    • Cannot void a PAID invoice — too late.
    • Cannot void an already-VOID invoice.
    • Marks status = VOID, records voided_at / voided_by / void_note.
    • Marks all associated sales records as voided (restores finished-goods stock).
    • Deletes payment_allocations for this invoice so the customer payment
      becomes unallocated and can be re-applied elsewhere.
    • If the invoice belongs to a customer order, resets qty_invoiced on the
      relevant order items so the order can be re-invoiced.
    • Writes a change_log audit entry.
    """
    inv = qry1("SELECT * FROM invoices WHERE id=?", (invoice_id,))
    if not inv:
        raise ValueError(f"Invoice not found: {invoice_id}")
    if inv['status'] == 'VOID':
        raise ValueError(f"{inv['invoice_number']} is already void")
    if inv['status'] == 'PAID':
        raise ValueError(
            f"{inv['invoice_number']} is fully paid — it cannot be voided. "
            "Record a credit note instead."
        )

    now  = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    note = (note or '').strip() or 'No reason given'

    c = _conn()
    try:
        # 1. Mark invoice as VOID
        c.execute("""
            UPDATE invoices
            SET status='VOID', voided_at=?, voided_by=?, void_note=?
            WHERE id=?
        """, (now, username, note, invoice_id))

        # 2. Mark associated sales as voided (restores finished-goods stock via get_finished_stock_map)
        c.execute("UPDATE sales SET voided=1 WHERE invoice_id=?", (invoice_id,))

        # 3. Un-allocate any customer payments from this invoice
        c.execute("DELETE FROM payment_allocations WHERE invoice_id=?", (invoice_id,))

        # 4. If linked to a customer order, reset qty_invoiced on order items
        items = qry("SELECT * FROM invoice_items WHERE invoice_id=?", (invoice_id,))
        for item in items:
            if item.get('sale_id'):
                c.execute("""
                    UPDATE customer_order_items
                    SET qty_invoiced = MAX(0, qty_invoiced - ?)
                    WHERE order_id IN (
                        SELECT customer_order_id FROM invoices WHERE id=?
                    )
                    AND product_variant_id = ?
                """, (item['quantity'], invoice_id, item.get('product_variant_id')))

        # 5. Detect if order needs post-commit status sync
        _void_order_id = None
        if inv.get('customer_order_id'):
            live_invoices = qry("""
                SELECT COUNT(*) as n FROM invoices
                WHERE customer_order_id=? AND status != 'VOID' AND id != ?
            """, (inv['customer_order_id'], invoice_id))
            live_count = live_invoices[0]['n'] if live_invoices else 0
            if live_count == 0:
                _void_order_id = inv['customer_order_id']

        # 6. Audit log
        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('invoices', ?, 'VOID', ?)
        """, (inv['invoice_number'], json.dumps({
            'void_note': note, 'voided_by': username,
            'voided_at': now, 'previous_status': inv['status']
        })))

        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()

    # Post-commit: derive order status from actual qty_invoiced totals
    if _void_order_id:
        new_ord_status = _order_status_fn(_void_order_id)
        run("UPDATE customer_orders SET status=? WHERE id=?", (new_ord_status, _void_order_id))

    save_db()
    _log('info', 'invoice_voided', invoice=inv['invoice_number'],
         by=username, status=inv['status'])
    return {'ok': True, 'invoice_number': inv['invoice_number']}


# ═══════════════════════════════════════════════════════════════════
#  PDF GENERATION
# ═══════════════════════════════════════════════════════════════════

def generate_invoice_pdf(inv_id: int) -> bytes:
    """Generate a professional invoice PDF and return the raw bytes."""
    from io import BytesIO
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                    TableStyle, HRFlowable)
    from reportlab.lib import colors as rl_colors

    inv = qry1("""
        SELECT inv.*, c.name as customer_name, c.customer_type, c.code as cust_code,
               c.account_number as cust_acct, c.email as customer_email,
               c.phone as customer_phone, c.city as customer_city,
               c.address as customer_address, c.credit_limit
        FROM invoices inv JOIN customers c ON c.id=inv.customer_id
        WHERE inv.id=?
    """, (inv_id,))
    if not inv:
        raise ValueError("Invoice not found")

    items  = qry("SELECT * FROM invoice_items WHERE invoice_id=?", (inv_id,))
    allocs = qry("""
        SELECT pa.allocated_amount, cp.payment_ref, cp.payment_date, cp.payment_mode
        FROM payment_allocations pa
        JOIN customer_payments cp ON cp.id=pa.payment_id
        WHERE pa.invoice_id=? ORDER BY cp.payment_date
    """, (inv_id,))
    s, t, total, paid, bal = compute_invoice_balance(inv_id)

    clr = _pdf_colors()
    buf = BytesIO()
    W, H = A4

    def pstyle(name, **kw):
        defaults = dict(fontName='Helvetica', fontSize=9, leading=12,
                        textColor=clr['dark'])
        defaults.update(kw)
        return ParagraphStyle(name, **defaults)

    s_title   = pstyle('title',  fontName='Helvetica-Bold', fontSize=22,
                        textColor=clr['white'], leading=28)
    s_sub     = pstyle('sub',    fontSize=8, textColor=clr['white'], leading=11)
    s_label   = pstyle('label',  fontName='Helvetica-Bold', fontSize=7,
                        textColor=clr['text_sub'], leading=10, spaceAfter=1)
    s_value   = pstyle('value',  fontName='Helvetica-Bold', fontSize=10, leading=13)
    s_normal  = pstyle('normal', fontSize=9)
    s_small   = pstyle('small',  fontSize=7.5, textColor=clr['text_sub'])
    s_right   = pstyle('right',  fontSize=9, alignment=TA_RIGHT)
    s_right_b = pstyle('right_b',fontName='Helvetica-Bold', fontSize=9, alignment=TA_RIGHT)
    s_center  = pstyle('center', fontSize=9, alignment=TA_CENTER)
    s_footer  = pstyle('footer', fontSize=7.5, textColor=clr['text_sub'], alignment=TA_CENTER)

    story = []

    # Header band
    header_data = [[
        Paragraph('<b>SPICETOPIA</b>', s_title),
        Paragraph(
            '<b>TAX INVOICE</b><br/>'
            '<font size="10">' + inv["invoice_number"] + '</font>',
            ParagraphStyle('invnum', fontName='Helvetica-Bold', fontSize=14,
                           leading=18, textColor=clr['white'], alignment=TA_RIGHT)
        )
    ]]
    header_tbl = Table(header_data, colWidths=[W * 0.55, W * 0.35])
    header_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), clr['saffron']),
        ('TOPPADDING',    (0,0), (-1,-1), 14),
        ('BOTTOMPADDING', (0,0), (-1,-1), 14),
        ('LEFTPADDING',   (0,0), (0,-1),  18),
        ('RIGHTPADDING',  (-1,0),(-1,-1), 18),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
    ]))
    story.append(header_tbl)
    story.append(Spacer(1, 8))

    # Invoice meta + Bill To
    inv_date_fmt = inv.get('invoice_date', '')
    due_date_fmt = inv.get('due_date', '')
    status       = inv.get('status', '')
    status_color = '#C0392B' if status in ('UNPAID','PARTIAL') else '#27AE60' if status == 'PAID' else '#888888'

    bill_to_lines = [
        Paragraph('BILL TO', s_label),
        Paragraph('<b>' + inv["customer_name"] + '</b>', s_value),
        Paragraph('Account: ' + (inv["cust_acct"] or inv["cust_code"]), s_small),
    ]
    if inv.get('customer_address'):
        bill_to_lines.append(Paragraph(inv['customer_address'], s_small))
    if inv.get('customer_city'):
        bill_to_lines.append(Paragraph(inv['customer_city'], s_small))
    if inv.get('customer_phone'):
        bill_to_lines.append(Paragraph('Tel: ' + inv["customer_phone"], s_small))
    if inv.get('customer_email'):
        bill_to_lines.append(Paragraph(inv['customer_email'], s_small))

    meta_data = [[
        bill_to_lines,
        [
            Paragraph('INVOICE DATE', s_label),
            Paragraph(inv_date_fmt, s_value),
            Spacer(1, 6),
            Paragraph('DUE DATE', s_label),
            Paragraph(due_date_fmt, s_value),
            Spacer(1, 6),
            Paragraph('STATUS', s_label),
            Paragraph('<font color="' + status_color + '"><b>' + status + '</b></font>', s_value),
        ]
    ]]
    meta_tbl = Table(meta_data, colWidths=[W * 0.55, W * 0.35])
    meta_tbl.setStyle(TableStyle([
        ('VALIGN',       (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING',  (0,0), (0,-1),  0),
        ('RIGHTPADDING', (-1,0),(-1,-1), 0),
        ('TOPPADDING',   (0,0), (-1,-1), 0),
        ('BOTTOMPADDING',(0,0), (-1,-1), 0),
    ]))
    story.append(meta_tbl)
    story.append(Spacer(1, 14))
    story.append(HRFlowable(width='100%', thickness=1, color=clr['gray_mid']))
    story.append(Spacer(1, 10))

    # Line items table
    th_style = ParagraphStyle('th', fontName='Helvetica-Bold', fontSize=8,
                               textColor=clr['white'], leading=10)
    item_rows = [[
        Paragraph('PRODUCT',    th_style),
        Paragraph('PACK SIZE',  th_style),
        Paragraph('QTY',        ParagraphStyle('thr',  fontName='Helvetica-Bold', fontSize=8,
                                                textColor=clr['white'], leading=10, alignment=TA_RIGHT)),
        Paragraph('UNIT PRICE', ParagraphStyle('thr2', fontName='Helvetica-Bold', fontSize=8,
                                                textColor=clr['white'], leading=10, alignment=TA_RIGHT)),
        Paragraph('LINE TOTAL', ParagraphStyle('thr3', fontName='Helvetica-Bold', fontSize=8,
                                                textColor=clr['white'], leading=10, alignment=TA_RIGHT)),
    ]]
    for idx, item in enumerate(items):
        item_rows.append([
            Paragraph(str(item.get('product_name', '')), s_normal),
            Paragraph(str(item.get('pack_size',    '')), s_normal),
            Paragraph(str(item.get('quantity',     '')), s_right),
            Paragraph(_pkr(item.get('unit_price',  0)),  s_right),
            Paragraph(_pkr(item.get('line_total',  0)),  s_right_b),
        ])

    col_w = [W*0.32, W*0.13, W*0.09, W*0.18, W*0.18]
    items_tbl = Table(item_rows, colWidths=col_w)
    items_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,0),  clr['dark']),
        ('ROWBACKGROUNDS',(0,1),(-1,-1),  [clr['gray_light'], clr['white']]),
        ('GRID',          (0,0), (-1,-1), 0.25, clr['gray_mid']),
        ('TOPPADDING',    (0,0), (-1,-1), 5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ('LEFTPADDING',   (0,0), (-1,-1), 7),
        ('RIGHTPADDING',  (0,0), (-1,-1), 7),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
    ]))
    story.append(items_tbl)
    story.append(Spacer(1, 12))

    # Totals block
    s_tot_lbl = ParagraphStyle('tl',  fontName='Helvetica', fontSize=9,
                                leading=12, alignment=TA_RIGHT, textColor=clr['text_sub'])
    s_tot_val = ParagraphStyle('tv',  fontName='Helvetica', fontSize=9,
                                leading=12, alignment=TA_RIGHT)
    s_tot_grand_lbl = ParagraphStyle('tgl', fontName='Helvetica-Bold', fontSize=11,
                                      leading=14, alignment=TA_RIGHT)
    s_tot_grand_val = ParagraphStyle('tgv', fontName='Helvetica-Bold', fontSize=11,
                                      leading=14, alignment=TA_RIGHT,
                                      textColor=clr['chili'] if bal > 0 else clr['cardamom'])

    totals_data = [
        [Paragraph('Subtotal (excl. GST):', s_tot_lbl), Paragraph(_pkr(s), s_tot_val)],
        [Paragraph('GST 18%:',             s_tot_lbl), Paragraph(_pkr(t), s_tot_val)],
        [Paragraph('<b>Invoice Total:</b>', s_tot_grand_lbl),
         Paragraph('<b>' + _pkr(total) + '</b>', s_tot_grand_val)],
        [Paragraph('Amount Paid:',         s_tot_lbl), Paragraph(_pkr(paid), s_tot_val)],
        [Paragraph('<b>Balance Due:</b>',  s_tot_grand_lbl),
         Paragraph('<b>' + _pkr(bal) + '</b>',
                   ParagraphStyle('bdue', fontName='Helvetica-Bold', fontSize=11, leading=14,
                                  alignment=TA_RIGHT,
                                  textColor=clr['chili'] if bal > 0 else clr['cardamom']))],
    ]
    totals_tbl = Table(totals_data, colWidths=[W*0.7, W*0.2])
    totals_tbl.setStyle(TableStyle([
        ('TOPPADDING',    (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('LINEABOVE',     (0,2), (-1,2),  0.5, clr['gray_mid']),
        ('LINEABOVE',     (0,4), (-1,4),  1.0, clr['dark']),
        ('BACKGROUND',    (0,4), (-1,4),  clr['gray_light']),
        ('LEFTPADDING',   (0,0), (-1,-1), 0),
        ('RIGHTPADDING',  (0,0), (-1,-1), 0),
    ]))
    story.append(totals_tbl)

    # Payment history
    if allocs:
        story.append(Spacer(1, 14))
        story.append(HRFlowable(width='100%', thickness=0.5, color=clr['gray_mid']))
        story.append(Spacer(1, 6))
        story.append(Paragraph('Payment History',
                                pstyle('ph', fontName='Helvetica-Bold', fontSize=9,
                                       textColor=clr['text_sub'])))
        story.append(Spacer(1, 4))
        pay_rows = [[
            Paragraph('Date',      ParagraphStyle('pth',  fontName='Helvetica-Bold', fontSize=7.5, textColor=clr['white'], leading=10)),
            Paragraph('Reference', ParagraphStyle('pth2', fontName='Helvetica-Bold', fontSize=7.5, textColor=clr['white'], leading=10)),
            Paragraph('Mode',      ParagraphStyle('pth3', fontName='Helvetica-Bold', fontSize=7.5, textColor=clr['white'], leading=10)),
            Paragraph('Amount',    ParagraphStyle('pth4', fontName='Helvetica-Bold', fontSize=7.5, textColor=clr['white'], leading=10, alignment=TA_RIGHT)),
        ]]
        for a in allocs:
            pay_rows.append([
                Paragraph(str(a.get('payment_date', '')), s_small),
                Paragraph(str(a.get('payment_ref',  '')), s_small),
                Paragraph(str(a.get('payment_mode', '')), s_small),
                Paragraph(_pkr(a.get('allocated_amount', 0)),
                          ParagraphStyle('pamnt', fontSize=7.5,
                                         textColor=clr['cardamom'], alignment=TA_RIGHT)),
            ])
        pay_tbl = Table(pay_rows, colWidths=[W*0.15, W*0.32, W*0.15, W*0.28])
        pay_tbl.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,0),  clr['text_sub']),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),  [clr['gray_light'], clr['white']]),
            ('GRID',          (0,0),(-1,-1),  0.25, clr['gray_mid']),
            ('TOPPADDING',    (0,0),(-1,-1),  3),
            ('BOTTOMPADDING', (0,0),(-1,-1),  3),
            ('LEFTPADDING',   (0,0),(-1,-1),  5),
            ('RIGHTPADDING',  (0,0),(-1,-1),  5),
        ]))
        story.append(pay_tbl)

    # Footer
    story.append(Spacer(1, 20))
    story.append(HRFlowable(width='100%', thickness=0.5, color=clr['gray_mid']))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        'Thank you for your business. Please quote the invoice number on all payments. '
        'For queries contact accounts@spicetopia.com',
        s_footer))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        'Generated by Spicetopia BMS — ' + date.today().isoformat(),
        s_footer))

    doc = SimpleDocTemplate(buf, pagesize=A4,
                            leftMargin=18*mm, rightMargin=18*mm,
                            topMargin=14*mm, bottomMargin=14*mm)
    doc.build(story)
    return buf.getvalue()


def generate_statement_pdf(cust_id: int) -> bytes:
    """Generate a customer account statement PDF and return the raw bytes."""
    from io import BytesIO
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                    TableStyle, HRFlowable)

    cust = qry1("SELECT * FROM customers WHERE id=?", (cust_id,))
    if not cust:
        raise ValueError("Customer not found")

    today_str = date.today().isoformat()
    inv_rows  = qry("""
        SELECT id, invoice_number, invoice_date, due_date, status
        FROM invoices WHERE customer_id=? ORDER BY invoice_date ASC, id ASC
    """, (cust['id'],))
    invoices = []
    for inv in inv_rows:
        s2, t2, total2, paid2, bal2 = compute_invoice_balance(inv['id'])
        rec = dict(inv)
        rec['total'] = total2; rec['paid'] = paid2; rec['balance'] = bal2
        overdue = bal2 > 0 and inv['due_date'] and inv['due_date'] < today_str
        rec['days_overdue'] = (date.today() - date.fromisoformat(inv['due_date'])).days if overdue else 0
        invoices.append(rec)
    payments = qry("""
        SELECT id, payment_ref, payment_date, payment_mode, amount, notes
        FROM customer_payments WHERE customer_id=? ORDER BY payment_date ASC, id ASC
    """, (cust['id'],))

    events = []
    for inv in invoices:
        events.append({'date': inv['invoice_date'], 'type': 'INVOICE', 'data': inv})
    for pay in payments:
        events.append({'date': pay['payment_date'], 'type': 'PAYMENT', 'data': dict(pay)})
    events.sort(key=lambda x: (x['date'], 0 if x['type'] == 'INVOICE' else 1))

    lines   = []
    running = 0.0
    for ev in events:
        if ev['type'] == 'INVOICE':
            running += ev['data']['total']
            lines.append({
                'date': ev['date'], 'ref': ev['data']['invoice_number'],
                'type': 'Invoice', 'debit': ev['data']['total'],
                'credit': 0.0, 'balance': r2(running),
                'status': ev['data']['status'],
            })
        else:
            running -= ev['data']['amount']
            lines.append({
                'date': ev['date'], 'ref': ev['data']['payment_ref'],
                'type': 'Payment (' + ev["data"]["payment_mode"] + ')',
                'debit': 0.0, 'credit': ev['data']['amount'],
                'balance': r2(running), 'status': '',
            })

    total_invoiced = sum(i['total']   for i in invoices)
    total_paid     = sum(i['paid']    for i in invoices)
    balance_due    = sum(i['balance'] for i in invoices)

    clr = _pdf_colors()
    buf = BytesIO()
    W, H = A4

    def pstyle(name, **kw):
        d = dict(fontName='Helvetica', fontSize=9, leading=12, textColor=clr['dark'])
        d.update(kw)
        return ParagraphStyle(name, **d)

    s_normal  = pstyle('n')
    s_small   = pstyle('sm',  fontSize=7.5, textColor=clr['text_sub'])
    s_right   = pstyle('r',   fontSize=8.5, alignment=TA_RIGHT)
    s_right_b = pstyle('rb',  fontName='Helvetica-Bold', fontSize=8.5, alignment=TA_RIGHT)
    s_center  = pstyle('c',   fontSize=8.5, alignment=TA_CENTER)
    s_footer  = pstyle('ft',  fontSize=7.5, textColor=clr['text_sub'], alignment=TA_CENTER)

    story = []

    # Header band
    header_data = [[
        Paragraph('<b>SPICETOPIA</b>',
                  ParagraphStyle('ht', fontName='Helvetica-Bold', fontSize=20,
                                  textColor=clr['white'], leading=26)),
        Paragraph('<b>ACCOUNT STATEMENT</b>',
                  ParagraphStyle('hts', fontName='Helvetica-Bold', fontSize=13,
                                  textColor=clr['white'], leading=18, alignment=TA_RIGHT))
    ]]
    hdr_tbl = Table(header_data, colWidths=[W*0.55, W*0.35])
    hdr_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,-1), clr['saffron']),
        ('TOPPADDING',    (0,0),(-1,-1), 14),
        ('BOTTOMPADDING', (0,0),(-1,-1), 14),
        ('LEFTPADDING',   (0,0),(0,-1),  18),
        ('RIGHTPADDING',  (-1,0),(-1,-1),18),
        ('VALIGN',        (0,0),(-1,-1), 'MIDDLE'),
    ]))
    story.append(hdr_tbl)
    story.append(Spacer(1, 8))

    # Customer info + Statement date
    meta_data = [[
        [
            Paragraph('CUSTOMER', pstyle('cl', fontName='Helvetica-Bold', fontSize=7,
                                          textColor=clr['text_sub'], leading=10)),
            Paragraph('<b>' + cust["name"] + '</b>',
                       pstyle('cn', fontName='Helvetica-Bold', fontSize=11, leading=14)),
            Paragraph('Account: ' + (cust.get("account_number") or cust.get("code", "")),
                       pstyle('ca', fontSize=8, textColor=clr['text_sub'])),
        ],
        [
            Paragraph('STATEMENT DATE', pstyle('sd_l', fontName='Helvetica-Bold', fontSize=7,
                                                textColor=clr['text_sub'], leading=10)),
            Paragraph(today_str, pstyle('sd_v', fontName='Helvetica-Bold', fontSize=11, leading=14)),
        ]
    ]]
    meta_tbl = Table(meta_data, colWidths=[W*0.55, W*0.35])
    meta_tbl.setStyle(TableStyle([
        ('VALIGN',        (0,0),(-1,-1), 'TOP'),
        ('LEFTPADDING',   (0,0),(0,-1),  0),
        ('RIGHTPADDING',  (-1,0),(-1,-1),0),
        ('TOPPADDING',    (0,0),(-1,-1), 0),
        ('BOTTOMPADDING', (0,0),(-1,-1), 0),
    ]))
    story.append(meta_tbl)
    story.append(Spacer(1, 10))

    # Summary box
    bal_color = clr['chili'] if balance_due > 0 else clr['cardamom']
    sum_data = [[
        [Paragraph('TOTAL INVOICED', pstyle('sl',  fontName='Helvetica-Bold', fontSize=7,
                                             textColor=clr['text_sub'], leading=9, alignment=TA_CENTER)),
         Paragraph(_pkr(total_invoiced),
                   pstyle('sv',  fontName='Helvetica-Bold', fontSize=12, leading=16, alignment=TA_CENTER))],
        [Paragraph('TOTAL PAID',    pstyle('sl2', fontName='Helvetica-Bold', fontSize=7,
                                             textColor=clr['text_sub'], leading=9, alignment=TA_CENTER)),
         Paragraph(_pkr(total_paid),
                   pstyle('sv2', fontName='Helvetica-Bold', fontSize=12, leading=16,
                           textColor=clr['cardamom'], alignment=TA_CENTER))],
        [Paragraph('BALANCE DUE',   pstyle('sl3', fontName='Helvetica-Bold', fontSize=7,
                                             textColor=clr['white'], leading=9, alignment=TA_CENTER)),
         Paragraph(_pkr(balance_due),
                   pstyle('sv3', fontName='Helvetica-Bold', fontSize=14, leading=18,
                           textColor=clr['white'], alignment=TA_CENTER))],
    ]]
    sum_tbl = Table(sum_data, colWidths=[W*0.28, W*0.28, W*0.34])
    sum_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(1,-1), clr['gray_light']),
        ('BACKGROUND',    (2,0),(2,-1), bal_color),
        ('BOX',           (0,0),(-1,-1), 0.5, clr['gray_mid']),
        ('LINEAFTER',     (0,0),(1,-1), 0.5, clr['gray_mid']),
        ('TOPPADDING',    (0,0),(-1,-1), 8),
        ('BOTTOMPADDING', (0,0),(-1,-1), 8),
        ('LEFTPADDING',   (0,0),(-1,-1), 8),
        ('RIGHTPADDING',  (0,0),(-1,-1), 8),
    ]))
    story.append(sum_tbl)
    story.append(Spacer(1, 14))
    story.append(HRFlowable(width='100%', thickness=1, color=clr['gray_mid']))
    story.append(Spacer(1, 8))

    # Transaction table
    th_s = ParagraphStyle('th',  fontName='Helvetica-Bold', fontSize=8,
                            textColor=clr['white'], leading=10)
    th_r = ParagraphStyle('thr', fontName='Helvetica-Bold', fontSize=8,
                            textColor=clr['white'], leading=10, alignment=TA_RIGHT)

    tx_rows = [[
        Paragraph('DATE',      th_s),
        Paragraph('REFERENCE', th_s),
        Paragraph('TYPE',      th_s),
        Paragraph('DEBIT',     th_r),
        Paragraph('CREDIT',    th_r),
        Paragraph('BALANCE',   th_r),
    ]]
    for ln in lines:
        is_inv  = ln['debit'] > 0
        is_pay  = ln['credit'] > 0
        bal_col = clr['chili'] if float(ln['balance']) > 0 else clr['cardamom']
        tx_rows.append([
            Paragraph(str(ln['date']), s_small),
            Paragraph(str(ln['ref']),  pstyle('ref', fontSize=8,
                                               fontName='Helvetica-Bold' if is_inv else 'Helvetica')),
            Paragraph(str(ln['type']), s_small),
            Paragraph(_pkr(ln['debit'])  if is_inv else '—',
                       pstyle('db', fontSize=8, alignment=TA_RIGHT, textColor=clr['dark'])),
            Paragraph(_pkr(ln['credit']) if is_pay else '—',
                       pstyle('cr', fontSize=8, alignment=TA_RIGHT, textColor=clr['cardamom'])),
            Paragraph(_pkr(ln['balance']),
                       pstyle('bl', fontSize=8, fontName='Helvetica-Bold',
                               alignment=TA_RIGHT, textColor=bal_col)),
        ])

    tx_tbl = Table(tx_rows, colWidths=[W*0.12, W*0.22, W*0.23, W*0.13, W*0.13, W*0.13])
    tx_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,0),  clr['dark']),
        ('ROWBACKGROUNDS',(0,1),(-1,-1), [clr['gray_light'], clr['white']]),
        ('GRID',          (0,0),(-1,-1), 0.25, clr['gray_mid']),
        ('TOPPADDING',    (0,0),(-1,-1), 4),
        ('BOTTOMPADDING', (0,0),(-1,-1), 4),
        ('LEFTPADDING',   (0,0),(-1,-1), 6),
        ('RIGHTPADDING',  (0,0),(-1,-1), 6),
        ('VALIGN',        (0,0),(-1,-1), 'MIDDLE'),
    ]))
    story.append(tx_tbl)

    # Footer
    story.append(Spacer(1, 16))
    story.append(HRFlowable(width='100%', thickness=0.5, color=clr['gray_mid']))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        'This statement is computer generated and is accurate as of the date shown. '
        'For queries contact accounts@spicetopia.com',
        s_footer))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        'Generated by Spicetopia BMS — ' + today_str,
        s_footer))

    doc = SimpleDocTemplate(buf, pagesize=A4,
                            leftMargin=18*mm, rightMargin=18*mm,
                            topMargin=14*mm, bottomMargin=14*mm)
    doc.build(story)
    return buf.getvalue()
