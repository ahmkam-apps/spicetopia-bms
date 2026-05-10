"""
modules/orders.py — Customer order CRUD, review queue, soft holds, WA notifications,
                    WO-from-order, invoice-from-order, credit limit enforcement.

Extracted from server.py (Sprint 9). Overrides originals via bottom-import pattern:
    from modules.orders import *   # just before __main__ in server.py

Module-level config vars — synced by server.py after _apply_startup_config():
    import modules.orders as _ord_mod
    _ord_mod.SOFT_HOLD_EXPIRY_HOURS = SOFT_HOLD_EXPIRY_HOURS
    _ord_mod.WA_ENABLED    = WA_ENABLED
    _ord_mod.WA_ADMIN_PHONE  = WA_ADMIN_PHONE
    _ord_mod.WA_ADMIN_APIKEY = WA_ADMIN_APIKEY

Callback hooks — wired at startup for functions still in server.py:
    import modules.orders as _ord_mod
    _ord_mod._is_out_of_route_fn       = _is_out_of_route
    _ord_mod._wa_notify_out_of_route_fn = _wa_notify_out_of_route
    _ord_mod._check_wo_feasibility_fn  = check_wo_feasibility
    _ord_mod._refresh_ref              = load_ref
"""

import threading
from datetime import datetime, timedelta, date

from modules.db    import _conn, qry, qry1, run, run_many, save_db, audit_log
from modules.utils import r2, fmtpkr, today, validate_fields, _log

__all__ = [
    # Credit limit
    '_enforce_credit_limit',
    # WhatsApp helpers
    '_wa_send', '_wa_admin', '_wa_rep',
    '_wa_order_row',
    '_wa_notify_order_queued', '_wa_notify_order_approved',
    '_wa_notify_order_rejected', '_wa_notify_hold_expiring', '_wa_notify_hold_expired',
    # Soft hold lifecycle
    'place_soft_hold', 'release_soft_hold', 'convert_soft_hold_to_hard_reservation',
    'check_and_expire_holds',
    # Review queue
    'create_customer_order_external', 'get_review_queue',
    'approve_order_with_edit', 'update_order_item_qty',
    'reject_order', 'reopen_rejected_order',
    # Order CRUD
    '_order_status', '_order_detail',
    'list_customer_orders', '_check_order_stock_warnings',
    'create_customer_order', 'update_customer_order',
    'add_customer_order_item', 'confirm_customer_order', 'cancel_customer_order',
    # WO + invoice from order
    'create_wo_from_order_item', 'generate_invoice_from_order',
]

# ── Module-level config — synced from server.py after _apply_startup_config() ──
SOFT_HOLD_EXPIRY_HOURS = 48
WA_ENABLED    = False
WA_ADMIN_PHONE  = ''
WA_ADMIN_APIKEY = ''

# ── Callbacks for functions still in server.py ───────────────────────────────
_is_out_of_route_fn        = lambda rep_id, cust_id: False
_wa_notify_out_of_route_fn = lambda order_id, rep_id: None
_check_wo_feasibility_fn   = lambda variant_id, qty, wo_id=None: {'feasible': True, 'shortfalls': []}
_refresh_ref               = lambda: None   # wired to load_ref at startup


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _lookup_customer(cust_code):
    """Return customer row by code (active only)."""
    return qry1("SELECT * FROM customers WHERE code=? AND active=1", (cust_code,))


def _lookup_variant(product_code, pack_size):
    """Return variant row by (product_code, pack_size_label)."""
    return qry1("""
        SELECT pv.*, p.code as product_code, p.name as product_name,
               ps.label as pack_size
        FROM product_variants pv
        JOIN products p    ON p.id  = pv.product_id
        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE p.code=? AND ps.label=? AND pv.active_flag=1
    """, (product_code, pack_size))


# ═══════════════════════════════════════════════════════════════════
#  CREDIT LIMIT ENFORCEMENT
# ═══════════════════════════════════════════════════════════════════

def _enforce_credit_limit(cust_id: int, new_invoice_total: float):
    """
    Hard-block guard — raises ValueError if adding new_invoice_total would push the
    customer over their credit limit. Called before any invoice is written.
    No-op when credit_limit = 0 (unlimited).
    """
    cust = qry1("SELECT name, credit_limit FROM customers WHERE id=?", (cust_id,))
    if not cust:
        return
    credit_limit = float(cust.get('credit_limit') or 0)
    if credit_limit <= 0:
        return   # 0 = unlimited

    ar_row = qry1("""
        SELECT COALESCE(SUM(
            COALESCE((SELECT SUM(quantity*unit_price) FROM invoice_items WHERE invoice_id=i.id), 0)
            - COALESCE((SELECT SUM(allocated_amount) FROM payment_allocations WHERE invoice_id=i.id), 0)
        ), 0) AS balance
        FROM invoices i
        WHERE i.customer_id=? AND i.status IN ('UNPAID','PARTIAL')
    """, (cust_id,))
    ar_balance = float((ar_row or {}).get('balance', 0))

    if ar_balance + new_invoice_total > credit_limit:
        raise ValueError(
            f"Credit limit exceeded for {cust['name']}: "
            f"outstanding AR {fmtpkr(ar_balance)} + this invoice {fmtpkr(new_invoice_total)} "
            f"= {fmtpkr(ar_balance + new_invoice_total)} — limit is {fmtpkr(credit_limit)}"
        )


# ═══════════════════════════════════════════════════════════════════
#  WHATSAPP NOTIFICATIONS (CallMeBot)
# ═══════════════════════════════════════════════════════════════════

def _wa_send(phone: str, apikey: str, message: str):
    """Non-blocking CallMeBot send. Fires in a daemon thread — never blocks the request."""
    import urllib.request, urllib.parse
    def _do():
        try:
            url = (
                f"https://api.callmebot.com/whatsapp.php"
                f"?phone={phone}&text={urllib.parse.quote(message)}&apikey={apikey}"
            )
            with urllib.request.urlopen(url, timeout=15) as resp:
                body = resp.read().decode('utf-8', errors='ignore')
            _log('info', f"whatsapp: Sent to {phone[:5]}**** — {body[:80]}")
        except Exception as e:
            _log('error', f"whatsapp: Failed to {phone[:5]}****: {e}")
    threading.Thread(target=_do, daemon=True).start()


def _wa_admin(message: str):
    """Send a notification to the configured admin WhatsApp number."""
    if not WA_ENABLED or not WA_ADMIN_PHONE or not WA_ADMIN_APIKEY:
        return
    _wa_send(WA_ADMIN_PHONE, WA_ADMIN_APIKEY, message)


def _wa_rep(rep_id, message: str):
    """Send a notification to a field rep if they have a WhatsApp API key registered."""
    if not WA_ENABLED or not rep_id:
        return
    rep = qry1("SELECT phone, whatsapp_apikey FROM sales_reps WHERE id=?", (rep_id,))
    if rep and rep.get('whatsapp_apikey') and rep.get('phone'):
        _wa_send(rep['phone'], rep['whatsapp_apikey'], message)


def _wa_order_row(order_id) -> dict:
    """Fetch the order fields needed for all notification messages."""
    return qry1("""
        SELECT co.order_number, co.order_source, co.created_by_rep_id,
               c.name as customer_name
        FROM customer_orders co
        JOIN customers c ON c.id = co.customer_id
        WHERE co.id = ?
    """, (order_id,))


def _wa_notify_order_queued(order_id):
    """Notify admin that a new external order is waiting in the review queue."""
    o = _wa_order_row(order_id)
    if not o:
        return
    src = {'consumer_website': '🛒 Website', 'retailer_self_service': '🏪 Retailer',
           'field_rep': '👤 Sales Rep'}.get(o['order_source'], o['order_source'])
    _wa_admin(
        f"🔔 *NEW ORDER IN QUEUE*\n"
        f"Order: {o['order_number']}\n"
        f"Customer: {o['customer_name']}\n"
        f"Source: {src}\n\n"
        f"Review at: BMS → Review Queue"
    )


def _wa_notify_order_approved(order_id, approval_note=''):
    """Notify admin and the creating field rep that an order was approved."""
    o = _wa_order_row(order_id)
    if not o:
        return
    note_line = f"\nNote: {approval_note}" if approval_note else ''
    msg = (
        f"✅ *ORDER APPROVED*\n"
        f"Order: {o['order_number']}\n"
        f"Customer: {o['customer_name']}{note_line}\n\n"
        f"Status: Confirmed — proceed to invoicing."
    )
    _wa_admin(msg)
    _wa_rep(o['created_by_rep_id'], msg)


def _wa_notify_order_rejected(order_id, reason: str):
    """Notify admin and the creating field rep that an order was rejected."""
    o = _wa_order_row(order_id)
    if not o:
        return
    msg = (
        f"❌ *ORDER REJECTED*\n"
        f"Order: {o['order_number']}\n"
        f"Customer: {o['customer_name']}\n"
        f"Reason: {reason}\n\n"
        f"Action required — contact customer or reopen from BMS → Review Queue."
    )
    _wa_admin(msg)
    _wa_rep(o['created_by_rep_id'], msg)


def _wa_notify_hold_expiring(order_id, hours_remaining: int):
    """Warn admin that a soft hold is approaching expiry."""
    o = _wa_order_row(order_id)
    if not o:
        return
    _wa_admin(
        f"⚠️ *HOLD EXPIRING SOON*\n"
        f"Order: {o['order_number']}\n"
        f"Customer: {o['customer_name']}\n"
        f"Hold expires in: ~{hours_remaining}h\n\n"
        f"Review at: BMS → Review Queue before stock is released."
    )


def _wa_notify_hold_expired(order_id):
    """Notify admin that a soft hold has lapsed and stock was released."""
    o = _wa_order_row(order_id)
    if not o:
        return
    _wa_admin(
        f"⌛ *HOLD EXPIRED*\n"
        f"Order: {o['order_number']}\n"
        f"Customer: {o['customer_name']}\n"
        f"Stock hold released automatically.\n\n"
        f"Reopen from BMS → Review Queue if still needed."
    )


# ═══════════════════════════════════════════════════════════════════
#  SOFT HOLD LIFECYCLE
# ═══════════════════════════════════════════════════════════════════

def place_soft_hold(order_id):
    """
    Place a soft hold on stock for all items in a pending_review order.
    Populates qty_soft_hold on each item and records the hold expiry time.
    Safe to call even if stock is insufficient — hold is placed anyway.
    """
    items      = qry("SELECT id, qty_ordered FROM customer_order_items WHERE order_id=?", (order_id,))
    now        = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    expires_at = (datetime.utcnow() + timedelta(hours=SOFT_HOLD_EXPIRY_HOURS)).strftime('%Y-%m-%dT%H:%M:%S')

    c = _conn()
    try:
        for item in items:
            c.execute(
                "UPDATE customer_order_items SET qty_soft_hold=? WHERE id=?",
                (item['qty_ordered'], item['id'])
            )
        # Upsert hold expiry record
        c.execute("""
            INSERT INTO order_hold_expiry
                (order_id, hold_placed_at, hold_expires_at, is_expired, notification_sent)
            VALUES (?, ?, ?, 0, 0)
            ON CONFLICT(order_id) DO UPDATE SET
                hold_placed_at  = excluded.hold_placed_at,
                hold_expires_at = excluded.hold_expires_at,
                is_expired      = 0,
                notification_sent = 0,
                expired_at      = NULL
        """, (order_id, now, expires_at))
        c.commit()
    finally:
        c.close()
    _log('info', 'soft_hold_placed', order_id=order_id, expires_at=expires_at)


def release_soft_hold(order_id):
    """Release soft holds for an order. Idempotent."""
    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    run("UPDATE customer_order_items SET qty_soft_hold=0 WHERE order_id=?", (order_id,))
    run("""
        UPDATE order_hold_expiry
        SET is_expired=1, expired_at=?
        WHERE order_id=? AND is_expired=0
    """, (now, order_id))
    _log('info', 'soft_hold_released', order_id=order_id)


def convert_soft_hold_to_hard_reservation(order_id):
    """
    When an order is approved/confirmed: clear qty_soft_hold.
    Hard reservation is implicitly the confirmed status itself.
    """
    run("UPDATE customer_order_items SET qty_soft_hold=0 WHERE order_id=?", (order_id,))
    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    run("""
        UPDATE order_hold_expiry
        SET is_expired=1, expired_at=?, notification_sent=1
        WHERE order_id=? AND is_expired=0
    """, (now, order_id))


def check_and_expire_holds():
    """
    Find all holds past expiry and release them.
    Called by the background thread every hour.
    Returns count of expired orders.
    """
    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    expired_rows = qry("""
        SELECT ohe.order_id
        FROM order_hold_expiry ohe
        JOIN customer_orders co ON co.id = ohe.order_id
        WHERE ohe.is_expired = 0
          AND ohe.hold_expires_at <= ?
          AND co.status = 'pending_review'
    """, (now,))

    count = 0
    for row in expired_rows:
        oid = row['order_id']
        release_soft_hold(oid)
        run("""
            UPDATE customer_orders
            SET status='expired', updated_at=datetime('now')
            WHERE id=? AND status='pending_review'
        """, (oid,))
        _wa_notify_hold_expired(oid)
        count += 1

    if count:
        _log('info', 'hold_expiry', f"Expired {count} soft holds")
    return count


# ═══════════════════════════════════════════════════════════════════
#  REVIEW QUEUE
# ═══════════════════════════════════════════════════════════════════

def create_customer_order_external(data):
    """
    Entry point for orders from non-BMS channels.
    order_source: 'consumer_website' | 'retailer_self_service' | 'field_rep' | 'rep_assisted'
    """
    from modules.inventory import get_stock_situation
    source = data.get('order_source', 'internal')
    if source not in ('consumer_website', 'retailer_self_service', 'field_rep', 'rep_assisted'):
        raise ValueError(f"Invalid order_source: {source}")

    # Translate portal format → internal format
    if 'customerId' in data and 'custCode' not in data:
        cust_row = qry1("SELECT code FROM customers WHERE id=?", (int(data['customerId']),))
        if not cust_row:
            raise ValueError(f"Customer not found: id={data['customerId']}")
        data['custCode'] = cust_row['code']

    if 'items' in data and 'lines' not in data:
        lines = []
        for item in data.get('items', []):
            vid = item.get('variantId')
            var = qry1("""
                SELECT p.code as productCode, ps.label as packSize
                FROM product_variants pv
                JOIN products p    ON p.id  = pv.product_id
                JOIN pack_sizes ps ON ps.id = pv.pack_size_id
                WHERE pv.id=?
            """, (vid,))
            if not var:
                raise ValueError(f"Product variant not found: variantId={vid}")
            lines.append({
                'productCode': var['productCode'],
                'packSize':    var['packSize'],
                'qty':         item.get('qty', 1),
                'unitPrice':   item.get('unitPrice', 0),
            })
        data['lines'] = lines

    # Idempotency check (rep_assisted orders)
    idem_key = data.get('idempotency_key')
    rep_id   = data.get('created_by_rep_id') or data.get('placed_by_rep_id')
    if idem_key and rep_id:
        existing = qry1("""
            SELECT id FROM customer_orders
            WHERE idempotency_key=? AND created_by_rep_id=?
            AND created_at >= datetime('now', '-24 hours')
        """, (idem_key, rep_id))
        if existing:
            result = _order_detail(existing['id'])
            result['_idempotent'] = True
            return result

    # Create the base order as draft
    order    = create_customer_order(data)
    order_id = order['orderId']

    # Tag order source, creating rep, and idempotency key
    rep_id = data.get('created_by_rep_id') or data.get('placed_by_rep_id')
    if rep_id:
        run("""UPDATE customer_orders
               SET order_source=?, created_by_rep_id=?, idempotency_key=?
               WHERE id=?""", (source, rep_id, idem_key, order_id))
    else:
        run("UPDATE customer_orders SET order_source=?, idempotency_key=? WHERE id=?",
            (source, idem_key, order_id))

    if source in ('field_rep', 'rep_assisted'):
        # Bypass review queue — stays as draft, no soft hold
        order['order_source'] = source
        order['inReviewQueue'] = False

        # Out-of-route check for rep_assisted orders
        if source == 'rep_assisted' and rep_id:
            customer_id = order.get('customerId') or qry1(
                "SELECT customer_id FROM customer_orders WHERE id=?", (order_id,))
            if customer_id:
                cid = customer_id if isinstance(customer_id, int) else customer_id['customer_id']
                if _is_out_of_route_fn(int(rep_id), cid):
                    run("UPDATE customer_orders SET out_of_route=1 WHERE id=?", (order_id,))
                    order['outOfRoute'] = True
                    try:
                        _wa_notify_out_of_route_fn(order_id, int(rep_id))
                    except Exception as e:
                        _log('warn', 'out_of_route_wa_failed', order_id=order_id, error=str(e))
                else:
                    order['outOfRoute'] = False
        return order

    # External orders → pending_review + soft hold
    run("""
        UPDATE customer_orders
        SET status='pending_review',
            order_source=?,
            approval_method='manual',
            updated_at=datetime('now')
        WHERE id=?
    """, (source, order_id))

    place_soft_hold(order_id)

    detail = _order_detail(order_id)

    # Compute stock warnings for acknowledgment
    items = qry("SELECT * FROM customer_order_items WHERE order_id=?", (order_id,))
    stock_warnings = []
    for item in items:
        sit = get_stock_situation(item['product_variant_id'])
        if item['qty_ordered'] > sit['available']:
            short = item['qty_ordered'] - sit['available']
            stock_warnings.append({
                'variantId':    item['product_variant_id'],
                'qtyOrdered':   item['qty_ordered'],
                'qtyAvailable': sit['available'],
                'shortfall':    short
            })

    hold_row = qry1("SELECT hold_expires_at FROM order_hold_expiry WHERE order_id=?", (order_id,))
    detail['holdExpiresAt']  = hold_row['hold_expires_at'] if hold_row else None
    detail['stockWarnings']  = stock_warnings
    detail['inReviewQueue']  = True

    _wa_notify_order_queued(order_id)
    return detail


def get_review_queue(filters=None):
    """Return all pending_review orders with stock context per item."""
    from modules.inventory import get_stock_situation
    filters = filters or {}
    where_clauses = ["co.status='pending_review'"]
    params = []
    if filters.get('order_source'):
        where_clauses.append("co.order_source=?")
        params.append(filters['order_source'])

    sql = f"""
        SELECT
            co.id, co.order_number, co.status, co.order_source,
            co.approval_method, co.created_at, co.updated_at,
            c.code as cust_code,
            c.name as customer_name,
            ohe.hold_placed_at, ohe.hold_expires_at, ohe.is_expired
        FROM customer_orders co
        LEFT JOIN customers c ON c.id = co.customer_id
        LEFT JOIN order_hold_expiry ohe ON ohe.order_id = co.id
        WHERE {' AND '.join(where_clauses)}
        ORDER BY co.created_at ASC
    """
    orders = qry(sql, tuple(params))
    result = []

    for o in orders:
        order_dict = dict(o)
        items = qry("""
            SELECT coi.*, pv.id as variant_id,
                   p.name as product_name, ps.label as pack_label
            FROM customer_order_items coi
            JOIN product_variants pv ON pv.id = coi.product_variant_id
            JOIN products p ON p.id = pv.product_id
            JOIN pack_sizes ps ON ps.id = pv.pack_size_id
            WHERE coi.order_id=?
        """, (o['id'],))

        enriched_items = []
        for item in items:
            sit = get_stock_situation(item['product_variant_id'])
            enriched_items.append({
                'id':              item['id'],
                'productVariantId': item['product_variant_id'],
                'productName':     item['product_name'],
                'packLabel':       item['pack_label'],
                'qtyOrdered':      item['qty_ordered'],
                'qtySoftHold':     item['qty_soft_hold'],
                'unitPrice':       item['unit_price'],
                'stock':           sit,
                'canFulfill':      item['qty_ordered'] <= sit['physical']
            })

        hold_remaining_seconds = None
        if o['hold_expires_at'] and not o['is_expired']:
            try:
                exp  = datetime.strptime(o['hold_expires_at'], '%Y-%m-%dT%H:%M:%S')
                diff = (exp - datetime.utcnow()).total_seconds()
                hold_remaining_seconds = max(0, int(diff))
            except Exception:
                pass

        order_dict['items'] = enriched_items
        order_dict['holdRemainingSeconds'] = hold_remaining_seconds
        result.append(order_dict)

    return result


def approve_order_with_edit(order_id, data):
    """
    Admin approves a pending_review order, optionally editing quantities.
    data: {quantities: [{itemId, qty}], approvalNote: str}
    """
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] != 'pending_review':
        raise ValueError(f"Order is {order['status']} — can only approve pending_review orders")

    for q in data.get('quantities', []):
        item_id = q.get('itemId')
        new_qty = q.get('qty')
        if item_id is None or new_qty is None:
            continue
        if new_qty <= 0:
            raise ValueError(f"Item {item_id}: quantity must be > 0")
        item = qry1("SELECT * FROM customer_order_items WHERE id=? AND order_id=?", (item_id, order_id))
        if not item:
            raise ValueError(f"Item {item_id} not found on order {order_id}")
        run("UPDATE customer_order_items SET qty_ordered=? WHERE id=?", (new_qty, item_id))

    approval_note = data.get('approvalNote', '')
    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

    convert_soft_hold_to_hard_reservation(order_id)
    run("""
        UPDATE customer_orders
        SET status='confirmed',
            approval_method='manual',
            approval_timestamp=?,
            approval_note=?,
            updated_at=datetime('now')
        WHERE id=?
    """, (now, approval_note, order_id))

    detail = _order_detail(order_id)
    detail['approved'] = True
    _wa_notify_order_approved(order_id, approval_note)
    return detail


def update_order_item_qty(order_id, item_id, new_qty):
    """Update qty_ordered on a single order line (admin/sales only)."""
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] in ('fully_invoiced', 'cancelled'):
        raise ValueError(f"Cannot edit items on a {order['status']} order")

    item = qry1("SELECT * FROM customer_order_items WHERE id=? AND order_id=?", (item_id, order_id))
    if not item:
        raise ValueError(f"Item {item_id} not found on order {order_id}")

    new_qty = int(new_qty)
    if new_qty <= 0:
        raise ValueError("Quantity must be greater than zero")

    committed = (item['qty_in_production'] or 0) + (item['qty_invoiced'] or 0)
    if new_qty < committed:
        raise ValueError(
            f"Cannot reduce below {committed} units "
            f"({item['qty_in_production'] or 0} in production + {item['qty_invoiced'] or 0} invoiced)"
        )

    run("UPDATE customer_order_items SET qty_ordered=? WHERE id=?", (new_qty, item_id))
    if order['status'] in ('draft', 'confirmed'):
        run("UPDATE customer_order_items SET qty_soft_hold=? WHERE id=?", (new_qty, item_id))
    return _order_detail(order_id)


def reject_order(order_id, reason):
    """Admin rejects a pending_review order. Releases soft hold."""
    if not reason or not reason.strip():
        raise ValueError("Rejection reason is mandatory")
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] != 'pending_review':
        raise ValueError(f"Order is {order['status']} — can only reject pending_review orders")

    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    release_soft_hold(order_id)
    run("""
        UPDATE customer_orders
        SET status='rejected',
            rejection_reason=?,
            approval_timestamp=?,
            updated_at=datetime('now')
        WHERE id=?
    """, (reason.strip(), now, order_id))

    _wa_notify_order_rejected(order_id, reason.strip())
    return {'ok': True, 'orderId': order_id, 'status': 'rejected', 'rejectionReason': reason.strip()}


def reopen_rejected_order(order_id):
    """Re-enter a rejected order into the review queue with a fresh soft hold."""
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] != 'rejected':
        raise ValueError(f"Order is {order['status']} — can only reopen rejected orders")

    run("""
        UPDATE customer_orders
        SET status='pending_review',
            rejection_reason='',
            approval_timestamp=NULL,
            updated_at=datetime('now')
        WHERE id=?
    """, (order_id,))
    place_soft_hold(order_id)
    return _order_detail(order_id)


# ═══════════════════════════════════════════════════════════════════
#  ORDER CRUD
# ═══════════════════════════════════════════════════════════════════

def _order_status(order_id):
    """Compute status from item quantities. Never reads the stored status column."""
    items = qry("SELECT qty_ordered, qty_invoiced FROM customer_order_items WHERE order_id=?", (order_id,))
    if not items:
        return 'draft'
    total_ordered  = sum(i['qty_ordered']  for i in items)
    total_invoiced = sum(i['qty_invoiced'] for i in items)
    if total_invoiced == 0:
        return 'confirmed'
    if total_invoiced < total_ordered:
        return 'partially_invoiced'
    return 'invoiced'


def _order_detail(order_id):
    """Return full order dict with items, linked WOs, linked invoices."""
    order = qry1("""
        SELECT co.*, c.name as customer_name, c.code as customer_code,
               c.phone as customer_phone, c.city as customer_city
        FROM customer_orders co
        JOIN customers c ON c.id = co.customer_id
        WHERE co.id = ?
    """, (order_id,))
    if not order:
        return None

    items = qry("""
        SELECT coi.*, p.name as product_name, p.code as product_code,
               ps.label as pack_size, pv.sku_code
        FROM customer_order_items coi
        JOIN product_variants pv ON pv.id = coi.product_variant_id
        JOIN products p           ON p.id  = pv.product_id
        JOIN pack_sizes ps        ON ps.id = pv.pack_size_id
        WHERE coi.order_id = ?
        ORDER BY coi.id
    """, (order_id,))

    wos = qry("""
        SELECT wo.id, wo.wo_number, wo.qty_units, wo.status, wo.target_date,
               wo.customer_order_item_id,
               p.name as product_name, ps.label as pack_size
        FROM work_orders wo
        JOIN product_variants pv ON pv.id = wo.product_variant_id
        JOIN products p           ON p.id  = pv.product_id
        JOIN pack_sizes ps        ON ps.id = pv.pack_size_id
        WHERE wo.customer_order_id = ?
        ORDER BY wo.id
    """, (order_id,))

    invs = qry("""
        SELECT inv.id, inv.invoice_number, inv.invoice_date, inv.status,
               COALESCE(SUM(ii.line_total), 0) as subtotal
        FROM invoices inv
        LEFT JOIN invoice_items ii ON ii.invoice_id = inv.id
        WHERE inv.customer_order_id = ?
        GROUP BY inv.id
        ORDER BY inv.id
    """, (order_id,))

    skip_recompute = ('draft', 'cancelled', 'pending_review', 'rejected', 'expired')
    computed_status = _order_status(order_id) if order['status'] not in skip_recompute else order['status']
    order['status']     = computed_status
    order['items']      = items
    order['workOrders'] = wos
    order['invoices']   = invs

    hold_row = qry1("SELECT hold_expires_at, is_expired FROM order_hold_expiry WHERE order_id=?", (order_id,))
    if hold_row and not hold_row['is_expired']:
        order['holdExpiresAt'] = hold_row['hold_expires_at']
        try:
            exp  = datetime.strptime(hold_row['hold_expires_at'], '%Y-%m-%dT%H:%M:%S')
            diff = (exp - datetime.utcnow()).total_seconds()
            order['holdRemainingSeconds'] = max(0, int(diff))
        except Exception:
            order['holdRemainingSeconds'] = None
    else:
        order['holdExpiresAt']       = None
        order['holdRemainingSeconds'] = None

    return order


def list_customer_orders(status_filter=None):
    """Return all orders with summary counts, newest first."""
    sql = """
        SELECT co.id, co.order_number, co.order_date, co.required_date,
               co.status, co.notes, co.created_at,
               c.name as customer_name, c.code as customer_code,
               COUNT(DISTINCT coi.id)  as item_count,
               COALESCE(SUM(coi.qty_ordered), 0)  as total_qty,
               COALESCE(SUM(coi.qty_invoiced), 0) as invoiced_qty,
               COALESCE(SUM(coi.line_total), 0)   as order_value,
               COUNT(DISTINCT wo.id)  as wo_count,
               COUNT(DISTINCT inv.id) as invoice_count
        FROM customer_orders co
        JOIN customers c ON c.id = co.customer_id
        LEFT JOIN customer_order_items coi ON coi.order_id = co.id
        LEFT JOIN work_orders wo  ON wo.customer_order_id = co.id
        LEFT JOIN invoices inv    ON inv.customer_order_id = co.id
    """
    params = ()
    if status_filter:
        sql += " WHERE co.status = ?"
        params = (status_filter,)
    sql += " GROUP BY co.id ORDER BY co.id DESC"
    return qry(sql, params)


def _check_order_stock_warnings(resolved):
    """
    Check finished-goods availability for resolved order lines.
    Returns list of warning strings. Does NOT raise.
    """
    from modules.inventory import get_finished_stock_map
    fg_stock = get_finished_stock_map()
    warnings = []
    for r in resolved:
        vid   = r['var']['id']
        avail = fg_stock.get(vid, 0)
        if r['qty'] > avail:
            short = r['qty'] - avail
            label = f"{r['var'].get('product_name','?')} {r['var'].get('pack_size','')}"
            warnings.append(
                f"⚠ STOCK WARNING — {label}: {avail:.0f} units available, "
                f"{r['qty']} ordered (shortfall: {short:.0f} units). "
                f"Run production before invoicing."
            )
    return warnings


def create_customer_order(data):
    """
    Create a new customer order (status=draft).
    data: {custCode, orderDate, requiredDate, notes, lines:[{productCode, packSize, qty, unitPrice}]}
    """
    from modules.id_gen import next_id, _sync_counter_to_max
    validate_fields(data, [
        {'field': 'custCode',     'label': 'Customer',       'type': 'str'},
        {'field': 'orderDate',    'label': 'Order date',     'required': False, 'type': 'date'},
        {'field': 'requiredDate', 'label': 'Required date',  'required': False, 'type': 'date'},
    ])
    cust = _lookup_customer(data.get('custCode', ''))
    if not cust:
        raise ValueError(f"Customer not found: {data.get('custCode')}")
    lines = data.get('lines', [])
    if not lines:
        raise ValueError("At least one line item is required")

    order_date    = data.get('orderDate', today())
    required_date = data.get('requiredDate', '')
    notes         = data.get('notes', '')
    _sync_counter_to_max('customer_order', 'customer_orders', 'order_number', 'SP-ORD-')
    order_number  = next_id('customer_order', 'ORD')

    resolved = []
    for i, line in enumerate(lines):
        var = _lookup_variant(line.get('productCode', ''), line.get('packSize', ''))
        if not var:
            raise ValueError(f"Line {i+1}: variant not found: {line.get('productCode')}/{line.get('packSize')}")
        qty = int(line.get('qty', 0))
        if qty <= 0:
            raise ValueError(f"Line {i+1}: qty must be positive")
        unit_price = r2(line.get('unitPrice', 0))
        resolved.append({
            'var': var, 'qty': qty,
            'unit_price': unit_price,
            'line_total': r2(qty * unit_price)
        })

    stock_warnings = _check_order_stock_warnings(resolved)

    c = _conn()
    try:
        c.execute("""
            INSERT INTO customer_orders
                (order_number, customer_id, order_date, required_date, status, notes)
            VALUES (?, ?, ?, ?, 'draft', ?)
        """, (order_number, cust['id'], order_date, required_date, notes))
        order_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        for r in resolved:
            c.execute("""
                INSERT INTO customer_order_items
                    (order_id, product_variant_id, qty_ordered, unit_price, line_total)
                VALUES (?, ?, ?, ?, ?)
            """, (order_id, r['var']['id'], r['qty'], r['unit_price'], r['line_total']))

        audit_log([], 'customer_orders', order_number, 'CREATE', new_val=data)
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()
    return {
        'orderId':       order_id,
        'orderNumber':   order_number,
        'customerId':    cust['id'],
        'stockWarnings': stock_warnings,
    }


def update_customer_order(order_id, data):
    """
    Amend a customer order safely.
    Allowed statuses: draft (full edit), confirmed (line amendment), partially_invoiced (header only).
    """
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")

    editable_statuses = ('draft', 'confirmed', 'partially_invoiced')
    if order['status'] not in editable_statuses:
        raise ValueError(f"Order is {order['status']} — cannot be edited")

    lines_locked  = order['status'] == 'partially_invoiced'
    required_date = data.get('requiredDate', order['required_date'])
    notes         = data.get('notes', order['notes'] or '')
    lines         = data.get('lines')

    if lines is not None and lines_locked:
        raise ValueError(
            "Line items cannot be changed once invoicing has started. "
            "Only notes and required delivery date can be updated."
        )

    resolved = []
    if lines is not None:
        if not lines:
            raise ValueError("At least one line item is required")
        for i, line in enumerate(lines):
            var = _lookup_variant(line.get('productCode', ''), line.get('packSize', ''))
            if not var:
                raise ValueError(f"Line {i+1}: variant not found: {line.get('productCode')}/{line.get('packSize')}")
            qty = int(line.get('qty', 0))
            if qty <= 0:
                raise ValueError(f"Line {i+1}: qty must be positive")
            unit_price = r2(line.get('unitPrice', 0))
            resolved.append({
                'var': var, 'qty': qty,
                'unit_price': unit_price,
                'line_total': r2(qty * unit_price)
            })

    stock_warnings = _check_order_stock_warnings(resolved) if resolved else []
    qty_increases  = []

    c = _conn()
    try:
        c.execute("""
            UPDATE customer_orders
            SET required_date=?, notes=?, updated_at=datetime('now')
            WHERE id=?
        """, (required_date, notes, order_id))

        if lines is not None:
            existing = {
                row['product_variant_id']: row
                for row in qry("SELECT * FROM customer_order_items WHERE order_id=?", (order_id,))
            }
            submitted_vids = set()

            for r in resolved:
                vid = r['var']['id']
                submitted_vids.add(vid)
                ex  = existing.get(vid)

                if ex:
                    committed = ex['qty_in_production'] + ex['qty_invoiced']
                    if r['qty'] < committed:
                        var_label = f"{r['var'].get('product_name','?')} {r['var'].get('pack_size','')}"
                        raise ValueError(
                            f"{var_label}: cannot reduce to {r['qty']} — "
                            f"{ex['qty_in_production']} in production, {ex['qty_invoiced']} invoiced "
                            f"(minimum: {committed})"
                        )
                    if r['qty'] > ex['qty_ordered']:
                        qty_increases.append({
                            'itemId':      ex['id'],
                            'productName': r['var'].get('product_name', ''),
                            'packSize':    r['var'].get('pack_size', ''),
                            'oldQty':      ex['qty_ordered'],
                            'newQty':      r['qty'],
                            'delta':       r['qty'] - ex['qty_ordered']
                        })
                    c.execute("""
                        UPDATE customer_order_items
                        SET qty_ordered=?, unit_price=?, line_total=?
                        WHERE id=?
                    """, (r['qty'], r['unit_price'], r['line_total'], ex['id']))
                else:
                    c.execute("""
                        INSERT INTO customer_order_items
                            (order_id, product_variant_id, qty_ordered, unit_price, line_total)
                        VALUES (?, ?, ?, ?, ?)
                    """, (order_id, vid, r['qty'], r['unit_price'], r['line_total']))

            for vid, ex in existing.items():
                if vid not in submitted_vids:
                    committed = ex['qty_in_production'] + ex['qty_invoiced']
                    if committed > 0:
                        var_info = qry1("""
                            SELECT p.name, ps.label as pack FROM product_variants pv
                            JOIN products p ON p.id=pv.product_id
                            JOIN pack_sizes ps ON ps.id=pv.pack_size_id
                            WHERE pv.id=?
                        """, (vid,))
                        label = f"{var_info['name']} {var_info['pack']}" if var_info else f"Variant {vid}"
                        raise ValueError(
                            f"Cannot remove '{label}' — it has {ex['qty_in_production']} unit(s) "
                            f"in production and {ex['qty_invoiced']} unit(s) invoiced."
                        )
                    c.execute("DELETE FROM customer_order_items WHERE id=?", (ex['id'],))

        audit_log([], 'customer_orders', order['order_number'], 'UPDATE',
                  old_val={'required_date': order['required_date'], 'notes': order['notes']},
                  new_val=data)
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()
    detail = _order_detail(order_id)
    detail['stockWarnings'] = stock_warnings
    detail['qtyIncreases']  = qty_increases
    return detail


def add_customer_order_item(order_id, data):
    """Add a new line item to an existing customer order (draft or confirmed only)."""
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] not in ('draft', 'confirmed'):
        raise ValueError(f"Cannot add items to a {order['status']} order")

    var = _lookup_variant(data.get('productCode', ''), data.get('packSize', ''))
    if not var:
        raise ValueError(f"Product not found: {data.get('productCode')}/{data.get('packSize')}")
    qty = int(data.get('qty', 0))
    if qty <= 0:
        raise ValueError("Quantity must be positive")
    unit_price = r2(data.get('unitPrice', 0))
    line_total = r2(qty * unit_price)

    existing = qry1("SELECT id FROM customer_order_items WHERE order_id=? AND product_variant_id=?",
                    (order_id, var['id']))
    if existing:
        raise ValueError(f"{data.get('productCode')} {data.get('packSize')} is already on this order — "
                         f"edit the existing line instead")

    c = _conn()
    try:
        c.execute("""
            INSERT INTO customer_order_items (order_id, product_variant_id, qty_ordered, unit_price, line_total)
            VALUES (?, ?, ?, ?, ?)
        """, (order_id, var['id'], qty, unit_price, line_total))
        c.execute("UPDATE customer_orders SET updated_at=datetime('now') WHERE id=?", (order_id,))
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return _order_detail(order_id)


def confirm_customer_order(order_id):
    """Move order from draft or pending_review → confirmed. Warns (but does not block) on stock shortfalls."""
    from modules.inventory import get_finished_stock_map
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] not in ('draft', 'pending_review'):
        raise ValueError(f"Order is already {order['status']} — cannot confirm")

    came_from_pending_review = order['status'] == 'pending_review'

    items    = qry("SELECT * FROM customer_order_items WHERE order_id=?", (order_id,))
    fg_stock = get_finished_stock_map()
    stock_warnings = []
    for item in items:
        avail = fg_stock.get(item['product_variant_id'], 0)
        if item['qty_ordered'] > avail:
            short    = item['qty_ordered'] - avail
            var_info = qry1("""
                SELECT p.name, ps.label as pack FROM product_variants pv
                JOIN products p ON p.id=pv.product_id
                JOIN pack_sizes ps ON ps.id=pv.pack_size_id
                WHERE pv.id=?
            """, (item['product_variant_id'],))
            label = f"{var_info['name']} {var_info['pack']}" if var_info else f"Variant {item['product_variant_id']}"
            stock_warnings.append(
                f"⚠ STOCK WARNING — {label}: {avail:.0f} units available, "
                f"{item['qty_ordered']} ordered (shortfall: {short:.0f} units). "
                f"Run production before invoicing."
            )

    # Credit limit check — warn but do not block
    cust_code = qry1("SELECT code FROM customer_orders WHERE id=?", (order_id,))
    if cust_code:
        cust_row = qry1("SELECT id, name, credit_limit FROM customers WHERE code=?",
                        (order.get('cust_code', ''),))
        if cust_row:
            credit_limit = float(cust_row.get('credit_limit') or 0)
            if credit_limit > 0:
                ar_row = qry1("""
                    SELECT COALESCE(SUM(
                        COALESCE((SELECT SUM(quantity*unit_price) FROM invoice_items WHERE invoice_id=i.id), 0)
                        - COALESCE((SELECT SUM(allocated_amount) FROM payment_allocations WHERE invoice_id=i.id), 0)
                    ), 0) AS balance
                    FROM invoices i
                    WHERE i.customer_id=? AND i.status IN ('UNPAID','PARTIAL')
                """, (cust_row['id'],))
                ar_balance  = float((ar_row or {}).get('balance', 0))
                order_value = sum(float(it.get('qty_ordered', 0)) * float(it.get('unit_price', 0))
                                  for it in items)
                if ar_balance + order_value > credit_limit:
                    stock_warnings.append(
                        f"⚠ CREDIT LIMIT — {cust_row['name']}: "
                        f"AR {fmtpkr(ar_balance)} + this order {fmtpkr(order_value)} "
                        f"= {fmtpkr(ar_balance + order_value)} exceeds limit of {fmtpkr(credit_limit)}"
                    )

    if came_from_pending_review:
        convert_soft_hold_to_hard_reservation(order_id)

    ops = [("UPDATE customer_orders SET status='confirmed', updated_at=datetime('now') WHERE id=?", (order_id,))]
    audit_log(ops, 'customer_orders', order['order_number'], 'UPDATE',
              old_val={'status': order['status']}, new_val={'status': 'confirmed'})
    run_many(ops)
    detail = _order_detail(order_id)
    detail['stockWarnings'] = stock_warnings
    return detail


def cancel_customer_order(order_id):
    """Cancel an order (draft, pending_review, or confirmed only)."""
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] in ('invoiced', 'partially_invoiced'):
        raise ValueError("Cannot cancel an order that has invoices")
    if order['status'] == 'pending_review':
        release_soft_hold(order_id)
    ops = [("UPDATE customer_orders SET status='cancelled', updated_at=datetime('now') WHERE id=?", (order_id,))]
    audit_log(ops, 'customer_orders', order['order_number'], 'UPDATE',
              old_val={'status': order['status']}, new_val={'status': 'cancelled'})
    run_many(ops)
    return {'ok': True}


# ═══════════════════════════════════════════════════════════════════
#  WORK ORDER FROM ORDER ITEM
# ═══════════════════════════════════════════════════════════════════

def create_wo_from_order_item(order_id, item_id, data):
    """Create a Work Order for a specific order item. data: {targetDate, notes}"""
    from modules.id_gen import next_id, _sync_counter_to_max
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] not in ('confirmed', 'partially_invoiced'):
        raise ValueError("Order must be confirmed before creating work orders")

    item = qry1("SELECT * FROM customer_order_items WHERE id=? AND order_id=?", (item_id, order_id))
    if not item:
        raise ValueError("Order item not found")

    existing_qty = qry1("""
        SELECT COALESCE(SUM(qty_units), 0) as total
        FROM work_orders
        WHERE customer_order_item_id=? AND status NOT IN ('cancelled', 'completed')
    """, (item_id,))
    already_planned = int((existing_qty or {}).get('total', 0))
    remaining = item['qty_ordered'] - item['qty_invoiced'] - already_planned
    if remaining <= 0:
        raise ValueError("All units for this item are already planned or invoiced")

    feasibility = _check_wo_feasibility_fn(item['product_variant_id'], remaining)
    _sync_counter_to_max('work_order', 'work_orders', 'wo_number', 'SP-WO-')
    wo_number = next_id('work_order', 'WO')
    c = _conn()
    try:
        c.execute("""
            INSERT INTO work_orders
                (wo_number, product_variant_id, qty_units, target_date, status,
                 notes, feasibility_ok, customer_order_id, customer_order_item_id)
            VALUES (?, ?, ?, ?, 'planned', ?, ?, ?, ?)
        """, (wo_number, item['product_variant_id'], remaining,
              data.get('targetDate') or order.get('required_date') or today(),
              data.get('notes', f"For {order['order_number']}"),
              1 if feasibility['feasible'] else 0,
              order_id, item_id))
        c.execute("""
            UPDATE customer_order_items
            SET qty_in_production = qty_in_production + ?
            WHERE id=?
        """, (remaining, item_id))
        c.commit()
        wo_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()
    return {
        'woId': wo_id, 'woNumber': wo_number,
        'qtyPlanned': remaining,
        'feasible':   feasibility['feasible'],
        'shortfalls': feasibility['shortfalls']
    }


# ═══════════════════════════════════════════════════════════════════
#  INVOICE FROM ORDER
# ═══════════════════════════════════════════════════════════════════

def generate_invoice_from_order(order_id, data):
    """
    Generate a (partial or full) invoice from a customer order.
    data: {lines: [{orderItemId, qty}], invoiceDate}
    """
    from modules.id_gen     import next_id, _sync_counter_to_max
    from modules.inventory  import get_finished_stock_map
    order = qry1("""
        SELECT co.*, c.name as customer_name, c.payment_terms_days
        FROM customer_orders co
        JOIN customers c ON c.id = co.customer_id
        WHERE co.id=?
    """, (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] == 'draft':
        raise ValueError("Order must be confirmed before invoicing")
    if order['status'] == 'cancelled':
        raise ValueError("Cannot invoice a cancelled order")

    lines = data.get('lines', [])
    if not lines:
        raise ValueError("Specify at least one line to invoice")

    fg_stock = get_finished_stock_map()
    resolved = []
    for entry in lines:
        item_id = int(entry.get('orderItemId', 0))
        qty     = int(entry.get('qty', 0))
        if qty <= 0:
            continue
        item = qry1("SELECT * FROM customer_order_items WHERE id=? AND order_id=?", (item_id, order_id))
        if not item:
            raise ValueError(f"Order item {item_id} not found")
        remaining = item['qty_ordered'] - item['qty_invoiced']
        if qty > remaining:
            raise ValueError(f"Cannot invoice {qty} units — only {remaining} remaining for item {item_id}")
        avail = fg_stock.get(item['product_variant_id'], 0)
        if qty > avail:
            var_info = qry1("""
                SELECT p.name, ps.label as pack FROM product_variants pv
                JOIN products p ON p.id=pv.product_id
                JOIN pack_sizes ps ON ps.id=pv.pack_size_id
                WHERE pv.id=?
            """, (item['product_variant_id'],))
            label = f"{var_info['name']} {var_info['pack']}" if var_info else f"Variant {item['product_variant_id']}"
            raise ValueError(f"{label}: only {avail:.0f} units in stock, {qty} requested")
        resolved.append({'item': item, 'qty': qty})

    if not resolved:
        raise ValueError("No valid lines to invoice")

    new_total = r2(sum(float(r['item']['unit_price']) * r['qty'] for r in resolved))
    _enforce_credit_limit(order['customer_id'], new_total)

    cust     = qry1("SELECT * FROM customers WHERE id=?", (order['customer_id'],))
    terms    = int(order.get('payment_terms_days') or cust.get('payment_terms_days') or 30)
    inv_date = data.get('invoiceDate', today())
    due_date = (date.fromisoformat(inv_date) + timedelta(days=terms)).isoformat()

    # Pre-generate all IDs BEFORE opening the write transaction (WAL deadlock prevention)
    _sync_counter_to_max('invoice', 'invoices', 'invoice_number', 'SP-INV-')
    _sync_counter_to_max('sale',    'sales',    'sale_id',        'SP-SALE-')
    inv_number   = next_id('invoice', 'INV')
    sale_ids_pre = [next_id('sale', 'SALE') for _ in resolved]

    mfg_type = qry1("SELECT id FROM price_types WHERE code='mfg_cost'")

    c = _conn()
    try:
        c.execute("""
            INSERT INTO invoices
                (invoice_number, customer_id, invoice_date, due_date, status, customer_order_id)
            VALUES (?, ?, ?, ?, 'UNPAID', ?)
        """, (inv_number, order['customer_id'], inv_date, due_date, order_id))
        inv_db_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        sale_ids = []
        for idx, r in enumerate(resolved):
            item       = r['item']
            qty        = r['qty']
            unit_price = r2(item['unit_price'])
            line_total = r2(qty * unit_price)

            # COGS from mfg_cost price
            cogs_price = 0.0
            if mfg_type:
                cp = qry1("""SELECT price FROM product_prices
                             WHERE product_variant_id=? AND price_type_id=? AND active_flag=1
                             ORDER BY effective_from DESC LIMIT 1""",
                          (item['product_variant_id'], mfg_type['id']))
                if cp:
                    cogs_price = r2(cp['price'] * qty)

            sale_id = sale_ids_pre[idx]
            sale_ids.append(sale_id)

            var_info = qry1("""
                SELECT p.name as product_name, p.code as product_code, ps.label as pack_size
                FROM product_variants pv
                JOIN products p ON p.id=pv.product_id
                JOIN pack_sizes ps ON ps.id=pv.pack_size_id
                WHERE pv.id=?
            """, (item['product_variant_id'],))

            c.execute("""
                INSERT INTO sales
                    (sale_id, sale_date, customer_id, cust_code, cust_name, customer_type,
                     product_variant_id, product_code, product_name, pack_size,
                     qty, unit_price, total, cogs, gross_profit, invoice_id, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (sale_id, inv_date, cust['id'], cust['code'], cust['name'],
                  cust.get('customer_type', 'RETAIL'),
                  item['product_variant_id'],
                  var_info['product_code'] if var_info else '',
                  var_info['product_name'] if var_info else '',
                  var_info['pack_size']    if var_info else '',
                  qty, unit_price, line_total, cogs_price,
                  r2(line_total - cogs_price), inv_db_id,
                  f"From Order {order['order_number']}"))

            c.execute("""
                INSERT INTO invoice_items
                    (invoice_id, sale_id, product_variant_id,
                     product_code, product_name, pack_size,
                     quantity, unit_price, line_total)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (inv_db_id, sale_id, item['product_variant_id'],
                  var_info['product_code'] if var_info else '',
                  var_info['product_name'] if var_info else '',
                  var_info['pack_size']    if var_info else '',
                  qty, unit_price, line_total))

            c.execute("""
                UPDATE customer_order_items
                SET qty_invoiced = qty_invoiced + ?
                WHERE id=?
            """, (qty, item['id']))

        c.execute("UPDATE customer_orders SET updated_at=datetime('now') WHERE id=?", (order_id,))
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()

    # Recompute + persist order status
    new_status = _order_status(order_id)
    run("UPDATE customer_orders SET status=? WHERE id=?", (new_status, order_id))

    inv_total = sum(r2(r['item']['unit_price'] * r['qty']) for r in resolved)
    return {
        'invoiceNumber': inv_number,
        'invoiceId':     inv_db_id,
        'total':         inv_total,
        'orderStatus':   new_status,
        'saleIds':       sale_ids,
    }
