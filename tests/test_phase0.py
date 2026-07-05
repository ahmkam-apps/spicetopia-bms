#!/usr/bin/env python3
"""
PHASE 0 TESTS — the pre-launch correctness/security fixes (2026-07-05).
Covers:
  P0-1  per-customer configurable GST (flag, global rate, per-invoice tax)
  P0-3  /api/reps pay/pin_hash not leaked to non-admins
  P0-4  _get_field_session enforces app_field (batch-only rep can't drive Sales)
  P0-5a convert_wo_to_batch refuses while a staged batch run is active
Stock/BOM-dependent assertions skip gracefully when the test DB has no FG/RM.
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(__file__))
from base import *


def run():
    _section("PHASE 0 — GST / reps pay / app_field / convert guard")
    reset_results()
    tok = login()
    if not tok:
        _fail("Login", "cannot proceed"); return summary()
    ts = int(time.time())

    # ── P0-1: per-customer GST flag ──────────────────────────────────────────
    rg = POST("/api/customers", {"name": f"GST Co {ts}", "city": "Karachi",
              "address": "1 A St", "customerType": "DIRECT", "gstApplicable": True}, token=tok)
    gst_cust = rg.json() if rg.status_code in (200, 201) else {}
    gst_cust_id = gst_cust.get("id")
    gst_cust_code = gst_cust.get("code")
    if gst_cust_id:
        _pass("GST customer stored gst_applicable=1") if gst_cust.get("gst_applicable") in (1, True) \
            else _fail("GST customer flag", str(gst_cust.get("gst_applicable")))
    else:
        _skip("GST customer flag", f"create failed {rg.status_code}")

    rn = POST("/api/customers", {"name": f"NoGST Co {ts}", "city": "Karachi",
              "address": "2 B St", "customerType": "RETAIL", "gstApplicable": False}, token=tok)
    if rn.status_code in (200, 201):
        _pass("Non-GST customer stored gst_applicable=0") if not rn.json().get("gst_applicable") \
            else _fail("Non-GST flag", str(rn.json().get("gst_applicable")))
    else:
        _skip("Non-GST customer flag", f"create failed {rn.status_code}")

    # ── GST rate config round-trip ───────────────────────────────────────────
    PUT("/api/costing/config", {"key": "gst_rate", "value": "17"}, token=tok)
    cfg = GET("/api/costing/config", token=tok).json()
    val = (cfg.get("gst_rate") or {}).get("value") if isinstance(cfg, dict) else None
    _pass("GST rate config round-trips (17)") if (_num(val) == 17) \
        else _fail("GST rate config", str(val))
    PUT("/api/costing/config", {"key": "gst_rate", "value": "18"}, token=tok)  # restore default

    # ── GST invoice end-to-end (stock-dependent → skip if unavailable) ────────
    pc, ppack, pvid = _first_product_variant(tok)
    if gst_cust_code and pc and ppack:
        ordr = POST("/api/customer-orders", {"custCode": gst_cust_code, "orderDate": TODAY,
               "requiredDate": FUTURE,
               "lines": [{"productCode": pc, "packSize": ppack, "qty": 1, "unitPrice": 200}]}, token=tok)
        if ordr.status_code == 201:
            oid = ordr.json().get("orderId")
            POST(f"/api/customer-orders/{oid}/confirm", {}, token=tok)
            od = GET(f"/api/customer-orders/{oid}", token=tok).json()
            items = od.get("items", [])
            if items:
                lines = [{"orderItemId": items[0]["id"], "qty": items[0]["qty_ordered"]}]
                inv = POST(f"/api/customer-orders/{oid}/invoice",
                           {"invoiceDate": TODAY, "dueDate": FUTURE, "lines": lines}, token=tok)
                if inv.status_code == 201:
                    det = GET(f"/api/invoices/{inv.json().get('invoiceId')}", token=tok).json()
                    sub = _num(det.get("subtotal")); tax = _num(det.get("tax") or det.get("gst")); tot = _num(det.get("total"))
                    if tax is not None:
                        _pass(f"GST customer invoice has GST (tax={tax})") if tax > 0 \
                            else _fail("GST customer invoice tax > 0", f"tax={tax}")
                    elif sub is not None and tot is not None:
                        _pass("GST customer invoice total > subtotal") if tot > sub \
                            else _fail("GST invoice total>subtotal", f"sub={sub} tot={tot}")
                    else:
                        _skip("GST invoice tax", "invoice detail lacks tax/total fields")
                else:
                    _skip("GST invoice tax", f"invoice not generated ({inv.status_code}; likely no FG stock)")
            else:
                _skip("GST invoice tax", "order had no items")
        else:
            _skip("GST invoice tax", f"order create failed {ordr.status_code}")
    else:
        _skip("GST invoice tax", "no product variant or GST customer available")

    # ── P0-3: reps pay / pin_hash not leaked ─────────────────────────────────
    reps = GET("/api/reps", token=tok).json()
    if isinstance(reps, list):
        _pass("Reps list omits pin_hash") if all("pin_hash" not in r for r in reps) \
            else _fail("pin_hash leaked in reps list")
    # a non-admin (viewer) must not see rep pay on the detail endpoint
    uname = f"viewer{ts}"
    cu = POST("/api/users", {"username": uname, "password": "Test12345!", "role": "user"}, token=tok)
    if cu.status_code in (200, 201) and isinstance(reps, list) and reps:
        vlog = POST("/api/auth/login", {"username": uname, "password": "Test12345!"}, token="")
        vtok = vlog.json().get("token") if vlog.status_code == 200 else None
        if vtok:
            rd = GET(f"/api/reps/{reps[0].get('id')}", token=vtok)
            if rd.status_code == 200:
                body = rd.json()
                leaked = [k for k in ("salary", "commission", "advances") if k in body]
                _pass("Non-admin cannot see rep pay") if not leaked \
                    else _fail("Non-admin sees rep pay", str(leaked))
            else:
                _pass(f"Non-admin rep detail gated ({rd.status_code})")
        else:
            _skip("Non-admin rep pay", "viewer login failed")
    else:
        _skip("Non-admin rep pay", "could not create viewer or no reps")

    # ── P0-4: app_field gating ───────────────────────────────────────────────
    phone = f"03{ts % 100000000:08d}"
    cr = POST("/api/reps", {"name": f"FieldRep {ts}", "phone": phone, "pin": "1234"}, token=tok)
    rep_id = cr.json().get("id") if cr.status_code in (200, 201) else None
    if rep_id:
        POST(f"/api/reps/{rep_id}/apps", {"field": False}, token=tok)  # Sales OFF
        fa = POST("/api/field/auth", {"phone": phone, "pin": "1234"}, token="")
        ftok = fa.json().get("token") if fa.status_code == 200 else None
        if ftok:
            r_off = GET("/api/field/products", token=ftok)
            _pass("app_field=0 blocks /api/field/products") if r_off.status_code in (401, 403) \
                else _fail("app_field=0 should block field", f"got {r_off.status_code}")
        else:
            _pass("app_field=0 blocks field login") if fa.status_code in (401, 403) \
                else _skip("app_field=0 gating", f"field auth {fa.status_code}")
        POST(f"/api/reps/{rep_id}/apps", {"field": True}, token=tok)   # Sales ON
        fa2 = POST("/api/field/auth", {"phone": phone, "pin": "1234"}, token="")
        ftok2 = fa2.json().get("token") if fa2.status_code == 200 else None
        if ftok2:
            r_on = GET("/api/field/products", token=ftok2)
            _pass("app_field=1 allows /api/field/products") if r_on.status_code == 200 \
                else _fail("app_field=1 should allow", f"got {r_on.status_code}")
        else:
            _skip("app_field=1 allow", f"field auth {fa2.status_code}")
    else:
        _skip("app_field gating", f"rep create failed {cr.status_code}")

    # ── P0-5a: convert guard vs active batch run (stock/BOM-dependent) ────────
    if pvid:
        wo = POST("/api/work-orders", {"productVariantId": pvid,
                  "qtyUnits": 10, "targetDate": FUTURE}, token=tok)
        wo_id = wo.json().get("id") if wo.status_code in (200, 201) else None
        if wo_id:
            sb = POST(f"/api/work-orders/{wo_id}/start-batch", {}, token=tok)
            if sb.status_code in (200, 201):
                cv = POST(f"/api/work-orders/{wo_id}/convert", {}, token=tok)
                blocked = cv.status_code >= 400 or ("active batch run" in (cv.text or "").lower())
                _pass("convert_wo_to_batch blocked while run active") if blocked \
                    else _fail("convert should be blocked", f"got {cv.status_code}: {cv.text[:120]}")
            else:
                _skip("convert guard", f"start-batch failed ({sb.status_code}; likely no BOM/RM)")
        else:
            _skip("convert guard", f"WO create failed {wo.status_code}")
    else:
        _skip("convert guard", "no product variant id available")

    logout(tok)
    print_summary()
    return summary()


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _first_product_variant(tok):
    """Return (productCode, packSize, variantId) for any active variant via /api/ref."""
    try:
        d = GET("/api/ref", token=tok).json()
        for v in [v for p in d.get("products", []) for v in p.get("variants", [])]:
            if v.get("active_flag") != 0 and v.get("product_code") and v.get("pack_size"):
                return v.get("product_code"), v.get("pack_size"), v.get("id")
    except Exception:
        pass
    return None, None, None


if __name__ == "__main__":
    run()
