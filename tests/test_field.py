#!/usr/bin/env python3
"""
FIELD MODULE TESTS
Covers: rep management, zones, routes, field login, field orders, beat visits
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(__file__))
from base import *

def run():
    _section("FIELD — Reps / Zones / Routes / Field Login / Field Orders")
    reset_results()
    tok = login()
    if not tok:
        _fail("Login", "Cannot proceed"); return summary()

    # ── Zones ──────────────────────────────────────────────────────────────
    r = GET("/api/zones", token=tok)
    assert_status(r, 200, "GET /api/zones returns 200")
    zones = r.json() if r.status_code == 200 else []
    if isinstance(zones, list):
        _pass(f"Zones list is array ({len(zones)} zones)")

    # ── Create zone ────────────────────────────────────────────────────────
    ts = int(time.time())
    zone_id = None
    r2 = POST("/api/zones", {"name": f"Test Zone {ts}", "city": "Karachi"}, token=tok)
    assert_status(r2, 201, "POST /api/zones creates zone")
    if r2.status_code == 201:
        zone_id = r2.json().get("id")
        _pass(f"Zone created id={zone_id}") if zone_id else _fail("Zone has id")

    # ── Routes ─────────────────────────────────────────────────────────────
    r3 = GET("/api/routes", token=tok)
    assert_status(r3, 200, "GET /api/routes returns 200")
    routes = r3.json() if r3.status_code == 200 else []
    if isinstance(routes, list):
        _pass(f"Routes list is array ({len(routes)} routes)")

    # ── Create route ───────────────────────────────────────────────────────
    route_id = None
    if zone_id:
        r4 = POST("/api/routes", {
            "zoneId": zone_id,
            "name": f"Test Route {ts}",
            "visitDays": "MON,WED"
        }, token=tok)
        assert_status(r4, 201, "POST /api/routes creates route")
        if r4.status_code == 201:
            route_id = r4.json().get("id")
            _pass(f"Route created id={route_id}") if route_id else _fail("Route has id")

    # ── Reps ───────────────────────────────────────────────────────────────
    r5 = GET("/api/reps", token=tok)
    assert_status(r5, 200, "GET /api/reps returns 200")
    reps = r5.json() if r5.status_code == 200 else []
    if isinstance(reps, list):
        _pass(f"Reps list is array ({len(reps)} reps)")

    # ── Field login ────────────────────────────────────────────────────────
    # Try field login — will fail with no real rep but endpoint must respond
    r6 = POST("/api/field/auth", {"phone": "03000000000", "pin": "9999"}, token="")
    if r6.status_code in (200, 401, 403):
        _pass("POST /api/field/auth endpoint responds (200/401/403)")
    else:
        _fail("POST /api/field/auth responds", f"got {r6.status_code}")

    # ── Field orders list ──────────────────────────────────────────────────
    r7 = GET("/api/field-orders", token=tok)
    assert_status(r7, 200, "GET /api/field-orders returns 200")
    field_orders = r7.json() if r7.status_code == 200 else []
    if isinstance(field_orders, list):
        _pass(f"Field orders list is array ({len(field_orders)} orders)")

    # ── Beat visits ────────────────────────────────────────────────────────
    r8 = GET("/api/beat-visits", token=tok)
    assert_status(r8, 200, "GET /api/beat-visits returns 200")

    # ── Rep with field login — create test rep and verify PIN auth ─────────
    if reps:
        rep = reps[0]
        rep_id = rep.get("id")
        rep_phone = rep.get("phone")
        if rep_id and rep_phone:
            # Can't test PIN without knowing it — just verify payroll preview works
            r9 = GET(f"/api/reps/{rep_id}/payroll-preview", token=tok)
            if r9.status_code in (200, 400):
                _pass(f"GET /api/reps/{rep_id}/payroll-preview responds")
            else:
                _fail(f"Payroll preview for rep {rep_id}", f"got {r9.status_code}")

    # ── Field product lookup ───────────────────────────────────────────────
    r10 = GET("/api/field/products", params={"customerType": "RETAIL"}, token=tok)
    if r10.status_code in (200, 401):
        _pass("GET /api/field/products endpoint responds")
    else:
        _fail("GET /api/field/products", f"got {r10.status_code}")

    logout(tok)
    print_summary()
    return summary()

if __name__ == "__main__":
    run()
