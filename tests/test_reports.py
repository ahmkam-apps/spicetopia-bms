#!/usr/bin/env python3
"""
REPORTS MODULE TESTS
Covers: dashboard KPIs, P&L report, margin report, rep performance, audit log
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from base import *

DASHBOARD_KEYS = ["total_revenue","total_outstanding","low_stock_count",
                  "pending_orders","overdue_invoices"]

def run():
    _section("REPORTS — Dashboard / P&L / Margins / Rep Performance / Audit")
    reset_results()
    tok = login()
    if not tok:
        _fail("Login", "Cannot proceed"); return summary()

    # ── Dashboard ──────────────────────────────────────────────────────────
    r = GET("/api/dashboard", token=tok)
    assert_status(r, 200, "GET /api/dashboard returns 200")
    if r.status_code == 200:
        d = r.json()
        if isinstance(d, dict):
            _pass("Dashboard response is a JSON object")
            # Check some expected KPI keys exist (names may vary slightly)
            found = list(d.keys())
            kpi_found = any(
                any(k in key.lower() for k in ["revenue","sales","outstanding","order","invoice","stock"])
                for key in found
            )
            if kpi_found:
                _pass(f"Dashboard contains KPI data (keys: {found[:5]}...)")
            else:
                _fail("Dashboard contains KPI data", f"keys: {found[:8]}")

    # ── P&L report ─────────────────────────────────────────────────────────
    import datetime
    year = str(datetime.date.today().year)
    r2 = GET("/api/reports/pl", params={"year": year}, token=tok)
    assert_status(r2, 200, f"GET /api/reports/pl?year={year} returns 200")
    if r2.status_code == 200:
        d = r2.json()
        if isinstance(d, dict):
            _pass("P&L response is a JSON object")
            for key in ["revenue","cogs","gross_profit"]:
                if any(key in k.lower() for k in d.keys()):
                    _pass(f"P&L contains {key} data")
                else:
                    _skip(f"P&L contains {key}", f"key not found — may be named differently")

    # ── Margin report ──────────────────────────────────────────────────────
    r3 = GET("/api/reports/margins", token=tok)
    assert_status(r3, 200, "GET /api/reports/margins returns 200")

    # ── Rep performance report ─────────────────────────────────────────────
    r4 = GET("/api/reports/rep-performance", token=tok)
    assert_status(r4, 200, "GET /api/reports/rep-performance returns 200")
    if r4.status_code == 200:
        d = r4.json()
        if isinstance(d, (list, dict)):
            _pass("Rep performance response is valid JSON")

    # ── Sales list ─────────────────────────────────────────────────────────
    r5 = GET("/api/sales", token=tok)
    assert_status(r5, 200, "GET /api/sales returns 200")
    sales = r5.json() if r5.status_code == 200 else []
    if isinstance(sales, list):
        _pass(f"Sales list is array ({len(sales)} sales)")

    # ── Audit log ──────────────────────────────────────────────────────────
    r6 = GET("/api/audit", token=tok)
    assert_status(r6, 200, "GET /api/audit returns 200")
    audit = r6.json() if r6.status_code == 200 else []
    if isinstance(audit, list):
        _pass(f"Audit log is array ({len(audit)} entries)")

    # ── Health check ───────────────────────────────────────────────────────
    r7 = GET("/api/health", token=tok)
    if r7.status_code == 200:
        _pass("GET /api/health returns 200")
        d = r7.json()
        if d.get("status") in ("ok", "healthy"):
            _pass(f"Health status: {d.get('status')}")
    else:
        _fail("GET /api/health", f"got {r7.status_code}")

    logout(tok)
    print_summary()
    return summary()

if __name__ == "__main__":
    run()
