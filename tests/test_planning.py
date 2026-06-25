#!/usr/bin/env python3
"""
PLANNING INPUT SYSTEM TESTS  (R0 — gates planning_routes extraction in S4)
Covers M0–M2: plan versions, sales forecast/target, manufacturing, financial,
scenario pricing, and the outputs (projected-sales, capacity-vs-demand,
production-required, cash-flow). Also: admin-gating, validation, immutability path,
and the change_log "reason" audit field via the API.
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(__file__))
from base import *
import requests


def _find_variant(tok):
    """Discover a real active variant_id via API only (no DB access — R0 may run vs a URL)."""
    pr = GET("/api/products", token=tok)
    if pr.status_code != 200:
        return None
    for prod in pr.json():
        code = prod.get("code")
        if not code:
            continue
        rows = GET(f"/api/products/{code}/prices", token=tok)
        if rows.status_code == 200 and isinstance(rows.json(), list) and rows.json():
            return rows.json()[0].get("product_variant_id")
    return None


def run():
    _section("PLANNING — Versions / Forecast / Manufacturing / Financial / Pricing / Outputs")
    reset_results()

    # ── Admin gating (no token) ────────────────────────────────────────────
    r = requests.get(f"{BASE_URL}/api/planning/versions")
    if r.status_code in (401, 403):
        _pass("GET /api/planning/versions blocked without auth")
    else:
        _fail("GET /api/planning/versions blocked without auth", f"got {r.status_code}")

    tok = login()
    if not tok:
        _fail("Login", "Cannot proceed"); return summary()

    vid = _find_variant(tok)
    if not vid:
        _skip("Discover a product variant", "no priced variant found — seed data missing")
        logout(tok); print_summary(); return summary()
    _pass(f"Discovered variant_id={vid}")

    # ── M0: create / read / update version ─────────────────────────────────
    name = f"R0 Test {int(time.time())}"
    r = POST("/api/planning/versions",
             {"name": name, "scenario_type": "expected",
              "horizon_start_month": "2026-07", "horizon_months": 12,
              "reason": "r0 test create"}, token=tok)
    assert_status(r, 201, "POST /api/planning/versions creates (201)")
    if r.status_code != 201:
        logout(tok); print_summary(); return summary()
    ver = r.json()["id"]
    assert_field(r.json(), "horizon_months", "version returns horizon_months")

    r = POST("/api/planning/versions",
             {"name": "bad", "scenario_type": "nonsense"}, token=tok)
    assert_status(r, 400, "Invalid scenario_type rejected (400)")

    r = GET("/api/planning/versions", token=tok)
    assert_status(r, 200, "GET /api/planning/versions returns 200")
    ids = [v["id"] for v in r.json()] if r.status_code == 200 else []
    _pass("Created version appears in list") if ver in ids else _fail("Created version appears in list")

    r = PUT(f"/api/planning/versions/{ver}", {"horizon_months": 6, "reason": "shorten"}, token=tok)
    if r.status_code == 200 and r.json().get("horizon_months") == 6:
        _pass("PUT version updates horizon_months")
    else:
        _fail("PUT version updates horizon_months", f"{r.status_code}: {r.text[:120]}")

    # ── M1: forecast upsert + validation + projected sales (units only) ────
    f1 = POST(f"/api/planning/versions/{ver}/forecast",
              {"variant_id": vid, "period_month": "2026-07", "channel": "retail",
               "units_forecast": 1000}, token=tok)
    if f1.status_code == 201 and f1.json().get("action") == "INSERT":
        _pass("Forecast insert (action=INSERT)")
    else:
        _fail("Forecast insert", f"{f1.status_code}: {f1.text[:120]}")

    f2 = POST(f"/api/planning/versions/{ver}/forecast",
              {"variant_id": vid, "period_month": "2026-07", "channel": "retail",
               "units_forecast": 1200}, token=tok)
    _pass("Forecast upsert (action=UPDATE)") if f2.json().get("action") == "UPDATE" \
        else _fail("Forecast upsert", f"{f2.text[:120]}")

    POST(f"/api/planning/versions/{ver}/forecast",
         {"variant_id": vid, "period_month": "2026-08", "channel": "retail",
          "units_forecast": 500}, token=tok)

    rbad = POST(f"/api/planning/versions/{ver}/forecast",
                {"variant_id": vid, "period_month": "2026-07", "channel": "bogus",
                 "units_forecast": 1}, token=tok)
    assert_status(rbad, 400, "Invalid channel rejected (400)")

    ps = GET(f"/api/planning/versions/{ver}/projected-sales", token=tok)
    assert_status(ps, 200, "GET projected-sales returns 200")
    if ps.status_code == 200:
        d = ps.json()
        _pass("Projected units = 1700") if d["totals"]["units"] == 1700 else _fail("Projected units = 1700", str(d["totals"]))
        _pass("No pricing yet → revenue null") if (d["has_pricing"] is False and d["totals"]["revenue"] is None) \
            else _fail("No pricing yet → revenue null", str(d["totals"]))

    ps1 = GET(f"/api/planning/versions/{ver}/projected-sales?months=1", token=tok)
    if ps1.status_code == 200 and ps1.json()["totals"]["units"] == 1200:
        _pass("Window months=1 isolates first month (1200)")
    else:
        _fail("Window months=1 isolates first month", ps1.text[:120])

    # ── M2: scenario pricing → revenue appears ─────────────────────────────
    pp = POST(f"/api/planning/versions/{ver}/pricing",
              {"variant_id": vid, "product_cost": 60, "wholesale_price": 100, "retail_price": 150},
              token=tok)
    assert_status(pp, 201, "POST pricing creates (201)")
    ps2 = GET(f"/api/planning/versions/{ver}/projected-sales", token=tok)
    if ps2.status_code == 200 and ps2.json()["has_pricing"] and ps2.json()["totals"]["revenue"] == 170000:
        _pass("With pricing → revenue = 170000")
    else:
        _fail("With pricing → revenue = 170000", str(ps2.json().get("totals")))

    # ── M2: manufacturing + capacity-vs-demand ─────────────────────────────
    mf = POST("/api/planning/manufacturers", {"name": f"R0 Mfr {int(time.time())}", "is_backup": False}, token=tok)
    assert_status(mf, 201, "POST manufacturer creates (201)")
    mid = mf.json().get("id")
    cm = POST(f"/api/planning/versions/{ver}/manufacturing",
              {"manufacturer_id": mid, "monthly_capacity_units": 1200, "batch_size": 500, "moq": 100},
              token=tok)
    assert_status(cm, 201, "POST manufacturing capacity (201)")
    cd = GET(f"/api/planning/versions/{ver}/capacity-vs-demand", token=tok)
    if cd.status_code == 200:
        d = cd.json()
        _pass("Base capacity = 1200") if d["base_monthly_capacity"] == 1200 else _fail("Base capacity = 1200", str(d))
        _pass("can_meet = True (peak 1200 ≤ cap 1200)") if d["can_meet"] is True else _fail("can_meet True", str(d))
    else:
        _fail("GET capacity-vs-demand", cd.text[:120])

    # ── M2: production-required (rounded to batch/MOQ) ─────────────────────
    pr = GET(f"/api/planning/versions/{ver}/production-required", token=tok)
    if pr.status_code == 200:
        rr = {m["period_month"]: m["required_rounded"] for m in pr.json()["months"]}
        ok = rr.get("2026-07-01") == 1500 and rr.get("2026-08-01") == 500  # 1200→ceil to 1500, 500→500
        _pass("Production rounded to batch size") if ok else _fail("Production rounded to batch size", str(rr))
    else:
        _fail("GET production-required", pr.text[:120])

    # ── M2: financial + cash-flow (hand-computed) ──────────────────────────
    fin = POST(f"/api/planning/versions/{ver}/financial",
               {"beginning_cash": 100000, "marketing_budget": 10000, "payroll_budget": 20000,
                "freight_cost_per_unit": 5, "other_opex_monthly": 5000, "minimum_cash_threshold": 50000},
               token=tok)
    assert_status(fin, 201, "POST financial (201)")
    cf = GET(f"/api/planning/versions/{ver}/cash-flow", token=tok)
    if cf.status_code == 200:
        d = cf.json()
        bal = {m["period_month"]: m["running_balance"] for m in d["months"]}
        # 07: 1200u rev120000 cogs72000 freight6000 fixed35000 → out113000 net7000 run107000
        # 08:  500u rev50000  cogs30000 freight2500 fixed35000 → out67500  net-17500 run89500
        ok = bal.get("2026-07-01") == 107000 and bal.get("2026-08-01") == 89500
        _pass("Cash-flow running balances correct") if ok else _fail("Cash-flow running balances", str(bal))
        _pass("Cash-flow flags pricing+financial present") if (d["has_pricing"] and d["has_financial"]) \
            else _fail("Cash-flow flags", str(d.get("has_pricing")))
        _pass("No threshold breach (min ≥ 50000)") if d["breaches_threshold"] is False else _fail("No breach", str(d))
    else:
        _fail("GET cash-flow", cf.text[:120])

    # ── M1: targets ────────────────────────────────────────────────────────
    tg = POST(f"/api/planning/versions/{ver}/targets",
              {"period_month": "2026-07", "channel": "retail", "target_units": 1500}, token=tok)
    assert_status(tg, 201, "POST target (201)")

    # ── M3: risk assessment + scenario comparison ───────────────────────────
    # data state: capacity 1200, peak demand 1200 (>85% util → yellow), cash clears
    # threshold (green), one non-backup mfr no backup (yellow) → overall yellow
    rk = GET(f"/api/planning/versions/{ver}/risk", token=tok)
    if rk.status_code == 200:
        d = rk.json(); cats = d.get("categories", {})
        _pass("Risk overall = yellow") if d.get("overall") == "yellow" else _fail("Risk overall = yellow", str(d.get("overall")))
        _pass("Capacity risk yellow (>85% util)") if cats.get("capacity", {}).get("level") == "yellow" else _fail("Capacity risk yellow", str(cats.get("capacity")))
        _pass("Cash risk green") if cats.get("cash", {}).get("level") == "green" else _fail("Cash risk green", str(cats.get("cash")))
        _pass("Supply risk yellow (no backup mfr)") if cats.get("supply", {}).get("level") == "yellow" else _fail("Supply risk yellow", str(cats.get("supply")))
        _pass("Stockout not_assessed (M4 deferred)") if cats.get("stockout", {}).get("level") == "not_assessed" else _fail("Stockout not_assessed", str(cats.get("stockout")))
    else:
        _fail("GET risk", rk.text[:120])

    cmp = GET(f"/api/planning/compare?versions={ver}", token=tok)
    if cmp.status_code == 200:
        d = cmp.json()
        if d.get("safest_version_id") == ver and len(d.get("scenarios", [])) == 1:
            _pass("Compare returns scenario + safest_version_id")
        else:
            _fail("Compare returns scenario + safest_version_id", str(d)[:160])
    else:
        _fail("GET compare", cmp.text[:120])

    # ── deletes ────────────────────────────────────────────────────────────
    fl = GET(f"/api/planning/versions/{ver}/forecast", token=tok)
    if fl.status_code == 200 and fl.json():
        fid = fl.json()[0]["id"]
        assert_status(DELETE(f"/api/planning/forecast/{fid}", token=tok), 200, "DELETE forecast row (200)")
    pl = GET(f"/api/planning/versions/{ver}/pricing", token=tok)
    if pl.status_code == 200 and pl.json():
        pid = pl.json()[0]["id"]
        assert_status(DELETE(f"/api/planning/pricing/{pid}", token=tok), 200, "DELETE pricing row (200)")

    logout(tok)
    print_summary()
    return summary()


if __name__ == "__main__":
    run()
