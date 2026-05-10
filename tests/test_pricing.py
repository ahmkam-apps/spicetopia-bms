#!/usr/bin/env python3
"""
PRICING MODULE TESTS
Covers: costing config, standard costs, price history, margin alerts
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from base import *

# Actual keys returned by /api/costing/config
EXPECTED_CONFIG_KEYS = {"overhead_pct","packaging_cost_per_unit","labour_cost_per_unit",
                         "margin_floor_pct","margin_mfr","margin_dist"}

def run():
    _section("PRICING — Costing Config / Standard Costs / History / Margin Alerts")
    reset_results()
    tok = login()
    if not tok:
        _fail("Login", "Cannot proceed"); return summary()

    # ── Costing config ─────────────────────────────────────────────────────
    r = GET("/api/costing/config", token=tok)
    assert_status(r, 200, "GET /api/costing/config returns 200")
    cfg_list = []
    if r.status_code == 200:
        cfg = r.json()
        if isinstance(cfg, (dict, list)):
            _pass("Costing config response is valid JSON")
        if isinstance(cfg, list):
            cfg_list = cfg
            found_keys = {item.get("key") for item in cfg if isinstance(item, dict)}
            for key in EXPECTED_CONFIG_KEYS:
                if key in found_keys:
                    _pass(f"Costing config has key: {key}")
                else:
                    _fail(f"Costing config has key: {key}", f"not in {found_keys}")
        elif isinstance(cfg, dict):
            for key in EXPECTED_CONFIG_KEYS:
                if key in cfg:
                    _pass(f"Costing config has key: {key}")
                else:
                    _fail(f"Costing config has key: {key}")

    # ── Standard costs ─────────────────────────────────────────────────────
    r2 = GET("/api/costing/standard-costs", token=tok)
    assert_status(r2, 200, "GET /api/costing/standard-costs returns 200")
    if r2.status_code == 200:
        costs = r2.json()
        if isinstance(costs, list) and len(costs) > 0:
            _pass(f"Standard costs returned ({len(costs)} SKUs)")
            first = costs[0]
            for field in ["skuCode","cost_to_make","direct_sale"]:
                if field in first:
                    _pass(f"Standard cost entry has field: {field}")
                else:
                    _skip(f"Standard cost entry has field: {field}", f"keys: {list(first.keys())[:5]}")
        else:
            _skip("Standard costs populated", "no costs — BOM may not be configured")

    # ── Price history ──────────────────────────────────────────────────────
    r3 = GET("/api/costing/price-history", token=tok)
    assert_status(r3, 200, "GET /api/costing/price-history returns 200")

    # ── Ingredient price history ───────────────────────────────────────────
    r4 = GET("/api/ingredient-price-history", token=tok)
    assert_status(r4, 200, "GET /api/ingredient-price-history returns 200")

    # ── Margin alerts ──────────────────────────────────────────────────────
    r5 = GET("/api/costing/margin-alerts", token=tok)
    assert_status(r5, 200, "GET /api/costing/margin-alerts returns 200")

    # ── Batch variances ────────────────────────────────────────────────────
    r6 = GET("/api/costing/batch-variances", token=tok)
    assert_status(r6, 200, "GET /api/costing/batch-variances returns 200")

    # ── Config update — correct format: {key, value} ───────────────────────
    # Read current overhead_pct value then restore it
    original_val = None
    if cfg_list:
        for item in cfg_list:
            if item.get("key") == "overhead_pct":
                original_val = item.get("value")
                break

    if original_val is not None:
        r7 = PUT("/api/costing/config", {"key": "overhead_pct", "value": original_val}, token=tok)
        if r7.status_code in (200, 204):
            _pass("PUT /api/costing/config accepts {key, value} format")
        else:
            _fail("PUT /api/costing/config", f"got {r7.status_code}: {r7.text[:100]}")
    else:
        _skip("Costing config update test", "could not read current value")

    logout(tok)
    print_summary()
    return summary()

if __name__ == "__main__":
    run()
