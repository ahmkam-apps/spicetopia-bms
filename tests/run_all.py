#!/usr/bin/env python3
"""
Spicetopia BMS — Test Orchestrator
Runs all 13 module test files and prints a consolidated summary.

Usage:
    python3 tests/run_all.py                    # run all modules
    python3 tests/run_all.py --module auth      # run single module
    python3 tests/run_all.py --baseline         # save results as baseline.json
    python3 tests/run_all.py --compare          # compare against baseline.json

Exit code: 0 if all pass, 1 if any fail.
"""

import argparse
import importlib
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Ensure tests/ is on the path ──────────────────────────────────────────────
TESTS_DIR = Path(__file__).parent
sys.path.insert(0, str(TESTS_DIR))

BASELINE_FILE = TESTS_DIR / "baseline.json"

# ── Colour output ─────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

# ── Module registry ───────────────────────────────────────────────────────────
# Order matters — lower-dependency modules first
MODULES = [
    ("auth",           "test_auth",           "Authentication & Sessions"),
    ("customers",      "test_customers",       "Customers"),
    ("suppliers",      "test_suppliers",       "Suppliers"),
    ("products",       "test_products",        "Products & SKUs"),
    ("inventory",      "test_inventory",       "Inventory & Ingredients"),
    ("pricing",        "test_pricing",         "Pricing & Costing"),
    ("orders",         "test_orders",          "Customer Orders"),
    ("invoices",       "test_invoices",        "Invoices & AR"),
    ("purchasing",     "test_purchasing",      "Purchasing & AP"),
    ("production",     "test_production",      "Production"),
    ("field",          "test_field",           "Field Operations"),
    ("reports",        "test_reports",         "Reports & Dashboard"),
    ("business_rules", "test_business_rules",  "Business Rules"),
]

def _bar(char="─", n=60):
    return char * n

def run_module(short_name, module_name, label):
    """Import and run one test module. Returns its summary dict."""
    print(f"\n{BOLD}{CYAN}{_bar()}{RESET}")
    print(f"{BOLD}{CYAN}  Running: {label}{RESET}")
    print(f"{BOLD}{CYAN}{_bar()}{RESET}")
    try:
        mod = importlib.import_module(module_name)
        importlib.reload(mod)   # ensure fresh state between runs
        result = mod.run()
        return {"module": short_name, "label": label, **result}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"module": short_name, "label": label,
                "total": 0, "passed": 0, "failed": 1, "skipped": 0,
                "error": str(e)}

def print_report(results, elapsed):
    """Print the consolidated pass/fail table."""
    print(f"\n\n{BOLD}{_bar('═')}{RESET}")
    print(f"{BOLD}  SPICETOPIA BMS — TEST RESULTS{RESET}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}   ({elapsed:.1f}s)")
    print(f"{BOLD}{_bar('═')}{RESET}")
    print(f"  {'MODULE':<24} {'PASS':>6} {'FAIL':>6} {'SKIP':>6} {'TOTAL':>6}")
    print(f"  {_bar('-', 52)}")

    total_pass = total_fail = total_skip = total_all = 0
    for r in results:
        p, f, s, t = r["passed"], r["failed"], r["skipped"], r["total"]
        total_pass += p; total_fail += f; total_skip += s; total_all += t
        status_col = GREEN if f == 0 else RED
        print(f"  {status_col}{r['label']:<24}{RESET} "
              f"{GREEN}{p:>6}{RESET} "
              f"{(RED if f>0 else RESET)}{f:>6}{RESET} "
              f"{YELLOW}{s:>6}{RESET} "
              f"{total_all - (total_all - t):>6}")

    print(f"  {_bar('-', 52)}")
    overall_col = GREEN if total_fail == 0 else RED
    print(f"  {BOLD}{overall_col}{'TOTAL':<24} {total_pass:>6} {total_fail:>6} {total_skip:>6} {total_all:>6}{RESET}")
    print(f"{BOLD}{_bar('═')}{RESET}\n")

    if total_fail == 0:
        print(f"  {GREEN}{BOLD}✓ ALL TESTS PASSED{RESET}\n")
    else:
        print(f"  {RED}{BOLD}✗ {total_fail} TEST(S) FAILED — refactor is NOT safe to merge{RESET}\n")

    return total_pass, total_fail, total_skip, total_all

def save_baseline(results, total_pass, total_fail, total_skip, total_all):
    baseline = {
        "captured_at": datetime.now().isoformat(),
        "summary": {"passed": total_pass, "failed": total_fail,
                    "skipped": total_skip, "total": total_all},
        "modules": results
    }
    BASELINE_FILE.write_text(json.dumps(baseline, indent=2))
    print(f"  {GREEN}Baseline saved → {BASELINE_FILE}{RESET}\n")

def compare_baseline(results, total_pass, total_fail):
    if not BASELINE_FILE.exists():
        print(f"  {YELLOW}No baseline found. Run with --baseline first.{RESET}\n")
        return
    base = json.loads(BASELINE_FILE.read_text())
    base_pass = base["summary"]["passed"]
    base_fail = base["summary"]["failed"]
    base_date = base.get("captured_at","unknown")

    print(f"\n  Comparing against baseline captured: {base_date}")
    print(f"  Baseline:  {base_pass} passed, {base_fail} failed")
    print(f"  Current:   {total_pass} passed, {total_fail} failed")

    if total_fail > base_fail:
        print(f"\n  {RED}{BOLD}⚠  REGRESSION DETECTED: {total_fail - base_fail} more failure(s) than baseline{RESET}")
        print(f"  {RED}  Do NOT merge — fix regressions first.{RESET}\n")
    elif total_fail < base_fail:
        print(f"\n  {GREEN}✓ IMPROVEMENT: {base_fail - total_fail} fewer failure(s) than baseline{RESET}\n")
    else:
        print(f"\n  {GREEN}✓ No regression vs baseline{RESET}\n")

def main():
    parser = argparse.ArgumentParser(description="Spicetopia BMS test orchestrator")
    parser.add_argument("--module", help="Run single module by short name")
    parser.add_argument("--baseline", action="store_true", help="Save results as baseline")
    parser.add_argument("--compare",  action="store_true", help="Compare results against baseline")
    parser.add_argument("--url",  default=None, help="Override server URL (default: http://localhost:3001)")
    args = parser.parse_args()

    if args.url:
        os.environ["BMS_URL"] = args.url

    print(f"\n{BOLD}{CYAN}  Spicetopia BMS — Sprint 0 Test Suite{RESET}")
    print(f"  Server: {os.environ.get('BMS_URL','http://localhost:3001')}")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Select modules to run
    if args.module:
        selected = [(sn, mn, lbl) for sn, mn, lbl in MODULES if sn == args.module]
        if not selected:
            print(f"{RED}Unknown module '{args.module}'. Valid names:{RESET}")
            for sn, _, lbl in MODULES:
                print(f"  {sn:<20} — {lbl}")
            sys.exit(1)
    else:
        selected = MODULES

    start = time.time()
    results = []
    for short_name, module_name, label in selected:
        r = run_module(short_name, module_name, label)
        results.append(r)
    elapsed = time.time() - start

    total_pass, total_fail, total_skip, total_all = print_report(results, elapsed)

    if args.baseline:
        save_baseline(results, total_pass, total_fail, total_skip, total_all)

    if args.compare:
        compare_baseline(results, total_pass, total_fail)

    sys.exit(0 if total_fail == 0 else 1)

if __name__ == "__main__":
    main()
