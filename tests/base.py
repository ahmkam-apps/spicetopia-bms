#!/usr/bin/env python3
"""
Spicetopia BMS — Shared test base
All test modules import from here for HTTP helpers, auth, and result tracking.
"""

import json
import os
import sys
import time

# Explicitly export everything including underscore-prefixed names
__all__ = [
    "_pass", "_fail", "_skip", "_section",
    "summary", "print_summary", "reset_results",
    "login", "logout", "GET", "POST", "PUT", "DELETE",
    "assert_status", "assert_field", "assert_contains",
    "BASE_URL", "TODAY", "FUTURE", "ADMIN_USER", "ADMIN_PASS",
    "GREEN", "RED", "YELLOW", "CYAN", "BOLD", "RESET",
]
from datetime import date, timedelta

try:
    import requests
except ImportError:
    print("ERROR: pip install requests --break-system-packages")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL = os.environ.get("BMS_URL", "http://localhost:3001")
ADMIN_USER = os.environ.get("BMS_USER", "admin")
ADMIN_PASS = os.environ.get("BMS_PASS", "admin123")
TODAY = date.today().isoformat()
FUTURE = (date.today() + timedelta(days=30)).isoformat()

# ── Colour output ─────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

# ── Shared result store (populated by each module) ────────────────────────────
_results: list[dict] = []

def _pass(msg: str):
    print(f"  {GREEN}✓ PASS{RESET}  {msg}")
    _results.append({"status": "PASS", "test": msg})

def _fail(msg: str, detail: str = ""):
    d = f"  → {detail}" if detail else ""
    print(f"  {RED}✗ FAIL{RESET}  {msg}{d}")
    _results.append({"status": "FAIL", "test": msg, "detail": detail})

def _skip(msg: str, reason: str = ""):
    r = f"  [{reason}]" if reason else ""
    print(f"  {YELLOW}⊘ SKIP{RESET}  {msg}{r}")
    _results.append({"status": "SKIP", "test": msg})

def _section(title: str):
    bar = "─" * 60
    print(f"\n{BOLD}{CYAN}{bar}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{bar}{RESET}")

def summary() -> dict:
    total  = len(_results)
    passed = sum(1 for r in _results if r["status"] == "PASS")
    failed = sum(1 for r in _results if r["status"] == "FAIL")
    skipped= sum(1 for r in _results if r["status"] == "SKIP")
    return {"total": total, "passed": passed, "failed": failed, "skipped": skipped}

def print_summary():
    s = summary()
    colour = GREEN if s["failed"] == 0 else RED
    print(f"\n{colour}{BOLD}  {s['passed']}/{s['total']} passed  |  {s['failed']} failed  |  {s['skipped']} skipped{RESET}\n")

def reset_results():
    _results.clear()

# ── HTTP helpers ──────────────────────────────────────────────────────────────
_token: str | None = None

def _headers(token=None) -> dict:
    t = token or _token
    h = {"Content-Type": "application/json"}
    if t:
        h["Authorization"] = f"Bearer {t}"
    return h

def login(username=ADMIN_USER, password=ADMIN_PASS) -> str | None:
    """Login and return token. Sets global _token."""
    global _token
    try:
        r = requests.post(f"{BASE_URL}/api/auth/login",
                          json={"username": username, "password": password},
                          timeout=10)
        if r.status_code == 200:
            _token = r.json().get("token")
            return _token
    except Exception as e:
        print(f"  {RED}Login failed: {e}{RESET}")
    return None

def logout(token=None):
    t = token or _token
    if t:
        try:
            requests.post(f"{BASE_URL}/api/auth/logout",
                          headers=_headers(t), timeout=5)
        except Exception:
            pass

def GET(path: str, params: dict = None, token=None):
    try:
        r = requests.get(f"{BASE_URL}{path}", params=params,
                         headers=_headers(token), timeout=15)
        return r
    except Exception as e:
        return _FakeResp(0, str(e))

def POST(path: str, body: dict = None, token=None):
    try:
        r = requests.post(f"{BASE_URL}{path}", json=body or {},
                          headers=_headers(token), timeout=15)
        return r
    except Exception as e:
        return _FakeResp(0, str(e))

def PUT(path: str, body: dict = None, token=None):
    try:
        r = requests.put(f"{BASE_URL}{path}", json=body or {},
                         headers=_headers(token), timeout=15)
        return r
    except Exception as e:
        return _FakeResp(0, str(e))

def DELETE(path: str, token=None):
    try:
        r = requests.delete(f"{BASE_URL}{path}",
                            headers=_headers(token), timeout=15)
        return r
    except Exception as e:
        return _FakeResp(0, str(e))

class _FakeResp:
    """Returned when a network error occurs so callers don't have to handle exceptions."""
    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text
    def json(self):
        return {}

# ── Assertion helpers ─────────────────────────────────────────────────────────
def assert_status(r, expected: int, label: str):
    if r.status_code == expected:
        _pass(label)
        return True
    _fail(label, f"expected {expected}, got {r.status_code}: {r.text[:200]}")
    return False

def assert_field(data: dict, field: str, label: str):
    if field in data:
        _pass(label)
        return True
    _fail(label, f"field '{field}' missing from response")
    return False

def assert_contains(r, key: str, label: str):
    try:
        data = r.json()
        if key in data:
            _pass(label)
            return True
        _fail(label, f"key '{key}' not in response: {list(data.keys())}")
    except Exception as e:
        _fail(label, str(e))
    return False
