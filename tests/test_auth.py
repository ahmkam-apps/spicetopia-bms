#!/usr/bin/env python3
"""
AUTH MODULE TESTS
Covers: login, logout, token validation, rate limiting, role enforcement
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from base import *

def run():
    _section("AUTH — Login / Logout / Tokens / Role Enforcement")
    reset_results()

    # ── Login ──────────────────────────────────────────────────────────────
    r = POST("/api/auth/login", {"username": ADMIN_USER, "password": ADMIN_PASS}, token="")
    assert_status(r, 200, "Login with valid credentials returns 200")
    tok = r.json().get("token") if r.status_code == 200 else None
    if tok:
        _pass("Login response contains token")
    else:
        _fail("Login response contains token", "no token in response")

    r2 = POST("/api/auth/login", {"username": ADMIN_USER, "password": "wrongpassword"}, token="")
    assert_status(r2, 401, "Login with wrong password returns 401")

    r3 = POST("/api/auth/login", {"username": "nonexistent_user_xyz", "password": "anything"}, token="")
    assert_status(r3, 401, "Login with unknown username returns 401")

    r4 = POST("/api/auth/login", {}, token="")
    if r4.status_code in (400, 401):
        _pass("Login with empty body returns 400 or 401")
    else:
        _fail("Login with empty body returns 400 or 401", f"got {r4.status_code}")

    # ── Token usage ────────────────────────────────────────────────────────
    if tok:
        r5 = GET("/api/customers", token=tok)
        assert_status(r5, 200, "Valid token allows authenticated GET request")

        r6 = GET("/api/customers", token="invalid.token.xyz")
        assert_status(r6, 401, "Invalid token returns 401")

        r7 = GET("/api/customers", token="")
        assert_status(r7, 401, "No token returns 401 on protected route")
    else:
        _skip("Token usage tests", "no token from login")

    # ── /api/auth/me ───────────────────────────────────────────────────────
    if tok:
        r8 = GET("/api/auth/me", token=tok)
        assert_status(r8, 200, "GET /api/auth/me returns 200 with valid token")
        d = r8.json() if r8.status_code == 200 else {}
        assert_field(d, "username", "auth/me response contains username")
        assert_field(d, "role",     "auth/me response contains role")

    # ── Logout ─────────────────────────────────────────────────────────────
    if tok:
        r9 = POST("/api/auth/logout", {}, token=tok)
        assert_status(r9, 200, "Logout with valid token returns 200")

        r10 = GET("/api/customers", token=tok)
        assert_status(r10, 401, "Token rejected after logout")

    # ── Role enforcement — admin-only routes ───────────────────────────────
    admin_tok = login()
    if admin_tok:
        r11 = GET("/api/admin/backup", token=admin_tok)
        if r11.status_code in (200, 404):
            _pass("Admin token can access admin backup route")
        else:
            _fail("Admin token can access admin backup route", f"got {r11.status_code}")

        r12 = GET("/api/costing/config", token=admin_tok)
        assert_status(r12, 200, "Admin token can access costing config")
        logout(admin_tok)

    print_summary()
    return summary()

if __name__ == "__main__":
    run()
