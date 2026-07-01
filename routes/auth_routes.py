"""
Auth & health routes — extracted from server.py's do_GET / do_POST.

Route-modularization pilot (roadmap sprints S1-S4: "routes scaffolding").
This is the lowest-risk domain to start with: every check here is either
public (no session) or does its own self-contained session lookup — none
of it depends on admin/permission branching or state set up elsewhere.

Business logic (login_user, get_session, rate limiting, etc.) already lives
in modules/auth.py and modules/db.py. This file only extracts the HTTP
dispatch glue that used to sit inline in server.py's do_GET/do_POST — no
logic was changed, just moved. See CLAUDE.md "POST-LAUNCH REFACTORING —
SPRINT TRACKER" for the full plan.

Each handle_* function returns True if it matched and fully handled the
request (it will already have called send_json/send_error), or False if
the path didn't match so the caller should keep checking other routes —
same contract a plain `if path == ...: ...; return` block had inline.
"""

import os
import time
import secrets
from datetime import datetime, timedelta

from modules.db import qry1, run
from modules.auth import (
    get_session, login_user, logout_user,
    _get_client_ip, _check_rate_limit, _clear_rate_limit, _record_failed_attempt,
    SESSION_EXPIRY_HOURS,
)


def handle_get_pre_gate(handler, path, server_start_time_float):
    """GET /health, GET /api/health — no auth required.

    Was server.py do_GET, checked BEFORE the auth gate (so it must stay
    reachable pre-gate here too — same position, same behavior).
    """
    from server import send_json

    if path == '/health' or path == '/api/health':
        db_ok = False
        try:
            qry1("SELECT 1", ())
            db_ok = True
        except Exception:
            pass
        uptime = int(time.time() - server_start_time_float)
        status = 'ok' if db_ok else 'degraded'
        code   = 200 if db_ok else 503
        send_json(handler, {
            'status':         status,
            'db':             'ok' if db_ok else 'error',
            'uptime_seconds': uptime,
            'version':        '2.0',
        }, code)
        return True

    return False


def handle_get_post_gate(handler, path, server_start_time_int):
    """GET /api/health (again) and GET /api/auth/me — checked AFTER the auth gate.

    The second /api/health check is unreachable in practice today (the
    pre-gate check above already matches '/api/health' first) — preserved
    verbatim from server.py rather than dropped, so this pass stays a pure
    move with zero behavior change. Safe to delete in a later cleanup pass.
    """
    from server import send_json

    if path == '/api/health':
        send_json(handler, {'ok': True, 'started_at': server_start_time_int})
        return True

    if path == '/api/auth/me':
        sess = get_session(handler)
        if sess:
            send_json(handler, {'authenticated': True, **sess})
        else:
            send_json(handler, {'authenticated': False})
        return True

    return False


def handle_post_login(handler, path, data):
    """POST /api/auth/login — no auth required. Checked before the POST auth gate."""
    from server import send_json

    if path != '/api/auth/login':
        return False

    ip = _get_client_ip(handler)
    # One-time bypass: if ADMIN_BYPASS_TOKEN env var is set and matches,
    # skip rate limiting and return admin session directly.
    bypass_token = os.environ.get('ADMIN_BYPASS_TOKEN', '').strip()
    if bypass_token and data.get('username') == 'admin' and data.get('password') == bypass_token:
        user = qry1("SELECT * FROM users WHERE username='admin' AND active=1")
        if user:
            token      = secrets.token_hex(32)
            now        = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
            expires_at = (datetime.utcnow() + timedelta(hours=SESSION_EXPIRY_HOURS)).strftime('%Y-%m-%dT%H:%M:%S')
            display    = user['display_name'] or user['username']
            run("""
                INSERT INTO sessions (token, user_id, username, display_name, role, permissions, created_at, expires_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (token, user['id'], user['username'], display, user['role'], user.get('permissions', '[]'), now, expires_at, now))
            _clear_rate_limit(ip)
            print(f"  ⚠ Admin bypass token used from {ip} — remove ADMIN_BYPASS_TOKEN now!")
            send_json(handler, {'token': token, 'role': user['role'],
                                 'username': user['username'],
                                 'displayName': display,
                                 'userId': user['id'], 'permissions': []})
            return True

    try:
        _check_rate_limit(ip)
        result = login_user(data.get('username', ''), data.get('password', ''))
        _clear_rate_limit(ip)
        send_json(handler, result)
    except ValueError as e:
        _record_failed_attempt(ip)
        send_json(handler, {'error': str(e)}, 401)
    return True


def handle_post_logout(handler, path):
    """POST /api/auth/logout — checked after the POST auth gate (a valid session is required to reach it)."""
    from server import send_json

    if path != '/api/auth/logout':
        return False

    auth = handler.headers.get('Authorization', '')
    if auth.startswith('Bearer '):
        logout_user(auth[7:])
    send_json(handler, {'ok': True})
    return True
