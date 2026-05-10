"""
modules/auth.py — Authentication, session management, and rate limiting.

Extracted from server.py (Sprint 2). Overrides originals via bottom-import pattern:
    from modules.auth import *   # just before __main__ in server.py

Module-level mutable config (SESSION_EXPIRY_HOURS) is synced from server.py
after _apply_startup_config() runs, via:
    import modules.auth as _auth_mod
    _auth_mod.SESSION_EXPIRY_HOURS = SESSION_EXPIRY_HOURS
"""

import hashlib
import hmac
import json
import os
import secrets
import time
from datetime import datetime, timedelta

# ── Argon2id (preferred) — falls back to SHA-256 if not installed ─────────────
try:
    from argon2 import PasswordHasher as _Argon2Hasher
    _argon2 = _Argon2Hasher(
        time_cost=2,
        memory_cost=65536,
        parallelism=2,
        hash_len=32,
        salt_len=16,
    )
    _ARGON2_AVAILABLE = True
except ImportError:
    _ARGON2_AVAILABLE = False
    _argon2 = None

from modules.db    import _conn, qry, qry1, run
from modules.utils import _log

__all__ = [
    # Config (mutable — synced at startup)
    'SESSION_EXPIRY_HOURS', 'RATE_MAX_ATTEMPTS', 'RATE_LOCKOUT_SECS',
    # Argon2 availability
    '_ARGON2_AVAILABLE', '_argon2',
    # Startup migrations
    'ensure_rate_limit_table', 'ensure_sessions_table',
    # Rate limiting
    '_get_client_ip', '_check_rate_limit', '_record_failed_attempt', '_clear_rate_limit',
    # Password helpers
    '_hash_pw', '_hash_pw_new', '_verify_pw',
    # Session management
    '_get_session_by_token', 'login_user', 'logout_user', 'get_session', '_session_cleanup',
    # Field rep auth
    'field_login', '_get_field_session',
]

# ── Module-level config ────────────────────────────────────────────────────────
# Defaults match server.py; overwritten by _apply_startup_config() at startup.
SESSION_EXPIRY_HOURS = 12
RATE_MAX_ATTEMPTS    = 5
RATE_LOCKOUT_SECS    = 900   # 15 minutes


# ═══════════════════════════════════════════════════════════════════
#  STARTUP MIGRATIONS
# ═══════════════════════════════════════════════════════════════════

def ensure_rate_limit_table():
    """
    Create login_rate_limits table (idempotent).
    Persisting rate limits to DB ensures lockouts survive server restarts —
    critical for cloud deployments where the process may restart under load.
    """
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS login_rate_limits (
                ip            TEXT PRIMARY KEY,
                attempts      INTEGER NOT NULL DEFAULT 0,
                locked_until  REAL    NOT NULL DEFAULT 0,
                last_attempt  REAL    NOT NULL DEFAULT 0
            )
        """)
        c.commit()
        print("  ✓ Rate limits: table ready (DB-persisted) — lockouts cleared")
    finally:
        c.close()


def ensure_sessions_table():
    """
    Create the `sessions` table for persistent, expiry-aware login sessions.
    Replaces the in-memory sessions={} dict so logins survive server restarts.
    """
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                token        TEXT PRIMARY KEY,
                user_id      TEXT NOT NULL,
                username     TEXT NOT NULL,
                display_name TEXT NOT NULL,
                role         TEXT NOT NULL,
                permissions  TEXT NOT NULL DEFAULT '[]',
                created_at   TEXT NOT NULL,
                expires_at   TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at)")
        c.commit()
        print("  ✓ Sessions: table ready (DB-persisted)")
    finally:
        c.close()


# ═══════════════════════════════════════════════════════════════════
#  RATE LIMITING
# ═══════════════════════════════════════════════════════════════════

def _get_client_ip(handler):
    """Return client IP, respecting X-Forwarded-For when behind a proxy."""
    forwarded = handler.headers.get('X-Forwarded-For', '')
    if forwarded:
        return forwarded.split(',')[0].strip()
    return handler.client_address[0]


def _check_rate_limit(ip):
    """Raise ValueError if IP is currently locked out. Reads from DB."""
    now = time.time()
    row = qry1("SELECT locked_until FROM login_rate_limits WHERE ip=?", (ip,))
    if row and row['locked_until'] > now:
        remaining = int(row['locked_until'] - now)
        raise ValueError(f"Too many failed attempts. Try again in {remaining // 60}m {remaining % 60}s.")


def _record_failed_attempt(ip):
    """Record a failed login; lock IP after RATE_MAX_ATTEMPTS. Writes to DB."""
    now = time.time()
    c = _conn()
    try:
        c.execute("""
            INSERT INTO login_rate_limits (ip, attempts, locked_until, last_attempt)
            VALUES (?, 1, 0, ?)
            ON CONFLICT(ip) DO UPDATE SET
                attempts     = attempts + 1,
                last_attempt = excluded.last_attempt
        """, (ip, now))
        row = c.execute("SELECT attempts FROM login_rate_limits WHERE ip=?", (ip,)).fetchone()
        attempts = row[0] if row else 1
        if attempts >= RATE_MAX_ATTEMPTS:
            locked_until = now + RATE_LOCKOUT_SECS
            c.execute("UPDATE login_rate_limits SET locked_until=? WHERE ip=?", (locked_until, ip))
            _log('warning', 'rate_limit_lockout', ip=ip, attempts=attempts)
            print(f"  ⚠ Rate limit: {ip} locked for {RATE_LOCKOUT_SECS // 60}m after {attempts} failed attempts")
        c.commit()
    finally:
        c.close()


def _clear_rate_limit(ip):
    """Clear failed attempts on successful login."""
    run("DELETE FROM login_rate_limits WHERE ip=?", (ip,))


# ═══════════════════════════════════════════════════════════════════
#  PASSWORD HASHING
# ═══════════════════════════════════════════════════════════════════

def _hash_pw(password, salt):
    """SHA-256 hash — kept for verifying legacy passwords only."""
    return hashlib.sha256((salt + password).encode()).hexdigest()


def _hash_pw_new(password: str) -> tuple:
    """
    Hash a password using Argon2id (preferred) or SHA-256 (fallback).
    Returns (password_hash, salt, auth_scheme).
    Argon2id embeds its own salt, so salt='' in that case.
    """
    if _ARGON2_AVAILABLE:
        return _argon2.hash(password), '', 'argon2id'
    salt    = secrets.token_hex(16)
    pw_hash = hashlib.sha256((salt + password).encode()).hexdigest()
    return pw_hash, salt, 'sha256'


def _verify_pw(password: str, stored_hash: str, salt: str, scheme: str) -> bool:
    """
    Verify a password against the stored hash.
    Supports both 'argon2id' and 'sha256' schemes for backwards compatibility.
    """
    if scheme == 'argon2id' and _ARGON2_AVAILABLE:
        try:
            return _argon2.verify(stored_hash, password)
        except Exception:
            return False
    # SHA-256 path (legacy or argon2-cffi not installed)
    return hmac.compare_digest(
        hashlib.sha256((salt + password).encode()).hexdigest(),
        stored_hash
    )


# ═══════════════════════════════════════════════════════════════════
#  SESSION MANAGEMENT
# ═══════════════════════════════════════════════════════════════════

def _get_session_by_token(token: str):
    """Look up a non-expired session in the DB. Updates last_seen_at (sliding expiry)."""
    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    row = qry1("SELECT * FROM sessions WHERE token=? AND expires_at > ?", (token, now))
    if not row:
        return None
    new_expiry = (datetime.utcnow() + timedelta(hours=SESSION_EXPIRY_HOURS)).strftime('%Y-%m-%dT%H:%M:%S')
    run("UPDATE sessions SET last_seen_at=?, expires_at=? WHERE token=?", (now, new_expiry, token))
    sess = {
        'userId':      row['user_id'],
        'username':    row['username'],
        'displayName': row['display_name'],
        'role':        row['role'],
        'permissions': json.loads(row['permissions'] or '[]'),
    }
    if row['role'] == 'field_rep':
        sess['repId'] = row['user_id']
    return sess


def login_user(username, password):
    user   = qry1("SELECT * FROM users WHERE username=? AND active=1", (username,))
    scheme = user['auth_scheme'] if user else 'sha256'
    if not user or not _verify_pw(password, user['password_hash'], user.get('salt', ''), scheme):
        raise ValueError("Invalid username or password")

    # Auto-upgrade SHA-256 → Argon2id on first successful login after migration
    if scheme != 'argon2id' and _ARGON2_AVAILABLE:
        new_hash, new_salt, new_scheme = _hash_pw_new(password)
        run("UPDATE users SET password_hash=?, salt=?, auth_scheme=? WHERE id=?",
            (new_hash, new_salt, new_scheme, user['id']))
        _log('info', 'password_upgraded', user=user['username'], from_scheme=scheme, to_scheme=new_scheme)

    raw_perms = user.get('permissions') or '[]'
    try:
        perms = json.loads(raw_perms)
    except Exception:
        perms = []

    token      = secrets.token_hex(32)
    now        = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    expires_at = (datetime.utcnow() + timedelta(hours=SESSION_EXPIRY_HOURS)).strftime('%Y-%m-%dT%H:%M:%S')
    display    = user['display_name'] or user['username']

    run("""
        INSERT INTO sessions (token, user_id, username, display_name, role, permissions, created_at, expires_at, last_seen_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (token, user['id'], user['username'], display, user['role'], json.dumps(perms), now, expires_at, now))

    sess = {
        'userId':      user['id'],
        'username':    user['username'],
        'displayName': display,
        'role':        user['role'],
        'permissions': perms,
    }
    _log('info', 'login', user=user['username'], role=user['role'])
    show_welcome = user['username'].lower() == 'fk_baba'
    dev_tools    = os.environ.get('DEV_TOOLS', '').lower() in ('1', 'true', 'yes')
    return {**sess, 'token': token, 'showWelcome': show_welcome, 'devTools': dev_tools}


def logout_user(token):
    try:
        run("DELETE FROM sessions WHERE token=?", (token,))
    except Exception:
        pass


def get_session(handler, qs=None):
    """Resolve session from Bearer header, cookie, or query-string token."""
    def _lookup(token):
        if not token:
            return None
        return _get_session_by_token(token)

    # 1. Authorization header (BMS and field app legacy)
    auth = handler.headers.get('Authorization', '')
    if auth.startswith('Bearer '):
        sess = _lookup(auth[7:])
        if sess:
            return sess

    # 2. httpOnly cookie (field app secure path)
    cookie_header = handler.headers.get('Cookie', '')
    for part in cookie_header.split(';'):
        part = part.strip()
        if part.startswith('field_token='):
            sess = _lookup(part[len('field_token='):])
            if sess:
                return sess

    # 3. Query-string token (used for file downloads where headers can't be set)
    if qs:
        token_list = qs.get('token', [])
        if token_list:
            sess = _lookup(token_list[0])
            if sess:
                return sess

    return None


def _session_cleanup():
    """Background thread — deletes expired sessions from DB every 30 minutes."""
    while True:
        time.sleep(1800)
        try:
            now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
            c = _conn()
            c.execute("DELETE FROM sessions WHERE expires_at <= ?", (now,))
            c.commit()
            c.close()
        except Exception as e:
            _log('warning', f'Session cleanup error: {e}')


# ═══════════════════════════════════════════════════════════════════
#  FIELD REP AUTH
# ═══════════════════════════════════════════════════════════════════

def field_login(phone, pin):
    """Authenticate a field rep by phone + PIN. Returns a session token."""
    rep = qry1(
        "SELECT * FROM sales_reps WHERE phone=? AND (status IS NULL OR status='active')",
        (phone,))
    if not rep:
        raise ValueError("Phone number not registered")
    if not rep['pin_hash']:
        raise ValueError("PIN not set — contact your manager to set your PIN")
    pin_hash = hashlib.sha256(str(pin).encode()).hexdigest()
    if pin_hash != rep['pin_hash']:
        raise ValueError("Incorrect PIN")

    token      = secrets.token_hex(24)
    now        = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    expires_at = (datetime.utcnow() + timedelta(hours=SESSION_EXPIRY_HOURS)).strftime('%Y-%m-%dT%H:%M:%S')
    perms      = json.dumps([])
    run("""
        INSERT INTO sessions (token, user_id, username, display_name, role, permissions, created_at, expires_at, last_seen_at)
        VALUES (?, ?, ?, ?, 'field_rep', ?, ?, ?, ?)
    """, (token, rep['id'], rep['phone'], rep['name'], perms, now, expires_at, now))
    return {'token': token, 'repId': rep['id'], 'name': rep['name'], 'setcookie': True}


def _get_field_session(handler, qs=None):
    """Get session and verify it is a field rep. Returns session dict or None."""
    sess = get_session(handler, qs)
    if not sess or sess.get('role') != 'field_rep':
        return None
    return sess
