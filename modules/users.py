"""
modules/users.py — User management (CRUD, password reset, startup migration).

Extracted from server.py (Sprint 3). Overrides originals via bottom-import pattern:
    from modules.users import *   # just before __main__ in server.py
"""

import hashlib
import json
import os
import secrets

from modules.db    import _conn, qry, qry1, run, save_db
from modules.auth  import _hash_pw_new, _ARGON2_AVAILABLE, _argon2
from modules.utils import VALID_ROLES

__all__ = [
    'ensure_users_table',
    'list_users',
    'create_user',
    'update_user',
    '_reset_admin_pw_if_requested',
]


# ═══════════════════════════════════════════════════════════════════
#  STARTUP MIGRATION
# ═══════════════════════════════════════════════════════════════════

def ensure_users_table():
    """Create users table if not exists. Seed default admin on first run."""
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                username      TEXT    UNIQUE NOT NULL COLLATE NOCASE,
                display_name  TEXT    NOT NULL DEFAULT '',
                password_hash TEXT    NOT NULL,
                salt          TEXT    NOT NULL,
                role          TEXT    NOT NULL DEFAULT 'user',
                active        INTEGER NOT NULL DEFAULT 1,
                permissions   TEXT    NOT NULL DEFAULT '[]',
                created_at    TEXT    DEFAULT (datetime('now'))
            )
        """)
        # Safe migrations — all idempotent
        for col_sql in [
            "ALTER TABLE users ADD COLUMN permissions TEXT NOT NULL DEFAULT '[]'",
            "ALTER TABLE users ADD COLUMN auth_scheme TEXT NOT NULL DEFAULT 'sha256'",
        ]:
            try:
                c.execute(col_sql)
                c.commit()
            except Exception:
                pass  # column already exists

        count = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        if count == 0:
            # Seed default admin using Argon2id if available, SHA-256 otherwise
            if _ARGON2_AVAILABLE:
                pw_hash = _argon2.hash('admin123')
                salt    = ''
                scheme  = 'argon2id'
            else:
                salt    = secrets.token_hex(16)
                pw_hash = hashlib.sha256((salt + 'admin123').encode()).hexdigest()
                scheme  = 'sha256'
            c.execute("""
                INSERT INTO users (username, display_name, password_hash, salt, role, permissions, auth_scheme)
                VALUES ('admin', 'Administrator', ?, ?, 'admin', '[]', ?)
            """, (pw_hash, salt, scheme))
            c.commit()
            print("  ✓ Users: default admin created  →  admin / admin123")
        else:
            print(f"  ✓ Users: {count} user(s) configured")

        c.commit()
    finally:
        c.close()
    save_db()


# ═══════════════════════════════════════════════════════════════════
#  USER CRUD
# ═══════════════════════════════════════════════════════════════════

def list_users():
    return qry("SELECT id, username, display_name, role, active, permissions, created_at FROM users ORDER BY id")


def create_user(data, requesting_role):
    if requesting_role != 'admin':
        raise ValueError("Only admins can create users")
    username = data.get('username', '').strip()
    password = data.get('password', '').strip()
    if not username or not password:
        raise ValueError("Username and password are required")
    if len(password) < 6:
        raise ValueError("Password must be at least 6 characters")
    if qry1("SELECT id FROM users WHERE username=?", (username,)):
        raise ValueError(f"Username '{username}' already exists")
    pw_hash, salt, scheme = _hash_pw_new(password)
    role = data.get('role', 'user')
    if role not in VALID_ROLES:
        role = 'user'
    disp       = data.get('displayName', username).strip() or username
    perms      = data.get('permissions', [])
    if not isinstance(perms, list):
        perms = []
    perms_json = json.dumps(perms)
    c = _conn()
    try:
        c.execute("""
            INSERT INTO users (username, display_name, password_hash, salt, role, permissions, auth_scheme)
            VALUES (?,?,?,?,?,?,?)
        """, (username, disp, pw_hash, salt, role, perms_json, scheme))
        uid = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('users',?,'INSERT',?)
        """, (str(uid), json.dumps({'username': username, 'role': role, 'permissions': perms})))
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return qry1("SELECT id, username, display_name, role, active, permissions FROM users WHERE id=?", (uid,))


def update_user(user_id, data, requesting_role, requesting_user_id):
    user = qry1("SELECT * FROM users WHERE id=?", (user_id,))
    if not user:
        raise ValueError("User not found")
    if requesting_role != 'admin' and requesting_user_id != user_id:
        raise ValueError("Permission denied")
    set_parts, vals = [], []
    new_pw = data.get('newPassword', '').strip()
    if new_pw:
        if len(new_pw) < 6:
            raise ValueError("Password must be at least 6 characters")
        pw_hash, salt, scheme = _hash_pw_new(new_pw)
        set_parts += ["password_hash=?", "salt=?", "auth_scheme=?"]
        vals      += [pw_hash, salt, scheme]
    if requesting_role == 'admin':
        if 'displayName' in data:
            dn = data['displayName'].strip()
            set_parts.append("display_name=?"); vals.append(dn)
        if 'role' in data and data['role'] in VALID_ROLES:
            set_parts.append("role=?");         vals.append(data['role'])
        if 'active' in data:
            set_parts.append("active=?");       vals.append(1 if data['active'] else 0)
        if 'permissions' in data:
            perms = data['permissions']
            if not isinstance(perms, list):
                perms = []
            set_parts.append("permissions=?"); vals.append(json.dumps(perms))
    if not set_parts:
        return dict(user)
    vals.append(user_id)
    c = _conn()
    try:
        c.execute(f"UPDATE users SET {', '.join(set_parts)} WHERE id=?", vals)
        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('users',?,'UPDATE',?)
        """, (str(user_id), json.dumps({'fields': list(data.keys())})))
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return qry1("SELECT id, username, display_name, role, active, permissions FROM users WHERE id=?", (user_id,))


# ═══════════════════════════════════════════════════════════════════
#  STARTUP UTILITY
# ═══════════════════════════════════════════════════════════════════

def _reset_admin_pw_if_requested():
    """If RESET_ADMIN_PW env var is set, reset admin password and clear all rate limits."""
    new_pw = os.environ.get('RESET_ADMIN_PW', '').strip()
    if not new_pw:
        return
    salt    = ''
    pw_hash = hashlib.sha256(new_pw.encode()).hexdigest()
    scheme  = 'sha256'
    c = _conn()
    try:
        c.execute("""
            UPDATE users SET password_hash=?, salt=?, auth_scheme=?
            WHERE username='admin'
        """, (pw_hash, salt, scheme))
        c.execute("DELETE FROM login_rate_limits")
        c.commit()
        print(f"  ✓ Admin password reset (SHA-256) via RESET_ADMIN_PW. Rate limits cleared. REMOVE the env var now!")
    finally:
        c.close()
