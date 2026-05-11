#!/usr/bin/env python3
"""
Spicetopia Business Management System — Python / SQLite Server
Schema: Fully normalized (products, product_variants, invoices, AP, AR, audit)
Run:    python3 server.py   OR double-click start.command
Access: http://localhost:3001

Design rules:
  • Totals NEVER stored — always computed via SQL
  • Inventory only changes via ledger entries
  • Production auto-deducts ingredients via BOM
  • AP: bill_balance = SUM(items) - SUM(allocations)
  • AR: invoice_balance = SUM(items)×1.18 - SUM(allocations)
  • GST 18% — computed on-the-fly, never stored
"""

import csv
import hashlib
import hmac
import json
import logging
import re
import logging.handlers

# Argon2id — preferred password hashing algorithm.
# Falls back gracefully if argon2-cffi is not installed
# (existing SHA-256 hashes will continue to work; new passwords will use SHA-256).
try:
    from argon2 import PasswordHasher as _Argon2Hasher
    from argon2.exceptions import VerifyMismatchError as _Argon2VerifyError
    _argon2 = _Argon2Hasher(
        time_cost=2,        # 2 iterations
        memory_cost=65536,  # 64 MB
        parallelism=2,
        hash_len=32,
        salt_len=16,
    )
    _ARGON2_AVAILABLE = True
except ImportError:
    _ARGON2_AVAILABLE = False
    print("  ⚠ argon2-cffi not installed — passwords will use SHA-256. "
          "Run: pip install argon2-cffi")
import sys
import os
import platform
import secrets
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
import webbrowser
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

# ── Paths ─────────────────────────────────────────────────────────────────────
# sys._MEIPASS is set by PyInstaller when running as a bundled .exe
# Falls back to the script's own directory when running normally
import sys as _sys
BASE_DIR    = Path(getattr(_sys, '_MEIPASS', Path(__file__).parent))
# CONFIG_FILE lives next to the .exe (or script), not inside the bundle
EXE_DIR     = Path(_sys.executable).parent if getattr(_sys, 'frozen', False) else Path(__file__).parent
CONFIG_FILE = EXE_DIR / 'config.json'
MASTERS_DIR = BASE_DIR / 'masters'
DB_TMP      = Path(tempfile.gettempdir()) / 'spicetopia_v3_live.db'
PUBLIC_DIR  = BASE_DIR / 'public'
PORT        = 3001           # overridden at startup from config.json / env var PORT
SERVER_START_TIME = int(time.time())   # set once at import time — changes on every restart
GST_RATE    = 0.18           # 18%
USER_NAME   = "FK_Baba"      # Display name — change to match the logged-in user
OS          = platform.system()   # 'Darwin' | 'Windows' | 'Linux'

# ── Cloud-ready runtime config (set by _apply_startup_config at startup) ──────
SESSION_EXPIRY_HOURS    = 12   # how long a login token is valid
SOFT_HOLD_EXPIRY_HOURS  = 48   # hours before an unapproved pending_review order releases its stock hold
LOG_PATH                = None # path to rotating JSON log file
BACKUP_PATH             = None # folder for automated SQLite backups
BACKUP_KEEP_DAYS        = 7    # days of backups to retain
CORS_ORIGINS: list      = []   # allowed CORS origins (empty = disabled)
# ── WhatsApp notifications (CallMeBot) ───────────────────────────────────────
WA_ENABLED           = False  # set True in config.json or env to activate
WA_ADMIN_PHONE       = ''     # admin's phone in international format, no + (e.g. 923001234567)
WA_ADMIN_APIKEY      = ''     # CallMeBot API key for admin
WA_EXPIRY_WARN_HOURS = 6      # warn admin when hold has fewer than this many hours left

_SERVER_START_TIME   = time.time()   # for /health uptime reporting
_logger: logging.Logger = None       # initialised by _setup_logging()

# DB_SRC is resolved at startup via resolve_db_path()
DB_SRC: Path = None

# ── Rate limiter — DB-persisted so lockouts survive server restarts ───────────
RATE_MAX_ATTEMPTS = 5      # failed attempts before lockout
RATE_LOCKOUT_SECS = 900    # 15 minutes (900 seconds)

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
        # Upsert: insert or increment
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
#  STARTUP — DB PATH RESOLUTION + WELCOME
# ═══════════════════════════════════════════════════════════════════

def _load_config() -> dict:
    try:
        if CONFIG_FILE.exists():
            return json.loads(CONFIG_FILE.read_text())
    except Exception:
        pass
    return {}

def _save_config(data: dict):
    try:
        CONFIG_FILE.write_text(json.dumps(data, indent=2))
    except Exception as e:
        print(f"  ⚠ Could not save config: {e}")


def ensure_system_settings_schema():
    """Create system_settings key-value table if not present."""
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS system_settings (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            )
        """)
        c.commit()
    finally:
        c.close()

def get_setting(key: str, default=None):
    """Read a value from system_settings. Returns default if not found."""
    try:
        row = qry1("SELECT value FROM system_settings WHERE key=?", (key,))
        return row['value'] if row else default
    except Exception:
        return default

def set_setting(key: str, value: str):
    """Upsert a value in system_settings."""
    run("INSERT INTO system_settings (key, value) VALUES (?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, str(value) if value is not None else ''))

def _reload_wa_from_db():
    """Hot-reload WhatsApp globals from system_settings DB (called after UI save)."""
    global WA_ENABLED, WA_ADMIN_PHONE, WA_ADMIN_APIKEY, WA_EXPIRY_WARN_HOURS
    # DB values override config.json but env vars still win
    if not os.environ.get('WA_ENABLED'):
        v = get_setting('whatsapp_enabled', '')
        if v != '':
            WA_ENABLED = v.lower() in ('1', 'true', 'yes')
    if not os.environ.get('WA_ADMIN_PHONE'):
        WA_ADMIN_PHONE = get_setting('whatsapp_admin_phone', WA_ADMIN_PHONE) or WA_ADMIN_PHONE
    if not os.environ.get('WA_ADMIN_APIKEY'):
        WA_ADMIN_APIKEY = get_setting('whatsapp_admin_apikey', WA_ADMIN_APIKEY) or WA_ADMIN_APIKEY


def _apply_startup_config():
    """
    Read config.json and environment variables to set runtime globals.
    Environment variables take precedence over config.json.
    config.json keys: port, session_expiry_hours, log_path, backup_path,
                      backup_keep_days, cors_origins (list).
    """
    global PORT, SESSION_EXPIRY_HOURS, SOFT_HOLD_EXPIRY_HOURS, LOG_PATH, BACKUP_PATH, BACKUP_KEEP_DAYS, CORS_ORIGINS
    cfg = _load_config()

    PORT                   = int(os.environ.get('PORT',                   cfg.get('port',                  PORT)))
    SESSION_EXPIRY_HOURS   = int(os.environ.get('SESSION_EXPIRY_HOURS',   cfg.get('session_expiry_hours',  SESSION_EXPIRY_HOURS)))
    SOFT_HOLD_EXPIRY_HOURS = int(os.environ.get('SOFT_HOLD_EXPIRY_HOURS', cfg.get('soft_hold_expiry_hours',SOFT_HOLD_EXPIRY_HOURS)))
    BACKUP_KEEP_DAYS       = int(os.environ.get('BACKUP_KEEP_DAYS',       cfg.get('backup_keep_days',      BACKUP_KEEP_DAYS)))

    # Log path — defaults to <exe_dir>/logs/server.log
    lp = os.environ.get('LOG_PATH') or cfg.get('log_path', '')
    LOG_PATH = Path(lp) if lp else EXE_DIR / 'logs' / 'server.log'

    # Backup path — defaults to <exe_dir>/backups/
    bp = os.environ.get('BACKUP_PATH') or cfg.get('backup_path', '')
    BACKUP_PATH = Path(bp) if bp else EXE_DIR / 'backups'

    # CORS origins — comma-separated env var or JSON array in config
    cors_env = os.environ.get('CORS_ORIGINS', '')
    if cors_env:
        CORS_ORIGINS = [o.strip() for o in cors_env.split(',') if o.strip()]
    else:
        CORS_ORIGINS = cfg.get('cors_origins', [])

    # WhatsApp (CallMeBot) notification config — env > config.json (DB loaded later via _reload_wa_from_db)
    global WA_ENABLED, WA_ADMIN_PHONE, WA_ADMIN_APIKEY, WA_EXPIRY_WARN_HOURS
    WA_ENABLED           = bool(int(os.environ.get('WA_ENABLED', '1' if cfg.get('whatsapp_enabled', False) else '0')))
    WA_ADMIN_PHONE       = os.environ.get('WA_ADMIN_PHONE',  cfg.get('whatsapp_admin_phone',  ''))
    WA_ADMIN_APIKEY      = os.environ.get('WA_ADMIN_APIKEY', cfg.get('whatsapp_admin_apikey', ''))
    WA_EXPIRY_WARN_HOURS = int(os.environ.get('WA_EXPIRY_WARN_HOURS', cfg.get('whatsapp_expiry_warn_hours', WA_EXPIRY_WARN_HOURS)))

    import modules.auth as _auth_mod; _auth_mod.SESSION_EXPIRY_HOURS = SESSION_EXPIRY_HOURS   # sync to module

    import modules.orders as _ord_mod                                                          # sync orders config
    _ord_mod.SOFT_HOLD_EXPIRY_HOURS = SOFT_HOLD_EXPIRY_HOURS
    _ord_mod.WA_ENABLED             = WA_ENABLED
    _ord_mod.WA_ADMIN_PHONE         = WA_ADMIN_PHONE
    _ord_mod.WA_ADMIN_APIKEY        = WA_ADMIN_APIKEY

    import modules.invoices as _inv_mod2                                                       # sync invoices config
    _inv_mod2.GST_RATE = GST_RATE

    print(f"  ✓ Config applied — PORT={PORT}, session_expiry={SESSION_EXPIRY_HOURS}h, "
          f"backup_keep={BACKUP_KEEP_DAYS}d, cors={CORS_ORIGINS or 'disabled'}")


def _setup_logging():
    """
    Configure structured JSON logging.
    Writes rotating JSON log to LOG_PATH (10 MB × 5 files).
    WARNING+ messages are also printed to stderr.
    Returns the logger instance and stores it in _logger.
    """
    global _logger

    class _JsonFormatter(logging.Formatter):
        def format(self, record):
            entry = {
                'ts':    self.formatTime(record, datefmt='%Y-%m-%dT%H:%M:%S'),
                'level': record.levelname,
                'msg':   record.getMessage(),
            }
            for field in ('req_id', 'user', 'path', 'status', 'ms', 'ip'):
                v = getattr(record, field, None)
                if v is not None:
                    entry[field] = v
            if record.exc_info:
                entry['exc'] = self.formatException(record.exc_info)
            return json.dumps(entry)

    logger = logging.getLogger('spicetopia')
    logger.setLevel(logging.INFO)
    logger.propagate = False   # don't double-log to root logger

    # File handler — rotating JSON log
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(
            str(LOG_PATH), maxBytes=10 * 1024 * 1024, backupCount=5, encoding='utf-8'
        )
        fh.setFormatter(_JsonFormatter())
        logger.addHandler(fh)
        print(f"  ✓ Logging → {LOG_PATH}")
    except Exception as e:
        print(f"  ⚠ Could not open log file {LOG_PATH}: {e}")

    # Stderr handler — warnings and above only
    sh = logging.StreamHandler()
    sh.setLevel(logging.WARNING)
    sh.setFormatter(_JsonFormatter())
    logger.addHandler(sh)

    _logger = logger
    import modules.utils as _utils_mod; _utils_mod._logger = _logger   # sync to module
    return logger


def _log(level: str, msg: str, **fields):
    """Convenience wrapper — logs to _logger if available, else prints.
    Pass exc_info=True to capture the current exception's stack trace."""
    if _logger is None:
        return
    exc_info = fields.pop('exc_info', False)
    extra = {k: v for k, v in fields.items()}
    getattr(_logger, level)(msg, extra=extra, exc_info=exc_info)

def _pick_file_gui(title: str, initial_dir: Path) -> str:
    """Open a native OS file picker and return the selected path (or '')."""
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()          # hide the empty root window
        root.attributes('-topmost', True)   # bring dialog to front
        root.lift()
        chosen = filedialog.askopenfilename(
            parent=root,
            title=title,
            initialdir=str(initial_dir),
            filetypes=[("Spicetopia Database", "*.db"), ("All files", "*.*")]
        )
        root.destroy()
        return chosen or ''
    except Exception as e:
        print(f"  ⚠ File picker unavailable ({e}). Falling back to terminal input.")
        return ''

def _build_welcome_html():
    """
    Generate the HTML for the /welcome splash page.
    Personalised for FK_Baba: shows photo if public/img/fk_baba.jpg exists.
    Auto-redirects to the main app after 4 seconds.
    """
    photo_path = next((PUBLIC_DIR / 'img' / f for f in ('fk_baba.jpg', 'fk_baba.jpeg', 'fk_baba.png') if (PUBLIC_DIR / 'img' / f).exists()), None)
    has_photo  = photo_path is not None

    # Greeting based on time of day
    hour = datetime.now().hour
    if hour < 12:
        greeting = "Good morning"
    elif hour < 17:
        greeting = "Good afternoon"
    else:
        greeting = "Good evening"

    # Photo block — shown only for FK_Baba and only if the file exists
    if USER_NAME == 'FK_Baba' and has_photo:
        photo_block = f"""
        <div class="avatar-wrap">
          <img src="/img/{photo_path.name}" class="avatar" alt="FK Baba" />
        </div>"""
    else:
        photo_block = """
        <div class="avatar-wrap">
          <div class="avatar-placeholder">🌶</div>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Welcome — Spicetopia BMS</title>
  <style>
    * {{ margin:0; padding:0; box-sizing:border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', sans-serif;
      background: linear-gradient(135deg, #1C0E04 0%, #3D1F0A 40%, #6B3A1F 100%);
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      color: #fff;
    }}
    .card {{
      background: rgba(255,255,255,0.08);
      backdrop-filter: blur(24px);
      -webkit-backdrop-filter: blur(24px);
      border: 1px solid rgba(255,255,255,0.15);
      border-radius: 24px;
      padding: 48px 56px;
      text-align: center;
      max-width: 440px;
      width: 90%;
      box-shadow: 0 32px 80px rgba(0,0,0,0.5);
      animation: pop .5s cubic-bezier(.34,1.56,.64,1) both;
    }}
    @keyframes pop {{
      from {{ opacity:0; transform:scale(0.85) translateY(20px); }}
      to   {{ opacity:1; transform:scale(1) translateY(0); }}
    }}
    .avatar-wrap {{
      margin-bottom: 24px;
    }}
    .avatar {{
      width: 110px;
      height: 110px;
      border-radius: 50%;
      object-fit: cover;
      border: 3px solid #E8901A;
      box-shadow: 0 0 0 6px rgba(232,144,26,.25), 0 8px 32px rgba(0,0,0,.4);
    }}
    .avatar-placeholder {{
      width: 110px;
      height: 110px;
      border-radius: 50%;
      background: rgba(232,144,26,.2);
      border: 3px solid #E8901A;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 52px;
      margin: 0 auto;
    }}
    .greeting {{
      font-size: 14px;
      color: #E8901A;
      letter-spacing: 2px;
      text-transform: uppercase;
      font-weight: 600;
      margin-bottom: 8px;
    }}
    .name {{
      font-size: 32px;
      font-weight: 700;
      letter-spacing: -0.5px;
      margin-bottom: 6px;
    }}
    .tagline {{
      font-size: 15px;
      color: rgba(255,255,255,0.6);
      margin-bottom: 32px;
      line-height: 1.5;
    }}
    .app-badge {{
      display: inline-block;
      background: #E8901A;
      color: #1C0E04;
      font-weight: 700;
      font-size: 12px;
      letter-spacing: 1px;
      padding: 4px 14px;
      border-radius: 999px;
      margin-bottom: 28px;
    }}
    .btn {{
      display: inline-block;
      background: #E8901A;
      color: #1C0E04;
      font-weight: 700;
      font-size: 15px;
      padding: 12px 36px;
      border-radius: 12px;
      border: none;
      cursor: pointer;
      text-decoration: none;
      transition: transform .15s, box-shadow .15s;
      box-shadow: 0 4px 20px rgba(232,144,26,.4);
    }}
    .btn:hover {{ transform:scale(1.04); box-shadow:0 6px 28px rgba(232,144,26,.55); }}
    .btn:active {{ transform:scale(0.97); }}
    .countdown {{
      font-size: 12px;
      color: rgba(255,255,255,0.35);
      margin-top: 16px;
    }}
    .dots {{ display:inline-flex; gap:6px; margin-top:12px; }}
    .dot {{
      width:6px; height:6px; border-radius:50%;
      background:#E8901A; opacity:0.3;
      animation: pulse 1.2s ease-in-out infinite;
    }}
    .dot:nth-child(2) {{ animation-delay:.2s; }}
    .dot:nth-child(3) {{ animation-delay:.4s; }}
    @keyframes pulse {{ 0%,100%{{opacity:.3}} 50%{{opacity:1}} }}
  </style>
</head>
<body>
  <div class="card">
    {photo_block}
    <div class="greeting">{greeting}</div>
    <div class="name">{USER_NAME.replace('_', ' ')}</div>
    <div class="tagline">Spicetopia Business Management System</div>
    <div class="app-badge">BUSINESS MANAGEMENT SYSTEM</div><br>
    <a href="/" class="btn">Let's Go! 🌶</a>
    <div class="countdown" id="cd">Opening automatically in <span id="sec">4</span>s…</div>
    <div class="dots"><div class="dot"></div><div class="dot"></div><div class="dot"></div></div>
  </div>
  <script>
    let s = 4;
    const el = document.getElementById('sec');
    const t = setInterval(() => {{
      s--;
      if (el) el.textContent = s;
      if (s <= 0) {{ clearInterval(t); window.location.replace('/'); }}
    }}, 1000);
  </script>
</body>
</html>"""


def _show_welcome_gui():
    """
    Browser-based welcome splash — works on all platforms, supports photos.
    Browser opens to /welcome which auto-redirects to / after 4 seconds.
    """
    pass   # Browser open is now handled in _open_browser (opens /welcome)

def _check_db_healthy(path: Path) -> bool:
    """Return True only if path is a non-empty, readable SQLite file with the core schema."""
    try:
        if not path.exists() or path.stat().st_size < 512:
            return False
        import sqlite3 as _sq
        # Copy to /tmp first — avoids WAL/journal issues on network/cloud drives
        _tmp_chk = Path('/tmp') / f'_chk_{path.stem}.db'
        shutil.copy2(str(path), str(_tmp_chk))
        con = _sq.connect(str(_tmp_chk))
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        con.close()
        _tmp_chk.unlink(missing_ok=True)
        return 'products' in tables
    except Exception:
        return False


def _auto_restore_from_backup(db_path: Path) -> bool:
    """
    If db_path is corrupt/empty, find the most recent healthy backup and restore it.
    Returns True if a restore was performed, False otherwise.
    """
    backup_dir = db_path.parent / 'spicetopia_backups'
    if not backup_dir.exists():
        return False
    candidates = sorted(
        backup_dir.glob(f"{db_path.stem}_*.db"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    for bk in candidates:
        if _check_db_healthy(bk):
            print(f"  🔄 DB was empty/corrupt — auto-restoring from backup: {bk.name}")
            with open(bk, 'rb') as f_in:
                data = f_in.read()
            with open(db_path, 'wb') as f_out:
                f_out.write(data)
            print(f"  ✓ Restored {len(data)//1024} KB from {bk.name}")
            return True
    print(f"  ✗ No healthy backup found in {backup_dir} — cannot auto-restore")
    return False


def resolve_db_path() -> Path:
    """
    Determine the DB file to use, in this order:
      0. RAILWAY_VOLUME env var (cloud deployment — persistent volume mount point)
      1. Saved path in config.json (if file is healthy)
      2. Default data/spicetopia.db (if healthy, or auto-restorable from backup)
      3. Ask user via native file picker
      4. Fallback to terminal input
    Saves result to config.json for next time.
    """
    global DB_SRC
    config = _load_config()

    # 0. Explicit DB_PATH env var — highest priority (useful for testing / CI)
    explicit_path = os.environ.get('DB_PATH', '')
    if explicit_path:
        ep = Path(explicit_path)
        ep.parent.mkdir(parents=True, exist_ok=True)
        DB_SRC = ep
        print(f"  ✓ DB_PATH override — database: {DB_SRC}")
        return DB_SRC

    # 0a. Cloud deployment — use env var path or auto-create at default location
    railway_vol = os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', '')
    if railway_vol:
        vol_path = Path(railway_vol) / 'spicetopia.db'
        vol_path.parent.mkdir(parents=True, exist_ok=True)
        DB_SRC = vol_path
        print(f"  ✓ Cloud volume detected — database: {DB_SRC}")
        return DB_SRC

    # 0b. Non-interactive environment (Railway, Docker, etc.) — auto-create DB
    if not sys.stdin.isatty() and OS == 'Linux':
        auto_path = BASE_DIR / 'data' / 'spicetopia.db'
        auto_path.parent.mkdir(parents=True, exist_ok=True)
        DB_SRC = auto_path
        print(f"  ✓ Non-interactive mode — database: {DB_SRC}")
        return DB_SRC

    def _accept(p: Path, label: str) -> Path:
        """Validate p; if unhealthy, try auto-restore; return p if OK, None otherwise."""
        if _check_db_healthy(p):
            return p
        print(f"  ⚠ {label} exists but is empty or corrupt: {p}")
        if _auto_restore_from_backup(p):
            return p   # now restored
        return None

    # 1. Config file has a saved path
    saved = config.get('db_path', '')
    if saved:
        sp = Path(saved)
        if sp.exists():
            result = _accept(sp, "Saved DB path")
            if result:
                DB_SRC = result
                print(f"  ✓ Using saved database: {DB_SRC}")
                return DB_SRC
            # file exists but couldn't be recovered → fall through
        else:
            print(f"  ⚠ Saved DB path not found: {saved}")
            print(f"    Will ask you to select again.")

    # 2. Default location
    default = BASE_DIR / 'data' / 'spicetopia.db'
    if default.exists():
        result = _accept(default, "Default DB")
        if result:
            DB_SRC = result
            _save_config({**config, 'db_path': str(DB_SRC)})
            print(f"  ✓ Found database at default location: {DB_SRC.name}")
            return DB_SRC

    # 3. GUI file picker
    print()
    print("  ┌─────────────────────────────────────────────────┐")
    print("  │  No database found. Please select your          │")
    print("  │  spicetopia.db file (e.g. on OneDrive).         │")
    print("  └─────────────────────────────────────────────────┘")
    print()

    # Suggest OneDrive location based on OS
    if OS == 'Windows':
        initial = Path.home() / 'OneDrive'
    elif OS == 'Darwin':
        # macOS OneDrive mount point
        od = Path.home() / 'Library' / 'CloudStorage'
        initial = od if od.exists() else Path.home()
    else:
        initial = Path.home()

    chosen = _pick_file_gui("Select your Spicetopia database (spicetopia.db)", initial)

    if not chosen:
        # 4. Terminal fallback
        print("  Enter the full path to your spicetopia.db file:")
        chosen = input("  > ").strip().strip('"').strip("'")

    if not chosen:
        raise FileNotFoundError("No database selected. Cannot start.")

    db_path = Path(chosen)
    if not db_path.exists():
        raise FileNotFoundError(f"File not found: {db_path}")
    if not db_path.suffix == '.db':
        print(f"  ⚠ Warning: selected file does not end in .db: {db_path.name}")

    DB_SRC = db_path
    _save_config({**config, 'db_path': str(DB_SRC)})
    print(f"  ✓ Database selected: {DB_SRC}")
    return DB_SRC


# ═══════════════════════════════════════════════════════════════════
#  DB BOOTSTRAP  (OneDrive → /tmp so WAL works)
# ═══════════════════════════════════════════════════════════════════

def bootstrap_db():
    if DB_SRC is None:
        raise FileNotFoundError("Database path not set.")
    if not DB_SRC.exists():
        # Cloud / first-run: create a fresh empty database
        DB_SRC.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DB_SRC))
        conn.close()
        print(f"  ✓ Fresh database created: {DB_SRC}")
    shutil.copy2(str(DB_SRC), str(DB_TMP))
    print(f"  ✓ Database loaded ({DB_SRC.name})")
    # ── Sync DB_SRC into foundation modules so save_db() works correctly ──
    import modules.db as _db_mod
    _db_mod.DB_SRC = DB_SRC

MAX_BACKUPS = 5   # keep the last 5 snapshots


def _migrate_invoice_items_line_total():
    """
    Migration: if invoice_items has a 'total' column (old schema) but no 'line_total',
    recreate the table with 'line_total'. Fixes 'no such column: line_total' on Railway
    instances created before the column was renamed. Idempotent — safe to run every startup.
    """
    c = _conn()
    try:
        cols = [r[1] for r in c.execute("PRAGMA table_info(invoice_items)").fetchall()]
        if 'line_total' not in cols and 'total' in cols:
            print("  ⚙ Migrating invoice_items: renaming 'total' → 'line_total'")
            c.execute("PRAGMA foreign_keys=OFF")
            c.execute("""
                CREATE TABLE invoice_items_new (
                    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                    invoice_id         INTEGER NOT NULL REFERENCES invoices(id),
                    product_variant_id INTEGER REFERENCES product_variants(id),
                    product_code       TEXT NOT NULL,
                    product_name       TEXT NOT NULL,
                    pack_size          TEXT NOT NULL,
                    quantity           INTEGER NOT NULL,
                    unit_price         REAL NOT NULL,
                    line_total         REAL NOT NULL,
                    sale_id            TEXT
                )
            """)
            c.execute("""
                INSERT INTO invoice_items_new
                    (id, invoice_id, product_variant_id, product_code, product_name,
                     pack_size, quantity, unit_price, line_total, sale_id)
                SELECT
                    id, invoice_id, product_variant_id, product_code, product_name,
                    pack_size, quantity, unit_price, total, sale_id
                FROM invoice_items
            """)
            c.execute("DROP TABLE invoice_items")
            c.execute("ALTER TABLE invoice_items_new RENAME TO invoice_items")
            c.execute("PRAGMA foreign_keys=ON")
            c.commit()
            print("  ✓ invoice_items migrated — 'line_total' column now in place")
        else:
            print("  ✓ invoice_items schema OK — 'line_total' column present")
    except Exception as e:
        print(f"  ⚠ invoice_items migration skipped: {e}")
        c.rollback()
    finally:
        c.close()


def ensure_full_schema():
    """
    Create ALL core tables on a fresh database (idempotent — safe to run on existing DBs).
    Must be called immediately after bootstrap_db(), before any other ensure_* functions.
    """
    c = _conn()
    try:
        stmts = [
            # ── Core counters ────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS id_counters (
                entity    TEXT PRIMARY KEY,
                last_num  INTEGER DEFAULT 0
            )""",
            # ── Reference / lookup tables ────────────────────────────
            """CREATE TABLE IF NOT EXISTS pack_sizes (
                id     INTEGER PRIMARY KEY AUTOINCREMENT,
                label  TEXT NOT NULL UNIQUE,
                grams  INTEGER NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS price_types (
                id    INTEGER PRIMARY KEY AUTOINCREMENT,
                code  TEXT NOT NULL UNIQUE,
                label TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS zones (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT NOT NULL,
                city       TEXT NOT NULL DEFAULT 'Karachi',
                active     INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now'))
            )""",
            # ── Core entities ────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS products (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                code       TEXT NOT NULL UNIQUE,
                name       TEXT NOT NULL,
                name_urdu  TEXT DEFAULT '',
                blend_code TEXT DEFAULT '',
                active     INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (date('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS product_variants (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                sku_code     TEXT NOT NULL UNIQUE,
                product_id   INTEGER NOT NULL REFERENCES products(id),
                pack_size_id INTEGER NOT NULL REFERENCES pack_sizes(id),
                active_flag  INTEGER DEFAULT 1,
                UNIQUE (product_id, pack_size_id)
            )""",
            """CREATE TABLE IF NOT EXISTS product_prices (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                product_variant_id INTEGER NOT NULL REFERENCES product_variants(id),
                price_type_id      INTEGER NOT NULL REFERENCES price_types(id),
                price              REAL NOT NULL,
                effective_from     TEXT NOT NULL,
                active_flag        INTEGER DEFAULT 1
            )""",
            """CREATE TABLE IF NOT EXISTS customers (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                code               TEXT NOT NULL UNIQUE,
                account_number     TEXT DEFAULT NULL,
                name               TEXT NOT NULL,
                customer_type      TEXT NOT NULL DEFAULT 'RETAIL'
                                   CHECK(customer_type IN ('RETAIL','DIRECT')),
                category           TEXT DEFAULT '',
                city               TEXT DEFAULT '',
                address            TEXT DEFAULT '',
                phone              TEXT DEFAULT '',
                email              TEXT DEFAULT '',
                default_pack       TEXT DEFAULT '50g',
                payment_terms_days INTEGER DEFAULT 30,
                credit_limit       REAL DEFAULT 0,
                active             INTEGER DEFAULT 1,
                created_at         TEXT DEFAULT (date('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS suppliers (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                code        TEXT NOT NULL UNIQUE,
                name        TEXT NOT NULL,
                contact     TEXT DEFAULT '',
                phone       TEXT DEFAULT '',
                email       TEXT DEFAULT '',
                city        TEXT DEFAULT '',
                address     TEXT DEFAULT '',
                active_flag INTEGER DEFAULT 1,
                zone_id     INTEGER REFERENCES zones(id),
                created_at  TEXT DEFAULT (date('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS ingredients (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                code          TEXT NOT NULL UNIQUE,
                name          TEXT NOT NULL DEFAULT '',
                opening_grams REAL DEFAULT 0,
                reorder_level REAL DEFAULT 0,
                cost_per_kg   REAL NOT NULL DEFAULT 0,
                active        INTEGER NOT NULL DEFAULT 1,
                created_at    TEXT DEFAULT (date('now'))
            )""",
            # ── BOM ──────────────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS bom_versions (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id       INTEGER NOT NULL REFERENCES products(id),
                version_no       INTEGER NOT NULL DEFAULT 1,
                batch_size_grams REAL NOT NULL DEFAULT 1000,
                effective_from   TEXT NOT NULL,
                active_flag      INTEGER DEFAULT 1,
                notes            TEXT DEFAULT '',
                UNIQUE (product_id, version_no)
            )""",
            """CREATE TABLE IF NOT EXISTS bom_items (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                bom_version_id INTEGER NOT NULL REFERENCES bom_versions(id),
                ingredient_id  INTEGER NOT NULL REFERENCES ingredients(id),
                quantity_grams REAL NOT NULL,
                UNIQUE (bom_version_id, ingredient_id)
            )""",
            # ── Inventory ────────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS inventory_ledger (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                ingredient_id INTEGER NOT NULL REFERENCES ingredients(id),
                movement_type TEXT NOT NULL
                              CHECK(movement_type IN ('OPENING','PURCHASE_IN','PRODUCTION_USE','ADJUSTMENT')),
                qty_grams     REAL NOT NULL,
                reference_id  TEXT DEFAULT '',
                notes         TEXT DEFAULT '',
                created_at    TEXT DEFAULT (datetime('now'))
            )""",
            # ── Production ───────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS production_batches (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id              TEXT NOT NULL UNIQUE,
                batch_date            TEXT NOT NULL,
                product_id            INTEGER NOT NULL REFERENCES products(id),
                product_variant_id    INTEGER REFERENCES product_variants(id),
                bom_version_id        INTEGER REFERENCES bom_versions(id),
                qty_grams             REAL NOT NULL,
                qty_units             INTEGER DEFAULT 0,
                pack_size             TEXT DEFAULT '',
                mfg_date              TEXT DEFAULT '',
                best_before           TEXT DEFAULT '',
                notes                 TEXT DEFAULT '',
                unit_cost_at_posting  REAL DEFAULT 0,
                created_at            TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS production_consumption (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id      INTEGER NOT NULL REFERENCES production_batches(id),
                ingredient_id INTEGER NOT NULL REFERENCES ingredients(id),
                qty_grams     REAL NOT NULL
            )""",
            # ── Sales & AR ───────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS invoices (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_number   TEXT NOT NULL UNIQUE,
                customer_id      INTEGER NOT NULL REFERENCES customers(id),
                invoice_date     TEXT NOT NULL,
                due_date         TEXT NOT NULL,
                status           TEXT DEFAULT 'UNPAID'
                                 CHECK(status IN ('DRAFT','UNPAID','PARTIAL','PAID','VOID')),
                notes            TEXT DEFAULT '',
                customer_order_id INTEGER REFERENCES customer_orders(id),
                created_at       TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS invoice_items (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_id         INTEGER NOT NULL REFERENCES invoices(id),
                product_variant_id INTEGER REFERENCES product_variants(id),
                product_code       TEXT NOT NULL,
                product_name       TEXT NOT NULL,
                pack_size          TEXT NOT NULL,
                quantity           INTEGER NOT NULL,
                unit_price         REAL NOT NULL,
                line_total         REAL NOT NULL,
                sale_id            TEXT
            )""",
            """CREATE TABLE IF NOT EXISTS sales (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                sale_id            TEXT NOT NULL UNIQUE,
                sale_date          TEXT NOT NULL,
                customer_id        INTEGER REFERENCES customers(id),
                cust_code          TEXT NOT NULL,
                cust_name          TEXT NOT NULL,
                customer_type      TEXT DEFAULT 'RETAIL',
                product_variant_id INTEGER REFERENCES product_variants(id),
                product_code       TEXT NOT NULL,
                product_name       TEXT NOT NULL,
                pack_size          TEXT NOT NULL,
                qty                INTEGER NOT NULL,
                unit_price         REAL NOT NULL,
                total              REAL NOT NULL,
                cogs               REAL DEFAULT 0,
                gross_profit       REAL DEFAULT 0,
                invoice_id         INTEGER REFERENCES invoices(id),
                notes              TEXT DEFAULT '',
                created_at         TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS customer_payments (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                payment_ref  TEXT NOT NULL UNIQUE,
                customer_id  INTEGER NOT NULL REFERENCES customers(id),
                payment_date TEXT NOT NULL,
                amount       REAL NOT NULL,
                payment_mode TEXT DEFAULT 'CASH'
                             CHECK(payment_mode IN ('CASH','BANK_TRANSFER','CHEQUE','OTHER')),
                notes        TEXT DEFAULT '',
                created_at   TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS payment_allocations (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                payment_id       INTEGER NOT NULL REFERENCES customer_payments(id),
                invoice_id       INTEGER NOT NULL REFERENCES invoices(id),
                allocated_amount REAL NOT NULL,
                UNIQUE (payment_id, invoice_id)
            )""",
            # ── Purchasing & AP ──────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS supplier_bills (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                bill_number  TEXT NOT NULL UNIQUE,
                supplier_id  INTEGER NOT NULL REFERENCES suppliers(id),
                bill_date    TEXT NOT NULL,
                due_date     TEXT NOT NULL,
                status       TEXT DEFAULT 'UNPAID'
                             CHECK(status IN ('UNPAID','PARTIAL','PAID','VOID')),
                notes        TEXT DEFAULT '',
                total_amount REAL DEFAULT 0,
                supplier_ref TEXT DEFAULT '',
                created_at   TEXT DEFAULT (datetime('now')),
                voided_at    TEXT DEFAULT NULL,
                voided_by    TEXT DEFAULT NULL,
                void_note    TEXT DEFAULT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS supplier_bill_items (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                bill_id       INTEGER NOT NULL REFERENCES supplier_bills(id),
                ingredient_id INTEGER NOT NULL REFERENCES ingredients(id),
                quantity_kg   REAL NOT NULL,
                unit_cost_kg  REAL NOT NULL,
                line_total    REAL NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS supplier_payments (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                payment_ref  TEXT NOT NULL UNIQUE,
                supplier_id  INTEGER NOT NULL REFERENCES suppliers(id),
                payment_date TEXT NOT NULL,
                amount       REAL NOT NULL,
                payment_mode TEXT DEFAULT 'BANK_TRANSFER'
                             CHECK(payment_mode IN ('CASH','BANK_TRANSFER','CHEQUE','OTHER')),
                notes        TEXT DEFAULT '',
                created_at   TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS supplier_payment_allocations (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                payment_id       INTEGER NOT NULL REFERENCES supplier_payments(id),
                bill_id          INTEGER NOT NULL REFERENCES supplier_bills(id),
                allocated_amount REAL NOT NULL,
                UNIQUE (payment_id, bill_id)
            )""",
            # ── Sales reps & field ops ───────────────────────────────
            """CREATE TABLE IF NOT EXISTS sales_reps (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                employee_id       TEXT UNIQUE NOT NULL,
                name              TEXT NOT NULL,
                phone             TEXT UNIQUE NOT NULL,
                pin_hash          TEXT NOT NULL,
                cnic              TEXT DEFAULT '',
                address           TEXT DEFAULT '',
                emergency_contact TEXT DEFAULT '',
                designation       TEXT NOT NULL DEFAULT 'SR',
                joining_date      TEXT DEFAULT '',
                reporting_to      INTEGER,
                primary_zone_id   INTEGER,
                status            TEXT NOT NULL DEFAULT 'active',
                pin_attempts      INTEGER NOT NULL DEFAULT 0,
                pin_locked        INTEGER NOT NULL DEFAULT 0,
                last_field_login  TEXT DEFAULT '',
                email             TEXT DEFAULT '',
                notes             TEXT DEFAULT '',
                whatsapp_apikey   TEXT DEFAULT '',
                created_at        TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS routes (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                zone_id    INTEGER NOT NULL,
                name       TEXT NOT NULL,
                visit_days TEXT DEFAULT '',
                active     INTEGER NOT NULL DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS rep_routes (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id        INTEGER NOT NULL,
                route_id      INTEGER NOT NULL,
                assigned_from TEXT NOT NULL,
                assigned_to   TEXT DEFAULT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS route_customers (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                route_id      INTEGER NOT NULL,
                customer_id   INTEGER NOT NULL,
                stop_sequence INTEGER NOT NULL DEFAULT 0,
                UNIQUE(route_id, customer_id)
            )""",
            """CREATE TABLE IF NOT EXISTS beat_visits (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id            INTEGER NOT NULL,
                customer_id       INTEGER NOT NULL,
                route_id          INTEGER NOT NULL,
                visit_date        TEXT NOT NULL,
                outcome           TEXT NOT NULL DEFAULT 'visited',
                payment_collected REAL NOT NULL DEFAULT 0,
                notes             TEXT DEFAULT '',
                created_at        TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS field_orders (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                order_ref           TEXT UNIQUE NOT NULL,
                rep_id              INTEGER NOT NULL,
                customer_id         INTEGER NOT NULL,
                visit_id            INTEGER,
                order_date          TEXT NOT NULL,
                status              TEXT NOT NULL DEFAULT 'pending',
                notes               TEXT DEFAULT '',
                invoice_id          INTEGER,
                route_id            INTEGER,
                cash_collected      REAL DEFAULT 0,
                confirmed_invoice_id INTEGER,
                created_at          TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS field_order_items (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id           INTEGER NOT NULL,
                product_variant_id INTEGER NOT NULL,
                quantity           INTEGER NOT NULL DEFAULT 0,
                unit_price         REAL NOT NULL DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS rep_salary_components (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id           INTEGER NOT NULL,
                basic_salary     REAL NOT NULL DEFAULT 0,
                fuel_allowance   REAL NOT NULL DEFAULT 0,
                mobile_allowance REAL NOT NULL DEFAULT 0,
                other_allowance  REAL NOT NULL DEFAULT 0,
                effective_from   TEXT NOT NULL,
                active           INTEGER NOT NULL DEFAULT 1
            )""",
            """CREATE TABLE IF NOT EXISTS rep_commission_rules (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id                INTEGER NOT NULL,
                base_commission_pct   REAL NOT NULL DEFAULT 0,
                accelerator_pct       REAL NOT NULL DEFAULT 0,
                target_bonus          REAL NOT NULL DEFAULT 0,
                effective_from        TEXT NOT NULL,
                active                INTEGER NOT NULL DEFAULT 1
            )""",
            """CREATE TABLE IF NOT EXISTS rep_targets (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id         INTEGER NOT NULL,
                month          TEXT NOT NULL,
                visit_target   INTEGER NOT NULL DEFAULT 0,
                revenue_target REAL NOT NULL DEFAULT 0,
                UNIQUE(rep_id, month)
            )""",
            """CREATE TABLE IF NOT EXISTS rep_advances (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id           INTEGER NOT NULL,
                advance_date     TEXT NOT NULL,
                amount           REAL NOT NULL,
                monthly_recovery REAL NOT NULL DEFAULT 0,
                outstanding      REAL NOT NULL,
                notes            TEXT DEFAULT '',
                approved_by      TEXT DEFAULT '',
                recovered        INTEGER DEFAULT 0,
                created_at       TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS rep_attendance (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id   INTEGER NOT NULL,
                att_date TEXT NOT NULL,
                status   TEXT NOT NULL DEFAULT 'present',
                notes    TEXT DEFAULT '',
                UNIQUE(rep_id, att_date)
            )""",
            """CREATE TABLE IF NOT EXISTS payroll_runs (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                rep_id                INTEGER NOT NULL,
                month                 TEXT NOT NULL,
                basic_salary          REAL NOT NULL DEFAULT 0,
                fuel_allowance        REAL NOT NULL DEFAULT 0,
                mobile_allowance      REAL NOT NULL DEFAULT 0,
                other_allowance       REAL NOT NULL DEFAULT 0,
                commission            REAL NOT NULL DEFAULT 0,
                accelerator_commission REAL NOT NULL DEFAULT 0,
                target_bonus          REAL NOT NULL DEFAULT 0,
                gross_pay             REAL NOT NULL DEFAULT 0,
                advance_recovery      REAL NOT NULL DEFAULT 0,
                absent_deduction      REAL NOT NULL DEFAULT 0,
                other_deductions      REAL NOT NULL DEFAULT 0,
                net_pay               REAL NOT NULL DEFAULT 0,
                sales_achieved        REAL NOT NULL DEFAULT 0,
                visits_done           INTEGER NOT NULL DEFAULT 0,
                status                TEXT NOT NULL DEFAULT 'draft',
                notes                 TEXT DEFAULT '',
                period                TEXT DEFAULT '',
                base_salary           REAL DEFAULT 0,
                actual_sales          REAL DEFAULT 0,
                target_amount         REAL DEFAULT 0,
                base_commission       REAL DEFAULT 0,
                accelerator_bonus     REAL DEFAULT 0,
                total_commission      REAL DEFAULT 0,
                total_advances        REAL DEFAULT 0,
                run_at                TEXT,
                created_at            TEXT DEFAULT (datetime('now')),
                UNIQUE(rep_id, month)
            )""",
            # ── Audit ────────────────────────────────────────────────
            """CREATE TABLE IF NOT EXISTS change_log (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                record_id  TEXT NOT NULL,
                action     TEXT NOT NULL CHECK(action IN ('INSERT','UPDATE','DELETE','VOID')),
                old_value  TEXT DEFAULT NULL,
                new_value  TEXT DEFAULT NULL,
                changed_by TEXT DEFAULT 'system',
                timestamp  TEXT DEFAULT (datetime('now'))
            )""",
        ]

        for sql in stmts:
            c.execute(sql)
        c.commit()

        # Seed id_counters rows
        for entity in ('work_order', 'customer_order', 'purchase_order', 'sku', 'ingredient'):
            exists = c.execute("SELECT 1 FROM id_counters WHERE entity=?", (entity,)).fetchone()
            if not exists:
                c.execute("INSERT INTO id_counters (entity, last_num) VALUES (?,0)", (entity,))
        c.commit()

        # Seed price_types (required for pricing and production cost features)
        for code, label in [
            ('mfg_cost',    'Manufacturing Cost'),
            ('ex_factory',  'Ex-Factory Price'),
            ('distributor', 'Distributor Price'),
            ('retail_mrp',  'Retail MRP'),
        ]:
            c.execute("INSERT OR IGNORE INTO price_types (code, label) VALUES (?,?)", (code, label))
        c.commit()

        print("  ✓ Full schema: all tables verified / created")
    finally:
        c.close()
    save_db()


def restore_db_interactive():
    """
    CLI restore mode — run with: python server.py --restore
    Lists available backups and lets the user pick one to restore.
    """
    resolve_db_path()
    backup_dir = DB_SRC.parent / 'spicetopia_backups'
    if not backup_dir.exists():
        print()
        print('  ✗ No backup folder found. No backups have been made yet.')
        print()
        return

    backups = sorted(backup_dir.glob(f"{DB_SRC.stem}_*.db"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not backups:
        print()
        print('  ✗ No backups found in spicetopia_backups/.')
        print()
        return

    print()
    print('╔══════════════════════════════════════════════════════════╗')
    print('║           SPICETOPIA BMS — RESTORE DATABASE              ║')
    print('╚══════════════════════════════════════════════════════════╝')
    print()
    print('  Available restore points:')
    print()
    for i, b in enumerate(backups, 1):
        size_kb = b.stat().st_size // 1024
        mtime   = datetime.fromtimestamp(b.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
        print(f'    [{i}]  {mtime}   ({size_kb} KB)   {b.name}')
    print()
    print('    [0]  Cancel — do not restore anything')
    print()

    try:
        choice = input('  Enter number to restore: ').strip()
        choice = int(choice)
    except (ValueError, KeyboardInterrupt):
        print('\n  Cancelled.')
        return

    if choice == 0:
        print('\n  Cancelled. Nothing was changed.')
        return

    if choice < 1 or choice > len(backups):
        print(f'\n  ✗ Invalid choice: {choice}')
        return

    selected = backups[choice - 1]
    mtime    = datetime.fromtimestamp(selected.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')

    print()
    print(f'  You selected: {selected.name}')
    print(f'  Saved at    : {mtime}')
    print()
    confirm = input('  Type YES to confirm restore (this will overwrite your current database): ').strip()
    if confirm != 'YES':
        print('\n  Cancelled. Nothing was changed.')
        return

    # Safety — back up the current live db before overwriting it
    ts           = datetime.now().strftime('%Y%m%d_%H%M%S')
    safety_copy  = backup_dir / f"{DB_SRC.stem}_pre_restore_{ts}.db"
    shutil.copy2(str(DB_SRC), str(safety_copy))
    print(f'\n  ✓ Current database backed up as: {safety_copy.name}')

    shutil.copy2(str(selected), str(DB_SRC))
    print(f'  ✓ Database restored to: {mtime}')
    print()
    print('  You can now start the server normally.')
    print()

def save_db():
    """Copy working DB back to persistent storage, keeping a rolling backup of the last 5 versions."""
    # ── Safety check: never write if the working copy is truly empty/corrupt ──
    if not DB_TMP.exists() or DB_TMP.stat().st_size < 512:
        print(f"  ✗ save_db aborted — working copy is empty or missing: {DB_TMP}")
        return

    # ── 1. Rotate backups before overwriting ─────────────────────
    if DB_SRC.exists() and DB_SRC.stat().st_size >= 512:
        # Only back up if the current source is actually valid (skip backing up an empty file)
        backup_dir = DB_SRC.parent / 'spicetopia_backups'
        backup_dir.mkdir(exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = backup_dir / f"{DB_SRC.stem}_{ts}.db"
        shutil.copy2(str(DB_SRC), str(backup_path))
        # Prune old backups — keep only the MAX_BACKUPS most recent
        backups = sorted(backup_dir.glob(f"{DB_SRC.stem}_*.db"), key=lambda p: p.stat().st_mtime)
        for old in backups[:-MAX_BACKUPS]:
            try:
                old.unlink()
            except Exception:
                pass
    # ── 2. Save current working copy ─────────────────────────────
    shutil.copy2(str(DB_TMP), str(DB_SRC))


# ═══════════════════════════════════════════════════════════════════
#  AUTOMATED BACKUP  (cloud-ready: SQLite online backup API)
# ═══════════════════════════════════════════════════════════════════

def run_backup() -> dict:
    """
    Create a timestamped SQLite backup in BACKUP_PATH using the SQLite
    online backup API — safe to run while the server is serving requests.
    Prunes files older than BACKUP_KEEP_DAYS.
    Returns {'path': str, 'size_kb': int, 'ts': str} on success.
    """
    if BACKUP_PATH is None:
        raise RuntimeError("BACKUP_PATH not configured — call _apply_startup_config() first")
    if not DB_TMP.exists() or DB_TMP.stat().st_size < 512:
        raise RuntimeError(f"Working DB missing or too small: {DB_TMP}")

    BACKUP_PATH.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now().strftime('%Y%m%d_%H%M%S')
    dest = BACKUP_PATH / f"spicetopia_{ts}.db"

    # SQLite online backup — no write lock needed on the source
    src_conn = sqlite3.connect(str(DB_TMP))
    dst_conn = sqlite3.connect(str(dest))
    try:
        src_conn.backup(dst_conn)
    finally:
        dst_conn.close()
        src_conn.close()

    # Prune backups older than BACKUP_KEEP_DAYS
    cutoff = datetime.now() - timedelta(days=BACKUP_KEEP_DAYS)
    for f in BACKUP_PATH.glob("spicetopia_*.db"):
        try:
            if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
                f.unlink()
        except Exception:
            pass

    size_kb = dest.stat().st_size // 1024
    _log('info', 'backup_created', path=str(dest), size_kb=size_kb)
    print(f"  ✓ Backup created: {dest.name} ({size_kb} KB)")
    return {'path': str(dest), 'filename': dest.name, 'size_kb': size_kb, 'ts': ts}


def _backup_thread():
    """
    Background thread — runs one backup every 24 hours.
    First backup fires 60 seconds after startup (to ensure DB is ready),
    then every 24 hours thereafter.
    """
    time.sleep(60)
    while True:
        try:
            run_backup()
        except Exception as e:
            _log('warning', f'Scheduled backup failed: {e}')
        time.sleep(86400)   # 24 hours


# ═══════════════════════════════════════════════════════════════════
#  ONEDRIVE LOCK FILE  — prevents two users running simultaneously
# ═══════════════════════════════════════════════════════════════════

LOCK_STALE_SECS   = 300   # lock older than 5 minutes is considered stale (crashed session)
LOCK_HEARTBEAT_SECS = 120  # update lock file every 2 minutes to stay fresh

def _lock_path():
    """Lock file lives next to the .db file on OneDrive."""
    return DB_SRC.parent / 'spicetopia.lock'

def _lock_info():
    """Return info to write into the lock file."""
    import socket as _socket
    return {
        'user':       os.environ.get('USERNAME') or os.environ.get('USER') or 'Unknown User',
        'hostname':   _socket.gethostname(),
        'started_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'pid':        os.getpid(),
        'heartbeat':  datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }

def acquire_db_lock():
    """
    Check for an existing lock file on OneDrive.
    If another user holds a fresh lock → print a clear message and exit.
    If no lock or lock is stale → write our own lock and continue.
    On Railway/cloud: lock is skipped entirely (single-instance, no OneDrive sharing).
    """
    # Cloud deployment — locking is meaningless and causes crash loops on redeploy
    if os.environ.get('RAILWAY_VOLUME_MOUNT_PATH') or (not sys.stdin.isatty() and OS == 'Linux'):
        print('  ✓ Cloud mode — DB lock skipped (single-instance deployment)')
        return

    lock = _lock_path()

    if lock.exists():
        try:
            with open(lock, 'r') as f:
                existing = json.load(f)

            # Check freshness by heartbeat timestamp
            heartbeat_str = existing.get('heartbeat') or existing.get('started_at', '')
            try:
                heartbeat_dt = datetime.strptime(heartbeat_str, '%Y-%m-%d %H:%M:%S')
                age_secs = (datetime.now() - heartbeat_dt).total_seconds()
            except Exception:
                age_secs = 9999  # unparseable → treat as stale

            if age_secs < LOCK_STALE_SECS:
                import socket as _socket
                lock_user     = existing.get('user', '')
                lock_hostname = existing.get('hostname', '')
                this_user     = os.environ.get('USERNAME') or os.environ.get('USER') or ''
                this_hostname = _socket.gethostname()

                # Same machine — this is a leftover lock from a previous crash on this PC
                if lock_hostname == this_hostname and lock_user == this_user:
                    print('  ⚠ Found a lock from a previous session on this machine — overriding automatically.')
                else:
                    # Different machine — genuinely in use by someone else
                    user    = lock_user    or 'Someone'
                    hostname= lock_hostname or 'unknown machine'
                    started = existing.get('started_at', 'unknown time')
                    print()
                    print('  ╔══════════════════════════════════════════════════════════╗')
                    print('  ║           ⛔  DATABASE IS CURRENTLY IN USE               ║')
                    print('  ╠══════════════════════════════════════════════════════════╣')
                    print(f'  ║  User     : {user:<46}║')
                    print(f'  ║  Machine  : {hostname:<46}║')
                    print(f'  ║  Since    : {started:<46}║')
                    print('  ╠══════════════════════════════════════════════════════════╣')
                    print('  ║  Please contact them before starting the server.         ║')
                    print('  ║  Only one person can run Spicetopia at a time.           ║')
                    print('  ╚══════════════════════════════════════════════════════════╝')
                    print()
                    raise SystemExit(1)
            else:
                print(f'  ⚠ Found stale lock file (last heartbeat {int(age_secs)}s ago) — overriding.')

        except (json.JSONDecodeError, KeyError):
            print('  ⚠ Found unreadable lock file — overriding.')

    # Write our own lock
    info = _lock_info()
    try:
        with open(lock, 'w') as f:
            json.dump(info, f, indent=2)
        print(f'  ✓ Database lock acquired ({info["user"]} @ {info["hostname"]})')
    except Exception as e:
        print(f'  ⚠ Could not write lock file: {e} (continuing without lock)')

def release_db_lock():
    """Remove the lock file when the server shuts down."""
    if os.environ.get('RAILWAY_VOLUME_MOUNT_PATH') or (not sys.stdin.isatty() and OS == 'Linux'):
        return  # no lock was acquired in cloud mode
    lock = _lock_path()
    try:
        if lock.exists():
            # Only remove if it's ours (same PID)
            with open(lock, 'r') as f:
                existing = json.load(f)
            if existing.get('pid') == os.getpid():
                lock.unlink()
                print('  ✓ Database lock released.')
    except Exception:
        pass

def _heartbeat_lock():
    """Background thread — updates heartbeat in lock file every 2 minutes."""
    if os.environ.get('RAILWAY_VOLUME_MOUNT_PATH') or (not sys.stdin.isatty() and OS == 'Linux'):
        return  # no lock to heartbeat in cloud mode
    lock = _lock_path()
    while True:
        time.sleep(LOCK_HEARTBEAT_SECS)
        try:
            if lock.exists():
                with open(lock, 'r') as f:
                    data = json.load(f)
                if data.get('pid') == os.getpid():
                    data['heartbeat'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    with open(lock, 'w') as f:
                        json.dump(data, f, indent=2)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════
#  DB HELPERS
# ═══════════════════════════════════════════════════════════════════

def _conn():
    # timeout=30: SQLite will retry for up to 30 s on SQLITE_BUSY instead of
    # immediately raising "database is locked".  Needed because ThreadingHTTPServer
    # spawns one thread per request, so concurrent write requests can collide.
    c = sqlite3.connect(str(DB_TMP), timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    return c

def qry(sql, params=()):
    c = _conn()
    try:
        return [dict(r) for r in c.execute(sql, params).fetchall()]
    finally:
        c.close()

def qry1(sql, params=()):
    rows = qry(sql, params)
    return rows[0] if rows else None

def run(sql, params=()):
    c = _conn()
    try:
        c.execute(sql, params)
        c.commit()
    finally:
        c.close()
    save_db()

def run_many(ops):
    """ops = list of (sql, params). All in one transaction."""
    c = _conn()
    try:
        for sql, params in ops:
            c.execute(sql, params)
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()

def r2(n):
    try:    return round(float(n or 0), 2)
    except: return 0.0

def fmtpkr(n):
    """Format a number as PKR string for server-side warning messages."""
    try:    return f"PKR {float(n or 0):,.0f}"
    except: return "PKR 0"

def today():
    return date.today().isoformat()


# ═══════════════════════════════════════════════════════════════════
#  RBAC — ROLES AND PERMISSION HELPER
# ═══════════════════════════════════════════════════════════════════

VALID_ROLES = ('admin', 'sales', 'warehouse', 'accountant', 'field_rep', 'user')

# Role hierarchy for display / UI
ROLE_LABELS = {
    'admin':       'Administrator',
    'sales':       'Sales',
    'warehouse':   'Warehouse',
    'accountant':  'Accountant',
    'field_rep':   'Sales Rep',
    'user':        'Viewer (read-only)',
}

def require(sess, *roles):
    """Return True iff the session's role is one of the given roles.
    Always False if sess is None.  Usage:
        if not require(sess, 'admin', 'sales'):
            send_error(self, 'Permission denied', 403); return
    """
    return bool(sess and sess.get('role') in roles)


# ═══════════════════════════════════════════════════════════════════
#  INPUT VALIDATION
# ═══════════════════════════════════════════════════════════════════

class ValidationError(Exception):
    """Raised when user input fails field-level validation.
    Carries a dict of {field_name: error_message} pairs so the caller
    can return a 422 response with per-field context.
    """
    def __init__(self, errors: dict):
        self.errors = errors
        super().__init__(json.dumps({'validationErrors': errors}))


def validate_fields(data: dict, rules: list):
    """
    Validate a data dict against a list of field rules.
    Each rule is a dict with:
      field    (str)   — JSON key in data
      label    (str)   — human-readable label for error messages (default: field)
      required (bool)  — if True and field missing/blank → error  (default: True)
      type     (str)   — 'str' | 'int' | 'float' | 'date'        (default: 'str')
      min      (num)   — minimum value (numeric) or min length (str)
      max      (num)   — maximum value (numeric) or max length (str)
      choices  (list)  — allowed values list (str comparison)

    Raises ValidationError if any rule fails.
    """
    errors = {}
    for rule in rules:
        field    = rule['field']
        label    = rule.get('label', field)
        required = rule.get('required', True)
        typ      = rule.get('type', 'str')
        val      = data.get(field)
        raw_str  = str(val).strip() if val is not None else ''

        # Required / blank check
        if required and not raw_str:
            errors[field] = f"{label} is required"
            continue
        if not raw_str:
            continue  # Optional and not provided — skip further checks

        # Type coercion + range checks
        if typ == 'int':
            try:
                val = int(val)
            except (TypeError, ValueError):
                errors[field] = f"{label} must be a whole number"
                continue
            if 'min' in rule and val < rule['min']:
                errors[field] = f"{label} must be at least {rule['min']}"
            elif 'max' in rule and val > rule['max']:
                errors[field] = f"{label} must be {rule['max']} or less"

        elif typ == 'float':
            try:
                val = float(str(val).replace(',', ''))
            except (TypeError, ValueError):
                errors[field] = f"{label} must be a number"
                continue
            if 'min' in rule and val < rule['min']:
                errors[field] = f"{label} must be {rule['min']} or more"
            elif 'max' in rule and val > rule['max']:
                errors[field] = f"{label} must be {rule['max']} or less"

        elif typ == 'date':
            try:
                date.fromisoformat(raw_str)
            except ValueError:
                errors[field] = f"{label} must be a valid date (YYYY-MM-DD)"

        else:  # 'str'
            if 'min' in rule and len(raw_str) < rule['min']:
                errors[field] = f"{label} must be at least {rule['min']} character(s)"
            elif 'max' in rule and len(raw_str) > rule['max']:
                errors[field] = f"{label} must be {rule['max']} characters or fewer"
            if 'choices' in rule and raw_str not in rule['choices']:
                errors[field] = f"{label} must be one of: {', '.join(rule['choices'])}"

    if errors:
        raise ValidationError(errors)


# ═══════════════════════════════════════════════════════════════════
#  ID GENERATION
# ═══════════════════════════════════════════════════════════════════

def _sync_counter_to_max(entity, table, code_col, full_prefix):
    """Ensure id_counters[entity] >= the highest number already used in table.code_col.
    Prevents UNIQUE constraint failures when the counter was freshly seeded at 0
    but rows with higher numbers already exist (e.g. after a Railway redeploy).
    full_prefix: the full code prefix string, e.g. 'SP-SUP-' or 'SP-SP-CUST-'."""
    c = _conn()
    try:
        rows = c.execute(f"SELECT {code_col} FROM {table}").fetchall()
        max_num = 0
        for (code,) in rows:
            if code and code.startswith(full_prefix):
                try:
                    max_num = max(max_num, int(code[len(full_prefix):]))
                except ValueError:
                    pass
        # Seed the row if missing, then bump if behind
        c.execute("INSERT OR IGNORE INTO id_counters (entity, last_num) VALUES (?,?)",
                  (entity, max_num))
        c.execute(
            "UPDATE id_counters SET last_num=? WHERE entity=? AND last_num<?",
            (max_num, entity, max_num)
        )
        c.commit()
    finally:
        c.close()


def next_id(entity, prefix, width=4, conn=None):
    """Atomically increment counter and return formatted SP-PREFIX-XXXX.
    If conn is provided, reuse it (no separate commit/close) so that callers
    inside an open write transaction don't open a second conflicting connection.
    """
    owned = conn is None
    c = _conn() if owned else conn
    try:
        c.execute("INSERT OR IGNORE INTO id_counters (entity, last_num) VALUES (?,0)", (entity,))
        c.execute("UPDATE id_counters SET last_num=last_num+1 WHERE entity=?", (entity,))
        num = c.execute("SELECT last_num FROM id_counters WHERE entity=?", (entity,)).fetchone()[0]
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()
    if owned:
        save_db()
    return f"SP-{prefix}-{num:0{width}d}"


def _sync_ingredient_counter(c=None):
    """Sync id_counters[ingredient] to the highest ING-NNNSP number already in the DB.
    Accepts an open connection or opens its own. Always safe to call multiple times."""
    owned = c is None
    if owned:
        c = _conn()
    try:
        rows = c.execute("SELECT code FROM ingredients").fetchall()
        max_num = 0
        for (code,) in rows:
            if code and code.startswith('ING-') and code.endswith('SP'):
                try:
                    max_num = max(max_num, int(code[4:-2]))
                except ValueError:
                    pass
        c.execute("INSERT OR IGNORE INTO id_counters (entity, last_num) VALUES ('ingredient', ?)",
                  (max_num,))
        c.execute(
            "UPDATE id_counters SET last_num=? WHERE entity='ingredient' AND last_num<?",
            (max_num, max_num)
        )
        if owned:
            c.commit()
    finally:
        if owned:
            c.close()


def next_ingredient_code():
    """Generate next coded ingredient ID: ING-001SP, ING-002SP, ...
    The 'SP' suffix ties the code to Spicetopia without revealing what the ingredient is.
    The real ingredient name is kept in a physical, off-system legend only."""
    c = _conn()
    try:
        _sync_ingredient_counter(c)
        c.execute("UPDATE id_counters SET last_num=last_num+1 WHERE entity='ingredient'")
        num = c.execute("SELECT last_num FROM id_counters WHERE entity='ingredient'").fetchone()[0]
        c.commit()
    finally:
        c.close()
    save_db()
    return f"ING-{num:03d}SP"


CITY_CODE_MAP = {
    'karachi':    'KHI',
    'hyderabad':  'HYD',
    'lahore':     'LHR',
    'islamabad':  'ISB',
    'peshawar':   'PSH',
    'quetta':     'QTA',
    'rawalpindi': 'RWP',
    'multan':     'MUL',
    'faisalabad': 'FSD',
    'dubai':      'DXB',
    'abu dhabi':  'AUH',
    'sharjah':    'SHJ',
}


def _city_to_code(city_str):
    """Normalize city name to 3-letter code for account number prefix."""
    key = (city_str or '').strip().lower()
    if key in CITY_CODE_MAP:
        return CITY_CODE_MAP[key]
    # Fallback: first 3 alpha chars, uppercase
    clean = ''.join(ch for ch in key if ch.isalpha())
    return (clean[:3].upper() if len(clean) >= 3 else clean.upper().ljust(3, 'X'))


def generate_account_number(city, customer_type='RETAIL'):
    """Generate {CITY3}-{TYPE}{NNN} account number (e.g. KHI-R001, KHI-D004, ISB-W001).
    Each city+type combination has its own independent counter in id_counters.
    Type codes: RETAIL→R, DIRECT→D, WHOLESALE→W.
    Existing customers with legacy SPKHI-NNN format are unaffected."""
    type_code = {'RETAIL': 'R', 'DIRECT': 'D', 'WHOLESALE': 'W'}.get(
        (customer_type or 'RETAIL').upper(), 'R')
    city3  = _city_to_code(city)
    entity = f'acct_{city3}_{type_code}'
    c = _conn()
    try:
        c.execute("INSERT OR IGNORE INTO id_counters (entity, last_num) VALUES (?,0)", (entity,))
        c.execute("UPDATE id_counters SET last_num=last_num+1 WHERE entity=?", (entity,))
        num = c.execute("SELECT last_num FROM id_counters WHERE entity=?", (entity,)).fetchone()[0]
        c.commit()
    finally:
        c.close()
    save_db()
    return f"{city3}-{type_code}{num:03d}"


def backfill_customer_account_numbers():
    """
    One-time startup task (idempotent):
      1. Delete test customers (SP-CUST-0001 … SP-CUST-0010) if they have no FK deps.
      2. Assign account_number to real customers that still have NULL.
    """
    c = _conn()
    try:
        # ── Delete test customers ─────────────────────────────────
        test_codes = [f'SP-CUST-{n:04d}' for n in range(1, 11)]
        for code in test_codes:
            row = c.execute("SELECT id FROM customers WHERE code=?", (code,)).fetchone()
            if not row:
                continue
            cid = row[0]
            has_deps = (
                c.execute("SELECT 1 FROM customer_orders   WHERE customer_id=? LIMIT 1", (cid,)).fetchone() or
                c.execute("SELECT 1 FROM invoices          WHERE customer_id=? LIMIT 1", (cid,)).fetchone() or
                c.execute("SELECT 1 FROM customer_payments WHERE customer_id=? LIMIT 1", (cid,)).fetchone() or
                c.execute("SELECT 1 FROM sales             WHERE customer_id=? LIMIT 1", (cid,)).fetchone()
            )
            if not has_deps:
                c.execute("DELETE FROM customers WHERE id=?", (cid,))
                print(f"  ✓ Backfill: deleted test customer {code}")
            else:
                print(f"  ⚠ Backfill: {code} has data, skipped deletion")

        # ── Assign account_number to real customers without one ───
        unassigned = c.execute(
            "SELECT id, name, city, customer_type FROM customers WHERE account_number IS NULL"
        ).fetchall()
        for (cid, name, city, ctype) in unassigned:
            city3     = _city_to_code(city or '')
            type_code = {'RETAIL': 'R', 'DIRECT': 'D', 'WHOLESALE': 'W'}.get((ctype or 'RETAIL').upper(), 'R')
            entity    = f'acct_{city3}_{type_code}'
            c.execute("INSERT OR IGNORE INTO id_counters (entity, last_num) VALUES (?,0)", (entity,))
            c.execute("UPDATE id_counters SET last_num=last_num+1 WHERE entity=?", (entity,))
            num = c.execute("SELECT last_num FROM id_counters WHERE entity=?", (entity,)).fetchone()[0]
            acc_num = f"{city3}-{type_code}{num:03d}"
            c.execute("UPDATE customers SET account_number=? WHERE id=?", (acc_num, cid))
            print(f"  ✓ Backfill: assigned {acc_num} to '{name}' (id={cid})")

        c.commit()
    finally:
        c.close()
    save_db()


def next_blend_code(prefix: str) -> str:
    """Generate next blend code for a given product-category prefix.
    Format: {PREFIX}-BC-{NNN}  e.g. GM-BC-001, CM-BC-001, RCP-BC-001
    Each prefix has its own independent counter so series don't clash.
    The code carries no ingredient or ratio information — IP safe."""
    prefix = prefix.strip().upper()
    if not prefix:
        raise ValueError("Blend code prefix is required (e.g. GM, CM, RCP)")
    entity = f"blend_{prefix}"
    c = _conn()
    try:
        # Auto-create counter row for new prefix on first use
        c.execute("INSERT OR IGNORE INTO id_counters (entity, last_num) VALUES (?,0)", (entity,))
        c.execute("UPDATE id_counters SET last_num=last_num+1 WHERE entity=?", (entity,))
        num = c.execute("SELECT last_num FROM id_counters WHERE entity=?", (entity,)).fetchone()[0]
        c.commit()
    finally:
        c.close()
    save_db()
    return f"{prefix}-BC-{num:03d}"


def peek_next_ingredient_code():
    """Return what the NEXT ingredient code would be, without incrementing the counter."""
    _sync_ingredient_counter()  # ensure counter reflects any existing codes in DB
    row = qry1("SELECT last_num FROM id_counters WHERE entity='ingredient'", ())
    num = ((row['last_num'] if row else 0) + 1)
    return f"ING-{num:03d}SP"


def peek_next_blend_code(prefix: str) -> str:
    """Return what the NEXT blend code would be for a prefix, without incrementing."""
    prefix = prefix.strip().upper()
    entity = f"blend_{prefix}"
    row = qry1("SELECT last_num FROM id_counters WHERE entity=?", (entity,))
    num = ((row['last_num'] if row else 0) + 1)
    return f"{prefix}-BC-{num:03d}"


# ═══════════════════════════════════════════════════════════════════
#  AUDIT TRAIL
# ═══════════════════════════════════════════════════════════════════

def audit_log(ops, table, record_id, action, old_val=None, new_val=None):
    """Append an audit entry to the ops list (for run_many transactions)."""
    ops.append((
        """INSERT INTO change_log (table_name, record_id, action, old_value, new_value)
           VALUES (?,?,?,?,?)""",
        (table, str(record_id), action,
         json.dumps(old_val) if old_val else None,
         json.dumps(new_val) if new_val else None)
    ))


# ═══════════════════════════════════════════════════════════════════
#  USER MANAGEMENT + AUTH
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
                salt    = ''   # Argon2id embeds its own salt in the hash string
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


def ensure_work_orders_table():
    """Create work_orders table if not exists. Also seed id_counters row."""
    c = _conn()
    try:
        # Ensure id_counters table exists (needed for fresh databases)
        c.execute("""
            CREATE TABLE IF NOT EXISTS id_counters (
                entity    TEXT PRIMARY KEY,
                last_num  INTEGER NOT NULL DEFAULT 0
            )
        """)
        c.commit()
        c.execute("""
            CREATE TABLE IF NOT EXISTS work_orders (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                wo_number           TEXT    NOT NULL UNIQUE,
                product_variant_id  INTEGER NOT NULL,
                qty_units           INTEGER NOT NULL,
                target_date         TEXT,
                status              TEXT    NOT NULL DEFAULT 'planned',
                notes               TEXT    DEFAULT '',
                feasibility_ok      INTEGER DEFAULT 0,
                batch_id            TEXT    DEFAULT NULL,
                created_at          TEXT    DEFAULT (datetime('now')),
                updated_at          TEXT    DEFAULT (datetime('now')),
                FOREIGN KEY (product_variant_id) REFERENCES product_variants(id)
            )
        """)
        # Ensure id_counters has a row for work_order
        existing = c.execute("SELECT 1 FROM id_counters WHERE entity='work_order'").fetchone()
        if not existing:
            c.execute("INSERT INTO id_counters (entity, last_num) VALUES ('work_order', 0)")
        c.commit()
        print("  ✓ Work Orders: table ready")
    finally:
        c.close()
    save_db()


def ensure_customer_orders_schema():
    """Create customer_orders + customer_order_items tables and add FK columns to work_orders/invoices."""
    c = _conn()
    try:
        # ── New tables ────────────────────────────────────────────
        c.execute("""
            CREATE TABLE IF NOT EXISTS customer_orders (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                order_number    TEXT    NOT NULL UNIQUE,
                customer_id     INTEGER NOT NULL REFERENCES customers(id),
                order_date      TEXT    NOT NULL,
                required_date   TEXT,
                status          TEXT    NOT NULL DEFAULT 'draft',
                notes           TEXT    DEFAULT '',
                created_at      TEXT    DEFAULT (datetime('now')),
                updated_at      TEXT    DEFAULT (datetime('now'))
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS customer_order_items (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id            INTEGER NOT NULL REFERENCES customer_orders(id),
                product_variant_id  INTEGER NOT NULL REFERENCES product_variants(id),
                qty_ordered         INTEGER NOT NULL,
                unit_price          REAL    NOT NULL DEFAULT 0,
                line_total          REAL    NOT NULL DEFAULT 0,
                qty_in_production   INTEGER NOT NULL DEFAULT 0,
                qty_invoiced        INTEGER NOT NULL DEFAULT 0
            )
        """)

        # ── Add FK columns to existing tables (idempotent) ───────
        for sql in [
            "ALTER TABLE work_orders    ADD COLUMN customer_order_id      INTEGER REFERENCES customer_orders(id)",
            "ALTER TABLE work_orders    ADD COLUMN customer_order_item_id  INTEGER REFERENCES customer_order_items(id)",
            "ALTER TABLE invoices       ADD COLUMN customer_order_id      INTEGER REFERENCES customer_orders(id)",
            "ALTER TABLE invoice_items  ADD COLUMN sale_id TEXT",
        ]:
            try:
                c.execute(sql)
            except Exception:
                pass   # column already exists

        # ── Seed id_counters ──────────────────────────────────────
        existing = c.execute("SELECT 1 FROM id_counters WHERE entity='customer_order'").fetchone()
        if not existing:
            c.execute("INSERT INTO id_counters (entity, last_num) VALUES ('customer_order', 0)")

        c.commit()
        print("  ✓ Customer Orders: schema ready")
    finally:
        c.close()
    save_db()


def ensure_review_queue_schema():
    """
    Phase 3 — Review Queue & Soft Hold schema (idempotent).
    Adds order_source, approval columns to customer_orders;
    qty_soft_hold to customer_order_items;
    creates order_hold_expiry and order_approval_rules tables.
    """
    c = _conn()
    try:
        # ── New columns on customer_orders ────────────────────────
        for sql in [
            "ALTER TABLE customer_orders ADD COLUMN order_source        TEXT    DEFAULT 'internal'",
            "ALTER TABLE customer_orders ADD COLUMN approval_method     TEXT    DEFAULT 'manual'",
            "ALTER TABLE customer_orders ADD COLUMN approval_timestamp  TEXT    DEFAULT NULL",
            "ALTER TABLE customer_orders ADD COLUMN approval_note       TEXT    DEFAULT ''",
            "ALTER TABLE customer_orders ADD COLUMN rejection_reason    TEXT    DEFAULT ''",
        ]:
            try:
                c.execute(sql); c.commit()
            except Exception:
                pass   # column already exists

        # ── Soft hold quantity on order items ─────────────────────
        try:
            c.execute("ALTER TABLE customer_order_items ADD COLUMN qty_soft_hold INTEGER DEFAULT 0")
            c.commit()
        except Exception:
            pass   # already exists

        # ── Hold tracking table ───────────────────────────────────
        c.execute("""
            CREATE TABLE IF NOT EXISTS order_hold_expiry (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id          INTEGER NOT NULL UNIQUE REFERENCES customer_orders(id),
                hold_placed_at    TEXT    NOT NULL,
                hold_expires_at   TEXT    NOT NULL,
                is_expired        INTEGER NOT NULL DEFAULT 0,
                notification_sent INTEGER NOT NULL DEFAULT 0,
                expired_at        TEXT    DEFAULT NULL
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_hold_expiry ON order_hold_expiry(hold_expires_at, is_expired)")

        # ── Auto-approval rules registry (infrastructure; all disabled by default) ──
        c.execute("""
            CREATE TABLE IF NOT EXISTS order_approval_rules (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_name   TEXT    NOT NULL UNIQUE,
                rule_code   TEXT    NOT NULL,
                enabled     INTEGER NOT NULL DEFAULT 0,
                created_at  TEXT    DEFAULT (datetime('now')),
                notes       TEXT    DEFAULT ''
            )
        """)

        # ── WhatsApp notification columns (Phase 4 — idempotent) ──────
        for sql in [
            "ALTER TABLE users        ADD COLUMN whatsapp_phone  TEXT DEFAULT ''",
            "ALTER TABLE users        ADD COLUMN whatsapp_apikey TEXT DEFAULT ''",
            "ALTER TABLE sales_reps   ADD COLUMN whatsapp_apikey TEXT DEFAULT ''",
            "ALTER TABLE customer_orders ADD COLUMN created_by_rep_id INTEGER DEFAULT NULL",
            "ALTER TABLE order_hold_expiry ADD COLUMN expiry_warning_sent INTEGER DEFAULT 0",
            "ALTER TABLE ingredients ADD COLUMN unit TEXT DEFAULT 'kg'",
            "ALTER TABLE ingredients ADD COLUMN updated_at TEXT DEFAULT NULL",
            "ALTER TABLE supplier_bills ADD COLUMN po_id INTEGER DEFAULT NULL REFERENCES purchase_orders(id)",
        ]:
            try:
                c.execute(sql); c.commit()
            except Exception:
                pass   # column already exists

        c.commit()
        print("  ✓ Review Queue: schema ready (soft hold, hold expiry, approval rules, WA columns)")
    finally:
        c.close()
    save_db()


def _hash_pw(password, salt):
    """SHA-256 hash — kept for verifying legacy passwords only."""
    return hashlib.sha256((salt + password).encode()).hexdigest()


def _hash_pw_new(password: str) -> tuple[str, str, str]:
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
        # Index for fast expiry queries during cleanup
        c.execute("CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at)")
        c.commit()
        print("  ✓ Sessions: table ready (DB-persisted)")
    finally:
        c.close()


def _get_session_by_token(token: str) -> dict | None:
    """Look up a non-expired session in the DB. Updates last_seen_at."""
    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    row = qry1("SELECT * FROM sessions WHERE token=? AND expires_at > ?", (token, now))
    if not row:
        return None
    # Slide expiry window on activity (keep-alive)
    new_expiry = (datetime.utcnow() + timedelta(hours=SESSION_EXPIRY_HOURS)).strftime('%Y-%m-%dT%H:%M:%S')
    run("UPDATE sessions SET last_seen_at=?, expires_at=? WHERE token=?", (now, new_expiry, token))
    sess = {
        'userId':      row['user_id'],
        'username':    row['username'],
        'displayName': row['display_name'],
        'role':        row['role'],
        'permissions': json.loads(row['permissions'] or '[]'),
    }
    # Field reps use userId as their repId (set separately for backwards compatibility)
    if row['role'] == 'field_rep':
        sess['repId'] = row['user_id']
    return sess


def login_user(username, password):
    user = qry1("SELECT * FROM users WHERE username=? AND active=1", (username,))
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
    role    = data.get('role', 'user')
    if role not in VALID_ROLES:
        role = 'user'
    disp    = data.get('displayName', username).strip() or username
    perms   = data.get('permissions', [])
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
#  REFERENCE DATA CACHE
# ═══════════════════════════════════════════════════════════════════

ref = {}   # global cache

def load_ref():
    global ref

    # Guard: if the DB has not been initialised yet (empty file), skip gracefully
    try:
        _tables = {r['name'] for r in qry("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'products' not in _tables:
            print("  ⚠ DB schema not initialised — ref will be empty until restart")
            ref.update({
                'products': [], 'variants': [], 'customers': [], 'suppliers': [],
                'ingredients': [], 'pack_sizes': [], 'price_types': [],
                'prod_by_code': {}, 'var_by_id': {}, 'var_by_sku': {},
                'cust_by_code': {}, 'ing_by_code': {},
            })
            return
    except Exception as _e:
        print(f"  ⚠ Could not check DB schema: {_e} — ref will be empty")
        return

    products = qry("""
        SELECT p.id, p.code, p.name, p.name_urdu, p.blend_code, p.active
        FROM products p WHERE p.active=1 ORDER BY p.code
    """)

    variants = qry("""
        SELECT pv.id, pv.sku_code, pv.product_id, pv.pack_size_id, pv.active_flag,
               pv.gtin,
               p.code as product_code, p.name as product_name,
               ps.label as pack_size, ps.grams as pack_grams
        FROM product_variants pv
        JOIN products p   ON p.id = pv.product_id
        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE pv.active_flag=1 ORDER BY p.code, ps.grams
    """)

    customers = qry("""
        SELECT * FROM customers WHERE COALESCE(active,1)=1 ORDER BY name
    """)

    suppliers = qry("""
        SELECT * FROM suppliers WHERE active_flag=1 ORDER BY name
    """)

    try:
        ingredients = qry("SELECT * FROM ingredients WHERE COALESCE(active,1)=1 ORDER BY code")
    except Exception:
        # Fallback: active column not yet migrated — return all
        ingredients = qry("SELECT * FROM ingredients ORDER BY code")

    pack_sizes = qry("SELECT * FROM pack_sizes ORDER BY grams")
    price_types = qry("SELECT * FROM price_types ORDER BY id")

    # Build quick-lookup maps
    prod_by_code = {p['code']: p for p in products}
    var_by_id    = {v['id']: v   for v in variants}
    var_by_sku   = {(v['product_code'], v['pack_size']): v for v in variants}
    cust_by_code = {c['code']: c for c in customers}
    ing_by_code  = {i['code']: i for i in ingredients}

    ref.update({
        'products':     products,
        'variants':     variants,
        'customers':    customers,
        'suppliers':    suppliers,
        'ingredients':  ingredients,
        'pack_sizes':   pack_sizes,
        'price_types':  price_types,
        'prod_by_code': prod_by_code,
        'var_by_id':    var_by_id,
        'var_by_sku':   var_by_sku,
        'cust_by_code': cust_by_code,
        'ing_by_code':  ing_by_code,
    })
    print(f"  ✓ Ref: {len(products)} products / {len(variants)} SKUs / "
          f"{len(customers)} customers / {len(ingredients)} ingredients")


# ═══════════════════════════════════════════════════════════════════
#  INVENTORY QUERIES
# ═══════════════════════════════════════════════════════════════════

def get_stock_map():
    """Return {ingredient_id: current_balance_grams}."""
    rows = qry("""
        SELECT ingredient_id, SUM(qty_grams) as balance
        FROM inventory_ledger GROUP BY ingredient_id
    """)
    return {r['ingredient_id']: r2(r['balance']) for r in rows}


def get_wo_reserved_stock_map(exclude_wo_id: int | None = None) -> dict:
    """
    Return {ingredient_id: grams_reserved} for all planned/in_progress work orders.

    Uses a single CTE JOIN to compute ingredient requirements via the active BOM
    for each open WO without N+1 queries.

    exclude_wo_id: exclude a specific WO from the reservation total — used when
    checking feasibility for that WO itself (it shouldn't reserve against itself).
    """
    exclude_clause = f"AND wo.id != {int(exclude_wo_id)}" if exclude_wo_id else ""
    rows = qry(f"""
        WITH active_boms AS (
            SELECT product_id, MAX(id) AS bom_id
            FROM bom_versions
            WHERE active_flag = 1
            GROUP BY product_id
        )
        SELECT bi.ingredient_id,
               ROUND(SUM(
                   bi.quantity_grams
                   * (wo.qty_units * COALESCE(ps.grams, 0))
                   / NULLIF(bv.batch_size_grams, 0)
               ), 2) AS reserved_grams
        FROM work_orders wo
        JOIN product_variants pv  ON pv.id  = wo.product_variant_id
        LEFT JOIN pack_sizes ps   ON ps.id  = pv.pack_size_id
        JOIN active_boms ab       ON ab.product_id = pv.product_id
        JOIN bom_versions bv      ON bv.id  = ab.bom_id
        JOIN bom_items bi         ON bi.bom_version_id = bv.id
        WHERE wo.status IN ('planned', 'in_progress')
          {exclude_clause}
        GROUP BY bi.ingredient_id
    """)
    return {r['ingredient_id']: r2(r['reserved_grams'] or 0) for r in rows}

def get_finished_stock_map():
    """Return {product_variant_id: qty_units_available}.
    Voided sales (voided=1) are excluded so that voiding an invoice restores
    finished-goods stock without physically recreating a production batch.
    """
    produced = qry("""
        SELECT product_variant_id, SUM(qty_units) as units
        FROM production_batches WHERE product_variant_id IS NOT NULL
        GROUP BY product_variant_id
    """)
    sold = qry("""
        SELECT product_variant_id, SUM(qty) as units
        FROM sales
        WHERE product_variant_id IS NOT NULL
          AND (voided IS NULL OR voided = 0)
        GROUP BY product_variant_id
    """)
    prod_map = {r['product_variant_id']: r2(r['units']) for r in produced}
    sold_map = {r['product_variant_id']: r2(r['units']) for r in sold}
    all_ids  = set(list(prod_map.keys()) + list(sold_map.keys()))
    return {vid: r2(prod_map.get(vid,0) - sold_map.get(vid,0)) for vid in all_ids}


# ═══════════════════════════════════════════════════════════════════
#  STOCK HOLD — SOFT HOLDS FOR REVIEW QUEUE (Phase 3)
# ═══════════════════════════════════════════════════════════════════

def get_soft_hold_qty(product_variant_id) -> float:
    """
    Total qty soft-held by ALL pending_review orders for a given variant.
    This stock is reserved but not confirmed — lower priority than hard reservations.
    """
    row = qry1("""
        SELECT COALESCE(SUM(coi.qty_soft_hold), 0) as held
        FROM customer_order_items coi
        JOIN customer_orders co ON co.id = coi.order_id
        WHERE co.status = 'pending_review'
          AND coi.product_variant_id = ?
    """, (product_variant_id,))
    return r2(row['held']) if row else 0.0


def get_hard_reserved_qty(product_variant_id) -> float:
    """
    Total qty committed to confirmed/invoiced orders (hard reservations).
    = qty_ordered for all items on confirmed, partially_invoiced orders
      minus units already invoiced (which have already left stock).
    """
    row = qry1("""
        SELECT COALESCE(SUM(coi.qty_ordered - coi.qty_invoiced), 0) as reserved
        FROM customer_order_items coi
        JOIN customer_orders co ON co.id = coi.order_id
        WHERE co.status IN ('confirmed', 'partially_invoiced')
          AND coi.product_variant_id = ?
    """, (product_variant_id,))
    return max(0.0, r2(row['reserved'])) if row else 0.0


def get_available_for_soft_hold(product_variant_id) -> float:
    """
    Stock available for a NEW soft hold placement.
    = physical stock − hard reservations − existing soft holds
    Returns max(0, result) — never negative.
    """
    physical      = get_finished_stock_map().get(product_variant_id, 0.0)
    hard_reserved = get_hard_reserved_qty(product_variant_id)
    soft_held     = get_soft_hold_qty(product_variant_id)
    return max(0.0, r2(physical - hard_reserved - soft_held))


def get_stock_situation(product_variant_id) -> dict:
    """
    Full stock breakdown for a variant — used in the review queue to give
    admin context before approving/rejecting an order.
    Returns physical, hard_reserved, soft_held, available_for_hold, and
    active production batch info if stock is short.
    """
    physical      = get_finished_stock_map().get(product_variant_id, 0.0)
    hard_reserved = get_hard_reserved_qty(product_variant_id)
    soft_held     = get_soft_hold_qty(product_variant_id)
    available     = max(0.0, r2(physical - hard_reserved - soft_held))

    # Active production batch for this variant (latest non-completed WO)
    active_batch = qry1("""
        SELECT wo.wo_number, wo.qty_units, wo.target_date, wo.status
        FROM work_orders wo
        WHERE wo.product_variant_id = ?
          AND wo.status IN ('planned', 'in_progress')
        ORDER BY wo.target_date ASC LIMIT 1
    """, (product_variant_id,))

    return {
        'physical':      physical,
        'hard_reserved': hard_reserved,
        'soft_held':     soft_held,
        'available':     available,        # for new soft holds
        'active_wo':     dict(active_batch) if active_batch else None,
    }


def place_soft_hold(order_id):
    """
    Place a soft hold on stock for all items in a pending_review order.
    Populates qty_soft_hold on each item and records the hold expiry time.
    Safe to call even if stock is insufficient — hold is placed anyway
    (admin sees the shortfall at review time and decides).
    """
    items = qry("SELECT id, qty_ordered FROM customer_order_items WHERE order_id=?", (order_id,))
    now        = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    expires_at = (datetime.utcnow() + timedelta(hours=SOFT_HOLD_EXPIRY_HOURS)).strftime('%Y-%m-%dT%H:%M:%S')

    c = _conn()
    try:
        for item in items:
            c.execute(
                "UPDATE customer_order_items SET qty_soft_hold=? WHERE id=?",
                (item['qty_ordered'], item['id'])
            )
        # Upsert hold expiry record
        c.execute("""
            INSERT INTO order_hold_expiry
                (order_id, hold_placed_at, hold_expires_at, is_expired, notification_sent)
            VALUES (?, ?, ?, 0, 0)
            ON CONFLICT(order_id) DO UPDATE SET
                hold_placed_at  = excluded.hold_placed_at,
                hold_expires_at = excluded.hold_expires_at,
                is_expired      = 0,
                notification_sent = 0,
                expired_at      = NULL
        """, (order_id, now, expires_at))
        c.commit()
    finally:
        c.close()
    _log('info', 'soft_hold_placed', order_id=order_id, expires_at=expires_at)


def release_soft_hold(order_id):
    """
    Release soft holds for an order (on rejection, expiry, or approval).
    Idempotent — safe to call multiple times.
    """
    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    run("UPDATE customer_order_items SET qty_soft_hold=0 WHERE order_id=?", (order_id,))
    run("""
        UPDATE order_hold_expiry
        SET is_expired=1, expired_at=?
        WHERE order_id=? AND is_expired=0
    """, (now, order_id))
    _log('info', 'soft_hold_released', order_id=order_id)


def convert_soft_hold_to_hard_reservation(order_id):
    """
    When an order is approved/confirmed: clear its qty_soft_hold values.
    The hard reservation is implicitly the confirmed status itself
    (get_hard_reserved_qty reads from status='confirmed').
    """
    run("UPDATE customer_order_items SET qty_soft_hold=0 WHERE order_id=?", (order_id,))
    # Mark hold as resolved (not expired — it was converted, not released)
    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    run("""
        UPDATE order_hold_expiry
        SET is_expired=1, expired_at=?, notification_sent=1
        WHERE order_id=? AND is_expired=0
    """, (now, order_id))


# ═══════════════════════════════════════════════════════════════════
#  WHATSAPP NOTIFICATIONS (CallMeBot)
# ═══════════════════════════════════════════════════════════════════

def _wa_send(phone: str, apikey: str, message: str):
    """Non-blocking CallMeBot send. Fires in a daemon thread — never blocks the request."""
    import urllib.request, urllib.parse
    def _do():
        try:
            url = (
                f"https://api.callmebot.com/whatsapp.php"
                f"?phone={phone}&text={urllib.parse.quote(message)}&apikey={apikey}"
            )
            with urllib.request.urlopen(url, timeout=15) as resp:
                body = resp.read().decode('utf-8', errors='ignore')
            _log('info', f"whatsapp: Sent to {phone[:5]}**** — {body[:80]}")
        except Exception as e:
            _log('error', f"whatsapp: Failed to {phone[:5]}****: {e}")
    threading.Thread(target=_do, daemon=True).start()


def _wa_admin(message: str):
    """Send a notification to the configured admin WhatsApp number."""
    if not WA_ENABLED or not WA_ADMIN_PHONE or not WA_ADMIN_APIKEY:
        return
    _wa_send(WA_ADMIN_PHONE, WA_ADMIN_APIKEY, message)


def _wa_rep(rep_id, message: str):
    """Send a notification to a field rep if they have a WhatsApp API key registered."""
    if not WA_ENABLED or not rep_id:
        return
    rep = qry1("SELECT phone, whatsapp_apikey FROM sales_reps WHERE id=?", (rep_id,))
    if rep and rep.get('whatsapp_apikey') and rep.get('phone'):
        _wa_send(rep['phone'], rep['whatsapp_apikey'], message)


def _wa_order_row(order_id) -> dict | None:
    """Fetch the order fields needed for all notification messages."""
    return qry1("""
        SELECT co.order_number, co.order_source, co.created_by_rep_id,
               c.name as customer_name
        FROM customer_orders co
        JOIN customers c ON c.id = co.customer_id
        WHERE co.id = ?
    """, (order_id,))


def _wa_notify_order_queued(order_id):
    """Notify admin that a new external order is waiting in the review queue."""
    o = _wa_order_row(order_id)
    if not o:
        return
    src = {'consumer_website': '🛒 Website', 'retailer_self_service': '🏪 Retailer',
           'field_rep': '👤 Sales Rep'}.get(o['order_source'], o['order_source'])
    _wa_admin(
        f"🔔 *NEW ORDER IN QUEUE*\n"
        f"Order: {o['order_number']}\n"
        f"Customer: {o['customer_name']}\n"
        f"Source: {src}\n\n"
        f"Review at: BMS → Review Queue"
    )


def _wa_notify_order_approved(order_id, approval_note=''):
    """Notify admin and the creating field rep that an order was approved."""
    o = _wa_order_row(order_id)
    if not o:
        return
    note_line = f"\nNote: {approval_note}" if approval_note else ''
    msg = (
        f"✅ *ORDER APPROVED*\n"
        f"Order: {o['order_number']}\n"
        f"Customer: {o['customer_name']}{note_line}\n\n"
        f"Status: Confirmed — proceed to invoicing."
    )
    _wa_admin(msg)
    _wa_rep(o['created_by_rep_id'], msg)


def _wa_notify_order_rejected(order_id, reason: str):
    """Notify admin and the creating field rep that an order was rejected."""
    o = _wa_order_row(order_id)
    if not o:
        return
    msg = (
        f"❌ *ORDER REJECTED*\n"
        f"Order: {o['order_number']}\n"
        f"Customer: {o['customer_name']}\n"
        f"Reason: {reason}\n\n"
        f"Action required — contact customer or reopen from BMS → Review Queue."
    )
    _wa_admin(msg)
    _wa_rep(o['created_by_rep_id'], msg)


def _wa_notify_hold_expiring(order_id, hours_remaining: int):
    """Warn admin that a soft hold is approaching expiry."""
    o = _wa_order_row(order_id)
    if not o:
        return
    _wa_admin(
        f"⚠️ *HOLD EXPIRING SOON*\n"
        f"Order: {o['order_number']}\n"
        f"Customer: {o['customer_name']}\n"
        f"Hold expires in: ~{hours_remaining}h\n\n"
        f"Review at: BMS → Review Queue before stock is released."
    )


def _wa_notify_hold_expired(order_id):
    """Notify admin that a soft hold has lapsed and stock was released."""
    o = _wa_order_row(order_id)
    if not o:
        return
    _wa_admin(
        f"⌛ *HOLD EXPIRED*\n"
        f"Order: {o['order_number']}\n"
        f"Customer: {o['customer_name']}\n"
        f"Stock hold released automatically.\n\n"
        f"Reopen from BMS → Review Queue if still needed."
    )


# ═══════════════════════════════════════════════════════════════════
#  REVIEW QUEUE — ORDER INTAKE & MANAGEMENT
# ═══════════════════════════════════════════════════════════════════

def create_customer_order_external(data):
    """
    Entry point for orders from non-BMS channels.
    order_source: 'consumer_website' | 'retailer_self_service' | 'field_rep' | 'rep_assisted'
    - field_rep / rep_assisted: bypass review queue, land as draft
    - consumer_website / retailer_self_service: pending_review + 48h soft hold
    rep_assisted: sales rep placing on behalf of a customer via B2B portal.
      Supports idempotency_key (dedup within 24h per rep).
      Triggers out-of-route warning if customer not on rep's routes.
    """
    source = data.get('order_source', 'internal')
    if source not in ('consumer_website', 'retailer_self_service', 'field_rep', 'rep_assisted'):
        raise ValueError(f"Invalid order_source: {source}")

    # ── Translate portal format → internal format ─────────────────
    # Portal sends: customerId (int) + items:[{variantId, qty, unitPrice}]
    # create_customer_order expects: custCode (str) + lines:[{productCode, packSize, qty, unitPrice}]
    if 'customerId' in data and 'custCode' not in data:
        cust_row = qry1("SELECT code FROM customers WHERE id=?", (int(data['customerId']),))
        if not cust_row:
            raise ValueError(f"Customer not found: id={data['customerId']}")
        data['custCode'] = cust_row['code']

    if 'items' in data and 'lines' not in data:
        lines = []
        for item in data.get('items', []):
            vid = item.get('variantId')
            var = qry1("""
                SELECT p.code as productCode, ps.label as packSize
                FROM product_variants pv
                JOIN products p    ON p.id  = pv.product_id
                JOIN pack_sizes ps ON ps.id = pv.pack_size_id
                WHERE pv.id=?
            """, (vid,))
            if not var:
                raise ValueError(f"Product variant not found: variantId={vid}")
            lines.append({
                'productCode': var['productCode'],
                'packSize':    var['packSize'],
                'qty':         item.get('qty', 1),
                'unitPrice':   item.get('unitPrice', 0),
            })
        data['lines'] = lines

    # ── Idempotency check (rep_assisted orders) ───────────────────
    idem_key = data.get('idempotency_key')
    rep_id   = data.get('created_by_rep_id') or data.get('placed_by_rep_id')
    if idem_key and rep_id:
        existing = qry1("""
            SELECT id FROM customer_orders
            WHERE idempotency_key=? AND created_by_rep_id=?
            AND created_at >= datetime('now', '-24 hours')
        """, (idem_key, rep_id))
        if existing:
            result = _order_detail(existing['id'])
            result['_idempotent'] = True
            return result

    # Create the base order as draft (standard path)
    order = create_customer_order(data)
    order_id = order['orderId']

    # Tag the order source, creating rep, and idempotency key
    rep_id = data.get('created_by_rep_id') or data.get('placed_by_rep_id')
    if rep_id:
        run("""UPDATE customer_orders
               SET order_source=?, created_by_rep_id=?, idempotency_key=?
               WHERE id=?""", (source, rep_id, idem_key, order_id))
    else:
        run("UPDATE customer_orders SET order_source=?, idempotency_key=? WHERE id=?",
            (source, idem_key, order_id))

    if source in ('field_rep', 'rep_assisted'):
        # These sources bypass the review queue — stays as draft, no soft hold
        order['order_source'] = source
        order['inReviewQueue'] = False

        # Out-of-route check for rep_assisted orders
        if source == 'rep_assisted' and rep_id:
            customer_id = order.get('customerId') or qry1(
                "SELECT customer_id FROM customer_orders WHERE id=?", (order_id,))
            if customer_id:
                cid = customer_id if isinstance(customer_id, int) else customer_id['customer_id']
                if _is_out_of_route(int(rep_id), cid):
                    run("UPDATE customer_orders SET out_of_route=1 WHERE id=?", (order_id,))
                    order['outOfRoute'] = True
                    try:
                        _wa_notify_out_of_route(order_id, int(rep_id))
                    except Exception as e:
                        _log('warn', 'out_of_route_wa_failed', order_id=order_id, error=str(e))
                else:
                    order['outOfRoute'] = False
        return order

    # External orders → pending_review + soft hold
    run("""
        UPDATE customer_orders
        SET status='pending_review',
            order_source=?,
            approval_method='manual',
            updated_at=datetime('now')
        WHERE id=?
    """, (source, order_id))

    # Place soft hold on all items
    place_soft_hold(order_id)

    detail = _order_detail(order_id)

    # Compute stock warnings for acknowledgment
    items = qry("SELECT * FROM customer_order_items WHERE order_id=?", (order_id,))
    stock_warnings = []
    for item in items:
        sit = get_stock_situation(item['product_variant_id'])
        if item['qty_ordered'] > sit['available']:
            short = item['qty_ordered'] - sit['available']
            stock_warnings.append({
                'variantId': item['product_variant_id'],
                'qtyOrdered': item['qty_ordered'],
                'qtyAvailable': sit['available'],
                'shortfall': short
            })

    hold_row = qry1("SELECT hold_expires_at FROM order_hold_expiry WHERE order_id=?", (order_id,))
    detail['holdExpiresAt'] = hold_row['hold_expires_at'] if hold_row else None
    detail['stockWarnings'] = stock_warnings
    detail['inReviewQueue'] = True

    # Notify admin that a new external order is waiting for review
    _wa_notify_order_queued(order_id)

    return detail


def get_review_queue(filters=None):
    """
    Return all pending_review orders with stock context per item.
    Optionally filter by order_source.
    """
    filters = filters or {}
    where_clauses = ["co.status='pending_review'"]
    params = []
    if filters.get('order_source'):
        where_clauses.append("co.order_source=?")
        params.append(filters['order_source'])

    sql = f"""
        SELECT
            co.id, co.order_number, co.status, co.order_source,
            co.approval_method, co.created_at, co.updated_at,
            c.code as cust_code,
            c.name as customer_name,
            ohe.hold_placed_at, ohe.hold_expires_at, ohe.is_expired
        FROM customer_orders co
        LEFT JOIN customers c ON c.id = co.customer_id
        LEFT JOIN order_hold_expiry ohe ON ohe.order_id = co.id
        WHERE {' AND '.join(where_clauses)}
        ORDER BY co.created_at ASC
    """
    orders = qry(sql, tuple(params))
    result = []
    now_ts = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

    for o in orders:
        order_dict = dict(o)
        items = qry("""
            SELECT coi.*, pv.id as variant_id,
                   p.name as product_name, ps.label as pack_label
            FROM customer_order_items coi
            JOIN product_variants pv ON pv.id = coi.product_variant_id
            JOIN products p ON p.id = pv.product_id
            JOIN pack_sizes ps ON ps.id = pv.pack_size_id
            WHERE coi.order_id=?
        """, (o['id'],))

        enriched_items = []
        for item in items:
            sit = get_stock_situation(item['product_variant_id'])
            enriched_items.append({
                'id': item['id'],
                'productVariantId': item['product_variant_id'],
                'productName': item['product_name'],
                'packLabel': item['pack_label'],
                'qtyOrdered': item['qty_ordered'],
                'qtySoftHold': item['qty_soft_hold'],
                'unitPrice': item['unit_price'],
                'stock': sit,
                'canFulfill': item['qty_ordered'] <= sit['physical']
            })

        # Compute hold time remaining
        hold_remaining_seconds = None
        if o['hold_expires_at'] and not o['is_expired']:
            try:
                exp = datetime.strptime(o['hold_expires_at'], '%Y-%m-%dT%H:%M:%S')
                now_dt = datetime.utcnow()
                diff = (exp - now_dt).total_seconds()
                hold_remaining_seconds = max(0, int(diff))
            except Exception:
                pass

        order_dict['items'] = enriched_items
        order_dict['holdRemainingSeconds'] = hold_remaining_seconds
        result.append(order_dict)

    return result


def approve_order_with_edit(order_id, data):
    """
    Admin approves a pending_review order, optionally editing quantities first.
    data: {quantities: [{itemId, qty}], approvalNote: str}
    Converts soft hold to hard reservation, sets status=confirmed.
    For retailer orders: generates invoice automatically.
    """
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] != 'pending_review':
        raise ValueError(f"Order is {order['status']} — can only approve pending_review orders")

    # Apply quantity edits if any
    quantities = data.get('quantities', [])
    for q in quantities:
        item_id = q.get('itemId')
        new_qty = q.get('qty')
        if item_id is None or new_qty is None:
            continue
        if new_qty <= 0:
            raise ValueError(f"Item {item_id}: quantity must be > 0 (use reject to cancel)")
        item = qry1("SELECT * FROM customer_order_items WHERE id=? AND order_id=?", (item_id, order_id))
        if not item:
            raise ValueError(f"Item {item_id} not found on order {order_id}")
        run("UPDATE customer_order_items SET qty_ordered=? WHERE id=?",
            (new_qty, item_id))

    approval_note = data.get('approvalNote', '')
    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

    # Convert soft hold → hard reservation, mark as confirmed
    convert_soft_hold_to_hard_reservation(order_id)
    run("""
        UPDATE customer_orders
        SET status='confirmed',
            approval_method='manual',
            approval_timestamp=?,
            approval_note=?,
            updated_at=datetime('now')
        WHERE id=?
    """, (now, approval_note, order_id))

    detail = _order_detail(order_id)
    detail['approved'] = True

    # Notify admin + field rep who created this order
    _wa_notify_order_approved(order_id, approval_note)

    return detail


def update_order_item_qty(order_id, item_id, new_qty):
    """Update qty_ordered on a single order line item (admin/sales only).
    Blocked if order is fully_invoiced or cancelled.
    Cannot reduce below qty_invoiced + qty_in_production (already committed)."""
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] in ('fully_invoiced', 'cancelled'):
        raise ValueError(f"Cannot edit items on a {order['status']} order")

    item = qry1("SELECT * FROM customer_order_items WHERE id=? AND order_id=?", (item_id, order_id))
    if not item:
        raise ValueError(f"Item {item_id} not found on order {order_id}")

    new_qty = int(new_qty)
    if new_qty <= 0:
        raise ValueError("Quantity must be greater than zero")

    committed = (item['qty_in_production'] or 0) + (item['qty_invoiced'] or 0)
    if new_qty < committed:
        raise ValueError(
            f"Cannot reduce below {committed} units "
            f"({item['qty_in_production'] or 0} in production + {item['qty_invoiced'] or 0} invoiced)"
        )

    run("UPDATE customer_order_items SET qty_ordered=? WHERE id=?", (new_qty, item_id))

    # Update soft hold to match new qty (only if order is still in draft/confirmed)
    if order['status'] in ('draft', 'confirmed'):
        run("UPDATE customer_order_items SET qty_soft_hold=? WHERE id=?", (new_qty, item_id))

    return _order_detail(order_id)


def reject_order(order_id, reason):
    """
    Admin rejects a pending_review order.
    Reason is mandatory. Releases soft hold.
    """
    if not reason or not reason.strip():
        raise ValueError("Rejection reason is mandatory")

    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] != 'pending_review':
        raise ValueError(f"Order is {order['status']} — can only reject pending_review orders")

    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    release_soft_hold(order_id)
    run("""
        UPDATE customer_orders
        SET status='rejected',
            rejection_reason=?,
            approval_timestamp=?,
            updated_at=datetime('now')
        WHERE id=?
    """, (reason.strip(), now, order_id))

    # Notify admin + field rep who created this order
    _wa_notify_order_rejected(order_id, reason.strip())

    return {'ok': True, 'orderId': order_id, 'status': 'rejected', 'rejectionReason': reason.strip()}


def reopen_rejected_order(order_id):
    """
    Re-enter a rejected order into the review queue.
    Places a fresh soft hold.
    """
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] != 'rejected':
        raise ValueError(f"Order is {order['status']} — can only reopen rejected orders")

    run("""
        UPDATE customer_orders
        SET status='pending_review',
            rejection_reason='',
            approval_timestamp=NULL,
            updated_at=datetime('now')
        WHERE id=?
    """, (order_id,))

    place_soft_hold(order_id)
    return _order_detail(order_id)


def check_and_expire_holds():
    """
    Find all holds that have passed their expiry time and release them.
    Called by the background thread every hour.
    Returns count of expired orders.
    """
    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    expired_rows = qry("""
        SELECT ohe.order_id
        FROM order_hold_expiry ohe
        JOIN customer_orders co ON co.id = ohe.order_id
        WHERE ohe.is_expired = 0
          AND ohe.hold_expires_at <= ?
          AND co.status = 'pending_review'
    """, (now,))

    count = 0
    for row in expired_rows:
        oid = row['order_id']
        release_soft_hold(oid)
        run("""
            UPDATE customer_orders
            SET status='expired', updated_at=datetime('now')
            WHERE id=? AND status='pending_review'
        """, (oid,))
        _wa_notify_hold_expired(oid)
        count += 1

    if count:
        _log('info', 'hold_expiry', f"Expired {count} soft holds")
    return count


def _check_expiry_warnings():
    """
    Find holds that are expiring soon (within WA_EXPIRY_WARN_HOURS) but haven't been warned yet.
    Sends a WhatsApp warning and marks expiry_warning_sent=1 to prevent repeat messages.
    """
    warn_threshold = datetime.utcnow() + timedelta(hours=WA_EXPIRY_WARN_HOURS)
    warn_ts = warn_threshold.strftime('%Y-%m-%dT%H:%M:%S')
    now_ts  = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

    soon_rows = qry("""
        SELECT ohe.order_id, ohe.hold_expires_at
        FROM order_hold_expiry ohe
        JOIN customer_orders co ON co.id = ohe.order_id
        WHERE ohe.is_expired = 0
          AND ohe.expiry_warning_sent = 0
          AND ohe.hold_expires_at <= ?
          AND ohe.hold_expires_at > ?
          AND co.status = 'pending_review'
    """, (warn_ts, now_ts))

    for row in soon_rows:
        oid = row['order_id']
        try:
            exp = datetime.strptime(row['hold_expires_at'], '%Y-%m-%dT%H:%M:%S')
            hours_left = max(0, int((exp - datetime.utcnow()).total_seconds() // 3600))
        except Exception:
            hours_left = WA_EXPIRY_WARN_HOURS
        _wa_notify_hold_expiring(oid, hours_left)
        run("UPDATE order_hold_expiry SET expiry_warning_sent=1 WHERE order_id=?", (oid,))


def _hold_expiry_thread():
    """Background thread: check for expired holds and expiry warnings every 30 minutes."""
    while True:
        time.sleep(1800)   # 30 minutes
        try:
            check_and_expire_holds()
        except Exception as e:
            _log('error', 'hold_expiry_thread', str(e))
        try:
            _check_expiry_warnings()
        except Exception as e:
            _log('error', 'hold_expiry_thread', f"warning check: {e}")


# ═══════════════════════════════════════════════════════════════════
#  ACCOUNTS RECEIVABLE HELPERS
# ═══════════════════════════════════════════════════════════════════

def _compute_invoice_status(invoice_id) -> str:
    """
    Derive the correct invoice status purely from the numbers — never from a stored flag.
    Returns 'UNPAID', 'PARTIAL', or 'PAID'.
    This is the single source of truth for invoice status.
    """
    _, _, total, paid, balance = compute_invoice_balance(invoice_id)
    if paid <= 0:
        return 'UNPAID'
    if balance > 0.001:   # leave a 0.001 rounding tolerance
        return 'PARTIAL'
    return 'PAID'


def _sync_invoice_status(invoice_id) -> str:
    """
    Compute invoice status from amounts and write it to the DB.
    Returns the new status string.
    Call after any payment allocation, item add/remove, or admin reconcile.
    """
    new_status = _compute_invoice_status(invoice_id)
    run("UPDATE invoices SET status=? WHERE id=?", (new_status, invoice_id))
    return new_status


def compute_invoice_balance(invoice_id):
    """Returns (subtotal, tax, total, paid, balance)."""
    subtotal_row = qry1(
        "SELECT COALESCE(SUM(line_total),0) as s FROM invoice_items WHERE invoice_id=?",
        (invoice_id,)
    )
    subtotal = r2(subtotal_row['s'])
    tax      = r2(subtotal * GST_RATE)
    total    = r2(subtotal + tax)
    paid_row = qry1(
        "SELECT COALESCE(SUM(allocated_amount),0) as p FROM payment_allocations WHERE invoice_id=?",
        (invoice_id,)
    )
    paid    = r2(paid_row['p'])
    balance = r2(total - paid)
    return subtotal, tax, total, paid, balance

def get_ar_aging():
    """Returns aging buckets for all unpaid/partial invoices."""
    invoices = qry("""
        SELECT inv.id, inv.invoice_number, inv.customer_id, inv.invoice_date, inv.due_date,
               inv.status, c.name as customer_name, c.customer_type
        FROM invoices inv JOIN customers c ON c.id = inv.customer_id
        WHERE inv.status IN ('UNPAID','PARTIAL')
        ORDER BY inv.due_date
    """)
    today_dt = date.today()
    result = []
    for inv in invoices:
        _, _, total, paid, balance = compute_invoice_balance(inv['id'])
        if balance <= 0:
            continue
        try:
            due = date.fromisoformat(inv['due_date'])
            days_over = (today_dt - due).days
        except Exception:
            days_over = 0
        bucket = 'current' if days_over <= 0 else (
                 '1_30'   if days_over <= 30  else (
                 '31_60'  if days_over <= 60  else (
                 '61_90'  if days_over <= 90  else '90plus')))
        result.append({**inv, 'total': total, 'paid': paid, 'balance': balance,
                        'days_overdue': days_over, 'aging_bucket': bucket})
    return result


# ═══════════════════════════════════════════════════════════════════
#  ACCOUNTS PAYABLE HELPERS
# ═══════════════════════════════════════════════════════════════════

def compute_bill_balance(bill_id):
    """Returns (total, paid, balance).
    Uses the larger of items-sum or stored total_amount, so zero-cost-item bills
    still track their correct total (stored at bill creation).
    """
    items_row = qry1(
        "SELECT COALESCE(SUM(line_total),0) as t FROM supplier_bill_items WHERE bill_id=?",
        (bill_id,)
    )
    items_sum = r2(items_row['t'])

    # Authoritative stored total — set when bill was created.
    bill_row = qry1(
        "SELECT COALESCE(total_amount,0) as ta FROM supplier_bills WHERE id=?",
        (bill_id,)
    )
    stored_total = r2(bill_row['ta']) if bill_row else 0

    # Use whichever is larger (items may have been updated; stored_total is the original)
    total = max(items_sum, stored_total)

    paid_row = qry1(
        "SELECT COALESCE(SUM(allocated_amount),0) as p FROM supplier_payment_allocations WHERE bill_id=?",
        (bill_id,)
    )
    paid    = r2(paid_row['p'])
    balance = r2(total - paid)
    return total, paid, balance


def _compute_bill_status(bill_id) -> str:
    """
    Derive the correct supplier bill status purely from the numbers — never from a stored flag.
    Returns 'UNPAID', 'PARTIAL', or 'PAID'.
    This is the single source of truth for bill status (mirrors _compute_invoice_status for AR).
    """
    total, paid, balance = compute_bill_balance(bill_id)
    if paid <= 0:
        return 'UNPAID'
    if balance > 0.001:   # 0.001 rounding tolerance
        return 'PARTIAL'
    return 'PAID'


def _sync_bill_status(bill_id) -> str:
    """
    Compute supplier bill status from amounts and write it to the DB.
    Returns the new status string.
    Call after any payment allocation, item add/remove, or admin reconcile.
    Preserves VOID status — never overwrites a voided bill.
    """
    existing = qry1("SELECT status FROM supplier_bills WHERE id=?", (bill_id,))
    if not existing or existing['status'] == 'VOID':
        return existing['status'] if existing else 'VOID'
    new_status = _compute_bill_status(bill_id)
    run("UPDATE supplier_bills SET status=? WHERE id=?", (new_status, bill_id))
    return new_status


# ═══════════════════════════════════════════════════════════════════
#  VOID TRANSACTIONS  (P2.5)
# ═══════════════════════════════════════════════════════════════════

def void_invoice(invoice_id: int, note: str, username: str):
    """
    Void an invoice.  Rules:
    • Cannot void a PAID invoice — too late.
    • Cannot void an already-VOID invoice.
    • Marks status = VOID, records voided_at / voided_by / void_note.
    • Marks all associated sales records as voided (restores finished-goods stock).
    • Deletes payment_allocations for this invoice so the customer payment
      becomes unallocated and can be re-applied elsewhere.
    • If the invoice belongs to a customer order, resets qty_invoiced on the
      relevant order items so the order can be re-invoiced.
    • Writes a change_log audit entry.
    """
    inv = qry1("SELECT * FROM invoices WHERE id=?", (invoice_id,))
    if not inv:
        raise ValueError(f"Invoice not found: {invoice_id}")
    if inv['status'] == 'VOID':
        raise ValueError(f"{inv['invoice_number']} is already void")
    if inv['status'] == 'PAID':
        raise ValueError(
            f"{inv['invoice_number']} is fully paid — it cannot be voided. "
            "Record a credit note instead."
        )

    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    note = (note or '').strip() or 'No reason given'

    c = _conn()
    try:
        # 1. Mark invoice as VOID
        c.execute("""
            UPDATE invoices
            SET status='VOID', voided_at=?, voided_by=?, void_note=?
            WHERE id=?
        """, (now, username, note, invoice_id))

        # 2. Mark associated sales as voided (restores finished-goods stock via get_finished_stock_map)
        c.execute("UPDATE sales SET voided=1 WHERE invoice_id=?", (invoice_id,))

        # 3. Un-allocate any customer payments from this invoice
        c.execute("DELETE FROM payment_allocations WHERE invoice_id=?", (invoice_id,))

        # 4. If linked to a customer order, reset qty_invoiced on order items
        #    so the order can be re-invoiced or re-confirmed.
        items = qry("SELECT * FROM invoice_items WHERE invoice_id=?", (invoice_id,))
        for item in items:
            if item.get('sale_id'):
                # Find customer_order_items linked through the sale
                c.execute("""
                    UPDATE customer_order_items
                    SET qty_invoiced = MAX(0, qty_invoiced - ?)
                    WHERE order_id IN (
                        SELECT customer_order_id FROM invoices WHERE id=?
                    )
                    AND product_variant_id = ?
                """, (item['quantity'], invoice_id, item.get('product_variant_id')))

        # 5. If order is fully void, revert order status (computed post-commit via _order_status)
        _void_order_id = None
        if inv.get('customer_order_id'):
            live_invoices = qry("""
                SELECT COUNT(*) as n FROM invoices
                WHERE customer_order_id=? AND status != 'VOID' AND id != ?
            """, (inv['customer_order_id'], invoice_id))
            live_count = live_invoices[0]['n'] if live_invoices else 0
            if live_count == 0:
                # Mark order as needing post-commit status sync (qty_invoiced already decremented above)
                _void_order_id = inv['customer_order_id']

        # 6. Audit log
        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('invoices', ?, 'VOID', ?)
        """, (inv['invoice_number'], json.dumps({
            'void_note': note, 'voided_by': username,
            'voided_at': now, 'previous_status': inv['status']
        })))

        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    # Post-commit: derive order status from actual qty_invoiced totals (never hardcode 'confirmed')
    if _void_order_id:
        new_ord_status = _order_status(_void_order_id)
        run("UPDATE customer_orders SET status=? WHERE id=?", (new_ord_status, _void_order_id))
    save_db()
    _log('info', 'invoice_voided', invoice=inv['invoice_number'],
         by=username, status=inv['status'])
    return {'ok': True, 'invoice_number': inv['invoice_number']}


def void_supplier_bill(bill_id: int, note: str, username: str):
    """
    Void a supplier bill.  Rules:
    • Cannot void a PAID bill.
    • Cannot void an already-VOID bill.
    • Marks status = VOID, records voided_at / voided_by / void_note.
    • Creates reversing inventory_ledger entries (positive) for every ingredient
      that was received on this bill, so physical stock is restored.
    • Deletes supplier_payment_allocations for this bill.
    • Writes a change_log audit entry.
    """
    bill = qry1("SELECT * FROM supplier_bills WHERE id=?", (bill_id,))
    if not bill:
        raise ValueError(f"Bill not found: {bill_id}")
    if bill['status'] == 'VOID':
        raise ValueError(f"{bill['bill_number']} is already void")
    if bill['status'] == 'PAID':
        raise ValueError(
            f"{bill['bill_number']} is fully paid — it cannot be voided. "
            "Record a debit note instead."
        )

    now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    note = (note or '').strip() or 'No reason given'

    # Fetch bill items (ingredient receipts to reverse)
    items = qry("SELECT * FROM supplier_bill_items WHERE bill_id=?", (bill_id,))

    c = _conn()
    try:
        # 1. Mark bill as VOID
        c.execute("""
            UPDATE supplier_bills
            SET status='VOID', voided_at=?, voided_by=?, void_note=?
            WHERE id=?
        """, (now, username, note, bill_id))

        # 2. Create reversing inventory_ledger entries (negative received qty)
        for item in items:
            # Original receipt was positive (PURCHASE_IN); reversal is a negative ADJUSTMENT
            reversal_grams = r2(-item['quantity_kg'] * 1000)  # kg → grams, then negate
            c.execute("""
                INSERT INTO inventory_ledger
                    (ingredient_id, movement_type, qty_grams, reference_id, notes)
                VALUES (?, 'ADJUSTMENT', ?, ?, ?)
            """, (item['ingredient_id'], reversal_grams, bill['bill_number'],
                  f"Void of bill {bill['bill_number']} — {note}"))

        # 3. Un-allocate AP payments from this bill
        c.execute("DELETE FROM supplier_payment_allocations WHERE bill_id=?", (bill_id,))

        # 4. Audit log
        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('supplier_bills', ?, 'VOID', ?)
        """, (bill['bill_number'], json.dumps({
            'void_note': note, 'voided_by': username,
            'voided_at': now, 'previous_status': bill['status'],
            'items_reversed': len(items)
        })))

        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()
    _log('info', 'bill_voided', bill=bill['bill_number'],
         by=username, status=bill['status'])
    return {'ok': True, 'bill_number': bill['bill_number']}


def get_ap_aging():
    """Returns aging buckets for all unpaid/partial bills."""
    bills = qry("""
        SELECT sb.id, sb.bill_number, sb.supplier_id, sb.bill_date, sb.due_date,
               sb.status, s.name as supplier_name
        FROM supplier_bills sb JOIN suppliers s ON s.id = sb.supplier_id
        WHERE sb.status IN ('UNPAID','PARTIAL')
        ORDER BY sb.due_date
    """)
    today_dt = date.today()
    result = []
    for bill in bills:
        total, paid, balance = compute_bill_balance(bill['id'])
        if balance <= 0:
            continue
        try:
            due = date.fromisoformat(bill['due_date'])
            days_over = (today_dt - due).days
        except Exception:
            days_over = 0
        bucket = 'current' if days_over <= 0 else (
                 '1_30'   if days_over <= 30  else (
                 '31_60'  if days_over <= 60  else (
                 '61_90'  if days_over <= 90  else '90plus')))
        result.append({**bill, 'total': total, 'paid': paid, 'balance': balance,
                        'days_overdue': days_over, 'aging_bucket': bucket})
    return result


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — CUSTOMERS
# ═══════════════════════════════════════════════════════════════════

def create_customer(data):
    validate_fields(data, [
        {'field': 'name',         'label': 'Customer name',  'type': 'str',  'min': 2, 'max': 120},
        {'field': 'city',         'label': 'City',           'type': 'str',  'min': 2, 'max': 60},
        {'field': 'address',      'label': 'Full address',   'type': 'str',  'required': False, 'min': 0, 'max': 250},
        {'field': 'customerType', 'label': 'Customer type',  'required': False,
         'choices': ['RETAIL', 'retail', 'DIRECT', 'direct', 'WHOLESALE', 'wholesale']},
        {'field': 'phone',        'label': 'Phone',          'required': False, 'type': 'str', 'max': 30},
        {'field': 'email',        'label': 'Email',          'required': False, 'type': 'str', 'max': 120},
    ])
    # Sync counter before use to prevent UNIQUE constraint failures on Railway
    _sync_counter_to_max('customer', 'customers', 'code', 'SP-CUST-')
    code    = next_id('customer', 'CUST')
    ctype   = data.get('customerType', 'RETAIL').upper()
    if ctype not in ('RETAIL', 'DIRECT', 'WHOLESALE'):
        raise ValueError(f"Invalid customer type: {ctype}")
    city           = data.get('city', '').strip()
    account_number = generate_account_number(city, ctype)
    ops = [("""
        INSERT INTO customers
            (code, account_number, name, customer_type, city, address,
             phone, email, default_pack, payment_terms_days)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (code, account_number, data['name'].strip(), ctype,
          city,
          data.get('address','').strip(),
          data.get('phone',''), data.get('email',''),
          data.get('defaultPack','50g'),
          int(data.get('paymentTermsDays', 30))))]
    audit_log(ops, 'customers', code, 'INSERT', new_val=data)
    run_many(ops)
    load_ref()
    return qry1("SELECT * FROM customers WHERE code=?", (code,))

def update_customer(cust_id, data):
    """Update customer by integer id."""
    existing = qry1("SELECT * FROM customers WHERE id=?", (cust_id,))
    if not existing:
        raise ValueError(f"Customer not found: {cust_id}")
    mapping = {
        'name':             'name',
        'customerType':     'customer_type',
        'city':             'city',
        'address':          'address',
        'phone':            'phone',
        'email':            'email',
        'defaultPack':      'default_pack',
        'paymentTermsDays': 'payment_terms_days',
        'creditLimit':      'credit_limit',
        'active':           'active',
    }
    set_parts, vals = [], []
    for js_key, db_col in mapping.items():
        if js_key in data:
            set_parts.append(f"{db_col}=?")
            vals.append(data[js_key])
    if not set_parts:
        return existing
    vals.append(cust_id)
    ops = [(f"UPDATE customers SET {', '.join(set_parts)} WHERE id=?", vals)]
    audit_log(ops, 'customers', existing['code'], 'UPDATE', old_val=dict(existing), new_val=data)
    run_many(ops)
    load_ref()
    return qry1("SELECT * FROM customers WHERE id=?", (cust_id,))


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — PRODUCTS
# ═══════════════════════════════════════════════════════════════════

def create_product(data):
    """
    Each pack size is treated as a separate product.
    One call = one product + one pack-size variant (one SKU).
    To add Chaat Masala in 50g AND 100g, call this twice.
    """
    code       = data.get('code', '').strip().upper()
    name       = data.get('name', '').strip()
    name_urdu  = data.get('nameUrdu', '').strip()
    blend_code = data.get('blendCode', '').strip()
    pack_size_id = data.get('packSizeId')          # single int

    if not code:
        raise ValueError("Product code is required")
    if not name:
        raise ValueError("Product name is required")
    if not pack_size_id:
        raise ValueError("Pack size is required")

    pack_size_id = int(pack_size_id)
    ps = qry1("SELECT id, label FROM pack_sizes WHERE id=?", (pack_size_id,))
    if not ps:
        raise ValueError("Invalid pack size")

    # Full product code format: SP-<CODE>-<PACKGRAMS>
    # e.g. base code BM + 50g → SP-BM-50
    pack_grams = ps['label'].replace('g', '')
    base = code if code.startswith('SP-') else f"SP-{code}"
    full_code = f"{base}-{pack_grams}"
    existing = qry1("SELECT id FROM products WHERE code=?", (full_code,))
    if existing:
        raise ValueError(f"Product '{full_code}' ({name} {ps['label']}) already exists")

    c = _conn()
    try:
        c.execute("""
            INSERT INTO products (code, name, name_urdu, blend_code, active)
            VALUES (?,?,?,?,1)
        """, (full_code, name, name_urdu, blend_code))
        prod_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        c.execute("INSERT OR IGNORE INTO id_counters (entity, last_num) VALUES ('sku', 0)")
        c.execute("UPDATE id_counters SET last_num=last_num+1 WHERE entity='sku'")
        num = c.execute("SELECT last_num FROM id_counters WHERE entity='sku'").fetchone()[0]
        sku_code = f"SP-SKU-{num:04d}"
        c.execute("""
            INSERT INTO product_variants (sku_code, product_id, pack_size_id, active_flag)
            VALUES (?,?,?,1)
        """, (sku_code, prod_id, pack_size_id))

        c.commit()
    finally:
        c.close()

    save_db()
    load_ref()
    return {'code': full_code, 'name': name, 'packSize': ps['label'], 'sku': sku_code}


def deactivate_product(code):
    prod = qry1("SELECT id, name FROM products WHERE code=?", (code,))
    if not prod:
        raise ValueError(f"Product '{code}' not found")

    # Check for open (UNPAID/PARTIAL) invoices referencing this product
    open_orders = qry("""
        SELECT COUNT(*) as cnt FROM invoice_items ii
        JOIN invoices inv ON inv.id = ii.invoice_id
        JOIN product_variants pv ON pv.id = ii.product_variant_id
        WHERE pv.product_id=? AND inv.status IN ('UNPAID','PARTIAL')
    """, (prod['id'],))
    if open_orders and open_orders[0]['cnt'] > 0:
        raise ValueError(f"Cannot remove: {open_orders[0]['cnt']} open invoice(s) reference this product. Close them first.")

    c = _conn()
    try:
        c.execute("UPDATE products SET active=0 WHERE id=?", (prod['id'],))
        c.execute("UPDATE product_variants SET active_flag=0 WHERE product_id=?", (prod['id'],))
        c.commit()
    finally:
        c.close()

    save_db()
    load_ref()
    return {'removed': code, 'name': prod['name']}


def update_product(code, data):
    """Edit product name, Urdu name, or blend code. Code is immutable."""
    prod = qry1("SELECT * FROM products WHERE code=?", (code,))
    if not prod:
        raise ValueError(f"Product not found: {code}")
    set_parts, vals = [], []
    if 'name' in data and str(data['name']).strip():
        set_parts.append("name=?"); vals.append(str(data['name']).strip())
    if 'nameUrdu' in data:
        set_parts.append("name_urdu=?"); vals.append(str(data.get('nameUrdu') or '').strip())
    if 'blendCode' in data:
        set_parts.append("blend_code=?"); vals.append(str(data.get('blendCode') or '').strip())
    if not set_parts:
        return dict(prod)
    vals.append(code)
    c = _conn()
    try:
        c.execute(f"UPDATE products SET {', '.join(set_parts)} WHERE code=?", vals)
        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('products',?,'UPDATE',?)
        """, (code, json.dumps({k: data[k] for k in data})))
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    load_ref()
    return qry1("SELECT * FROM products WHERE code=?", (code,))


def deactivate_variant(variant_id):
    """Remove a single pack-size SKU (treated as an independent product)."""
    v = qry1("""
        SELECT pv.id, pv.sku_code, pv.product_id, p.name, ps.label as pack_size
        FROM product_variants pv
        JOIN products p    ON p.id  = pv.product_id
        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE pv.id=?
    """, (variant_id,))
    if not v:
        raise ValueError(f"SKU id {variant_id} not found")

    # Block if open invoices reference this SKU
    open_cnt = qry1("""
        SELECT COUNT(*) as cnt FROM invoice_items ii
        JOIN invoices inv ON inv.id = ii.invoice_id
        WHERE ii.product_variant_id=? AND inv.status IN ('UNPAID','PARTIAL')
    """, (variant_id,))
    if open_cnt and open_cnt['cnt'] > 0:
        raise ValueError(f"Cannot remove: {open_cnt['cnt']} open invoice(s) use this SKU. Close them first.")

    c = _conn()
    try:
        # Deactivate the variant
        c.execute("UPDATE product_variants SET active_flag=0 WHERE id=?", (variant_id,))
        # If no active variants remain, deactivate the parent product too
        remaining = c.execute(
            "SELECT COUNT(*) FROM product_variants WHERE product_id=? AND active_flag=1",
            (v['product_id'],)
        ).fetchone()[0]
        if remaining == 0:
            c.execute("UPDATE products SET active=0 WHERE id=?", (v['product_id'],))
        c.commit()
    finally:
        c.close()

    save_db()
    load_ref()
    return {'removed': v['sku_code'], 'name': v['name'], 'packSize': v['pack_size']}


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — SUPPLIERS
# ═══════════════════════════════════════════════════════════════════

def _migrate_supplier_bills_void():
    """Migration: add voided_at/voided_by/void_note columns and VOID status to supplier_bills.
    SQLite doesn't allow ALTER TABLE to change a CHECK constraint, so we recreate the table."""
    c = _conn()
    try:
        c.execute("PRAGMA foreign_keys=OFF")
        # Check if VOID is already in the constraint
        tbl = c.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='supplier_bills'"
        ).fetchone()
        if not tbl:
            return  # table doesn't exist yet — schema creation handles it
        tbl_sql = tbl[0] or ''
        cols = [row[1] for row in c.execute("PRAGMA table_info(supplier_bills)")]

        needs_rebuild = "'VOID'" not in tbl_sql and '"VOID"' not in tbl_sql
        needs_voided_cols = 'voided_at' not in cols

        if needs_rebuild:
            # Recreate table with VOID in CHECK constraint + voided columns
            c.execute("""CREATE TABLE IF NOT EXISTS supplier_bills_new (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                bill_number  TEXT NOT NULL UNIQUE,
                supplier_id  INTEGER NOT NULL REFERENCES suppliers(id),
                bill_date    TEXT NOT NULL,
                due_date     TEXT NOT NULL,
                status       TEXT DEFAULT 'UNPAID'
                             CHECK(status IN ('UNPAID','PARTIAL','PAID','VOID')),
                notes        TEXT DEFAULT '',
                total_amount REAL DEFAULT 0,
                supplier_ref TEXT DEFAULT '',
                created_at   TEXT DEFAULT (datetime('now')),
                voided_at    TEXT DEFAULT NULL,
                voided_by    TEXT DEFAULT NULL,
                void_note    TEXT DEFAULT NULL
            )""")
            existing_cols = [row[1] for row in c.execute("PRAGMA table_info(supplier_bills)")]
            void_cols = 'voided_at' if 'voided_at' in existing_cols else 'NULL'
            c.execute(f"""INSERT INTO supplier_bills_new
                SELECT id, bill_number, supplier_id, bill_date, due_date, status,
                       notes, total_amount, COALESCE(supplier_ref,''), created_at,
                       {'voided_at' if 'voided_at' in existing_cols else 'NULL'},
                       {'voided_by' if 'voided_by' in existing_cols else 'NULL'},
                       {'void_note' if 'void_note' in existing_cols else 'NULL'}
                FROM supplier_bills""")
            c.execute("DROP TABLE supplier_bills")
            c.execute("ALTER TABLE supplier_bills_new RENAME TO supplier_bills")
            print("  ✓ supplier_bills: migrated — added VOID status + voided columns")
        elif needs_voided_cols:
            # Table has VOID but missing the voided columns — just add them
            for col in ['voided_at', 'voided_by', 'void_note']:
                if col not in cols:
                    try:
                        c.execute(f"ALTER TABLE supplier_bills ADD COLUMN {col} TEXT DEFAULT NULL")
                    except Exception:
                        pass
            print("  ✓ supplier_bills: added voided_at/voided_by/void_note columns")
        c.commit()
    except Exception as e:
        print(f"  ⚠ supplier_bills migration error: {e}")
        try: c.rollback()
        except: pass
    finally:
        try: c.execute("PRAGMA foreign_keys=ON")
        except: pass
        c.close()


def _migrate_change_log_void_action():
    """Migration: widen change_log CHECK constraint to include 'VOID'.
    SQLite doesn't allow ALTER TABLE to change a CHECK constraint, so we recreate the table."""
    c = _conn()
    try:
        c.execute("PRAGMA foreign_keys=OFF")
        tbl = c.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='change_log'"
        ).fetchone()
        if not tbl:
            return  # table doesn't exist yet — schema creation handles it
        tbl_sql = tbl[0] or ''
        if "'VOID'" in tbl_sql or '"VOID"' in tbl_sql:
            return  # already has VOID — nothing to do
        # Recreate with the wider constraint
        c.execute("""CREATE TABLE IF NOT EXISTS change_log_new (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            record_id  TEXT NOT NULL,
            action     TEXT NOT NULL CHECK(action IN ('INSERT','UPDATE','DELETE','VOID')),
            old_value  TEXT DEFAULT NULL,
            new_value  TEXT DEFAULT NULL,
            changed_by TEXT DEFAULT 'system',
            timestamp  TEXT DEFAULT (datetime('now'))
        )""")
        c.execute("""INSERT INTO change_log_new
            SELECT id, table_name, record_id, action, old_value, new_value, changed_by, timestamp
            FROM change_log""")
        c.execute("DROP TABLE change_log")
        c.execute("ALTER TABLE change_log_new RENAME TO change_log")
        c.commit()
        print("  ✓ change_log: widened CHECK constraint to include VOID action")
    except Exception as e:
        print(f"  ⚠ change_log migration error: {e}")
        try: c.rollback()
        except: pass
    finally:
        try: c.execute("PRAGMA foreign_keys=ON")
        except: pass
        c.close()


def _migrate_customer_type_wholesale():
    """Migration: add WHOLESALE to customer_type CHECK constraint (idempotent).
    SQLite doesn't allow ALTER COLUMN — recreates the customers table with updated CHECK."""
    c = _conn()
    try:
        schema = c.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='customers'"
        ).fetchone()
        if schema and 'WHOLESALE' in schema[0]:
            return  # already done
        c.execute("PRAGMA foreign_keys=OFF")
        c.execute("""
            CREATE TABLE IF NOT EXISTS customers_new (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                code               TEXT NOT NULL UNIQUE,
                account_number     TEXT DEFAULT NULL,
                name               TEXT NOT NULL,
                customer_type      TEXT NOT NULL DEFAULT 'RETAIL'
                                   CHECK(customer_type IN ('RETAIL','DIRECT','WHOLESALE')),
                category           TEXT DEFAULT '',
                city               TEXT DEFAULT '',
                address            TEXT DEFAULT '',
                phone              TEXT DEFAULT '',
                email              TEXT DEFAULT '',
                default_pack       TEXT DEFAULT '50g',
                payment_terms_days INTEGER DEFAULT 30,
                credit_limit       REAL DEFAULT 0,
                active             INTEGER DEFAULT 1,
                created_at         TEXT DEFAULT (date('now'))
            )
        """)
        c.execute("""
            INSERT INTO customers_new
            SELECT id, code, account_number, name,
                   CASE WHEN customer_type IN ('RETAIL','DIRECT','WHOLESALE')
                        THEN customer_type ELSE 'RETAIL' END,
                   category, city, address, phone, email, default_pack,
                   payment_terms_days, credit_limit, active, created_at
            FROM customers
        """)
        c.execute("DROP TABLE customers")
        c.execute("ALTER TABLE customers_new RENAME TO customers")
        c.commit()
        print("  ✓ Migration: customer_type CHECK updated — WHOLESALE added")
    except Exception as e:
        print(f"  ⚠ _migrate_customer_type_wholesale error: {e}")
        try: c.rollback()
        except: pass
    finally:
        try: c.execute("PRAGMA foreign_keys=ON")
        except: pass
        c.close()
    save_db()


def _ensure_b2b_order_columns():
    """Idempotent: add out_of_route + idempotency_key columns to customer_orders for B2B portal."""
    c = _conn()
    try:
        for sql in [
            "ALTER TABLE customer_orders ADD COLUMN out_of_route    INTEGER DEFAULT 0",
            "ALTER TABLE customer_orders ADD COLUMN idempotency_key TEXT    DEFAULT NULL",
        ]:
            try:
                c.execute(sql)
            except Exception:
                pass  # column already exists
        c.commit()
        print("  ✓ B2B columns: out_of_route + idempotency_key ready")
    finally:
        c.close()
    save_db()


def _ensure_supplier_zone_col():
    """Safe migration: add zone_id column to suppliers if not present.
    Also syncs the supplier id_counter to the actual max existing supplier number."""
    c = _conn()
    try:
        c.execute("ALTER TABLE suppliers ADD COLUMN zone_id INTEGER REFERENCES zones(id)")
        c.commit()
        print("  ✓ Suppliers: added zone_id column")
    except Exception:
        pass  # column already exists
    finally:
        c.close()
    # Sync counter so it's never behind existing data (prevents UNIQUE constraint failures)
    _sync_counter_to_max('supplier', 'suppliers', 'code', 'SUP-')


def _suppliers_with_zones():
    _ensure_supplier_zone_col()
    return qry("""
        SELECT s.*, z.name as zone_name
        FROM suppliers s
        LEFT JOIN zones z ON z.id = s.zone_id
        WHERE s.active_flag=1
        ORDER BY s.name
    """)


def create_supplier(data):
    validate_fields(data, [
        {'field': 'name',    'label': 'Supplier name', 'type': 'str', 'min': 2, 'max': 120},
        {'field': 'phone',   'label': 'Phone',         'required': False, 'type': 'str', 'max': 30},
        {'field': 'email',   'label': 'Email',         'required': False, 'type': 'str', 'max': 120},
        {'field': 'city',    'label': 'City',          'required': False, 'type': 'str', 'max': 60},
    ])
    _ensure_supplier_zone_col()
    _sync_counter_to_max('supplier', 'suppliers', 'code', 'SUP-')
    # Generate SUP-NNN format (not SP-SUP-* like next_id would produce)
    _raw = next_id('supplier', 'SUP')  # increments counter atomically
    _num = int(_raw.split('-')[-1])
    code = f"SUP-{_num:03d}"
    zone_id = data.get('zoneId') or None
    if zone_id is not None:
        zone_id = int(zone_id)
    ops  = [("""
        INSERT INTO suppliers (code, name, contact, phone, email, city, address, zone_id)
        VALUES (?,?,?,?,?,?,?,?)
    """, (code, data['name'].strip(),
          data.get('contact',''), data.get('phone',''),
          data.get('email',''), data.get('city',''),
          data.get('address',''), zone_id))]
    audit_log(ops, 'suppliers', code, 'INSERT', new_val=data)
    run_many(ops)
    load_ref()
    return qry1("SELECT * FROM suppliers WHERE code=?", (code,))


def update_supplier(sup_id, data):
    _ensure_supplier_zone_col()
    existing = qry1("SELECT * FROM suppliers WHERE id=?", (sup_id,))
    if not existing:
        raise ValueError(f"Supplier not found: {sup_id}")
    mapping = {'name':'name','contact':'contact','phone':'phone',
               'email':'email','city':'city','address':'address','active_flag':'active_flag'}
    set_parts, vals = [], []
    for js_key, db_col in mapping.items():
        if js_key in data:
            set_parts.append(f"{db_col}=?")
            vals.append(data[js_key])
    # Handle zone_id explicitly (can be null to clear it)
    if 'zoneId' in data:
        set_parts.append("zone_id=?")
        vals.append(int(data['zoneId']) if data['zoneId'] else None)
    if not set_parts:
        return existing
    vals.append(sup_id)
    ops = [(f"UPDATE suppliers SET {', '.join(set_parts)} WHERE id=?", vals)]
    audit_log(ops, 'suppliers', str(sup_id), 'UPDATE', old_val=dict(existing), new_val=data)
    run_many(ops)
    load_ref()
    return qry1("SELECT * FROM suppliers WHERE id=?", (sup_id,))


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — SALES + INVOICES (AR)
# ═══════════════════════════════════════════════════════════════════

def create_sale(data):
    """
    Create a sale record and auto-generate an invoice for it.
    data: {custCode, productCode, packSize, qty, unitPrice, saleDate, notes}
    """
    validate_fields(data, [
        {'field': 'custCode',    'label': 'Customer',      'type': 'str'},
        {'field': 'productCode', 'label': 'Product code',  'type': 'str'},
        {'field': 'packSize',    'label': 'Pack size',     'type': 'str'},
        {'field': 'qty',         'label': 'Quantity',      'type': 'int', 'min': 1},
        {'field': 'unitPrice',   'label': 'Unit price',    'type': 'float', 'min': 0},
        {'field': 'saleDate',    'label': 'Sale date',     'required': False, 'type': 'date'},
    ])
    cust = ref['cust_by_code'].get(data.get('custCode', ''))
    if not cust:
        raise ValueError(f"Customer not found: {data.get('custCode')}")

    var = ref['var_by_sku'].get((data.get('productCode',''), data.get('packSize','')))
    if not var:
        raise ValueError(f"Product variant not found: {data.get('productCode')}/{data.get('packSize')}")

    qty        = int(data.get('qty', 0))
    unit_price = r2(data.get('unitPrice', 0))
    if qty <= 0:
        raise ValueError("Quantity must be positive")
    if unit_price < 0:
        raise ValueError("Unit price cannot be negative")

    # Check finished goods availability
    fg_stock = get_finished_stock_map()
    avail = fg_stock.get(var['id'], 0)
    if qty > avail:
        raise ValueError(f"Insufficient finished goods: {avail:.0f} units available, {qty} requested")

    sale_date  = data.get('saleDate', today())
    total      = r2(qty * unit_price)
    line_total = total  # pre-GST

    # Compute due_date
    terms = int(cust.get('payment_terms_days', 30))
    try:
        due_date = (date.fromisoformat(sale_date) + timedelta(days=terms)).isoformat()
    except Exception:
        due_date = sale_date

    # Look up mfg_cost for gross margin
    mfg_type = qry1("SELECT id FROM price_types WHERE code='mfg_cost'")
    cogs_price = 0.0
    if mfg_type:
        cp = qry1("""
            SELECT price FROM product_prices
            WHERE product_variant_id=? AND price_type_id=? AND active_flag=1
            ORDER BY effective_from DESC LIMIT 1
        """, (var['id'], mfg_type['id']))
        if cp:
            cogs_price = r2(cp['price'] * qty)

    gross_profit = r2(total - cogs_price)

    _sync_counter_to_max('sale',    'sales',    'sale_id',        'SP-SALE-')
    _sync_counter_to_max('invoice', 'invoices', 'invoice_number', 'SP-INV-')
    sale_id = next_id('sale', 'SALE')
    inv_num = next_id('invoice', 'INV')

    c = _conn()
    try:
        # Insert invoice first (need its rowid for FK)
        c.execute("""
            INSERT INTO invoices (invoice_number, customer_id, invoice_date, due_date, status)
            VALUES (?,?,?,?,'UNPAID')
        """, (inv_num, cust['id'], sale_date, due_date))
        inv_db_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Insert sale
        c.execute("""
            INSERT INTO sales
                (sale_id, sale_date, customer_id, cust_code, cust_name, customer_type,
                 product_variant_id, product_code, product_name, pack_size,
                 qty, unit_price, total, cogs, gross_profit, invoice_id, notes)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (sale_id, sale_date, cust['id'], cust['code'], cust['name'],
              cust.get('customer_type','RETAIL'),
              var['id'], var['product_code'], var['product_name'], var['pack_size'],
              qty, unit_price, total, cogs_price, gross_profit, inv_db_id,
              data.get('notes','')))

        # Insert invoice item
        c.execute("""
            INSERT INTO invoice_items
                (invoice_id, product_variant_id, product_code, product_name,
                 pack_size, quantity, unit_price, line_total)
            VALUES (?,?,?,?,?,?,?,?)
        """, (inv_db_id, var['id'], var['product_code'], var['product_name'],
              var['pack_size'], qty, unit_price, line_total))

        # Audit
        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('sales',?,'INSERT',?)
        """, (sale_id, json.dumps({'custCode': data.get('custCode'),
              'productCode': data.get('productCode'), 'qty': qty, 'total': total})))

        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()

    return {'saleId': sale_id, 'invoiceNumber': inv_num,
            'total': total, 'invoiceId': inv_db_id}

def _enforce_credit_limit(cust_id: int, new_invoice_total: float):
    """
    Hard-block guard — raises ValueError if adding new_invoice_total would push the
    customer over their credit limit.  Called before any invoice is written.
    No-op when credit_limit = 0 (unlimited).
    """
    cust = qry1("SELECT name, credit_limit FROM customers WHERE id=?", (cust_id,))
    if not cust:
        return
    credit_limit = float(cust.get('credit_limit') or 0)
    if credit_limit <= 0:
        return   # 0 = unlimited

    ar_row = qry1("""
        SELECT COALESCE(SUM(
            COALESCE((SELECT SUM(quantity*unit_price) FROM invoice_items WHERE invoice_id=i.id), 0)
            - COALESCE((SELECT SUM(allocated_amount) FROM payment_allocations WHERE invoice_id=i.id), 0)
        ), 0) AS balance
        FROM invoices i
        WHERE i.customer_id=? AND i.status IN ('UNPAID','PARTIAL')
    """, (cust_id,))
    ar_balance = float((ar_row or {}).get('balance', 0))

    if ar_balance + new_invoice_total > credit_limit:
        raise ValueError(
            f"Credit limit exceeded for {cust['name']}: "
            f"outstanding AR {fmtpkr(ar_balance)} + this invoice {fmtpkr(new_invoice_total)} "
            f"= {fmtpkr(ar_balance + new_invoice_total)} — limit is {fmtpkr(credit_limit)}"
        )


def create_multi_sale(data):
    """
    Create a multi-line sale and a single invoice covering all lines.
    data: {custCode, saleDate, notes, lines: [{productCode, packSize, qty, unitPrice}]}
    """
    cust = ref['cust_by_code'].get(data.get('custCode', ''))
    if not cust:
        raise ValueError(f"Customer not found: {data.get('custCode')}")

    lines = data.get('lines', [])
    if not lines:
        raise ValueError("At least one line item is required")

    sale_date = data.get('saleDate', today())
    notes     = data.get('notes', '')
    terms     = int(cust.get('payment_terms_days', 30))
    try:
        due_date = (date.fromisoformat(sale_date) + timedelta(days=terms)).isoformat()
    except Exception:
        due_date = sale_date

    mfg_type = qry1("SELECT id FROM price_types WHERE code='mfg_cost'")
    fg_stock = get_finished_stock_map()

    # Validate all lines before writing anything
    resolved = []
    for i, line in enumerate(lines):
        var = ref['var_by_sku'].get((line.get('productCode',''), line.get('packSize','')))
        if not var:
            raise ValueError(f"Line {i+1}: Product variant not found: {line.get('productCode')}/{line.get('packSize')}")
        qty        = int(line.get('qty', 0))
        unit_price = r2(line.get('unitPrice', 0))
        if qty <= 0:
            raise ValueError(f"Line {i+1}: Quantity must be positive")
        if unit_price < 0:
            raise ValueError(f"Line {i+1}: Unit price cannot be negative")
        avail = fg_stock.get(var['id'], 0)
        if qty > avail:
            raise ValueError(f"Line {i+1} ({var['product_name']} {var['pack_size']}): Insufficient stock — {avail:.0f} available, {qty} requested")
        cogs_price = 0.0
        if mfg_type:
            cp = qry1("""SELECT price FROM product_prices
                         WHERE product_variant_id=? AND price_type_id=? AND active_flag=1
                         ORDER BY effective_from DESC LIMIT 1""", (var['id'], mfg_type['id']))
            if cp:
                cogs_price = r2(cp['price'] * qty)
        line_total   = r2(qty * unit_price)
        gross_profit = r2(line_total - cogs_price)
        resolved.append({
            'var': var, 'qty': qty, 'unit_price': unit_price,
            'line_total': line_total, 'cogs': cogs_price, 'gross_profit': gross_profit
        })

    invoice_total = r2(sum(r['line_total'] for r in resolved))

    # ── Credit limit hard block ───────────────────────────────────────────
    cust_full = qry1("SELECT id FROM customers WHERE code=?", (data.get('custCode',''),))
    if cust_full:
        _enforce_credit_limit(cust_full['id'], invoice_total)

    # Generate all IDs BEFORE opening the main transaction.
    # next_id() opens its own connection; calling it inside an open transaction
    # causes a write-lock deadlock (SQLite only allows one writer at a time).
    _sync_counter_to_max('invoice', 'invoices', 'invoice_number', 'SP-INV-')
    _sync_counter_to_max('sale',    'sales',    'sale_id',        'SP-SALE-')
    inv_num  = next_id('invoice', 'INV')
    sale_ids = [next_id('sale', 'SALE') for _ in resolved]

    c = _conn()
    try:
        # Single invoice for all lines
        c.execute("""INSERT INTO invoices (invoice_number, customer_id, invoice_date, due_date, status)
                     VALUES (?,?,?,?,'UNPAID')""",
                  (inv_num, cust['id'], sale_date, due_date))
        inv_db_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        for sale_id, r in zip(sale_ids, resolved):
            var = r['var']
            c.execute("""INSERT INTO sales
                (sale_id, sale_date, customer_id, cust_code, cust_name, customer_type,
                 product_variant_id, product_code, product_name, pack_size,
                 qty, unit_price, total, cogs, gross_profit, invoice_id, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (sale_id, sale_date, cust['id'], cust['code'], cust['name'],
                 cust.get('customer_type','RETAIL'),
                 var['id'], var['product_code'], var['product_name'], var['pack_size'],
                 r['qty'], r['unit_price'], r['line_total'], r['cogs'], r['gross_profit'],
                 inv_db_id, notes))
            c.execute("""INSERT INTO invoice_items
                (invoice_id, product_variant_id, product_code, product_name,
                 pack_size, quantity, unit_price, line_total)
                VALUES (?,?,?,?,?,?,?,?)""",
                (inv_db_id, var['id'], var['product_code'], var['product_name'],
                 var['pack_size'], r['qty'], r['unit_price'], r['line_total']))

        c.execute("""INSERT INTO change_log (table_name, record_id, action, new_value)
                     VALUES ('sales',?,'INSERT',?)""",
                  (inv_num, json.dumps({'custCode': data.get('custCode'),
                   'lines': len(resolved), 'total': invoice_total})))
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()

    return {'invoiceNumber': inv_num, 'invoiceId': inv_db_id,
            'total': invoice_total, 'saleIds': sale_ids, 'lineCount': len(resolved)}


# ═══════════════════════════════════════════════════════════════════
#  CUSTOMER ORDERS  (Sales Order → Production → Invoice flow)
# ═══════════════════════════════════════════════════════════════════

def _order_status(order_id):
    """Compute status from item quantities. Never reads the stored status column."""
    items = qry("SELECT qty_ordered, qty_invoiced FROM customer_order_items WHERE order_id=?", (order_id,))
    if not items:
        return 'draft'
    total_ordered  = sum(i['qty_ordered']  for i in items)
    total_invoiced = sum(i['qty_invoiced'] for i in items)
    if total_invoiced == 0:
        return 'confirmed'
    if total_invoiced < total_ordered:
        return 'partially_invoiced'
    return 'invoiced'


def _order_detail(order_id):
    """Return full order dict with items, linked WOs, linked invoices."""
    order = qry1("""
        SELECT co.*, c.name as customer_name, c.code as customer_code,
               c.phone as customer_phone, c.city as customer_city
        FROM customer_orders co
        JOIN customers c ON c.id = co.customer_id
        WHERE co.id = ?
    """, (order_id,))
    if not order:
        return None

    items = qry("""
        SELECT coi.*, p.name as product_name, p.code as product_code,
               ps.label as pack_size, pv.sku_code
        FROM customer_order_items coi
        JOIN product_variants pv ON pv.id = coi.product_variant_id
        JOIN products p           ON p.id  = pv.product_id
        JOIN pack_sizes ps        ON ps.id = pv.pack_size_id
        WHERE coi.order_id = ?
        ORDER BY coi.id
    """, (order_id,))

    # Linked work orders
    wos = qry("""
        SELECT wo.id, wo.wo_number, wo.qty_units, wo.status, wo.target_date,
               wo.customer_order_item_id,
               p.name as product_name, ps.label as pack_size
        FROM work_orders wo
        JOIN product_variants pv ON pv.id = wo.product_variant_id
        JOIN products p           ON p.id  = pv.product_id
        JOIN pack_sizes ps        ON ps.id = pv.pack_size_id
        WHERE wo.customer_order_id = ?
        ORDER BY wo.id
    """, (order_id,))

    # Linked invoices
    invs = qry("""
        SELECT inv.id, inv.invoice_number, inv.invoice_date, inv.status,
               COALESCE(SUM(ii.line_total), 0) as subtotal
        FROM invoices inv
        LEFT JOIN invoice_items ii ON ii.invoice_id = inv.id
        WHERE inv.customer_order_id = ?
        GROUP BY inv.id
        ORDER BY inv.id
    """, (order_id,))

    # Recompute live status
    skip_recompute = ('draft', 'cancelled', 'pending_review', 'rejected', 'expired')
    computed_status = _order_status(order_id) if order['status'] not in skip_recompute else order['status']
    order['status']   = computed_status
    order['items']    = items
    order['workOrders'] = wos
    order['invoices'] = invs

    # Attach review queue metadata
    hold_row = qry1("SELECT hold_expires_at, is_expired FROM order_hold_expiry WHERE order_id=?", (order_id,))
    if hold_row and not hold_row['is_expired']:
        order['holdExpiresAt'] = hold_row['hold_expires_at']
        try:
            exp = datetime.strptime(hold_row['hold_expires_at'], '%Y-%m-%dT%H:%M:%S')
            diff = (exp - datetime.utcnow()).total_seconds()
            order['holdRemainingSeconds'] = max(0, int(diff))
        except Exception:
            order['holdRemainingSeconds'] = None
    else:
        order['holdExpiresAt'] = None
        order['holdRemainingSeconds'] = None

    return order


def list_customer_orders(status_filter=None):
    """Return all orders with summary counts, newest first."""
    sql = """
        SELECT co.id, co.order_number, co.order_date, co.required_date,
               co.status, co.notes, co.created_at,
               c.name as customer_name, c.code as customer_code,
               COUNT(DISTINCT coi.id)  as item_count,
               COALESCE(SUM(coi.qty_ordered), 0)  as total_qty,
               COALESCE(SUM(coi.qty_invoiced), 0) as invoiced_qty,
               COALESCE(SUM(coi.line_total), 0)   as order_value,
               COUNT(DISTINCT wo.id)  as wo_count,
               COUNT(DISTINCT inv.id) as invoice_count
        FROM customer_orders co
        JOIN customers c ON c.id = co.customer_id
        LEFT JOIN customer_order_items coi ON coi.order_id = co.id
        LEFT JOIN work_orders wo  ON wo.customer_order_id = co.id
        LEFT JOIN invoices inv    ON inv.customer_order_id = co.id
    """
    params = ()
    if status_filter:
        sql += " WHERE co.status = ?"
        params = (status_filter,)
    sql += " GROUP BY co.id ORDER BY co.id DESC"
    return qry(sql, params)


def _check_order_stock_warnings(resolved):
    """
    Check finished-goods availability for a list of resolved order lines.
    Returns a list of warning strings — one per under-stocked line.
    Does NOT raise; callers decide whether to block or warn.
    """
    fg_stock  = get_finished_stock_map()
    warnings  = []
    for r in resolved:
        vid   = r['var']['id']
        avail = fg_stock.get(vid, 0)
        if r['qty'] > avail:
            short = r['qty'] - avail
            label = f"{r['var'].get('product_name','?')} {r['var'].get('pack_size','')}"
            warnings.append(
                f"⚠ STOCK WARNING — {label}: {avail:.0f} units available, "
                f"{r['qty']} ordered (shortfall: {short:.0f} units). "
                f"Run production before invoicing."
            )
    return warnings


def create_customer_order(data):
    """
    Create a new customer order (status=draft).
    data: {custCode, orderDate, requiredDate, notes, lines:[{productCode, packSize, qty, unitPrice}]}
    Stock shortfalls produce warnings in the response but do NOT block the order.
    """
    validate_fields(data, [
        {'field': 'custCode',     'label': 'Customer',       'type': 'str'},
        {'field': 'orderDate',    'label': 'Order date',     'required': False, 'type': 'date'},
        {'field': 'requiredDate', 'label': 'Required date',  'required': False, 'type': 'date'},
    ])
    cust = ref['cust_by_code'].get(data.get('custCode', ''))
    if not cust:
        raise ValueError(f"Customer not found: {data.get('custCode')}")
    lines = data.get('lines', [])
    if not lines:
        raise ValueError("At least one line item is required")

    order_date    = data.get('orderDate', today())
    required_date = data.get('requiredDate', '')
    notes         = data.get('notes', '')
    _sync_counter_to_max('customer_order', 'customer_orders', 'order_number', 'SP-ORD-')
    order_number  = next_id('customer_order', 'ORD')

    # Validate + resolve all lines before writing
    resolved = []
    for i, line in enumerate(lines):
        var = ref['var_by_sku'].get((line.get('productCode', ''), line.get('packSize', '')))
        if not var:
            raise ValueError(f"Line {i+1}: variant not found: {line.get('productCode')}/{line.get('packSize')}")
        qty = int(line.get('qty', 0))
        if qty <= 0:
            raise ValueError(f"Line {i+1}: qty must be positive")
        unit_price = r2(line.get('unitPrice', 0))
        resolved.append({
            'var': var, 'qty': qty,
            'unit_price': unit_price,
            'line_total': r2(qty * unit_price)
        })

    # Stock availability check — warns but never blocks order creation
    stock_warnings = _check_order_stock_warnings(resolved)

    c = _conn()
    try:
        c.execute("""
            INSERT INTO customer_orders
                (order_number, customer_id, order_date, required_date, status, notes)
            VALUES (?, ?, ?, ?, 'draft', ?)
        """, (order_number, cust['id'], order_date, required_date, notes))
        order_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        for r in resolved:
            c.execute("""
                INSERT INTO customer_order_items
                    (order_id, product_variant_id, qty_ordered, unit_price, line_total)
                VALUES (?, ?, ?, ?, ?)
            """, (order_id, r['var']['id'], r['qty'], r['unit_price'], r['line_total']))

        audit_log([], 'customer_orders', order_number, 'CREATE', new_val=data)
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()
    return {
        'orderId': order_id,
        'orderNumber': order_number,
        'stockWarnings': stock_warnings   # empty list = all stock available
    }


def update_customer_order(order_id, data):
    """
    Amend a customer order safely.

    Allowed statuses:
      draft              — header + full line edit (add / change / remove)
      confirmed          — header + line amendment (safe in-place update)
      partially_invoiced — header only (lines locked)

    Line amendment rules (confirmed orders):
      - INCREASE qty: always allowed — user should then create a new WO for the delta
      - DECREASE qty: only if new_qty >= qty_in_production + qty_invoiced
      - REMOVE a line: only if qty_in_production == 0 AND qty_invoiced == 0
      - ADD a new line: always allowed

    Never does DELETE+INSERT on confirmed/partially_invoiced orders to avoid
    orphaning work orders that reference customer_order_item_id.
    """
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")

    editable_statuses = ('draft', 'confirmed', 'partially_invoiced')
    if order['status'] not in editable_statuses:
        raise ValueError(f"Order is {order['status']} — cannot be edited")

    lines_locked = order['status'] == 'partially_invoiced'

    # Header fields (partial — only overwrite what's supplied)
    required_date = data.get('requiredDate', order['required_date'])
    notes         = data.get('notes', order['notes'] or '')

    lines = data.get('lines')  # None = leave lines unchanged

    if lines is not None and lines_locked:
        raise ValueError(
            "Line items cannot be changed once invoicing has started. "
            "Only notes and required delivery date can be updated."
        )

    # Resolve submitted lines
    resolved = []
    if lines is not None:
        if not lines:
            raise ValueError("At least one line item is required")
        for i, line in enumerate(lines):
            var = ref['var_by_sku'].get((line.get('productCode', ''), line.get('packSize', '')))
            if not var:
                raise ValueError(f"Line {i+1}: variant not found: {line.get('productCode')}/{line.get('packSize')}")
            qty = int(line.get('qty', 0))
            if qty <= 0:
                raise ValueError(f"Line {i+1}: qty must be positive")
            unit_price = r2(line.get('unitPrice', 0))
            resolved.append({
                'var': var, 'qty': qty,
                'unit_price': unit_price,
                'line_total': r2(qty * unit_price)
            })

    # Stock availability check — warns but never blocks
    stock_warnings = _check_order_stock_warnings(resolved) if resolved else []

    # Track which items increased so frontend can prompt for new WO
    qty_increases = []   # [{productName, packSize, delta}]

    c = _conn()
    try:
        c.execute("""
            UPDATE customer_orders
            SET required_date=?, notes=?, updated_at=datetime('now')
            WHERE id=?
        """, (required_date, notes, order_id))

        if lines is not None:
            existing = {
                row['product_variant_id']: row
                for row in qry("SELECT * FROM customer_order_items WHERE order_id=?", (order_id,))
            }
            submitted_vids = set()

            for r in resolved:
                vid  = r['var']['id']
                submitted_vids.add(vid)
                ex   = existing.get(vid)

                if ex:
                    # --- UPDATE existing line ---
                    committed = ex['qty_in_production'] + ex['qty_invoiced']
                    if r['qty'] < committed:
                        var_label = f"{r['var'].get('product_name','?')} {r['var'].get('pack_size','')}"
                        raise ValueError(
                            f"{var_label}: cannot reduce to {r['qty']} — "
                            f"{ex['qty_in_production']} in production, {ex['qty_invoiced']} invoiced "
                            f"(minimum: {committed})"
                        )
                    if r['qty'] > ex['qty_ordered']:
                        qty_increases.append({
                            'itemId':      ex['id'],
                            'productName': r['var'].get('product_name', ''),
                            'packSize':    r['var'].get('pack_size', ''),
                            'oldQty':      ex['qty_ordered'],
                            'newQty':      r['qty'],
                            'delta':       r['qty'] - ex['qty_ordered']
                        })
                    c.execute("""
                        UPDATE customer_order_items
                        SET qty_ordered=?, unit_price=?, line_total=?
                        WHERE id=?
                    """, (r['qty'], r['unit_price'], r['line_total'], ex['id']))
                else:
                    # --- INSERT new line ---
                    c.execute("""
                        INSERT INTO customer_order_items
                            (order_id, product_variant_id, qty_ordered, unit_price, line_total)
                        VALUES (?, ?, ?, ?, ?)
                    """, (order_id, vid, r['qty'], r['unit_price'], r['line_total']))

            # Remove lines not in submission (only if safe to do so)
            for vid, ex in existing.items():
                if vid not in submitted_vids:
                    committed = ex['qty_in_production'] + ex['qty_invoiced']
                    if committed > 0:
                        var_info = qry1("""
                            SELECT p.name, ps.label as pack FROM product_variants pv
                            JOIN products p ON p.id=pv.product_id
                            JOIN pack_sizes ps ON ps.id=pv.pack_size_id
                            WHERE pv.id=?
                        """, (vid,))
                        label = f"{var_info['name']} {var_info['pack']}" if var_info else f"Variant {vid}"
                        raise ValueError(
                            f"Cannot remove '{label}' — it has {ex['qty_in_production']} unit(s) "
                            f"in production and {ex['qty_invoiced']} unit(s) invoiced."
                        )
                    c.execute("DELETE FROM customer_order_items WHERE id=?", (ex['id'],))

        audit_log([], 'customer_orders', order['order_number'], 'UPDATE',
                  old_val={'required_date': order['required_date'], 'notes': order['notes']},
                  new_val=data)
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()
    detail = _order_detail(order_id)
    detail['stockWarnings'] = stock_warnings
    detail['qtyIncreases']  = qty_increases   # frontend uses this to suggest new WOs
    return detail


def add_customer_order_item(order_id, data):
    """
    Add a new line item to an existing customer order.
    Allowed for draft and confirmed orders only.
    data: {productCode, packSize, qty, unitPrice}
    """
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] not in ('draft', 'confirmed'):
        raise ValueError(f"Cannot add items to a {order['status']} order")

    var = ref['var_by_sku'].get((data.get('productCode', ''), data.get('packSize', '')))
    if not var:
        raise ValueError(f"Product not found: {data.get('productCode')}/{data.get('packSize')}")
    qty = int(data.get('qty', 0))
    if qty <= 0:
        raise ValueError("Quantity must be positive")
    unit_price = r2(data.get('unitPrice', 0))
    line_total = r2(qty * unit_price)

    # Check for duplicate line (same variant already on order)
    existing = qry1("SELECT id FROM customer_order_items WHERE order_id=? AND product_variant_id=?",
                    (order_id, var['id']))
    if existing:
        raise ValueError(f"{data.get('productCode')} {data.get('packSize')} is already on this order — "
                         f"edit the existing line instead")

    c = _conn()
    try:
        c.execute("""
            INSERT INTO customer_order_items (order_id, product_variant_id, qty_ordered, unit_price, line_total)
            VALUES (?, ?, ?, ?, ?)
        """, (order_id, var['id'], qty, unit_price, line_total))
        c.execute("UPDATE customer_orders SET updated_at=datetime('now') WHERE id=?", (order_id,))
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    return _order_detail(order_id)


def confirm_customer_order(order_id):
    """Move order from draft or pending_review → confirmed. Warns (but does not block) on stock shortfalls."""
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] not in ('draft', 'pending_review'):
        raise ValueError(f"Order is already {order['status']} — cannot confirm")

    came_from_pending_review = order['status'] == 'pending_review'

    # Stock check at confirmation — last chance to warn before order is locked
    items = qry("SELECT * FROM customer_order_items WHERE order_id=?", (order_id,))
    fg_stock = get_finished_stock_map()
    stock_warnings = []
    for item in items:
        avail = fg_stock.get(item['product_variant_id'], 0)
        if item['qty_ordered'] > avail:
            short = item['qty_ordered'] - avail
            var_info = qry1("""
                SELECT p.name, ps.label as pack FROM product_variants pv
                JOIN products p ON p.id=pv.product_id
                JOIN pack_sizes ps ON ps.id=pv.pack_size_id
                WHERE pv.id=?
            """, (item['product_variant_id'],))
            label = f"{var_info['name']} {var_info['pack']}" if var_info else f"Variant {item['product_variant_id']}"
            stock_warnings.append(
                f"⚠ STOCK WARNING — {label}: {avail:.0f} units available, "
                f"{item['qty_ordered']} ordered (shortfall: {short:.0f} units). "
                f"Run production before invoicing."
            )

    # Credit limit check — warn but do not block (field ops may need override)
    cust_row = qry1("SELECT id, name, credit_limit FROM customers WHERE code=?", (order.get('cust_code',''),))
    if cust_row:
        credit_limit = float(cust_row.get('credit_limit') or 0)
        if credit_limit > 0:
            ar_row = qry1("""
                SELECT COALESCE(SUM(
                    COALESCE((SELECT SUM(quantity*unit_price) FROM invoice_items WHERE invoice_id=i.id), 0)
                    - COALESCE((SELECT SUM(allocated_amount) FROM payment_allocations WHERE invoice_id=i.id), 0)
                ), 0) AS balance
                FROM invoices i
                WHERE i.customer_id=? AND i.status IN ('UNPAID','PARTIAL')
            """, (cust_row['id'],))
            ar_balance = float((ar_row or {}).get('balance', 0))
            order_value = sum(
                float(it.get('qty_ordered', 0)) * float(it.get('unit_price', 0))
                for it in items
            )
            if ar_balance + order_value > credit_limit:
                stock_warnings.append(
                    f"⚠ CREDIT LIMIT — {cust_row['name']}: "
                    f"AR {fmtpkr(ar_balance)} + this order {fmtpkr(order_value)} "
                    f"= {fmtpkr(ar_balance + order_value)} exceeds limit of {fmtpkr(credit_limit)}"
                )

    # If coming from pending_review, convert soft hold to hard reservation
    if came_from_pending_review:
        convert_soft_hold_to_hard_reservation(order_id)

    ops = [
        ("UPDATE customer_orders SET status='confirmed', updated_at=datetime('now') WHERE id=?", (order_id,))
    ]
    audit_log(ops, 'customer_orders', order['order_number'], 'UPDATE',
              old_val={'status': order['status']}, new_val={'status': 'confirmed'})
    run_many(ops)
    detail = _order_detail(order_id)
    detail['stockWarnings'] = stock_warnings
    return detail


def cancel_customer_order(order_id):
    """Cancel an order (must be draft, pending_review, or confirmed, not already invoiced)."""
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] in ('invoiced', 'partially_invoiced'):
        raise ValueError("Cannot cancel an order that has invoices")
    # Release soft hold if cancelling from review queue
    if order['status'] == 'pending_review':
        release_soft_hold(order_id)
    ops = [
        ("UPDATE customer_orders SET status='cancelled', updated_at=datetime('now') WHERE id=?", (order_id,))
    ]
    audit_log(ops, 'customer_orders', order['order_number'], 'UPDATE',
              old_val={'status': order['status']}, new_val={'status': 'cancelled'})
    run_many(ops)
    return {'ok': True}


def create_wo_from_order_item(order_id, item_id, data):
    """
    Create a Work Order for a specific order item.
    data: {targetDate, notes}
    """
    order = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] not in ('confirmed', 'partially_invoiced'):
        raise ValueError("Order must be confirmed before creating work orders")

    item = qry1("SELECT * FROM customer_order_items WHERE id=? AND order_id=?", (item_id, order_id))
    if not item:
        raise ValueError("Order item not found")

    # Check how many units already have a WO for this item
    existing_qty = qry1("""
        SELECT COALESCE(SUM(qty_units), 0) as total
        FROM work_orders
        WHERE customer_order_item_id=? AND status NOT IN ('cancelled', 'completed')
    """, (item_id,))
    already_planned = int((existing_qty or {}).get('total', 0))
    remaining = item['qty_ordered'] - item['qty_invoiced'] - already_planned
    if remaining <= 0:
        raise ValueError("All units for this item are already planned or invoiced")

    # Create the work order
    feasibility = check_wo_feasibility(item['product_variant_id'], remaining)
    _sync_counter_to_max('work_order', 'work_orders', 'wo_number', 'SP-WO-')
    wo_number   = next_id('work_order', 'WO')
    c = _conn()
    try:
        c.execute("""
            INSERT INTO work_orders
                (wo_number, product_variant_id, qty_units, target_date, status,
                 notes, feasibility_ok, customer_order_id, customer_order_item_id)
            VALUES (?, ?, ?, ?, 'planned', ?, ?, ?, ?)
        """, (wo_number, item['product_variant_id'], remaining,
              data.get('targetDate') or order.get('required_date') or today(),
              data.get('notes', f"For {order['order_number']}"),
              1 if feasibility['feasible'] else 0,
              order_id, item_id))

        # Update qty_in_production on the item
        c.execute("""
            UPDATE customer_order_items
            SET qty_in_production = qty_in_production + ?
            WHERE id=?
        """, (remaining, item_id))

        c.commit()
        wo_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()
    return {
        'woId': wo_id, 'woNumber': wo_number,
        'qtyPlanned': remaining,
        'feasible': feasibility['feasible'],
        'shortfalls': feasibility['shortfalls']
    }


def generate_invoice_from_order(order_id, data):
    """
    Generate a (partial or full) invoice from a customer order.
    data: {lines: [{orderItemId, qty}]}   qty=0 means skip that item.
    Enforces: qty <= (qty_ordered - qty_invoiced) per item.
    """
    order = qry1("""
        SELECT co.*, c.name as customer_name, c.payment_terms_days
        FROM customer_orders co
        JOIN customers c ON c.id = co.customer_id
        WHERE co.id=?
    """, (order_id,))
    if not order:
        raise ValueError("Order not found")
    if order['status'] == 'draft':
        raise ValueError("Order must be confirmed before invoicing")
    if order['status'] == 'cancelled':
        raise ValueError("Cannot invoice a cancelled order")

    lines = data.get('lines', [])
    if not lines:
        raise ValueError("Specify at least one line to invoice")

    # Resolve and validate
    resolved = []
    for entry in lines:
        item_id = int(entry.get('orderItemId', 0))
        qty     = int(entry.get('qty', 0))
        if qty <= 0:
            continue

        item = qry1("SELECT * FROM customer_order_items WHERE id=? AND order_id=?", (item_id, order_id))
        if not item:
            raise ValueError(f"Order item {item_id} not found")
        remaining = item['qty_ordered'] - item['qty_invoiced']
        if qty > remaining:
            raise ValueError(f"Cannot invoice {qty} units — only {remaining} remaining for item {item_id}")

        # Check finished goods stock
        fg_stock = get_finished_stock_map()
        avail = fg_stock.get(item['product_variant_id'], 0)
        if qty > avail:
            var_info = qry1("""
                SELECT p.name, ps.label as pack FROM product_variants pv
                JOIN products p ON p.id=pv.product_id
                JOIN pack_sizes ps ON ps.id=pv.pack_size_id
                WHERE pv.id=?
            """, (item['product_variant_id'],))
            label = f"{var_info['name']} {var_info['pack']}" if var_info else f"Variant {item['product_variant_id']}"
            raise ValueError(f"{label}: only {avail:.0f} units in stock, {qty} requested")

        resolved.append({'item': item, 'qty': qty})

    if not resolved:
        raise ValueError("No valid lines to invoice")

    # ── Credit limit hard block ───────────────────────────────────────────
    new_total = r2(sum(
        float(r['item']['unit_price']) * r['qty'] for r in resolved
    ))
    _enforce_credit_limit(order['customer_id'], new_total)

    # Build invoice
    cust       = qry1("SELECT * FROM customers WHERE id=?", (order['customer_id'],))
    terms      = int(order.get('payment_terms_days') or cust.get('payment_terms_days') or 30)
    inv_date   = data.get('invoiceDate', today())
    due_date   = (date.fromisoformat(inv_date) + timedelta(days=terms)).isoformat()

    # Pre-generate all IDs BEFORE opening the write transaction.
    # next_id() opens its own write connection; calling it inside an open
    # transaction causes SQLite SQLITE_BUSY ("database is locked") in WAL mode
    # because only one writer is allowed at a time.
    _sync_counter_to_max('invoice', 'invoices', 'invoice_number', 'SP-INV-')
    _sync_counter_to_max('sale',    'sales',    'sale_id',        'SP-SALE-')
    inv_number   = next_id('invoice', 'INV')
    sale_ids_pre = [next_id('sale', 'SALE') for _ in resolved]

    mfg_type = qry1("SELECT id FROM price_types WHERE code='mfg_cost'")

    c = _conn()
    try:
        c.execute("""
            INSERT INTO invoices
                (invoice_number, customer_id, invoice_date, due_date, status, customer_order_id)
            VALUES (?, ?, ?, ?, 'UNPAID', ?)
        """, (inv_number, order['customer_id'], inv_date, due_date, order_id))
        inv_db_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        sale_ids = []
        for idx, r in enumerate(resolved):
            item = r['item']
            qty  = r['qty']
            unit_price = r2(item['unit_price'])
            line_total = r2(qty * unit_price)

            # COGS
            cogs_price = 0.0
            if mfg_type:
                cp = qry1("""SELECT price FROM product_prices
                             WHERE product_variant_id=? AND price_type_id=? AND active_flag=1
                             ORDER BY effective_from DESC LIMIT 1""",
                          (item['product_variant_id'], mfg_type['id']))
                if cp:
                    cogs_price = r2(cp['price'] * qty)

            sale_id = sale_ids_pre[idx]
            sale_ids.append(sale_id)

            # Resolve variant/product names
            var_info = qry1("""
                SELECT p.name as product_name, p.code as product_code, ps.label as pack_size
                FROM product_variants pv
                JOIN products p ON p.id=pv.product_id
                JOIN pack_sizes ps ON ps.id=pv.pack_size_id
                WHERE pv.id=?
            """, (item['product_variant_id'],))

            c.execute("""
                INSERT INTO sales
                    (sale_id, sale_date, customer_id, cust_code, cust_name, customer_type,
                     product_variant_id, product_code, product_name, pack_size,
                     qty, unit_price, total, cogs, gross_profit, invoice_id, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (sale_id, inv_date, cust['id'], cust['code'], cust['name'],
                  cust.get('customer_type', 'RETAIL'),
                  item['product_variant_id'],
                  var_info['product_code'] if var_info else '',
                  var_info['product_name'] if var_info else '',
                  var_info['pack_size']    if var_info else '',
                  qty, unit_price, line_total, cogs_price,
                  r2(line_total - cogs_price), inv_db_id,
                  f"From Order {order['order_number']}"))

            c.execute("""
                INSERT INTO invoice_items
                    (invoice_id, sale_id, product_variant_id,
                     product_code, product_name, pack_size,
                     quantity, unit_price, line_total)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (inv_db_id, sale_id, item['product_variant_id'],
                  var_info['product_code'] if var_info else '',
                  var_info['product_name'] if var_info else '',
                  var_info['pack_size']    if var_info else '',
                  qty, unit_price, line_total))

            # Update qty_invoiced on order item
            c.execute("""
                UPDATE customer_order_items
                SET qty_invoiced = qty_invoiced + ?
                WHERE id=?
            """, (qty, item['id']))

        # Recompute + store order status
        c.execute("UPDATE customer_orders SET updated_at=datetime('now') WHERE id=?", (order_id,))
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()

    # Recompute status after save
    new_status = _order_status(order_id)
    run(f"UPDATE customer_orders SET status=? WHERE id=?", (new_status, order_id))

    inv_total = sum(r2(r['item']['unit_price'] * r['qty']) for r in resolved)
    return {
        'invoiceNumber': inv_number, 'invoiceId': inv_db_id,
        'total': inv_total, 'orderStatus': new_status, 'saleIds': sale_ids
    }


def record_customer_payment(data):
    """
    Record a payment from a customer. Does NOT auto-allocate.
    data: {customerId, paymentDate, amount, paymentMode, notes}
    """
    cust = qry1("SELECT * FROM customers WHERE id=?", (data.get('customerId'),))
    if not cust:
        raise ValueError("Customer not found")
    amount = r2(data.get('amount', 0))
    if amount <= 0:
        raise ValueError("Payment amount must be positive")

    _sync_counter_to_max('payment', 'customer_payments', 'payment_ref', 'SP-PAY-')
    pay_ref = next_id('payment', 'PAY')
    ops = [("""
        INSERT INTO customer_payments
            (payment_ref, customer_id, payment_date, amount, payment_mode, notes)
        VALUES (?,?,?,?,?,?)
    """, (pay_ref, cust['id'], data.get('paymentDate', today()),
          amount, data.get('paymentMode','CASH'), data.get('notes','')))]
    audit_log(ops, 'customer_payments', pay_ref, 'INSERT', new_val=data)
    run_many(ops)
    return qry1("SELECT * FROM customer_payments WHERE payment_ref=?", (pay_ref,))

def allocate_customer_payment(payment_id, invoice_id, amount):
    """Allocate a payment amount to a specific invoice."""
    pay = qry1("SELECT * FROM customer_payments WHERE id=?", (payment_id,))
    if not pay:
        raise ValueError("Payment not found")
    inv = qry1("SELECT * FROM invoices WHERE id=?", (invoice_id,))
    if not inv:
        raise ValueError("Invoice not found")

    # Check available payment balance
    already_alloc = r2(qry1(
        "SELECT COALESCE(SUM(allocated_amount),0) as s FROM payment_allocations WHERE payment_id=?",
        (payment_id,)
    )['s'])
    available = r2(pay['amount'] - already_alloc)
    if amount > available + 0.001:
        raise ValueError(f"Exceeds available payment balance: {available:.2f}")

    # Check invoice balance
    _, _, inv_total, inv_paid, inv_balance = compute_invoice_balance(invoice_id)
    if amount > inv_balance + 0.001:
        raise ValueError(f"Exceeds invoice balance due: {inv_balance:.2f}")

    amount = r2(min(amount, available, inv_balance))

    ops = [("""
        INSERT INTO payment_allocations (payment_id, invoice_id, allocated_amount)
        VALUES (?,?,?)
        ON CONFLICT(payment_id, invoice_id) DO UPDATE SET
            allocated_amount = allocated_amount + excluded.allocated_amount
    """, (payment_id, invoice_id, amount))]
    audit_log(ops, 'payment_allocations', f"{payment_id}-{invoice_id}", 'INSERT')
    run_many(ops)

    # Status is always derived from amounts — never set manually
    new_status = _sync_invoice_status(invoice_id)
    return {'allocated': amount, 'invoiceStatus': new_status}

def pay_invoice_direct(invoice_id, data):
    """
    Record a new payment AND immediately allocate it to a specific invoice in one call.
    data: {amount, paymentDate, paymentMode, notes}
    """
    inv = qry1("SELECT * FROM invoices WHERE id=?", (invoice_id,))
    if not inv:
        raise ValueError("Invoice not found")
    _, _, inv_total, inv_paid, inv_balance = compute_invoice_balance(invoice_id)
    if inv_balance <= 0:
        raise ValueError("Invoice is already fully paid")

    amount = r2(data.get('amount', inv_balance))
    if amount <= 0:
        raise ValueError("Payment amount must be positive")

    # Record the payment
    _sync_counter_to_max('payment', 'customer_payments', 'payment_ref', 'SP-PAY-')
    pay_ref  = next_id('payment', 'PAY')
    pay_date = data.get('paymentDate', today())
    ops = [("""INSERT INTO customer_payments
                (payment_ref, customer_id, payment_date, amount, payment_mode, notes)
               VALUES (?,?,?,?,?,?)""",
            (pay_ref, inv['customer_id'], pay_date, amount,
             data.get('paymentMode', 'CASH'), data.get('notes', '')))]
    audit_log(ops, 'customer_payments', pay_ref, 'INSERT', new_val=data)
    run_many(ops)

    pay = qry1("SELECT * FROM customer_payments WHERE payment_ref=?", (pay_ref,))
    # Allocate — cap at invoice balance
    alloc_amount = r2(min(amount, inv_balance))
    result = allocate_customer_payment(pay['id'], invoice_id, alloc_amount)
    return {'paymentRef': pay_ref, 'paymentId': pay['id'],
            'allocated': alloc_amount, 'invoiceStatus': result['invoiceStatus']}

def add_invoice_item(invoice_id, data):
    """Add a line item to an existing UNPAID invoice."""
    inv = qry1("SELECT * FROM invoices WHERE id=?", (invoice_id,))
    if not inv:
        raise ValueError("Invoice not found")
    if inv['status'] not in ('UNPAID', 'PARTIAL'):
        raise ValueError(f"Cannot edit a {inv['status']} invoice")
    var = ref['var_by_sku'].get((data.get('productCode',''), data.get('packSize','')))
    if not var:
        raise ValueError("Product variant not found")
    qty        = int(data.get('qty', 0))
    unit_price = r2(data.get('unitPrice', 0))
    if qty <= 0:
        raise ValueError("Quantity must be positive")
    line_total = r2(qty * unit_price)
    run("""INSERT INTO invoice_items
           (invoice_id, product_variant_id, product_code, product_name, pack_size,
            quantity, unit_price, line_total)
           VALUES (?,?,?,?,?,?,?,?)""",
        (invoice_id, var['id'], var['product_code'], var['product_name'],
         var['pack_size'], qty, unit_price, line_total))
    _sync_invoice_status(invoice_id)   # re-derive status after item change
    s, t, total, paid, bal = compute_invoice_balance(invoice_id)
    return {'subtotal': s, 'tax': t, 'total': total, 'paid': paid, 'balance': bal}


def remove_invoice_item(item_id):
    """
    Remove a line item from an invoice.
    Blocked if: invoice is PAID, or any payment has been allocated to it.
    """
    item = qry1("SELECT * FROM invoice_items WHERE id=?", (item_id,))
    if not item:
        raise ValueError("Invoice item not found")
    inv = qry1("SELECT * FROM invoices WHERE id=?", (item['invoice_id'],))
    if not inv:
        raise ValueError("Invoice not found")
    if inv['status'] == 'PAID':
        raise ValueError("Cannot edit a PAID invoice — it is locked once fully paid")
    # Extra immutability guard: block edits once any payment has been allocated
    has_payment = qry1(
        "SELECT id FROM payment_allocations WHERE invoice_id=? LIMIT 1",
        (item['invoice_id'],)
    )
    if has_payment:
        raise ValueError("Cannot remove items from an invoice that has payments recorded — "
                         "reverse the payment first")
    remaining = qry("SELECT id FROM invoice_items WHERE invoice_id=?", (item['invoice_id'],))
    if len(remaining) <= 1:
        raise ValueError("Cannot remove the last line item from an invoice")
    run("DELETE FROM invoice_items WHERE id=?", (item_id,))
    _sync_invoice_status(item['invoice_id'])   # re-derive status after item change
    s, t, total, paid, bal = compute_invoice_balance(item['invoice_id'])
    return {'subtotal': s, 'tax': t, 'total': total, 'paid': paid, 'balance': bal}


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — PURCHASE ORDERS
# ═══════════════════════════════════════════════════════════════════

def ensure_supplier_bills_schema():
    """Add total_amount and supplier_ref columns to supplier_bills (idempotent migration)."""
    c = _conn()
    try:
        # Add stored total_amount column — used as authoritative total when items have zero costs
        try:
            c.execute("ALTER TABLE supplier_bills ADD COLUMN total_amount REAL DEFAULT 0")
            c.commit()
            print("  ✓ Supplier Bills: added total_amount column")
        except Exception:
            pass  # column already exists

        # Add supplier_ref column — supplier's own invoice/reference number (prevents duplicates)
        try:
            c.execute("ALTER TABLE supplier_bills ADD COLUMN supplier_ref TEXT DEFAULT ''")
            c.commit()
            print("  ✓ Supplier Bills: added supplier_ref column")
        except Exception:
            pass  # column already exists

        # Back-fill total_amount from existing bill items for any bills where it's still NULL/0
        bills_to_fix = c.execute("""
            SELECT sb.id, COALESCE(SUM(sbi.line_total),0) as items_sum
            FROM supplier_bills sb
            LEFT JOIN supplier_bill_items sbi ON sbi.bill_id = sb.id
            WHERE sb.total_amount IS NULL OR sb.total_amount = 0
            GROUP BY sb.id
        """).fetchall()
        fixed = 0
        for row in bills_to_fix:
            if row[1] > 0:
                c.execute("UPDATE supplier_bills SET total_amount=? WHERE id=?", (row[1], row[0]))
                fixed += 1
        if fixed:
            c.commit()
            print(f"  ✓ Supplier Bills: back-filled total_amount for {fixed} bill(s)")
        else:
            c.commit()
    finally:
        c.close()
    save_db()


def ensure_purchase_orders_schema():
    """Create purchase_orders and po_items tables if not exists."""
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS purchase_orders (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                po_number      TEXT    NOT NULL UNIQUE,
                supplier_id    INTEGER NOT NULL REFERENCES suppliers(id),
                po_date        TEXT    NOT NULL,
                expected_date  TEXT,
                status         TEXT    NOT NULL DEFAULT 'draft',
                notes          TEXT    DEFAULT '',
                payment_terms  TEXT    NOT NULL DEFAULT 'CREDIT',
                bill_id        INTEGER REFERENCES supplier_bills(id),
                created_at     TEXT    DEFAULT (datetime('now')),
                updated_at     TEXT    DEFAULT (datetime('now'))
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS po_items (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                po_id          INTEGER NOT NULL REFERENCES purchase_orders(id),
                ingredient_id  INTEGER NOT NULL REFERENCES ingredients(id),
                quantity_kg    REAL    NOT NULL,
                received_kg    REAL    NOT NULL DEFAULT 0,
                unit_cost_kg   REAL    NOT NULL DEFAULT 0,
                notes          TEXT    DEFAULT ''
            )
        """)
        # id_counters row for purchase_order
        existing = c.execute("SELECT 1 FROM id_counters WHERE entity='purchase_order'").fetchone()
        if not existing:
            c.execute("INSERT INTO id_counters (entity, last_num) VALUES ('purchase_order', 0)")
        c.commit()
        print("  ✓ Purchase Orders: tables ready")
    finally:
        c.close()
    save_db()


def ensure_batch_cost_column():
    """
    Add unit_cost_at_posting to production_batches (idempotent migration).
    This column freezes the ingredient cost at the moment a batch is posted —
    so historical COGS are never affected by future price changes.
    """
    c = _conn()
    try:
        try:
            c.execute("ALTER TABLE production_batches ADD COLUMN unit_cost_at_posting REAL DEFAULT 0")
            c.commit()
            print("  ✓ Production Batches: added unit_cost_at_posting column")
        except Exception:
            pass  # column already exists
    finally:
        c.close()


def list_purchase_orders(status_filter=None):
    sql = """
        SELECT po.*, s.name as supplier_name,
               COALESCE(SUM(pi.quantity_kg * pi.unit_cost_kg), 0) as total_cost,
               COUNT(pi.id) as item_count
        FROM purchase_orders po
        JOIN suppliers s ON s.id = po.supplier_id
        LEFT JOIN po_items pi ON pi.po_id = po.id
    """
    params = []
    if status_filter:
        sql += " WHERE po.status=?"
        params.append(status_filter)
    sql += " GROUP BY po.id ORDER BY po.id DESC"
    return qry(sql, params)


def get_purchase_order(po_id):
    po = qry1("""
        SELECT po.*, s.name as supplier_name
        FROM purchase_orders po JOIN suppliers s ON s.id=po.supplier_id
        WHERE po.id=?
    """, (po_id,))
    if not po:
        raise ValueError("Purchase order not found")
    items = qry("""
        SELECT pi.*, i.code as ing_code, i.name as ing_name
        FROM po_items pi JOIN ingredients i ON i.id=pi.ingredient_id
        WHERE pi.po_id=? ORDER BY pi.id
    """, (po_id,))
    po = dict(po)
    po['items'] = [dict(i) for i in items]
    po['total_cost'] = sum(i['quantity_kg'] * i['unit_cost_kg'] for i in po['items'])

    # 3-way match: if received and a bill was auto-created, attach billed amounts per ingredient
    if po.get('bill_id'):
        bill_items = qry("""
            SELECT sbi.ingredient_id,
                   sbi.quantity_kg  as billed_kg,
                   sbi.unit_cost_kg as billed_unit_cost,
                   sbi.line_total   as billed_amount
            FROM supplier_bill_items sbi
            WHERE sbi.bill_id=?
        """, (po['bill_id'],))
        bill_map = {b['ingredient_id']: dict(b) for b in bill_items}
        for item in po['items']:
            brow = bill_map.get(item['ingredient_id'], {})
            item['billed_kg']         = brow.get('billed_kg', 0)
            item['billed_unit_cost']  = brow.get('billed_unit_cost', 0)
            item['billed_amount']     = brow.get('billed_amount', 0)
        # attach bill number for UI link
        bill_row = qry1("SELECT bill_number FROM supplier_bills WHERE id=?", (po['bill_id'],))
        po['bill_number'] = bill_row['bill_number'] if bill_row else None

    return po


def create_purchase_order(data):
    """
    Create a purchase order (draft).
    data: {supplierId, poDate, expectedDate, notes, paymentTerms, items:[{ingredientId, quantityKg, unitCostKg}]}
    """
    sup = qry1("SELECT * FROM suppliers WHERE id=?", (data.get('supplierId'),))
    if not sup:
        raise ValueError("Supplier not found")
    items = data.get('items', [])
    if not items:
        raise ValueError("Purchase order must have at least one item")

    _sync_counter_to_max('purchase_order', 'purchase_orders', 'po_number', 'SP-PO-')
    po_num       = next_id('purchase_order', 'PO')
    po_date      = data.get('poDate', today())
    expected     = data.get('expectedDate', '')
    notes        = data.get('notes', '')
    pay_terms    = data.get('paymentTerms', 'CREDIT')  # CREDIT or COD

    c = _conn()
    try:
        c.execute("""
            INSERT INTO purchase_orders
                (po_number, supplier_id, po_date, expected_date, status, notes, payment_terms)
            VALUES (?,?,?,?,'draft',?,?)
        """, (po_num, sup['id'], po_date, expected, notes, pay_terms))
        po_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        for item in items:
            ing = qry1("SELECT * FROM ingredients WHERE id=?", (item.get('ingredientId'),))
            if not ing:
                raise ValueError(f"Ingredient not found: {item.get('ingredientId')}")
            qty_kg    = r2(float(item.get('quantityKg', 0)))
            unit_cost = r2(float(item.get('unitCostKg', 0)))
            c.execute("""
                INSERT INTO po_items (po_id, ingredient_id, quantity_kg, unit_cost_kg)
                VALUES (?,?,?,?)
            """, (po_id, ing['id'], qty_kg, unit_cost))

        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('purchase_orders',?,'INSERT',?)
        """, (po_num, json.dumps({'supplierId': sup['id'], 'items': len(items)})))
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()
    return get_purchase_order(po_id)


def update_purchase_order(po_id, data):
    """Edit PO header fields: expected_date, notes, payment_terms.
    Only allowed when PO status is 'draft' or 'sent' (not received/cancelled)."""
    po = qry1("SELECT * FROM purchase_orders WHERE id=?", (po_id,))
    if not po:
        raise ValueError("Purchase order not found")
    if po['status'] in ('received', 'cancelled'):
        raise ValueError(f"Cannot edit a {po['status']} purchase order")
    set_parts, vals = [], []
    if 'expectedDate' in data:
        set_parts.append("expected_date=?"); vals.append(data['expectedDate'] or None)
    if 'notes' in data:
        set_parts.append("notes=?"); vals.append(str(data.get('notes', '')).strip())
    if 'paymentTerms' in data:
        set_parts.append("payment_terms=?"); vals.append(str(data['paymentTerms']).strip())
    if not set_parts:
        return po
    set_parts.append("updated_at=?"); vals.append(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'))
    vals.append(po_id)
    run(f"UPDATE purchase_orders SET {', '.join(set_parts)} WHERE id=?", vals)
    return qry1("""
        SELECT po.*, s.name as supplier_name
        FROM purchase_orders po JOIN suppliers s ON s.id=po.supplier_id
        WHERE po.id=?
    """, (po_id,))


def update_supplier_bill(bill_id, data):
    """Edit supplier bill header fields: due_date, notes, supplier_ref.
    Only allowed on UNPAID or PARTIAL bills."""
    bill = qry1("SELECT * FROM supplier_bills WHERE id=?", (bill_id,))
    if not bill:
        raise ValueError("Supplier bill not found")
    if bill['status'] not in ('UNPAID', 'PARTIAL'):
        raise ValueError(f"Cannot edit a {bill['status']} bill")
    set_parts, vals = [], []
    if 'dueDate' in data:
        set_parts.append("due_date=?"); vals.append(data['dueDate'])
    if 'notes' in data:
        set_parts.append("notes=?"); vals.append(str(data.get('notes', '')).strip())
    if 'supplierRef' in data:
        set_parts.append("supplier_ref=?"); vals.append(str(data.get('supplierRef', '')).strip())
    if not set_parts:
        return bill
    vals.append(bill_id)
    run(f"UPDATE supplier_bills SET {', '.join(set_parts)} WHERE id=?", vals)
    save_db()
    return qry1("SELECT * FROM supplier_bills WHERE id=?", (bill_id,))


def update_purchase_order_status(po_id, new_status, data=None):
    """
    Update PO status. On 'received': update received_kg, update inventory, auto-create bill.
    Allowed transitions: draft→sent, sent→received, sent→cancelled, draft→cancelled
    """
    data = data or {}
    po = qry1("SELECT * FROM purchase_orders WHERE id=?", (po_id,))
    if not po:
        raise ValueError("Purchase order not found")

    allowed = {
        'draft':    ['sent', 'cancelled'],
        'sent':     ['received', 'partial', 'cancelled'],
        'partial':  ['received', 'cancelled'],
    }
    if new_status not in allowed.get(po['status'], []):
        raise ValueError(f"Cannot move from {po['status']} to {new_status}")

    # Sync bill counter BEFORE opening transaction (next_id uses conn= inside the tx)
    if new_status in ('received', 'partial') and not po.get('bill_id'):
        _sync_counter_to_max('bill', 'supplier_bills', 'bill_number', 'SP-BILL-')

    _cod_bill_id = None  # track COD bill id for post-commit status sync
    c = _conn()
    try:
        if new_status in ('received', 'partial'):
            # Update received quantities and unit costs per item
            received_items = data.get('receivedItems', [])  # [{id, receivedKg, unitCostKg?}]
            for ri in received_items:
                new_cost = ri.get('unitCostKg')
                if new_cost is not None and float(new_cost) > 0:
                    c.execute("UPDATE po_items SET received_kg=?, unit_cost_kg=? WHERE id=? AND po_id=?",
                              (r2(float(ri.get('receivedKg', 0))), r2(float(new_cost)), ri['id'], po_id))
                else:
                    c.execute("UPDATE po_items SET received_kg=? WHERE id=? AND po_id=?",
                              (r2(float(ri.get('receivedKg', 0))), ri['id'], po_id))

            # Check if fully received
            items_after = qry("""
                SELECT pi.*, i.code as ing_code
                FROM po_items pi JOIN ingredients i ON i.id=pi.ingredient_id
                WHERE pi.po_id=?
            """, (po_id,))
            fully_received = all(
                r2(float(i['received_kg'])) >= r2(float(i['quantity_kg']))
                for i in items_after
            )
            actual_status = 'received' if fully_received else 'partial'

            # Inventory update for received quantities (only the delta since last partial)
            # We add PURCHASE_IN for what was newly received
            for item in items_after:
                newly_received_kg = r2(float(item['received_kg']))
                if newly_received_kg > 0:
                    # Check if we already logged this (avoid double-counting)
                    already_logged = qry1("""
                        SELECT COALESCE(SUM(qty_grams),0) as g FROM inventory_ledger
                        WHERE reference_id=? AND ingredient_id=? AND movement_type='PURCHASE_IN'
                    """, (po['po_number'], item['ingredient_id']))
                    already_kg = r2((already_logged['g'] or 0) / 1000)
                    delta_kg   = r2(newly_received_kg - already_kg)
                    if delta_kg > 0:
                        c.execute("""
                            INSERT INTO inventory_ledger
                                (ingredient_id, movement_type, qty_grams, reference_id, notes)
                            VALUES (?,?,?,?,?)
                        """, (item['ingredient_id'], 'PURCHASE_IN', r2(delta_kg * 1000),
                              po['po_number'], f"Received via {po['po_number']}"))

            # Auto-create supplier bill when fully or partially received
            sup = qry1("SELECT * FROM suppliers WHERE id=?", (po['supplier_id'],))
            bill_id = po.get('bill_id')
            if not bill_id:
                # Create bill
                bill_num  = next_id('bill', 'BILL', conn=c)
                bill_date = today()
                pay_terms = po.get('payment_terms', 'CREDIT')
                if pay_terms == 'COD':
                    due_date = bill_date
                else:
                    due_date = (date.fromisoformat(bill_date) + timedelta(days=30)).isoformat()

                # Pre-compute total from received items so we can store it on the bill header
                po_bill_total = r2(sum(
                    r2(float(item['received_kg']) * float(item['unit_cost_kg']))
                    for item in items_after
                    if float(item['received_kg']) > 0
                ))

                c.execute("""
                    INSERT INTO supplier_bills
                        (bill_number, supplier_id, bill_date, due_date, status, notes, total_amount, po_id)
                    VALUES (?,?,?,?,'UNPAID',?,?,?)
                """, (bill_num, po['supplier_id'], bill_date, due_date,
                      f"From PO {po['po_number']}", po_bill_total, po_id))
                bill_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

                for item in items_after:
                    rcv = r2(float(item['received_kg']))
                    if rcv > 0:
                        unit_cost  = r2(float(item['unit_cost_kg']))
                        line_total = r2(rcv * unit_cost)
                        c.execute("""
                            INSERT INTO supplier_bill_items
                                (bill_id, ingredient_id, quantity_kg, unit_cost_kg, line_total)
                            VALUES (?,?,?,?,?)
                        """, (bill_id, item['ingredient_id'], rcv, unit_cost, line_total))

                c.execute("UPDATE purchase_orders SET bill_id=? WHERE id=?", (bill_id, po_id))

                # If COD — mark bill as paid immediately (only if total > 0)
                if pay_terms == 'COD' and po_bill_total > 0:
                    pay_ref = next_id('spay', 'SPAY', conn=c)
                    c.execute("""
                        INSERT INTO supplier_payments
                            (payment_ref, supplier_id, payment_date, amount, payment_mode, notes)
                        VALUES (?,?,?,?,'CASH','Cash on Delivery - auto from PO')
                    """, (pay_ref, po['supplier_id'], bill_date, po_bill_total))
                    pay_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
                    c.execute("""
                        INSERT INTO supplier_payment_allocations
                            (payment_id, bill_id, allocated_amount)
                        VALUES (?,?,?)
                    """, (pay_id, bill_id, po_bill_total))
                    # Status will be synced post-commit via _sync_bill_status (don't set inside tx)
                    _cod_bill_id = bill_id

            c.execute("""
                UPDATE purchase_orders SET status=?, updated_at=datetime('now') WHERE id=?
            """, (actual_status, po_id))

        else:
            c.execute("""
                UPDATE purchase_orders SET status=?, updated_at=datetime('now') WHERE id=?
            """, (new_status, po_id))

        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('purchase_orders',?,'UPDATE',?)
        """, (po['po_number'], json.dumps({'status': new_status})))
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    # Post-commit: sync COD bill status from actual allocations (consistent with _sync_bill_status pattern)
    if _cod_bill_id:
        _sync_bill_status(_cod_bill_id)
    save_db()
    return get_purchase_order(po_id)


def bom_calculate_ingredients(variant_id, qty_units):
    """
    Given a product variant + qty, return ingredient needs vs current stock.
    Returns list of {ingId, ingCode, neededKg, availableKg, toOrderKg, sufficient}
    NOTE: ingredient names are intentionally excluded from this response for IP protection.
    The physical legend (ingCode → real ingredient name) is kept off-system.
    """
    var = qry1("""
        SELECT pv.*, ps.grams as pack_grams, p.name as product_name, p.code as product_code
        FROM product_variants pv
        LEFT JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        JOIN products p ON p.id = pv.product_id
        WHERE pv.id=?
    """, (variant_id,))
    if not var:
        raise ValueError("Product variant not found")
    bom_ver = qry1("""
        SELECT * FROM bom_versions WHERE product_id=? AND active_flag=1
        ORDER BY version_no DESC LIMIT 1
    """, (var['product_id'],))
    if not bom_ver:
        raise ValueError(
            f"No active BOM for {var['product_name']}. "
            f"Go to Production → BOM Setup → click the red chip for {var.get('product_code', var['product_name'])} to define ingredients."
        )
    bom_items_list = qry("""
        SELECT bi.*, i.id as ing_id, i.code as ing_code
        FROM bom_items bi JOIN ingredients i ON i.id=bi.ingredient_id
        WHERE bi.bom_version_id=?
    """, (bom_ver['id'],))
    pack_grams  = float(var.get('pack_grams') or 0)
    total_grams = qty_units * pack_grams
    scale       = total_grams / float(bom_ver['batch_size_grams']) if bom_ver['batch_size_grams'] else 0
    stock_map   = get_stock_map()

    result = []
    for b in bom_items_list:
        needed_g    = r2(b['quantity_grams'] * scale)
        needed_kg   = r2(needed_g / 1000)
        avail_g     = stock_map.get(b['ingredient_id'], 0)
        avail_kg    = r2(avail_g / 1000)
        to_order_kg = r2(max(0.0, needed_kg - avail_kg))
        result.append({
            'ingId':       b['ing_id'],
            'ingCode':     b['ing_code'],   # e.g. ING-001SP — name withheld (IP protection)
            'neededKg':    needed_kg,
            'availableKg': avail_kg,
            'toOrderKg':   to_order_kg,
            'sufficient':  to_order_kg < 0.001,
        })
    return {
        'productName': var['product_name'],
        'packSize':    var.get('pack_size_label') or f"{pack_grams:.0f}g",
        'qtyUnits':    qty_units,
        'ingredients': result,
        'anyShort':    any(r['toOrderKg'] >= 0.001 for r in result),
    }


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — BOM MANAGEMENT
# ═══════════════════════════════════════════════════════════════════

def create_or_update_bom(data):
    """
    Create or replace the active BOM for a product.
    data: {
      productCode: str,
      batchSizeGrams: float,      # e.g. 1000 for a 1kg reference batch
      effectiveFrom: 'YYYY-MM-DD',
      items: [{ ingCode: str, quantityGrams: float }]
    }
    Deactivates any existing active BOM for the product, then inserts a new
    bom_version (version_no = prev_max + 1) and its bom_items.
    """
    prod = qry1("SELECT id, code, name FROM products WHERE code=?",
                (data.get('productCode','').upper().strip(),))
    if not prod:
        raise ValueError(f"Product not found: {data.get('productCode')}")

    items = data.get('items', [])
    if not items:
        raise ValueError("BOM must have at least one ingredient")

    batch_size = float(data.get('batchSizeGrams', 1000) or 1000)
    if batch_size <= 0:
        raise ValueError("batchSizeGrams must be positive")

    eff_from = data.get('effectiveFrom') or today()

    # Resolve ingredient codes → ids
    resolved_items = []
    for it in items:
        ing_code = str(it.get('ingCode', '')).strip()
        qty_g    = float(it.get('quantityGrams', 0))
        if not ing_code:
            raise ValueError("Each item must have ingCode")
        if qty_g <= 0:
            raise ValueError(f"quantityGrams must be positive for {ing_code}")
        ing = qry1("SELECT id, code FROM ingredients WHERE code=? AND COALESCE(active,1)=1", (ing_code,))
        if not ing:
            raise ValueError(f"Ingredient not found or inactive: {ing_code}")
        resolved_items.append({'ing_id': ing['id'], 'qty_g': qty_g})

    c = _conn()
    try:
        # Deactivate old BOMs for this product
        c.execute("UPDATE bom_versions SET active_flag=0 WHERE product_id=? AND active_flag=1",
                  (prod['id'],))

        # Next version number
        row = c.execute("SELECT MAX(version_no) FROM bom_versions WHERE product_id=?",
                        (prod['id'],)).fetchone()
        next_ver = (row[0] or 0) + 1

        # Insert new BOM version
        c.execute("""
            INSERT INTO bom_versions (product_id, version_no, batch_size_grams, effective_from, active_flag, notes)
            VALUES (?,?,?,?,1,?)
        """, (prod['id'], next_ver, batch_size, eff_from, data.get('notes', '')))
        bom_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Insert items
        for it in resolved_items:
            c.execute("""
                INSERT INTO bom_items (bom_version_id, ingredient_id, quantity_grams)
                VALUES (?,?,?)
            """, (bom_id, it['ing_id'], it['qty_g']))

        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()

    # Return full BOM
    bv = qry1("SELECT * FROM bom_versions WHERE id=?", (bom_id,))
    bi = qry("""
        SELECT bi.*, i.code as ing_code, i.name as ing_name
        FROM bom_items bi JOIN ingredients i ON i.id=bi.ingredient_id
        WHERE bi.bom_version_id=?
    """, (bom_id,))
    return {**bv, 'productCode': prod['code'], 'productName': prod['name'], 'items': bi}


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — ACCOUNTS PAYABLE
# ═══════════════════════════════════════════════════════════════════

def create_supplier_bill(data):
    """
    Create a supplier bill + items + auto inventory_ledger PURCHASE_IN entries.
    data: {supplierId, billDate, dueDate, supplierRef, notes, items:[{ingredientId, quantityKg, unitCostKg}]}
    """
    validate_fields(data, [
        {'field': 'supplierId', 'label': 'Supplier',   'type': 'int', 'min': 1},
        {'field': 'billDate',   'label': 'Bill date',  'type': 'date'},
        {'field': 'dueDate',    'label': 'Due date',   'type': 'date'},
    ])
    sup = qry1("SELECT * FROM suppliers WHERE id=?", (data.get('supplierId'),))
    if not sup:
        raise ValueError("Supplier not found")
    items = data.get('items', [])
    if not items:
        raise ValueError("Bill must have at least one item")

    bill_date = data.get('billDate', today())
    due_date  = data.get('dueDate', '')
    if not due_date:
        raise ValueError("Due date is required")

    # Check for duplicate supplier reference (if provided)
    supplier_ref = (data.get('supplierRef') or '').strip()
    if supplier_ref:
        existing = qry1(
            "SELECT bill_number FROM supplier_bills WHERE supplier_id=? AND supplier_ref=?",
            (sup['id'], supplier_ref)
        )
        if existing:
            raise ValueError(f"Duplicate: Supplier ref '{supplier_ref}' already recorded as {existing['bill_number']}")

    _sync_counter_to_max('bill', 'supplier_bills', 'bill_number', 'SP-BILL-')
    bill_num  = next_id('bill', 'BILL')

    c = _conn()
    try:
        # Pre-compute total so we can store it on the bill header
        computed_total = r2(sum(
            r2(float(item.get('quantityKg', 0)) * float(item.get('unitCostKg', 0)))
            for item in items
        ))

        # Insert bill header (with authoritative total_amount)
        c.execute("""
            INSERT INTO supplier_bills (bill_number, supplier_id, bill_date, due_date, status, notes, total_amount, supplier_ref)
            VALUES (?,?,?,?,'UNPAID',?,?,?)
        """, (bill_num, sup['id'], bill_date, due_date, data.get('notes',''), computed_total, supplier_ref))
        bill_db_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        ledger_entries = []
        for item in items:
            ing = qry1("SELECT * FROM ingredients WHERE id=?", (item.get('ingredientId'),))
            if not ing:
                raise ValueError(f"Ingredient not found: {item.get('ingredientId')}")
            qty_kg     = r2(item.get('quantityKg', 0))
            unit_cost  = r2(item.get('unitCostKg', 0))
            line_total = r2(qty_kg * unit_cost)
            qty_grams  = r2(qty_kg * 1000)

            # Bill item
            c.execute("""
                INSERT INTO supplier_bill_items
                    (bill_id, ingredient_id, quantity_kg, unit_cost_kg, line_total)
                VALUES (?,?,?,?,?)
            """, (bill_db_id, ing['id'], qty_kg, unit_cost, line_total))

            # Auto inventory ledger entry
            c.execute("""
                INSERT INTO inventory_ledger
                    (ingredient_id, movement_type, qty_grams, reference_id, notes)
                VALUES (?,?,?,?,?)
            """, (ing['id'], 'PURCHASE_IN', qty_grams, bill_num,
                  f"Purchase from {sup['name']}"))

        # Audit
        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('supplier_bills',?,'INSERT',?)
        """, (bill_num, json.dumps({'supplierId': sup['id'], 'items': len(items)})))

        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()

    total = r2(qry1(
        "SELECT COALESCE(SUM(line_total),0) as t FROM supplier_bill_items WHERE bill_id=?",
        (bill_db_id,)
    )['t'])
    return {'billNumber': bill_num, 'billId': bill_db_id, 'total': total}

def record_supplier_payment(data):
    """Record a payment to a supplier."""
    sup = qry1("SELECT * FROM suppliers WHERE id=?", (data.get('supplierId'),))
    if not sup:
        raise ValueError("Supplier not found")
    amount = r2(data.get('amount', 0))
    if amount <= 0:
        raise ValueError("Amount must be positive")

    _sync_counter_to_max('spay', 'supplier_payments', 'payment_ref', 'SP-SPAY-')
    pay_ref = next_id('spay', 'SPAY')
    ops = [("""
        INSERT INTO supplier_payments
            (payment_ref, supplier_id, payment_date, amount, payment_mode, notes)
        VALUES (?,?,?,?,?,?)
    """, (pay_ref, sup['id'], data.get('paymentDate', today()),
          amount, data.get('paymentMode','BANK_TRANSFER'), data.get('notes','')))]
    audit_log(ops, 'supplier_payments', pay_ref, 'INSERT', new_val=data)
    run_many(ops)
    return qry1("SELECT * FROM supplier_payments WHERE payment_ref=?", (pay_ref,))

def allocate_supplier_payment(payment_id, bill_id, amount):
    """Allocate a supplier payment to a bill."""
    pay  = qry1("SELECT * FROM supplier_payments WHERE id=?", (payment_id,))
    if not pay:
        raise ValueError("Payment not found")
    bill = qry1("SELECT * FROM supplier_bills WHERE id=?", (bill_id,))
    if not bill:
        raise ValueError("Bill not found")

    already_alloc = r2(qry1(
        "SELECT COALESCE(SUM(allocated_amount),0) as s FROM supplier_payment_allocations WHERE payment_id=?",
        (payment_id,)
    )['s'])
    available = r2(pay['amount'] - already_alloc)
    if amount > available + 0.001:
        raise ValueError(f"Exceeds available payment balance: {available:.2f}")

    bill_total, bill_paid, bill_balance = compute_bill_balance(bill_id)
    if amount > bill_balance + 0.001:
        raise ValueError(f"Exceeds bill balance due: {bill_balance:.2f}")

    amount = r2(min(amount, available, bill_balance))

    ops = [("""
        INSERT INTO supplier_payment_allocations (payment_id, bill_id, allocated_amount)
        VALUES (?,?,?)
        ON CONFLICT(payment_id, bill_id) DO UPDATE SET
            allocated_amount = allocated_amount + excluded.allocated_amount
    """, (payment_id, bill_id, amount))]

    audit_log(ops, 'supplier_payment_allocations', f"{payment_id}-{bill_id}", 'INSERT')
    run_many(ops)
    # Derive status from actual amounts — consistent with AR _sync_invoice_status pattern
    new_status = _sync_bill_status(bill_id)
    return {'allocated': amount, 'billStatus': new_status}

def pay_bill_direct(bill_id, data):
    """Record a new supplier payment AND immediately allocate it to a specific bill."""
    bill = qry1("SELECT * FROM supplier_bills WHERE id=?", (bill_id,))
    if not bill:
        raise ValueError("Bill not found")
    if bill['status'] == 'PAID':
        raise ValueError("Bill is already fully paid")

    bill_total, bill_paid, bill_balance = compute_bill_balance(bill_id)
    amount = r2(data.get('amount', bill_balance if bill_balance > 0 else 0))
    if amount <= 0:
        raise ValueError("Payment amount must be positive")

    # If bill has no cost on record (zero-cost PO), update total_amount to this payment
    # so subsequent balance calculations are meaningful.
    if bill_total <= 0.001:
        c = _conn()
        try:
            c.execute("UPDATE supplier_bills SET total_amount=? WHERE id=?", (amount, bill_id))
            c.commit()
        finally:
            c.close()
        save_db()
        bill_balance = amount  # effective balance = amount being paid

    _sync_counter_to_max('spay', 'supplier_payments', 'payment_ref', 'SP-SPAY-')
    pay_ref  = next_id('spay', 'SPAY')
    pay_date = data.get('paymentDate', today())
    ops = [("""INSERT INTO supplier_payments
                (payment_ref, supplier_id, payment_date, amount, payment_mode, notes)
               VALUES (?,?,?,?,?,?)""",
            (pay_ref, bill['supplier_id'], pay_date, amount,
             data.get('paymentMode','BANK_TRANSFER'), data.get('notes','')))]
    audit_log(ops, 'supplier_payments', pay_ref, 'INSERT', new_val=data)
    run_many(ops)

    pay = qry1("SELECT * FROM supplier_payments WHERE payment_ref=?", (pay_ref,))
    alloc_amount = r2(min(amount, bill_balance)) if bill_balance > 0 else amount
    result = allocate_supplier_payment(pay['id'], bill_id, alloc_amount)
    return {'paymentRef': pay_ref, 'paymentId': pay['id'],
            'allocated': alloc_amount, 'billStatus': result['billStatus']}


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — PAYMENT SIMPLIFICATION (Sprint P1)
# ═══════════════════════════════════════════════════════════════════

def deallocate_payment(allocation_id):
    """
    Remove a single AR payment allocation by its ID.
    Restores the payment's unallocated balance and re-syncs the invoice status.
    Only allowed on non-PAID invoices (if invoice is PAID, use adjust instead).
    """
    alloc = qry1("SELECT * FROM payment_allocations WHERE id=?", (allocation_id,))
    if not alloc:
        raise ValueError("Allocation not found")
    inv = qry1("SELECT * FROM invoices WHERE id=?", (alloc['invoice_id'],))
    if not inv:
        raise ValueError("Invoice not found")
    if inv['status'] == 'VOID':
        raise ValueError("Cannot modify allocations on a VOID invoice")
    run("DELETE FROM payment_allocations WHERE id=?", (allocation_id,))
    audit_log([], 'payment_allocations', str(allocation_id), 'DELETE',
              old_val={'payment_id': alloc['payment_id'], 'invoice_id': alloc['invoice_id'],
                       'allocated_amount': alloc['allocated_amount']})
    new_status = _sync_invoice_status(alloc['invoice_id'])
    save_db()
    return {'ok': True, 'invoiceId': alloc['invoice_id'], 'invoiceStatus': new_status,
            'amountRestored': alloc['allocated_amount']}


def deallocate_supplier_payment(allocation_id):
    """
    Remove a single AP payment allocation by its ID.
    Restores the payment's unallocated balance and re-syncs the bill status.
    """
    alloc = qry1("SELECT * FROM supplier_payment_allocations WHERE id=?", (allocation_id,))
    if not alloc:
        raise ValueError("Allocation not found")
    bill = qry1("SELECT * FROM supplier_bills WHERE id=?", (alloc['bill_id'],))
    if not bill:
        raise ValueError("Bill not found")
    if bill['status'] == 'VOID':
        raise ValueError("Cannot modify allocations on a VOID bill")
    run("DELETE FROM supplier_payment_allocations WHERE id=?", (allocation_id,))
    audit_log([], 'supplier_payment_allocations', str(allocation_id), 'DELETE',
              old_val={'payment_id': alloc['payment_id'], 'bill_id': alloc['bill_id'],
                       'allocated_amount': alloc['allocated_amount']})
    new_status = _sync_bill_status(alloc['bill_id'])
    save_db()
    return {'ok': True, 'billId': alloc['bill_id'], 'billStatus': new_status,
            'amountRestored': alloc['allocated_amount']}


def adjust_invoice(invoice_id, data):
    """
    Record a signed payment adjustment against an invoice.
    Positive amount  = additional payment received (reduces balance).
    Negative amount  = refund / credit (increases balance, can reopen a PAID invoice).
    Uses payment_mode='ADJUSTMENT' — no new tables needed.
    """
    inv = qry1("SELECT * FROM invoices WHERE id=?", (invoice_id,))
    if not inv:
        raise ValueError("Invoice not found")
    if inv['status'] == 'VOID':
        raise ValueError("Cannot adjust a VOID invoice")

    amount = r2(data.get('amount', 0))
    if amount == 0:
        raise ValueError("Adjustment amount cannot be zero")

    reason    = (data.get('reason') or 'Manual adjustment').strip()
    adj_date  = data.get('date', today())
    customer  = qry1("SELECT * FROM customers WHERE id=?", (inv['customer_id'],))
    if not customer:
        raise ValueError("Customer not found")

    _sync_counter_to_max('pay', 'customer_payments', 'payment_ref', 'SP-PAY-')
    pay_ref = next_id('pay', 'PAY')

    ops = [("""
        INSERT INTO customer_payments
            (payment_ref, customer_id, payment_date, amount, payment_mode, notes)
        VALUES (?,?,?,?,'ADJUSTMENT',?)
    """, (pay_ref, customer['id'], adj_date, amount, reason))]
    audit_log(ops, 'customer_payments', pay_ref, 'INSERT', new_val=data)
    run_many(ops)

    pay = qry1("SELECT * FROM customer_payments WHERE payment_ref=?", (pay_ref,))

    # For positive: allocate up to the invoice balance.
    # For negative: insert a negative allocation (reduces paid total → reopens invoice).
    _subtotal, _tax, _total, _paid, inv_balance = compute_invoice_balance(invoice_id)
    alloc_amount = r2(min(amount, inv_balance)) if amount > 0 else amount

    run("""
        INSERT INTO payment_allocations (payment_id, invoice_id, allocated_amount)
        VALUES (?,?,?)
        ON CONFLICT(payment_id, invoice_id) DO UPDATE SET
            allocated_amount = allocated_amount + excluded.allocated_amount
    """, (pay['id'], invoice_id, alloc_amount))

    new_status = _sync_invoice_status(invoice_id)
    save_db()
    return {'paymentRef': pay_ref, 'adjusted': alloc_amount,
            'invoiceStatus': new_status}


def adjust_bill(bill_id, data):
    """
    Record a signed payment adjustment against a supplier bill.
    Positive amount  = additional payment made (reduces balance).
    Negative amount  = supplier credit / refund (increases balance).
    Uses payment_mode='ADJUSTMENT' — no new tables needed.
    """
    bill = qry1("SELECT * FROM supplier_bills WHERE id=?", (bill_id,))
    if not bill:
        raise ValueError("Bill not found")
    if bill['status'] == 'VOID':
        raise ValueError("Cannot adjust a VOID bill")

    amount = r2(data.get('amount', 0))
    if amount == 0:
        raise ValueError("Adjustment amount cannot be zero")

    reason   = (data.get('reason') or 'Manual adjustment').strip()
    adj_date = data.get('date', today())

    _sync_counter_to_max('spay', 'supplier_payments', 'payment_ref', 'SP-SPAY-')
    pay_ref = next_id('spay', 'SPAY')

    ops = [("""
        INSERT INTO supplier_payments
            (payment_ref, supplier_id, payment_date, amount, payment_mode, notes)
        VALUES (?,?,?,?,'ADJUSTMENT',?)
    """, (pay_ref, bill['supplier_id'], adj_date, amount, reason))]
    audit_log(ops, 'supplier_payments', pay_ref, 'INSERT', new_val=data)
    run_many(ops)

    pay = qry1("SELECT * FROM supplier_payments WHERE payment_ref=?", (pay_ref,))

    _, _, bill_balance = compute_bill_balance(bill_id)
    alloc_amount = r2(min(amount, bill_balance)) if amount > 0 else amount

    run("""
        INSERT INTO supplier_payment_allocations (payment_id, bill_id, allocated_amount)
        VALUES (?,?,?)
        ON CONFLICT(payment_id, bill_id) DO UPDATE SET
            allocated_amount = allocated_amount + excluded.allocated_amount
    """, (pay['id'], bill_id, alloc_amount))

    new_status = _sync_bill_status(bill_id)
    save_db()
    return {'paymentRef': pay_ref, 'adjusted': alloc_amount,
            'billStatus': new_status}


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — PRODUCTION PLANNING (WORK ORDERS)
# ═══════════════════════════════════════════════════════════════════

def check_wo_feasibility(variant_id, qty_units, wo_id=None):
    """
    Check if stock is sufficient to produce qty_units of the given variant.
    wo_id: if provided, excludes this WO's own reservations from the available
           calculation (prevents a WO blocking itself when re-checking).
    Returns dict with feasible bool, shortfalls list, requirements list.
    Each requirement now includes physicalGrams, reservedGrams, availableGrams.
    """
    var = qry1("""
        SELECT pv.*, ps.grams as pack_grams
        FROM product_variants pv
        LEFT JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE pv.id=?
    """, (variant_id,))
    if not var:
        return {'feasible': False, 'shortfalls': ['Product variant not found'], 'requirements': []}
    bom_ver = qry1("""
        SELECT * FROM bom_versions WHERE product_id=? AND active_flag=1
        ORDER BY version_no DESC LIMIT 1
    """, (var['product_id'],))
    if not bom_ver:
        return {'feasible': False, 'shortfalls': ['No active BOM for this product'], 'requirements': []}
    bom_items_list = qry("""
        SELECT bi.*, i.code as ing_code, i.name as ing_name
        FROM bom_items bi JOIN ingredients i ON i.id=bi.ingredient_id
        WHERE bi.bom_version_id=?
    """, (bom_ver['id'],))
    pack_grams = var.get('pack_grams', 0) or 0
    total_grams = qty_units * pack_grams
    scale = total_grams / float(bom_ver['batch_size_grams'])
    stock_map    = get_stock_map()
    reserved_map = get_wo_reserved_stock_map(exclude_wo_id=wo_id)
    requirements = []
    shortfalls = []
    for b in bom_items_list:
        needed   = r2(b['quantity_grams'] * scale)
        physical = stock_map.get(b['ingredient_id'], 0)
        reserved = reserved_map.get(b['ingredient_id'], 0)
        available = max(0.0, r2(physical - reserved))
        deficit   = max(0.0, needed - available)
        requirements.append({
            'ingCode':        b['ing_code'],
            'ingName':        b['ing_name'] or b['ing_code'],
            'neededGrams':    needed,
            'physicalGrams':  physical,
            'reservedGrams':  reserved,
            'availableGrams': available,
            'deficitGrams':   deficit,
            'ok':             deficit < 0.001,
        })
        if deficit >= 0.001:
            shortfalls.append(
                f"{b['ing_code']}: need {needed:.0f}g, "
                f"available {available:.0f}g (physical {physical:.0f}g − reserved {reserved:.0f}g)"
            )
    return {'feasible': len(shortfalls) == 0, 'shortfalls': shortfalls, 'requirements': requirements}


def get_procurement_list(wo_id):
    """
    Full ingredient procurement list for a work order.
    Returns: work order meta + per-ingredient breakdown:
      needed_grams, available_grams, to_procure_grams, cost_per_kg,
      estimated_cost_pkr, in_stock (bool)
    """
    wo = qry1("""
        SELECT wo.*, p.name as product_name, p.code as product_code,
               ps.label as pack_size, ps.grams as pack_grams, pv.sku_code,
               p.id as product_id
        FROM work_orders wo
        JOIN product_variants pv ON pv.id = wo.product_variant_id
        JOIN products p ON p.id = pv.product_id
        LEFT JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE wo.id=?
    """, (wo_id,))
    if not wo:
        raise ValueError("Work order not found")

    bom_ver = qry1("""
        SELECT * FROM bom_versions WHERE product_id=? AND active_flag=1
        ORDER BY version_no DESC LIMIT 1
    """, (wo['product_id'],))
    if not bom_ver:
        raise ValueError(
            f"No active BOM for {wo['product_name']}. "
            f"Go to Production → BOM Setup → click the red chip for {wo['product_code']} to define ingredients."
        )

    bom_items_list = qry("""
        SELECT bi.quantity_grams,
               i.id as ingredient_id, i.code as ing_code, i.name as ing_name,
               i.cost_per_kg
        FROM bom_items bi JOIN ingredients i ON i.id = bi.ingredient_id
        WHERE bi.bom_version_id = ?
        ORDER BY i.code
    """, (bom_ver['id'],))

    total_grams  = wo['qty_units'] * (wo['pack_grams'] or 0)
    scale        = total_grams / float(bom_ver['batch_size_grams']) if bom_ver['batch_size_grams'] else 0
    stock_map    = get_stock_map()

    lines        = []
    total_needed_cost  = 0.0
    total_procure_cost = 0.0

    for b in bom_items_list:
        needed   = r2(b['quantity_grams'] * scale)
        avail    = r2(stock_map.get(b['ingredient_id'], 0))
        procure  = r2(max(0.0, needed - avail))
        cpk      = float(b['cost_per_kg'] or 0)
        est_cost = r2(procure / 1000 * cpk)
        total_needed_cost  += r2(needed / 1000 * cpk)
        total_procure_cost += est_cost
        lines.append({
            'ingCode':          b['ing_code'],
            'ingName':          b['ing_name'] or b['ing_code'],
            'neededGrams':      needed,
            'availableGrams':   avail,
            'toProcureGrams':   procure,
            'toProcureKg':      r2(procure / 1000),
            'costPerKg':        cpk,
            'estimatedCostPKR': est_cost,
            'inStock':          procure < 0.001,
        })

    return {
        'woNumber':          wo['wo_number'],
        'productName':       wo['product_name'],
        'productCode':       wo['product_code'],
        'packSize':          wo['pack_size'],
        'skuCode':           wo['sku_code'],
        'qtyUnits':          wo['qty_units'],
        'totalGrams':        total_grams,
        'targetDate':        wo['target_date'],
        'status':            wo['status'],
        'bomVersion':        bom_ver['version_no'],
        'batchSizeGrams':    bom_ver['batch_size_grams'],
        'lines':             lines,
        'totalNeededCostPKR':   r2(total_needed_cost),
        'totalProcureCostPKR':  r2(total_procure_cost),
        'itemsToProc':       sum(1 for l in lines if not l['inStock']),
        'allInStock':        all(l['inStock'] for l in lines),
    }


def dev_reset_all():
    """
    DEV ONLY — wipe all transactional and master data.
    Keeps: users, sessions, pack_sizes, price_types, zones.
    Only callable when DEV_TOOLS env var is set.
    Returns row counts cleared per table.
    """
    if os.environ.get('DEV_TOOLS', '').lower() not in ('1', 'true', 'yes'):
        raise PermissionError("DEV_TOOLS not enabled on this environment")

    # Delete order matters — children before parents
    tables = [
        'payment_allocations', 'customer_payments',
        'supplier_payment_allocations', 'supplier_payments',
        'invoice_items', 'invoices', 'sales',
        'supplier_bill_items', 'supplier_bills',
        'po_items', 'purchase_orders',
        'production_consumption', 'production_batches',
        'field_order_items', 'field_orders',
        'customer_order_items', 'customer_orders',
        'work_orders',
        'order_hold_expiry',
        'beat_visits', 'route_customers',
        'inventory_ledger', 'ingredient_price_history',
        'bom_items', 'bom_versions',
        'product_prices', 'product_variants', 'products',
        'customers', 'suppliers', 'ingredients',
        'change_log', 'error_log',
    ]
    counts = {}
    c = _conn()
    try:
        for t in tables:
            try:
                n = c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                c.execute(f"DELETE FROM {t}")
                counts[t] = n
            except Exception:
                counts[t] = 0  # table may not exist yet
        # Reset all ID counters to 0
        c.execute("UPDATE id_counters SET last_num = 0")
        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()
    load_ref()  # reload ref cache — now empty
    return {'reset': True, 'cleared': counts, 'total_rows': sum(counts.values())}


def list_work_orders():
    return qry("""
        SELECT wo.*, p.name as product_name, p.code as product_code,
               ps.label as pack_size, pv.sku_code,
               co.order_number as customer_order_number
        FROM work_orders wo
        JOIN product_variants pv ON pv.id = wo.product_variant_id
        JOIN products p ON p.id = pv.product_id
        LEFT JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        LEFT JOIN customer_orders co ON co.id = wo.customer_order_id
        ORDER BY wo.created_at DESC LIMIT 200
    """)


def create_work_order(data):
    validate_fields(data, [
        {'field': 'productVariantId', 'label': 'Product variant', 'type': 'int', 'min': 1},
        {'field': 'qtyUnits',         'label': 'Quantity',         'type': 'int', 'min': 1},
        {'field': 'targetDate',       'label': 'Target date',      'required': False, 'type': 'date'},
    ])
    variant_id = int(data.get('productVariantId', 0))
    qty_units  = int(data.get('qtyUnits', 0))
    if qty_units <= 0:
        raise ValueError("Quantity must be positive")
    var = ref['var_by_id'].get(variant_id) or qry1("""
        SELECT pv.*, ps.label as pack_size, ps.grams as pack_grams
        FROM product_variants pv JOIN pack_sizes ps ON ps.id=pv.pack_size_id
        WHERE pv.id=?
    """, (variant_id,))
    if not var:
        raise ValueError("Product variant not found")
    feasibility = check_wo_feasibility(variant_id, qty_units)
    _sync_counter_to_max('work_order', 'work_orders', 'wo_number', 'SP-WO-')
    wo_number = next_id('work_order', 'WO')
    c = _conn()
    try:
        c.execute("""
            INSERT INTO work_orders
                (wo_number, product_variant_id, qty_units, target_date, status, notes, feasibility_ok)
            VALUES (?,?,?,?,?,?,?)
        """, (wo_number, variant_id, qty_units,
              data.get('targetDate') or today(),
              'planned',
              data.get('notes', ''),
              1 if feasibility['feasible'] else 0))
        c.commit()
        wo_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        c.close()
    save_db()
    return {
        'id': wo_id,
        'woNumber': wo_number,
        'feasible': feasibility['feasible'],
        'shortfalls': feasibility['shortfalls'],
        'requirements': feasibility['requirements']
    }


def convert_wo_to_batch(wo_id):
    """Convert a planned work order into a production batch."""
    wo = qry1("SELECT * FROM work_orders WHERE id=?", (wo_id,))
    if not wo:
        raise ValueError("Work order not found")
    if wo['status'] not in ('planned', 'in_progress'):
        raise ValueError(f"Work order is {wo['status']} — cannot convert")
    var = ref['var_by_id'].get(wo['product_variant_id'])
    if not var:
        # Fallback: join query
        var = qry1("""
            SELECT pv.*, ps.label as pack_size, ps.grams as pack_grams, p.code as product_code
            FROM product_variants pv
            JOIN pack_sizes ps ON ps.id = pv.pack_size_id
            JOIN products p ON p.id = pv.product_id
            WHERE pv.id=?
        """, (wo['product_variant_id'],))
    if not var:
        raise ValueError("Product variant not found")
    prod = qry1("SELECT * FROM products WHERE id=?", (var['product_id'],))
    # Re-use existing create_production_batch; pass wo_id so the WO's own
    # reservation is excluded from the availability check (prevents self-blocking).
    result = create_production_batch({
        'productCode': prod['code'],
        'packSize':    var['pack_size'],
        'qtyUnits':    wo['qty_units'],
        'batchDate':   today(),
        'mfgDate':     today(),
        'bestBefore':  '',
        'notes':       f"From Work Order {wo['wo_number']}"
    }, exclude_wo_id=wo_id)
    # Mark work order as completed
    run("UPDATE work_orders SET status='completed', batch_id=?, updated_at=datetime('now') WHERE id=?",
        (result['batchId'], wo_id))
    result['woNumber'] = wo['wo_number']
    return result


def update_work_order(wo_id, data):
    """Edit qty, target date, and notes on a planned or in_progress work order."""
    wo = qry1("SELECT * FROM work_orders WHERE id=?", (wo_id,))
    if not wo:
        raise ValueError("Work order not found")
    if wo['status'] not in ('planned', 'in_progress'):
        raise ValueError(f"Cannot edit a work order that is {wo['status']}")
    qty_units = int(data.get('qtyUnits', wo['qty_units']))
    if qty_units <= 0:
        raise ValueError("Quantity must be positive")
    feasibility = check_wo_feasibility(wo['product_variant_id'], qty_units)
    run("""
        UPDATE work_orders
        SET qty_units=?, target_date=?, notes=?, feasibility_ok=?, updated_at=datetime('now')
        WHERE id=?
    """, (qty_units,
          data.get('targetDate') or wo['target_date'] or today(),
          data.get('notes', wo['notes'] or ''),
          1 if feasibility['feasible'] else 0,
          wo_id))
    save_db()
    return {'id': wo_id, 'feasible': feasibility['feasible'], 'shortfalls': feasibility['shortfalls']}


def update_work_order_status(wo_id, status):
    allowed = ('planned', 'in_progress', 'cancelled')
    if status not in allowed:
        raise ValueError(f"Invalid status. Must be one of: {', '.join(allowed)}")
    wo = qry1("SELECT id FROM work_orders WHERE id=?", (wo_id,))
    if not wo:
        raise ValueError("Work order not found")
    run("UPDATE work_orders SET status=?, updated_at=datetime('now') WHERE id=?", (status, wo_id))
    return {'id': wo_id, 'status': status}


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — PRODUCTION
# ═══════════════════════════════════════════════════════════════════

def create_production_batch(data, exclude_wo_id=None):
    """
    Create a production batch using the active BOM.
    data: {productCode, packSize, qtyUnits, batchDate, mfgDate, bestBefore, notes}
    exclude_wo_id: WO being converted to this batch — excluded from reserved stock
                   so the WO's own reservation doesn't block itself.
    Automatically deducts raw material ingredients.
    """
    var = ref['var_by_sku'].get((data.get('productCode',''), data.get('packSize','')))
    if not var:
        raise ValueError(f"Product variant not found: {data.get('productCode')}/{data.get('packSize')}")

    prod = qry1("SELECT * FROM products WHERE id=?", (var['product_id'],))
    qty_units = int(data.get('qtyUnits', 0))
    if qty_units <= 0:
        raise ValueError("Quantity must be positive")

    pack_grams = var['pack_grams']
    total_grams = r2(qty_units * pack_grams)

    # Get active BOM version
    bom_ver = qry1("""
        SELECT * FROM bom_versions WHERE product_id=? AND active_flag=1
        ORDER BY version_no DESC LIMIT 1
    """, (var['product_id'],))
    if not bom_ver:
        raise ValueError(
            f"No active BOM found for {var['product_name']}. "
            f"Go to Production → BOM Setup → click the red chip for {data.get('productCode','this product')} to define ingredients."
        )

    bom_items_list = qry("""
        SELECT bi.*, i.code as ing_code, i.id as ingredient_id
        FROM bom_items bi JOIN ingredients i ON i.id=bi.ingredient_id
        WHERE bi.bom_version_id=?
    """, (bom_ver['id'],))

    # Compute ingredient requirements (BOM is per 1000g of output)
    scale = total_grams / float(bom_ver['batch_size_grams'])
    requirements = []
    for b in bom_items_list:
        needed = r2(b['quantity_grams'] * scale)
        # Fetch current ingredient cost — frozen at batch posting time
        ing_row = qry1("SELECT cost_per_kg FROM ingredients WHERE id=?", (b['ingredient_id'],))
        cost_per_kg = r2(ing_row['cost_per_kg']) if ing_row else 0.0
        requirements.append({'ingredient_id': b['ingredient_id'],
                              'ing_code': b['ing_code'],
                              'needed_grams': needed,
                              'cost_per_kg': cost_per_kg})

    # Freeze total ingredient cost at posting time (unit_cost_at_posting = cost per finished unit)
    total_ingredient_cost = r2(sum(
        (req['needed_grams'] / 1000.0) * req['cost_per_kg'] for req in requirements
    ))
    unit_cost_at_posting = r2(total_ingredient_cost / qty_units) if qty_units else 0.0

    # Check stock availability (negative stock prevention)
    # Use available = physical − reserved-by-other-WOs to prevent double-allocation.
    stock_map    = get_stock_map()
    reserved_map = get_wo_reserved_stock_map(exclude_wo_id=exclude_wo_id)
    shortfalls = []
    for req in requirements:
        physical  = stock_map.get(req['ingredient_id'], 0)
        reserved  = reserved_map.get(req['ingredient_id'], 0)
        available = max(0.0, r2(physical - reserved))
        if req['needed_grams'] > available + 0.001:
            shortfalls.append(
                f"{req['ing_code']}: need {req['needed_grams']:.1f}g, "
                f"available {available:.1f}g (physical {physical:.1f}g − reserved {reserved:.1f}g)"
            )
    if shortfalls:
        raise ValueError("Insufficient stock:\n" + "\n".join(shortfalls))

    _sync_counter_to_max('batch', 'production_batches', 'batch_id', 'SP-BATCH-')
    batch_id   = next_id('batch', 'BATCH')
    batch_date = data.get('batchDate', today())

    c = _conn()
    try:
        # Insert production batch — cost frozen at posting time (unit_cost_at_posting)
        c.execute("""
            INSERT INTO production_batches
                (batch_id, batch_date, product_id, product_variant_id, bom_version_id,
                 qty_grams, qty_units, pack_size, mfg_date, best_before, notes,
                 unit_cost_at_posting)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (batch_id, batch_date, var['product_id'], var['id'], bom_ver['id'],
              total_grams, qty_units, var['pack_size'],
              data.get('mfgDate',''), data.get('bestBefore',''), data.get('notes',''),
              unit_cost_at_posting))
        batch_db_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        for req in requirements:
            # Production consumption record
            c.execute("""
                INSERT INTO production_consumption (batch_id, ingredient_id, qty_grams)
                VALUES (?,?,?)
            """, (batch_db_id, req['ingredient_id'], req['needed_grams']))

            # Inventory ledger deduction (negative = out)
            c.execute("""
                INSERT INTO inventory_ledger
                    (ingredient_id, movement_type, qty_grams, reference_id, notes)
                VALUES (?,?,?,?,?)
            """, (req['ingredient_id'], 'PRODUCTION_USE',
                  -req['needed_grams'], batch_id,
                  f"Production batch {batch_id}"))

        # Audit
        c.execute("""
            INSERT INTO change_log (table_name, record_id, action, new_value)
            VALUES ('production_batches',?,'INSERT',?)
        """, (batch_id, json.dumps({
              'product': data.get('productCode'), 'qty_units': qty_units,
              'pack_size': var['pack_size'], 'total_grams': total_grams})))

        c.commit()
    except Exception:
        c.rollback()
        raise
    finally:
        c.close()
    save_db()

    return {
        'batchId':            batch_id,
        'totalGrams':         total_grams,
        'qtyUnits':           qty_units,
        'ingredients':        requirements,
        'bomVersion':         bom_ver['version_no'],
        'totalIngredientCost': total_ingredient_cost,
        'unitCostAtPosting':  unit_cost_at_posting,   # frozen — never changes after posting
    }


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — INVENTORY ADJUSTMENT
# ═══════════════════════════════════════════════════════════════════

def create_adjustment(data):
    """
    Manual inventory adjustment.
    data: {ingredientId, qtyGrams, notes}  — positive or negative
    """
    ing = qry1("SELECT * FROM ingredients WHERE id=?", (data.get('ingredientId'),))
    if not ing:
        raise ValueError("Ingredient not found")
    qty = r2(data.get('qtyGrams', 0))
    if qty == 0:
        raise ValueError("Adjustment quantity cannot be zero")

    # Negative adjustment: ensure we don't go below zero
    if qty < 0:
        stock_map = get_stock_map()
        current = stock_map.get(ing['id'], 0)
        if current + qty < -0.001:
            raise ValueError(f"Adjustment would create negative stock. Current: {current:.1f}g")

    ops = [("""
        INSERT INTO inventory_ledger
            (ingredient_id, movement_type, qty_grams, notes)
        VALUES (?,?,?,?)
    """, (ing['id'], 'ADJUSTMENT', qty, data.get('notes','Manual adjustment')))]
    audit_log(ops, 'inventory_ledger', str(ing['id']), 'INSERT',
              new_val={'ingredient': ing['code'], 'qty_grams': qty})
    run_many(ops)
    return {'ingredientCode': ing['code'], 'adjustment': qty}


# ═══════════════════════════════════════════════════════════════════
#  PRODUCT PRICES
# ═══════════════════════════════════════════════════════════════════

def set_product_price(data):
    """
    Set a price for a product variant + price type.
    Deactivates any existing active price first.
    data: {productVariantId, priceTypeId, price, effectiveFrom}
    """
    pv_id = data.get('productVariantId')
    pt_id = data.get('priceTypeId')
    price = r2(data.get('price', 0))
    eff   = data.get('effectiveFrom', today())

    if price < 0:
        raise ValueError("Price cannot be negative")

    ops = [
        # Deactivate existing
        ("UPDATE product_prices SET active_flag=0 WHERE product_variant_id=? AND price_type_id=?",
         (pv_id, pt_id)),
        # Insert new
        ("""INSERT INTO product_prices (product_variant_id, price_type_id, price, effective_from, active_flag)
            VALUES (?,?,?,?,1)""",
         (pv_id, pt_id, price, eff))
    ]
    audit_log(ops, 'product_prices', f"{pv_id}-{pt_id}", 'UPDATE', new_val=data)
    run_many(ops)
    return qry1("""
        SELECT pp.*, pt.code as price_type_code, pt.label as price_type_label,
               pv.sku_code
        FROM product_prices pp
        JOIN price_types pt ON pt.id=pp.price_type_id
        JOIN product_variants pv ON pv.id=pp.product_variant_id
        WHERE pp.product_variant_id=? AND pp.price_type_id=? AND pp.active_flag=1
    """, (pv_id, pt_id))


# ═══════════════════════════════════════════════════════════════════
#  MASTER DATA IMPORT / EXPORT
# ═══════════════════════════════════════════════════════════════════

def _parse_upload_file(raw_bytes, filename):
    """Parse CSV or XLSX bytes into list of dicts. Returns (rows, error_string)."""
    fname = (filename or '').lower()
    try:
        if fname.endswith('.xlsx') or fname.endswith('.xls'):
            try:
                import openpyxl as _xl
            except ImportError:
                return None, "openpyxl not installed — only CSV uploads supported"
            import io as _io
            wb = _xl.load_workbook(_io.BytesIO(raw_bytes), read_only=True, data_only=True)
            ws = wb.active
            rows_raw = list(ws.iter_rows(values_only=True))
            wb.close()
            if not rows_raw:
                return [], None
            headers = [str(h).strip() if h is not None else '' for h in rows_raw[0]]
            result = []
            for row in rows_raw[1:]:
                if all(v is None or str(v).strip() == '' for v in row):
                    continue
                result.append({headers[i]: (str(row[i]).strip() if row[i] is not None else '') for i in range(len(headers))})
            return result, None
        else:
            # CSV
            import io as _io
            text = raw_bytes.decode('utf-8-sig', errors='replace')
            reader = csv.DictReader(_io.StringIO(text))
            result = []
            for row in reader:
                if all(str(v).strip() == '' for v in row.values()):
                    continue
                result.append({k.strip(): str(v).strip() for k, v in row.items()})
            return result, None
    except Exception as e:
        return None, str(e)


def import_customers_master(rows):
    """Upsert customers from master rows. Returns {imported, updated, errors}."""
    imported = updated = 0
    errors = []
    c = _conn()
    try:
        for i, row in enumerate(rows, 1):
            code = row.get('code', '').strip().upper()
            name = row.get('name', '').strip()
            if not code or not name:
                errors.append(f"Row {i}: code and name are required"); continue
            ctype = row.get('customer_type', 'RETAIL').strip().upper()
            if ctype not in ('RETAIL', 'DIRECT', 'WHOLESALE'):
                ctype = 'RETAIL'
            address = row.get('address', '').strip()
            existing = c.execute("SELECT id FROM customers WHERE code=?", (code,)).fetchone()
            if existing:
                c.execute("""UPDATE customers SET name=?, customer_type=?,
                             city=?, address=?, phone=?, email=?, payment_terms_days=?, active=1
                             WHERE code=?""",
                          (name, ctype,
                           row.get('city',''), address,
                           row.get('phone',''), row.get('email',''),
                           int(row.get('payment_terms_days', 30) or 30), code))
                updated += 1
            else:
                # account_number left NULL — backfill_customer_account_numbers() assigns
                # it automatically on next server startup using city+type convention
                c.execute("""INSERT INTO customers (code, name, customer_type,
                             city, address, phone, email, payment_terms_days, active)
                             VALUES (?,?,?,?,?,?,?,?,1)""",
                          (code, name, ctype,
                           row.get('city',''), address,
                           row.get('phone',''), row.get('email',''),
                           int(row.get('payment_terms_days', 30) or 30)))
                imported += 1
        c.commit()
    finally:
        c.close()
    save_db()
    load_ref()
    return {'imported': imported, 'updated': updated, 'errors': errors}


def import_suppliers_master(rows):
    """Upsert suppliers from master rows. Returns {imported, updated, errors}."""
    imported = updated = 0
    errors = []
    c = _conn()
    try:
        for i, row in enumerate(rows, 1):
            code = row.get('code', '').strip().upper()
            name = row.get('name', '').strip()
            if not code or not name:
                errors.append(f"Row {i}: code and name are required"); continue
            existing = c.execute("SELECT id FROM suppliers WHERE code=?", (code,)).fetchone()
            if existing:
                c.execute("""UPDATE suppliers SET name=?, contact=?, phone=?, email=?,
                             city=?, address=?, active_flag=1 WHERE code=?""",
                          (name, row.get('contact',''), row.get('phone',''),
                           row.get('email',''), row.get('city',''),
                           row.get('address',''), code))
                updated += 1
            else:
                c.execute("""INSERT INTO suppliers (code, name, contact, phone, email,
                             city, address, active_flag)
                             VALUES (?,?,?,?,?,?,?,1)""",
                          (code, name, row.get('contact',''), row.get('phone',''),
                           row.get('email',''), row.get('city',''),
                           row.get('address','')))
                imported += 1
        c.commit()
    finally:
        c.close()
    save_db()
    load_ref()
    return {'imported': imported, 'updated': updated, 'errors': errors}


def import_products_master(rows):
    """Upsert products and their variants from master rows.

    Supports one-row-per-variant format with explicit sku_code:
      product_code, product_name, sku_code, pack_size, gtin (optional)
      GM, Garam Masala, SPGM-50, 50g, 8966000086920

    Returns {imported, updated, errors}.
    'imported'/'updated' count product rows (not variants).
    """
    imported = updated = 0
    variant_imported = variant_updated = 0
    errors = []
    seen_products = {}   # product_code -> prod_id, to avoid redundant UPDATE per row
    c = _conn()
    try:
        for i, row in enumerate(rows, 1):
            # Support both 'code' (old) and 'product_code' (new) column names
            code = (row.get('product_code') or row.get('code') or '').strip().upper()
            name = (row.get('product_name') or row.get('name') or '').strip()
            sku_code  = row.get('sku_code', '').strip()
            ps_label  = row.get('pack_size', '').strip()

            if not code or not name:
                errors.append(f"Row {i}: product_code and product_name are required"); continue
            if not sku_code or not ps_label:
                errors.append(f"Row {i}: sku_code and pack_size are required"); continue

            # Upsert product (once per unique product_code)
            if code not in seen_products:
                existing = c.execute("SELECT id FROM products WHERE code=?", (code,)).fetchone()
                if existing:
                    prod_id = existing[0]
                    c.execute("""UPDATE products SET name=?, active=1 WHERE code=?""", (name, code))
                    updated += 1
                else:
                    cur = c.execute("""INSERT INTO products (code, name, name_urdu, blend_code, active)
                                 VALUES (?,?,?,?,1)""", (code, name, '', ''))
                    prod_id = cur.lastrowid
                    imported += 1
                seen_products[code] = prod_id
            else:
                prod_id = seen_products[code]

            # Ensure pack_size exists
            ps_row = c.execute("SELECT id FROM pack_sizes WHERE label=?", (ps_label,)).fetchone()
            if not ps_row:
                grams = int(''.join(filter(str.isdigit, ps_label)) or 0)
                if 'kg' in ps_label.lower():
                    grams *= 1000
                c.execute("INSERT OR IGNORE INTO pack_sizes (label, grams) VALUES (?,?)", (ps_label, grams))
                ps_row = c.execute("SELECT id FROM pack_sizes WHERE label=?", (ps_label,)).fetchone()
            ps_id = ps_row[0]

            # Optional gtin — validate if provided
            gtin_val = row.get('gtin', '').strip() or None
            if gtin_val:
                if not gtin_val.isdigit() or not (8 <= len(gtin_val) <= 14):
                    errors.append(f"Row {i}: gtin '{gtin_val}' must be 8–14 digits — skipping gtin")
                    gtin_val = None

            # Upsert variant with explicit sku_code
            existing_var = c.execute(
                "SELECT id FROM product_variants WHERE sku_code=?", (sku_code,)).fetchone()
            if existing_var:
                if gtin_val is not None:
                    c.execute("""UPDATE product_variants SET product_id=?, pack_size_id=?, active_flag=1, gtin=?
                                 WHERE sku_code=?""", (prod_id, ps_id, gtin_val, sku_code))
                else:
                    c.execute("""UPDATE product_variants SET product_id=?, pack_size_id=?, active_flag=1
                                 WHERE sku_code=?""", (prod_id, ps_id, sku_code))
                variant_updated += 1
            else:
                c.execute("""INSERT INTO product_variants (sku_code, product_id, pack_size_id, active_flag, gtin)
                             VALUES (?,?,?,1,?)""", (sku_code, prod_id, ps_id, gtin_val))
                variant_imported += 1

        c.commit()
    finally:
        c.close()
    save_db()
    load_ref()
    return {
        'imported': imported, 'updated': updated,
        'variants_imported': variant_imported, 'variants_updated': variant_updated,
        'errors': errors
    }


def import_prices_master(rows):
    """Full replace of prices from master rows. Returns {imported, errors}."""
    imported = 0
    errors = []
    for i, row in enumerate(rows, 1):
        pcode = row.get('product_code', '').strip().upper()
        psize = row.get('pack_size', '').strip()
        ptype = row.get('price_type', '').strip().lower()
        price_str = row.get('price', '0')
        eff   = row.get('effective_from', today()).strip() or today()
        if not pcode or not psize or not ptype:
            errors.append(f"Row {i}: product_code, pack_size, price_type required"); continue
        try:
            price_val = float(str(price_str).replace(',',''))
        except ValueError:
            errors.append(f"Row {i}: invalid price '{price_str}'"); continue
        pt = qry1("SELECT id FROM price_types WHERE code=?", (ptype,))
        if not pt:
            errors.append(f"Row {i}: unknown price_type '{ptype}'"); continue
        var = qry1("""SELECT pv.id FROM product_variants pv
                      JOIN products p ON p.id=pv.product_id
                      JOIN pack_sizes ps ON ps.id=pv.pack_size_id
                      WHERE p.code=? AND ps.label=? AND pv.active_flag=1""", (pcode, psize))
        if not var:
            errors.append(f"Row {i}: product '{pcode}' / pack '{psize}' not found"); continue
        try:
            set_product_price({'productVariantId': var['id'], 'priceTypeId': pt['id'],
                               'price': price_val, 'effectiveFrom': eff})
            imported += 1
        except Exception as e:
            errors.append(f"Row {i}: {e}")
    return {'imported': imported, 'errors': errors}


def import_ingredients_master(rows):
    """Full-sync ingredients from master rows.
    Columns: code (required), cost_per_kg (required), name (optional),
             unit (optional), reorder_level (optional — ignored, set manually in app)
    Accepts 'name' or 'Ingredient Name (English)' as the name column.

    FULL SYNC behaviour: after upserting all rows in the file, any existing
    active ingredient whose code is NOT in the file is deactivated. This means
    uploading a fresh file always produces exactly the ingredients in that file —
    no stale duplicates from previous uploads with different codes.

    Returns {imported, updated, deactivated, errors}.
    """
    imported   = 0
    updated    = 0
    deactivated = 0
    errors     = []
    incoming_codes = set()

    # ── Pass 1: validate all rows and collect codes ──────────────────
    parsed = []
    for i, row in enumerate(rows, 1):
        code = row.get('code', '').strip().upper()
        if not code:
            errors.append(f"Row {i}: code is required"); continue
        cost_str = str(row.get('cost_per_kg', '0')).replace(',', '')
        try:
            cost = float(cost_str)
        except ValueError:
            errors.append(f"Row {i}: invalid cost_per_kg '{cost_str}'"); continue
        if cost < 0:
            errors.append(f"Row {i}: cost_per_kg cannot be negative"); continue
        name = str(row.get('name') or row.get('Ingredient Name (English)') or '').strip()
        unit = str(row.get('unit', 'kg')).strip() or 'kg'
        incoming_codes.add(code)
        parsed.append((i, code, cost, name, unit))

    # ── Pass 2: upsert each valid row ────────────────────────────────
    for i, code, cost, name, unit in parsed:
        existing = qry1("SELECT id FROM ingredients WHERE code=?", (code,))
        try:
            if existing:
                c = _conn()
                try:
                    c.execute("""UPDATE ingredients
                                 SET cost_per_kg=?, unit=?, name=?, active=1, updated_at=?
                                 WHERE code=?""",
                              (cost, unit, name, today(), code))
                    c.commit()
                finally:
                    c.close()
                updated += 1
            else:
                c = _conn()
                try:
                    c.execute("""INSERT INTO ingredients (code, name, unit, cost_per_kg, reorder_level, active, created_at)
                                 VALUES (?, ?, ?, ?, 0, 1, ?)""",
                              (code, name, unit, cost, today()))
                    c.commit()
                finally:
                    c.close()
                imported += 1
        except Exception as e:
            errors.append(f"Row {i} ({code}): {e}")

    # ── Pass 3: deactivate any active ingredient NOT in the new file ─
    if incoming_codes:
        existing_active = qry("SELECT code FROM ingredients WHERE COALESCE(active,1)=1")
        stale = [r['code'] for r in existing_active if r['code'] not in incoming_codes]
        for code in stale:
            try:
                c = _conn()
                try:
                    c.execute("UPDATE ingredients SET active=0, updated_at=? WHERE code=?",
                              (today(), code))
                    c.commit()
                finally:
                    c.close()
                deactivated += 1
            except Exception as e:
                errors.append(f"Deactivate {code}: {e}")

    save_db()
    load_ref()
    return {'imported': imported, 'updated': updated, 'deactivated': deactivated, 'errors': errors}


def import_bom_master(rows):
    """
    Upload BOMs from a CSV/XLSX file.

    Required columns: product_code, ing_code, quantity_grams
    Optional columns: batch_size_grams (default 1000), notes, effective_from

    One row per ingredient per product. Multiple rows with the same product_code
    are grouped into a single BOM. Any existing active BOM for that product is
    replaced with a new version.

    Example:
        product_code | batch_size_grams | ing_code | quantity_grams
        SPCM         | 1000             | ING-001  | 300
        SPCM         | 1000             | ING-002  | 700
        SPGM         | 1000             | ING-003  | 400
    """
    from collections import defaultdict

    errors   = []
    imported = 0   # products with a new BOM created
    skipped  = 0   # rows skipped due to errors

    # Normalise column names (lowercase, strip spaces)
    def _col(row, *names):
        for n in names:
            for k, v in row.items():
                if k.strip().lower() == n.lower():
                    return str(v).strip()
        return ''

    # Group rows by product_code
    groups = defaultdict(list)
    for i, row in enumerate(rows, 1):
        pcode   = _col(row, 'product_code', 'product code').upper()
        ing     = _col(row, 'ing_code', 'ingredient_code', 'ingredient code').upper()
        qty_raw = _col(row, 'quantity_grams', 'qty_grams', 'grams')
        if not pcode:
            errors.append(f"Row {i}: missing product_code — skipped"); skipped += 1; continue
        if not ing:
            errors.append(f"Row {i}: missing ing_code — skipped"); skipped += 1; continue
        try:
            qty = float(qty_raw.replace(',', ''))
            if qty <= 0: raise ValueError()
        except (ValueError, AttributeError):
            errors.append(f"Row {i}: invalid quantity_grams '{qty_raw}' — skipped"); skipped += 1; continue

        batch_raw = _col(row, 'batch_size_grams', 'batch_size', 'batch size')
        try:
            batch_size = float(batch_raw.replace(',', '')) if batch_raw else 1000
        except (ValueError, AttributeError):
            batch_size = 1000

        groups[pcode].append({
            'ing_code':    ing,
            'qty_g':       qty,
            'batch_size':  batch_size,
            'notes':       _col(row, 'notes'),
            'eff_from':    _col(row, 'effective_from', 'effective from') or today(),
        })

    # Build one BOM per product
    for pcode, items in groups.items():
        prod = qry1("SELECT id, name FROM products WHERE code=?", (pcode,))
        if not prod:
            errors.append(f"Product not found: {pcode} — skipped")
            skipped += len(items)
            continue

        # Use batch_size and effective_from from first row
        batch_size = items[0]['batch_size']
        eff_from   = items[0]['eff_from']
        notes      = items[0]['notes']

        # Resolve ingredient codes
        resolved = []
        bad = False
        for it in items:
            ing = qry1("SELECT id FROM ingredients WHERE code=? AND COALESCE(active,1)=1", (it['ing_code'],))
            if not ing:
                errors.append(f"Ingredient not found: {it['ing_code']} (product {pcode}) — BOM skipped")
                bad = True; break
            resolved.append({'ing_id': ing['id'], 'qty_g': it['qty_g']})
        if bad:
            skipped += len(items)
            continue

        # Create new BOM version (deactivate old)
        c = _conn()
        try:
            c.execute("UPDATE bom_versions SET active_flag=0 WHERE product_id=? AND active_flag=1",
                      (prod['id'],))
            row_ver = c.execute("SELECT MAX(version_no) FROM bom_versions WHERE product_id=?",
                                (prod['id'],)).fetchone()
            next_ver = (row_ver[0] or 0) + 1
            c.execute("""
                INSERT INTO bom_versions
                    (product_id, version_no, batch_size_grams, effective_from, active_flag, notes)
                VALUES (?,?,?,?,1,?)
            """, (prod['id'], next_ver, batch_size, eff_from, notes))
            bom_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
            for r in resolved:
                c.execute("""
                    INSERT INTO bom_items (bom_version_id, ingredient_id, quantity_grams)
                    VALUES (?,?,?)
                """, (bom_id, r['ing_id'], r['qty_g']))
            c.commit()
            imported += 1
        except Exception as e:
            c.rollback()
            errors.append(f"DB error for {pcode}: {e}")
            skipped += len(items)
        finally:
            c.close()

    save_db()
    return {
        'imported': imported,
        'skipped':  skipped,
        'errors':   errors,
        'message':  f"{imported} product BOM(s) created/updated, {skipped} rows skipped"
    }


def create_ingredient(data):
    """Create a new ingredient."""
    code = str(data.get('code', '')).strip().upper()
    if not code:
        raise ValueError("Code is required (e.g. ING-020SP)")
    if qry1("SELECT id FROM ingredients WHERE code=?", (code,)):
        raise ValueError(f"Ingredient '{code}' already exists")
    name    = str(data.get('name', '')).strip()
    cost = float(str(data.get('cost_per_kg', 0)).replace(',', '') or 0)
    if cost < 0:
        raise ValueError("Cost cannot be negative")
    unit    = str(data.get('unit', 'kg')).strip() or 'kg'
    reorder = float(str(data.get('reorder_level', 0)).replace(',', '') or 0)
    c = _conn()
    try:
        cur = c.execute("""
            INSERT INTO ingredients (code, name, unit, cost_per_kg, reorder_level, active, created_at)
            VALUES (?, ?, ?, ?, ?, 1, ?)
        """, (code, name, unit, cost, reorder, today()))
        iid = cur.lastrowid
        if cost > 0:
            c.execute("""
                INSERT INTO ingredient_price_history (ingredient_id, new_cost_per_kg, source)
                VALUES (?, ?, 'manual')
            """, (iid, cost))
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    load_ref()
    return qry1("SELECT * FROM ingredients WHERE code=?", (code,))


def update_ingredient(code, data):
    """Update name, cost_per_kg, unit, or reorder_level for an ingredient."""
    ing = qry1("SELECT * FROM ingredients WHERE code=?", (code,))
    if not ing:
        raise ValueError(f"Ingredient not found: {code}")
    set_parts, vals = [], []
    if 'name' in data:
        set_parts.append("name=?"); vals.append(str(data['name']).strip())
    if 'cost_per_kg' in data:
        new_cost = float(str(data['cost_per_kg']).replace(',', '') or 0)
        if new_cost < 0:
            raise ValueError("Cost cannot be negative")
        old_cost = float(ing.get('cost_per_kg') or 0)
        pct = round(((new_cost - old_cost) / old_cost * 100), 2) if old_cost > 0 else None
        set_parts.append("cost_per_kg=?"); vals.append(new_cost)
        # Log price history immediately (separate connection to avoid nesting)
        changed_by = data.get('changed_by', 'admin')
        c2 = _conn()
        try:
            c2.execute("""
                INSERT INTO ingredient_price_history
                    (ingredient_id, old_cost_per_kg, new_cost_per_kg, pct_change,
                     change_type, changed_by, source)
                VALUES (?, ?, ?, ?, 'ingredient', ?, 'manual')
            """, (ing['id'], old_cost if old_cost > 0 else None, new_cost, pct, changed_by))
            c2.commit()
        finally:
            c2.close()
    if 'unit' in data:
        set_parts.append("unit=?"); vals.append(str(data['unit']).strip() or 'kg')
    if 'reorder_level' in data:
        set_parts.append("reorder_level=?"); vals.append(float(str(data['reorder_level']).replace(',','') or 0))
    if not set_parts:
        return dict(ing)
    set_parts.append("updated_at=?"); vals.append(today())
    vals.append(code)
    c = _conn()
    try:
        c.execute(f"UPDATE ingredients SET {', '.join(set_parts)} WHERE code=?", vals)
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    load_ref()
    return qry1("SELECT * FROM ingredients WHERE code=?", (code,))


def bulk_update_ingredient_costs(rows, username):
    """
    Bulk update cost_per_kg for multiple ingredients.
    rows: list of {code, cost_per_kg}
    Logs each change to ingredient_price_history.
    Returns {updated, skipped, errors}.
    """
    updated, skipped, errors = 0, 0, []
    for row in rows:
        code = str(row.get('code', '')).strip()
        if not code:
            skipped += 1
            continue
        try:
            new_cost = float(str(row.get('cost_per_kg', '')).replace(',', '') or 0)
        except (ValueError, TypeError):
            errors.append({'code': code, 'error': 'Invalid cost_per_kg'})
            continue
        if new_cost < 0:
            errors.append({'code': code, 'error': 'Cost cannot be negative'})
            continue
        ing = qry1("SELECT id, cost_per_kg FROM ingredients WHERE code=? AND COALESCE(active,1)=1", (code,))
        if not ing:
            skipped += 1
            continue
        old_cost = float(ing['cost_per_kg'] or 0)
        if old_cost == new_cost:
            skipped += 1
            continue
        pct = round(((new_cost - old_cost) / old_cost * 100), 2) if old_cost > 0 else None
        c = _conn()
        try:
            c.execute("UPDATE ingredients SET cost_per_kg=?, updated_at=? WHERE code=?",
                      (new_cost, today(), code))
            c.execute("""
                INSERT INTO ingredient_price_history
                    (ingredient_id, old_cost_per_kg, new_cost_per_kg, pct_change,
                     change_type, changed_by, source)
                VALUES (?, ?, ?, ?, 'ingredient', ?, 'bulk')
            """, (ing['id'], old_cost if old_cost > 0 else None, new_cost, pct, username))
            c.commit()
            updated += 1
        except Exception as e:
            errors.append({'code': code, 'error': str(e)})
        finally:
            c.close()
    if updated > 0:
        save_db()
        load_ref()
    return {'updated': updated, 'skipped': skipped, 'errors': errors}


def deactivate_ingredient(code):
    """Soft-delete an ingredient (sets active=0). Blocks if stock is held."""
    ing = qry1("SELECT id, code FROM ingredients WHERE code=?", (code,))
    if not ing:
        raise ValueError(f"Ingredient not found: {code}")
    stock = get_stock_map().get(ing['id'], 0)
    if stock > 0:
        raise ValueError(
            f"Cannot deactivate {code} — {stock:,.0f}g still in stock. "
            "Consume or write off the balance first."
        )
    c = _conn()
    try:
        c.execute("UPDATE ingredients SET active=0, updated_at=? WHERE code=?", (today(), code))
        c.commit()
    except Exception:
        c.rollback(); raise
    finally:
        c.close()
    save_db()
    load_ref()
    return {'ok': True, 'deactivated': code}


def reactivate_ingredient(code):
    """Re-activate a previously deactivated ingredient."""
    if not qry1("SELECT id FROM ingredients WHERE code=?", (code,)):
        raise ValueError(f"Ingredient not found: {code}")
    c = _conn()
    try:
        c.execute("UPDATE ingredients SET active=1, updated_at=? WHERE code=?", (today(), code))
        c.commit()
    finally:
        c.close()
    save_db()
    load_ref()
    return {'ok': True, 'reactivated': code}


def _master_template_csv(master_type):
    """Return CSV template bytes for the given master type."""
    templates = {
        'customers':    'code,name,customer_type,city,address,phone,email,payment_terms_days\nSP-CUST-0001,Example Customer,RETAIL,Karachi,123 Example St,0300-0000000,email@example.com,30\n',
        'suppliers':    'code,name,contact,phone,email,city,address\nSP-SUP-0001,Example Supplier,Contact Name,0300-0000000,email@example.com,Karachi,Address here\n',
        'products':     'product_code,product_name,sku_code,pack_size,gtin\nSPGM,Garam Masala,SPGM-50,50g,8966000086920\nSPGM,Garam Masala,SPGM-100,100g,\nSPGM,Garam Masala,SPGM-1000,1000g,\n',
        'prices':       'product_code,pack_size,price_type,price,effective_from\nP001,50g,retail_mrp,150,2026-01-01\nP001,50g,ex_factory,120,2026-01-01\n',
        'ingredients':  'code,name,cost_per_kg,unit\nING-001SP,Zeera (Pakistani),1380,kg\nING-002SP,Dhaniya (Sabit),520,kg\n',
    }
    return templates.get(master_type, '').encode('utf-8')


# ═══════════════════════════════════════════════════════════════════
#  PDF GENERATION HELPERS
# ═══════════════════════════════════════════════════════════════════

def _pdf_colors():
    """Return brand colour objects for use in reportlab drawings."""
    from reportlab.lib.colors import HexColor
    return {
        'saffron':   HexColor('#E8960A'),
        'dark':      HexColor('#1a1a2e'),
        'chili':     HexColor('#C0392B'),
        'cardamom':  HexColor('#27AE60'),
        'gray_light':HexColor('#F5F5F5'),
        'gray_mid':  HexColor('#CCCCCC'),
        'text_sub':  HexColor('#666666'),
        'white':     HexColor('#FFFFFF'),
    }

def _pkr(v):
    """Format a number as PKR currency string."""
    try:
        v = float(v or 0)
        sign = '-' if v < 0 else ''
        return f"{sign}PKR {abs(v):,.2f}"
    except Exception:
        return 'PKR 0.00'

def generate_invoice_pdf(inv_id: int) -> bytes:
    """Generate a professional invoice PDF and return the raw bytes."""
    from io import BytesIO
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                    TableStyle, HRFlowable)
    from reportlab.lib import colors as rl_colors

    # ── Fetch data ──────────────────────────────────────────────────
    inv = qry1("""
        SELECT inv.*, c.name as customer_name, c.customer_type, c.code as cust_code,
               c.account_number as cust_acct, c.email as customer_email,
               c.phone as customer_phone, c.city as customer_city,
               c.address as customer_address, c.credit_limit
        FROM invoices inv JOIN customers c ON c.id=inv.customer_id
        WHERE inv.id=?
    """, (inv_id,))
    if not inv:
        raise ValueError("Invoice not found")

    items  = qry("SELECT * FROM invoice_items WHERE invoice_id=?", (inv_id,))
    allocs = qry("""
        SELECT pa.allocated_amount, cp.payment_ref, cp.payment_date, cp.payment_mode
        FROM payment_allocations pa
        JOIN customer_payments cp ON cp.id=pa.payment_id
        WHERE pa.invoice_id=? ORDER BY cp.payment_date
    """, (inv_id,))
    s, t, total, paid, bal = compute_invoice_balance(inv_id)

    clr = _pdf_colors()
    buf = BytesIO()
    W, H = A4  # 595.27 x 841.89 pts

    # ── Styles ──────────────────────────────────────────────────────
    def pstyle(name, **kw):
        defaults = dict(fontName='Helvetica', fontSize=9, leading=12,
                        textColor=clr['dark'])
        defaults.update(kw)
        return ParagraphStyle(name, **defaults)

    s_title    = pstyle('title',   fontName='Helvetica-Bold', fontSize=22,
                         textColor=clr['white'], leading=28)
    s_sub      = pstyle('sub',     fontSize=8, textColor=clr['white'], leading=11)
    s_label    = pstyle('label',   fontName='Helvetica-Bold', fontSize=7,
                         textColor=clr['text_sub'], leading=10,
                         spaceAfter=1)
    s_value    = pstyle('value',   fontName='Helvetica-Bold', fontSize=10, leading=13)
    s_normal   = pstyle('normal',  fontSize=9)
    s_small    = pstyle('small',   fontSize=7.5, textColor=clr['text_sub'])
    s_right    = pstyle('right',   fontSize=9, alignment=TA_RIGHT)
    s_right_b  = pstyle('right_b', fontName='Helvetica-Bold', fontSize=9,
                          alignment=TA_RIGHT)
    s_center   = pstyle('center',  fontSize=9, alignment=TA_CENTER)
    s_footer   = pstyle('footer',  fontSize=7.5, textColor=clr['text_sub'],
                          alignment=TA_CENTER)

    story = []

    # ── Header band ─────────────────────────────────────────────────
    from reportlab.platypus import FrameBreak
    header_data = [[
        Paragraph('<b>SPICETOPIA</b>', s_title),
        Paragraph(
            f'<b>TAX INVOICE</b><br/>'
            f'<font size="10">{inv["invoice_number"]}</font>',
            ParagraphStyle('invnum', fontName='Helvetica-Bold', fontSize=14,
                           leading=18, textColor=clr['white'], alignment=TA_RIGHT)
        )
    ]]
    header_tbl = Table(header_data, colWidths=[W * 0.55, W * 0.35])
    header_tbl.setStyle(TableStyle([
        ('BACKGROUND',   (0,0), (-1,-1), clr['saffron']),
        ('TOPPADDING',   (0,0), (-1,-1), 14),
        ('BOTTOMPADDING',(0,0), (-1,-1), 14),
        ('LEFTPADDING',  (0,0), (0,-1),  18),
        ('RIGHTPADDING', (-1,0),(-1,-1), 18),
        ('VALIGN',       (0,0), (-1,-1), 'MIDDLE'),
    ]))
    story.append(header_tbl)
    story.append(Spacer(1, 8))

    # ── Invoice meta + Bill To (two columns) ────────────────────────
    inv_date_fmt  = inv.get('invoice_date', '')
    due_date_fmt  = inv.get('due_date', '')
    status        = inv.get('status', '')
    status_color  = '#C0392B' if status in ('UNPAID','PARTIAL') else '#27AE60' if status == 'PAID' else '#888888'

    bill_to_lines = [
        Paragraph('BILL TO', s_label),
        Paragraph(f'<b>{inv["customer_name"]}</b>', s_value),
        Paragraph(f'Account: {inv["cust_acct"] or inv["cust_code"]}', s_small),
    ]
    if inv.get('customer_address'):
        bill_to_lines.append(Paragraph(inv['customer_address'], s_small))
    if inv.get('customer_city'):
        bill_to_lines.append(Paragraph(inv['customer_city'], s_small))
    if inv.get('customer_phone'):
        bill_to_lines.append(Paragraph(f'Tel: {inv["customer_phone"]}', s_small))
    if inv.get('customer_email'):
        bill_to_lines.append(Paragraph(inv['customer_email'], s_small))

    meta_data = [[
        bill_to_lines,
        [
            Paragraph('INVOICE DATE', s_label),
            Paragraph(inv_date_fmt, s_value),
            Spacer(1, 6),
            Paragraph('DUE DATE', s_label),
            Paragraph(due_date_fmt, s_value),
            Spacer(1, 6),
            Paragraph('STATUS', s_label),
            Paragraph(f'<font color="{status_color}"><b>{status}</b></font>', s_value),
        ]
    ]]
    meta_tbl = Table(meta_data, colWidths=[W * 0.55, W * 0.35])
    meta_tbl.setStyle(TableStyle([
        ('VALIGN',       (0,0), (-1,-1), 'TOP'),
        ('LEFTPADDING',  (0,0), (0,-1),  0),
        ('RIGHTPADDING', (-1,0),(-1,-1), 0),
        ('TOPPADDING',   (0,0), (-1,-1), 0),
        ('BOTTOMPADDING',(0,0), (-1,-1), 0),
    ]))
    story.append(meta_tbl)
    story.append(Spacer(1, 14))
    story.append(HRFlowable(width='100%', thickness=1, color=clr['gray_mid']))
    story.append(Spacer(1, 10))

    # ── Line items table ─────────────────────────────────────────────
    th_style = ParagraphStyle('th', fontName='Helvetica-Bold', fontSize=8,
                               textColor=clr['white'], leading=10)

    item_rows = [[
        Paragraph('PRODUCT', th_style),
        Paragraph('PACK SIZE', th_style),
        Paragraph('QTY', ParagraphStyle('thr', fontName='Helvetica-Bold', fontSize=8,
                                         textColor=clr['white'], leading=10, alignment=TA_RIGHT)),
        Paragraph('UNIT PRICE', ParagraphStyle('thr2', fontName='Helvetica-Bold', fontSize=8,
                                                textColor=clr['white'], leading=10, alignment=TA_RIGHT)),
        Paragraph('LINE TOTAL', ParagraphStyle('thr3', fontName='Helvetica-Bold', fontSize=8,
                                                textColor=clr['white'], leading=10, alignment=TA_RIGHT)),
    ]]
    for idx, item in enumerate(items):
        row_bg = clr['gray_light'] if idx % 2 == 0 else clr['white']
        item_rows.append([
            Paragraph(str(item.get('product_name', '')), s_normal),
            Paragraph(str(item.get('pack_size', '')),    s_normal),
            Paragraph(str(item.get('quantity', '')),     s_right),
            Paragraph(_pkr(item.get('unit_price', 0)),   s_right),
            Paragraph(_pkr(item.get('line_total', 0)),   s_right_b),
        ])

    col_w = [W * 0.32, W * 0.13, W * 0.09, W * 0.18, W * 0.18]
    items_tbl = Table(item_rows, colWidths=col_w)
    items_style = [
        ('BACKGROUND',   (0,0), (-1,0),  clr['dark']),
        ('ROWBACKGROUNDS',(0,1),(-1,-1), [clr['gray_light'], clr['white']]),
        ('GRID',         (0,0), (-1,-1), 0.25, clr['gray_mid']),
        ('TOPPADDING',   (0,0), (-1,-1), 5),
        ('BOTTOMPADDING',(0,0), (-1,-1), 5),
        ('LEFTPADDING',  (0,0), (-1,-1), 7),
        ('RIGHTPADDING', (0,0), (-1,-1), 7),
        ('VALIGN',       (0,0), (-1,-1), 'MIDDLE'),
    ]
    items_tbl.setStyle(TableStyle(items_style))
    story.append(items_tbl)
    story.append(Spacer(1, 12))

    # ── Totals block ────────────────────────────────────────────────
    s_tot_lbl = ParagraphStyle('tl', fontName='Helvetica', fontSize=9,
                                leading=12, alignment=TA_RIGHT, textColor=clr['text_sub'])
    s_tot_val = ParagraphStyle('tv', fontName='Helvetica', fontSize=9,
                                leading=12, alignment=TA_RIGHT)
    s_tot_grand_lbl = ParagraphStyle('tgl', fontName='Helvetica-Bold', fontSize=11,
                                      leading=14, alignment=TA_RIGHT)
    s_tot_grand_val = ParagraphStyle('tgv', fontName='Helvetica-Bold', fontSize=11,
                                      leading=14, alignment=TA_RIGHT,
                                      textColor=clr['chili'] if bal > 0 else clr['cardamom'])

    totals_data = [
        [Paragraph('Subtotal (excl. GST):', s_tot_lbl), Paragraph(_pkr(s), s_tot_val)],
        [Paragraph('GST 18%:', s_tot_lbl),              Paragraph(_pkr(t), s_tot_val)],
        [Paragraph('<b>Invoice Total:</b>', s_tot_grand_lbl), Paragraph(f'<b>{_pkr(total)}</b>', s_tot_grand_val)],
        [Paragraph('Amount Paid:', s_tot_lbl),          Paragraph(_pkr(paid), s_tot_val)],
        [Paragraph('<b>Balance Due:</b>', s_tot_grand_lbl), Paragraph(f'<b>{_pkr(bal)}</b>',
            ParagraphStyle('bdue', fontName='Helvetica-Bold', fontSize=11, leading=14,
                           alignment=TA_RIGHT,
                           textColor=clr['chili'] if bal > 0 else clr['cardamom']))],
    ]
    totals_tbl = Table(totals_data, colWidths=[W * 0.7, W * 0.2])
    totals_tbl.setStyle(TableStyle([
        ('TOPPADDING',    (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('LINEABOVE',     (0,2), (-1,2), 0.5, clr['gray_mid']),
        ('LINEABOVE',     (0,4), (-1,4), 1.0, clr['dark']),
        ('BACKGROUND',    (0,4), (-1,4), clr['gray_light']),
        ('LEFTPADDING',   (0,0), (-1,-1), 0),
        ('RIGHTPADDING',  (0,0), (-1,-1), 0),
    ]))
    story.append(totals_tbl)

    # ── Payment history ──────────────────────────────────────────────
    if allocs:
        story.append(Spacer(1, 14))
        story.append(HRFlowable(width='100%', thickness=0.5, color=clr['gray_mid']))
        story.append(Spacer(1, 6))
        story.append(Paragraph('Payment History', pstyle('ph', fontName='Helvetica-Bold',
                                                          fontSize=9, textColor=clr['text_sub'])))
        story.append(Spacer(1, 4))
        pay_rows = [[
            Paragraph('Date', ParagraphStyle('pth', fontName='Helvetica-Bold', fontSize=7.5,
                                              textColor=clr['white'], leading=10)),
            Paragraph('Reference', ParagraphStyle('pth2', fontName='Helvetica-Bold', fontSize=7.5,
                                                   textColor=clr['white'], leading=10)),
            Paragraph('Mode', ParagraphStyle('pth3', fontName='Helvetica-Bold', fontSize=7.5,
                                              textColor=clr['white'], leading=10)),
            Paragraph('Amount', ParagraphStyle('pth4', fontName='Helvetica-Bold', fontSize=7.5,
                                                textColor=clr['white'], leading=10,
                                                alignment=TA_RIGHT)),
        ]]
        for a in allocs:
            pay_rows.append([
                Paragraph(str(a.get('payment_date', '')), s_small),
                Paragraph(str(a.get('payment_ref',  '')), s_small),
                Paragraph(str(a.get('payment_mode', '')), s_small),
                Paragraph(_pkr(a.get('allocated_amount', 0)),
                          ParagraphStyle('pamnt', fontSize=7.5,
                                         textColor=clr['cardamom'], alignment=TA_RIGHT)),
            ])
        pay_tbl = Table(pay_rows, colWidths=[W*0.15, W*0.32, W*0.15, W*0.28])
        pay_tbl.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,0), clr['text_sub']),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[clr['gray_light'], clr['white']]),
            ('GRID',          (0,0),(-1,-1), 0.25, clr['gray_mid']),
            ('TOPPADDING',    (0,0),(-1,-1), 3),
            ('BOTTOMPADDING', (0,0),(-1,-1), 3),
            ('LEFTPADDING',   (0,0),(-1,-1), 5),
            ('RIGHTPADDING',  (0,0),(-1,-1), 5),
        ]))
        story.append(pay_tbl)

    # ── Footer ────────────────────────────────────────────────────────
    story.append(Spacer(1, 20))
    story.append(HRFlowable(width='100%', thickness=0.5, color=clr['gray_mid']))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        'Thank you for your business. Please quote the invoice number on all payments. '
        'For queries contact accounts@spicetopia.com',
        s_footer))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        f'Generated by Spicetopia BMS — {date.today().isoformat()}',
        s_footer))

    doc = SimpleDocTemplate(buf, pagesize=A4,
                            leftMargin=18*mm, rightMargin=18*mm,
                            topMargin=14*mm, bottomMargin=14*mm)
    doc.build(story)
    return buf.getvalue()


def generate_statement_pdf(cust_id: int) -> bytes:
    """Generate a customer account statement PDF and return the raw bytes."""
    from io import BytesIO
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                    TableStyle, HRFlowable)

    # ── Fetch data (reuse statement logic) ──────────────────────────
    cust = qry1("SELECT * FROM customers WHERE id=?", (cust_id,))
    if not cust:
        raise ValueError("Customer not found")

    today_str = date.today().isoformat()
    inv_rows = qry("""
        SELECT id, invoice_number, invoice_date, due_date, status
        FROM invoices WHERE customer_id=? ORDER BY invoice_date ASC, id ASC
    """, (cust['id'],))
    invoices = []
    for inv in inv_rows:
        s2, t2, total2, paid2, bal2 = compute_invoice_balance(inv['id'])
        rec = dict(inv)
        rec['total'] = total2; rec['paid'] = paid2; rec['balance'] = bal2
        overdue = bal2 > 0 and inv['due_date'] and inv['due_date'] < today_str
        rec['days_overdue'] = (date.today() - date.fromisoformat(inv['due_date'])).days if overdue else 0
        invoices.append(rec)
    payments = qry("""
        SELECT id, payment_ref, payment_date, payment_mode, amount, notes
        FROM customer_payments WHERE customer_id=? ORDER BY payment_date ASC, id ASC
    """, (cust['id'],))

    events = []
    for inv in invoices:
        events.append({'date': inv['invoice_date'], 'type': 'INVOICE', 'data': inv})
    for pay in payments:
        events.append({'date': pay['payment_date'], 'type': 'PAYMENT', 'data': dict(pay)})
    events.sort(key=lambda x: (x['date'], 0 if x['type'] == 'INVOICE' else 1))

    lines = []
    running = 0.0
    for ev in events:
        if ev['type'] == 'INVOICE':
            running += ev['data']['total']
            lines.append({
                'date': ev['date'], 'ref': ev['data']['invoice_number'],
                'type': 'Invoice', 'debit': ev['data']['total'],
                'credit': 0.0, 'balance': r2(running),
                'status': ev['data']['status'],
            })
        else:
            running -= ev['data']['amount']
            lines.append({
                'date': ev['date'], 'ref': ev['data']['payment_ref'],
                'type': f'Payment ({ev["data"]["payment_mode"]})',
                'debit': 0.0, 'credit': ev['data']['amount'],
                'balance': r2(running), 'status': '',
            })

    total_invoiced = sum(i['total']   for i in invoices)
    total_paid     = sum(i['paid']    for i in invoices)
    balance_due    = sum(i['balance'] for i in invoices)

    clr = _pdf_colors()
    buf = BytesIO()
    W, H = A4

    def pstyle(name, **kw):
        d = dict(fontName='Helvetica', fontSize=9, leading=12, textColor=clr['dark'])
        d.update(kw)
        return ParagraphStyle(name, **d)

    s_normal  = pstyle('n')
    s_small   = pstyle('sm',  fontSize=7.5, textColor=clr['text_sub'])
    s_right   = pstyle('r',   fontSize=8.5, alignment=TA_RIGHT)
    s_right_b = pstyle('rb',  fontName='Helvetica-Bold', fontSize=8.5, alignment=TA_RIGHT)
    s_center  = pstyle('c',   fontSize=8.5, alignment=TA_CENTER)
    s_footer  = pstyle('ft',  fontSize=7.5, textColor=clr['text_sub'], alignment=TA_CENTER)

    story = []

    # Header band
    header_data = [[
        Paragraph('<b>SPICETOPIA</b>',
                  ParagraphStyle('ht', fontName='Helvetica-Bold', fontSize=20,
                                  textColor=clr['white'], leading=26)),
        Paragraph('<b>ACCOUNT STATEMENT</b>',
                  ParagraphStyle('hts', fontName='Helvetica-Bold', fontSize=13,
                                  textColor=clr['white'], leading=18, alignment=TA_RIGHT))
    ]]
    hdr_tbl = Table(header_data, colWidths=[W*0.55, W*0.35])
    hdr_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,-1), clr['saffron']),
        ('TOPPADDING',    (0,0),(-1,-1), 14),
        ('BOTTOMPADDING', (0,0),(-1,-1), 14),
        ('LEFTPADDING',   (0,0),(0,-1),  18),
        ('RIGHTPADDING',  (-1,0),(-1,-1),18),
        ('VALIGN',        (0,0),(-1,-1), 'MIDDLE'),
    ]))
    story.append(hdr_tbl)
    story.append(Spacer(1, 8))

    # Customer info + Statement date
    meta_data = [[
        [
            Paragraph('CUSTOMER', pstyle('cl', fontName='Helvetica-Bold', fontSize=7,
                                          textColor=clr['text_sub'], leading=10)),
            Paragraph(f'<b>{cust["name"]}</b>', pstyle('cn', fontName='Helvetica-Bold',
                                                          fontSize=11, leading=14)),
            Paragraph(f'Account: {cust.get("account_number") or cust.get("code","")}',
                       pstyle('ca', fontSize=8, textColor=clr['text_sub'])),
        ],
        [
            Paragraph('STATEMENT DATE', pstyle('sd_l', fontName='Helvetica-Bold', fontSize=7,
                                                textColor=clr['text_sub'], leading=10)),
            Paragraph(today_str, pstyle('sd_v', fontName='Helvetica-Bold', fontSize=11, leading=14)),
        ]
    ]]
    meta_tbl = Table(meta_data, colWidths=[W*0.55, W*0.35])
    meta_tbl.setStyle(TableStyle([
        ('VALIGN', (0,0),(-1,-1),'TOP'),
        ('LEFTPADDING',(0,0),(0,-1),0), ('RIGHTPADDING',(-1,0),(-1,-1),0),
        ('TOPPADDING',(0,0),(-1,-1),0), ('BOTTOMPADDING',(0,0),(-1,-1),0),
    ]))
    story.append(meta_tbl)
    story.append(Spacer(1, 10))

    # Summary box
    bal_color = clr['chili'] if balance_due > 0 else clr['cardamom']
    sum_data = [[
        [Paragraph('TOTAL INVOICED', pstyle('sl', fontName='Helvetica-Bold', fontSize=7,
                                             textColor=clr['text_sub'], leading=9, alignment=TA_CENTER)),
         Paragraph(_pkr(total_invoiced),
                   pstyle('sv', fontName='Helvetica-Bold', fontSize=12, leading=16, alignment=TA_CENTER))],
        [Paragraph('TOTAL PAID', pstyle('sl2', fontName='Helvetica-Bold', fontSize=7,
                                         textColor=clr['text_sub'], leading=9, alignment=TA_CENTER)),
         Paragraph(_pkr(total_paid),
                   pstyle('sv2', fontName='Helvetica-Bold', fontSize=12, leading=16,
                           textColor=clr['cardamom'], alignment=TA_CENTER))],
        [Paragraph('BALANCE DUE', pstyle('sl3', fontName='Helvetica-Bold', fontSize=7,
                                          textColor=clr['white'], leading=9, alignment=TA_CENTER)),
         Paragraph(_pkr(balance_due),
                   pstyle('sv3', fontName='Helvetica-Bold', fontSize=14, leading=18,
                           textColor=clr['white'], alignment=TA_CENTER))],
    ]]
    sum_tbl = Table(sum_data, colWidths=[W*0.28, W*0.28, W*0.34])
    sum_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(1,-1), clr['gray_light']),
        ('BACKGROUND',    (2,0),(2,-1), bal_color),
        ('BOX',           (0,0),(-1,-1), 0.5, clr['gray_mid']),
        ('LINEAFTER',     (0,0),(1,-1), 0.5, clr['gray_mid']),
        ('TOPPADDING',    (0,0),(-1,-1), 8),
        ('BOTTOMPADDING', (0,0),(-1,-1), 8),
        ('LEFTPADDING',   (0,0),(-1,-1), 8),
        ('RIGHTPADDING',  (0,0),(-1,-1), 8),
    ]))
    story.append(sum_tbl)
    story.append(Spacer(1, 14))
    story.append(HRFlowable(width='100%', thickness=1, color=clr['gray_mid']))
    story.append(Spacer(1, 8))

    # Transaction table
    th_s = ParagraphStyle('th', fontName='Helvetica-Bold', fontSize=8,
                            textColor=clr['white'], leading=10)
    th_r = ParagraphStyle('thr', fontName='Helvetica-Bold', fontSize=8,
                            textColor=clr['white'], leading=10, alignment=TA_RIGHT)

    tx_rows = [[
        Paragraph('DATE', th_s),
        Paragraph('REFERENCE', th_s),
        Paragraph('TYPE', th_s),
        Paragraph('DEBIT', th_r),
        Paragraph('CREDIT', th_r),
        Paragraph('BALANCE', th_r),
    ]]
    for ln in lines:
        is_inv  = ln['debit'] > 0
        is_pay  = ln['credit'] > 0
        bal_col = clr['chili'] if float(ln['balance']) > 0 else clr['cardamom']
        tx_rows.append([
            Paragraph(str(ln['date']), s_small),
            Paragraph(str(ln['ref']), pstyle('ref', fontSize=8, fontName='Helvetica-Bold' if is_inv else 'Helvetica')),
            Paragraph(str(ln['type']), s_small),
            Paragraph(_pkr(ln['debit'])  if is_inv else '—',
                       pstyle('db', fontSize=8, alignment=TA_RIGHT, textColor=clr['dark'])),
            Paragraph(_pkr(ln['credit']) if is_pay else '—',
                       pstyle('cr', fontSize=8, alignment=TA_RIGHT, textColor=clr['cardamom'])),
            Paragraph(_pkr(ln['balance']),
                       pstyle('bl', fontSize=8, fontName='Helvetica-Bold',
                               alignment=TA_RIGHT, textColor=bal_col)),
        ])

    tx_tbl = Table(tx_rows, colWidths=[W*0.12, W*0.22, W*0.23, W*0.13, W*0.13, W*0.13])
    tx_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0),(-1,0),  clr['dark']),
        ('ROWBACKGROUNDS',(0,1),(-1,-1), [clr['gray_light'], clr['white']]),
        ('GRID',          (0,0),(-1,-1), 0.25, clr['gray_mid']),
        ('TOPPADDING',    (0,0),(-1,-1), 4),
        ('BOTTOMPADDING', (0,0),(-1,-1), 4),
        ('LEFTPADDING',   (0,0),(-1,-1), 6),
        ('RIGHTPADDING',  (0,0),(-1,-1), 6),
        ('VALIGN',        (0,0),(-1,-1), 'MIDDLE'),
    ]))
    story.append(tx_tbl)

    # Footer
    story.append(Spacer(1, 16))
    story.append(HRFlowable(width='100%', thickness=0.5, color=clr['gray_mid']))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        'This statement is computer generated and is accurate as of the date shown. '
        'For queries contact accounts@spicetopia.com',
        s_footer))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        f'Generated by Spicetopia BMS — {today_str}',
        s_footer))

    doc = SimpleDocTemplate(buf, pagesize=A4,
                            leftMargin=18*mm, rightMargin=18*mm,
                            topMargin=14*mm, bottomMargin=14*mm)
    doc.build(story)
    return buf.getvalue()


def generate_po_pdf(po_id: int) -> bytes:
    """Generate a professional Purchase Order PDF and return raw bytes."""
    from io import BytesIO
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                    TableStyle, HRFlowable)
    from reportlab.lib import colors as rl_colors

    po = get_purchase_order(po_id)
    clr = _pdf_colors()
    buf = BytesIO()
    W, H = A4

    N  = ParagraphStyle('N',  fontName='Helvetica',       fontSize=9,  leading=12)
    NR = ParagraphStyle('NR', fontName='Helvetica',       fontSize=9,  leading=12, alignment=TA_RIGHT)
    NB = ParagraphStyle('NB', fontName='Helvetica-Bold',  fontSize=9,  leading=12)
    NBR= ParagraphStyle('NBR',fontName='Helvetica-Bold',  fontSize=9,  leading=12, alignment=TA_RIGHT)
    H1 = ParagraphStyle('H1', fontName='Helvetica-Bold',  fontSize=18, leading=22, textColor=clr['saffron'])
    H2 = ParagraphStyle('H2', fontName='Helvetica-Bold',  fontSize=12, leading=15, textColor=clr['dark'])
    SM = ParagraphStyle('SM', fontName='Helvetica',       fontSize=8,  leading=10, textColor=clr['text_sub'])

    doc = SimpleDocTemplate(buf, pagesize=A4,
                            leftMargin=18*mm, rightMargin=18*mm,
                            topMargin=14*mm, bottomMargin=14*mm)
    story = []

    # ── Header ─────────────────────────────────────────────────────────
    header_data = [[
        Paragraph('<b>SPICETOPIA</b>', ParagraphStyle('LG', fontName='Helvetica-Bold', fontSize=20,
                  leading=24, textColor=clr['saffron'])),
        Paragraph(f'PURCHASE ORDER<br/><font size="14" color="#E8960A"><b>{po["po_number"]}</b></font>',
                  ParagraphStyle('RT', fontName='Helvetica-Bold', fontSize=11, leading=16,
                  alignment=TA_RIGHT, textColor=clr['dark'])),
    ]]
    hdr_tbl = Table(header_data, colWidths=[W*0.5 - 18*mm, W*0.5 - 18*mm])
    hdr_tbl.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('LINEBELOW', (0,0), (-1,0), 1.5, clr['saffron']),
        ('BOTTOMPADDING', (0,0), (-1,0), 8),
    ]))
    story.append(hdr_tbl)
    story.append(Spacer(1, 10))

    # ── Supplier & PO meta ────────────────────────────────────────────
    pt = po['payment_terms'] or 'Credit'
    terms_label = 'Cash on Delivery' if pt == 'COD' else f'{pt}'
    meta_data = [[
        Paragraph(f'<b>To:</b> {po["supplier_name"]}', NB),
        Paragraph(f'<b>PO Date:</b> {po["po_date"]}', NBR),
    ],[
        Paragraph(po.get('notes','') or '', SM),
        Paragraph(
            f'<b>Expected:</b> {po["expected_date"] or "—"}<br/>'
            f'<b>Payment Terms:</b> {terms_label}<br/>'
            f'<b>Status:</b> {po["status"].capitalize()}',
            NR),
    ]]
    meta_tbl = Table(meta_data, colWidths=[W*0.5 - 18*mm, W*0.5 - 18*mm])
    meta_tbl.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
    ]))
    story.append(meta_tbl)
    story.append(Spacer(1, 12))

    # ── Items table ────────────────────────────────────────────────────
    col_heads = ['Code', 'Ingredient', 'Ordered (kg)', 'Received (kg)', 'Unit Cost (PKR/kg)', 'Line Total (PKR)']
    rows = [col_heads]
    for item in po['items']:
        rcv = item.get('received_kg', 0) or 0
        rcv_str = f'{float(rcv):.3f}' if (po['status'] not in ('draft','sent')) else '—'
        uc = item.get('unit_cost_kg', 0) or 0
        lt = float(item['quantity_kg']) * float(uc) if uc else 0
        rows.append([
            Paragraph(item['ing_code'], ParagraphStyle('M', fontName='Courier', fontSize=8, leading=10)),
            Paragraph(item.get('ing_name',''), N),
            Paragraph(f'{float(item["quantity_kg"]):.3f}', NR),
            Paragraph(rcv_str, NR),
            Paragraph(f'{float(uc):,.2f}' if uc else '—', NR),
            Paragraph(f'{lt:,.2f}' if uc else '—', NBR),
        ])
    # Grand total row
    rows.append([
        Paragraph('', N), Paragraph('', N), Paragraph('', N), Paragraph('', N),
        Paragraph('<b>Grand Total</b>', NBR),
        Paragraph(f'<b>{_pkr(po["total_cost"])}</b>', NBR),
    ])

    cw = [22*mm, 60*mm, 28*mm, 28*mm, 38*mm, 40*mm]
    items_tbl = Table(rows, colWidths=cw, repeatRows=1)
    items_style = [
        ('BACKGROUND',   (0,0), (-1,0), clr['saffron']),
        ('TEXTCOLOR',    (0,0), (-1,0), clr['white']),
        ('FONTNAME',     (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE',     (0,0), (-1,0), 8),
        ('ALIGN',        (2,0), (-1,-1), 'RIGHT'),
        ('FONTSIZE',     (0,1), (-1,-1), 8),
        ('ROWBACKGROUNDS',(0,1),(-1,-2),[clr['white'], clr['gray_light']]),
        ('BACKGROUND',   (0,-1),(-1,-1), clr['gray_light']),
        ('GRID',         (0,0), (-1,-1), 0.25, clr['gray_mid']),
        ('TOPPADDING',   (0,0), (-1,-1), 4),
        ('BOTTOMPADDING',(0,0), (-1,-1), 4),
        ('LEFTPADDING',  (0,0), (-1,-1), 5),
        ('RIGHTPADDING', (0,0), (-1,-1), 5),
    ]
    items_tbl.setStyle(TableStyle(items_style))
    story.append(items_tbl)

    # ── 3-way match section if billed ─────────────────────────────────
    if po.get('bill_id'):
        story.append(Spacer(1, 14))
        story.append(Paragraph('3-Way Match Verification', H2))
        story.append(Spacer(1, 4))
        match_heads = ['Ingredient', 'Ordered (kg)', 'Received (kg)', 'Billed (kg)', 'Billed Amount (PKR)']
        match_rows = [match_heads]
        for item in po['items']:
            match_rows.append([
                Paragraph(f'{item["ing_code"]} – {item.get("ing_name","")}', N),
                Paragraph(f'{float(item["quantity_kg"]):.3f}', NR),
                Paragraph(f'{float(item.get("received_kg",0) or 0):.3f}', NR),
                Paragraph(f'{float(item.get("billed_kg",0) or 0):.3f}', NR),
                Paragraph(_pkr(item.get("billed_amount",0)), NBR),
            ])
        mcw = [80*mm, 25*mm, 25*mm, 25*mm, 40*mm]
        mtbl = Table(match_rows, colWidths=mcw, repeatRows=1)
        mtbl.setStyle(TableStyle([
            ('BACKGROUND',   (0,0), (-1,0), clr['dark']),
            ('TEXTCOLOR',    (0,0), (-1,0), clr['white']),
            ('FONTNAME',     (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE',     (0,0), (-1,-1), 8),
            ('ALIGN',        (1,0), (-1,-1), 'RIGHT'),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[clr['white'], clr['gray_light']]),
            ('GRID',         (0,0), (-1,-1), 0.25, clr['gray_mid']),
            ('TOPPADDING',   (0,0), (-1,-1), 4),
            ('BOTTOMPADDING',(0,0), (-1,-1), 4),
            ('LEFTPADDING',  (0,0), (-1,-1), 5),
            ('RIGHTPADDING', (0,0), (-1,-1), 5),
        ]))
        story.append(mtbl)
        bill_num = po.get('bill_number','')
        story.append(Spacer(1, 4))
        story.append(Paragraph(f'Linked Supplier Bill: <b>{bill_num}</b>', SM))

    # ── Footer ─────────────────────────────────────────────────────────
    story.append(Spacer(1, 20))
    story.append(HRFlowable(width='100%', thickness=0.5, color=clr['gray_mid']))
    story.append(Spacer(1, 6))
    story.append(Paragraph('Spicetopia — Generated by Spicetopia BMS', SM))

    doc.build(story)
    return buf.getvalue()


# ═══════════════════════════════════════════════════════════════════
#  DASHBOARD & REPORTS
# ═══════════════════════════════════════════════════════════════════

def get_pl_report(year: str) -> dict:
    """Monthly P&L for the given year. Returns per-month and YTD totals."""
    MONTH_LABELS = ['Jan','Feb','Mar','Apr','May','Jun',
                    'Jul','Aug','Sep','Oct','Nov','Dec']

    # --- Sales (revenue / COGS / GP) grouped by month ---
    sales_rows = qry("""
        SELECT strftime('%m', sale_date) AS mo,
               COALESCE(SUM(total),        0) AS revenue,
               COALESCE(SUM(cogs),         0) AS cogs,
               COALESCE(SUM(gross_profit), 0) AS gp,
               COUNT(*)                       AS tx_count
        FROM sales
        WHERE strftime('%Y', sale_date) = ?
        GROUP BY mo
    """, (year,))
    sales_map = {row['mo']: row for row in sales_rows}

    # --- Customer receipts grouped by month ---
    receipt_rows = qry("""
        SELECT strftime('%m', payment_date) AS mo,
               COALESCE(SUM(amount), 0) AS receipts
        FROM customer_payments
        WHERE strftime('%Y', payment_date) = ?
        GROUP BY mo
    """, (year,))
    receipt_map = {row['mo']: row['receipts'] for row in receipt_rows}

    # --- Supplier payments (cash out) grouped by month ---
    payment_rows = qry("""
        SELECT strftime('%m', payment_date) AS mo,
               COALESCE(SUM(amount), 0) AS paid_out
        FROM supplier_payments
        WHERE strftime('%Y', payment_date) = ?
        GROUP BY mo
    """, (year,))
    payment_map = {row['mo']: row['paid_out'] for row in payment_rows}

    months = []
    ytd = {'revenue': 0, 'cogs': 0, 'gp': 0, 'tx_count': 0,
           'receipts': 0, 'paid_out': 0}

    for m in range(1, 13):
        mo_str = f"{m:02d}"
        s  = sales_map.get(mo_str, {})
        rev  = float(s.get('revenue', 0) or 0)
        cogs = float(s.get('cogs',    0) or 0)
        gp   = float(s.get('gp',      0) or 0)
        cnt  = int(s.get('tx_count',  0) or 0)
        rec  = float(receipt_map.get(mo_str, 0) or 0)
        out  = float(payment_map.get(mo_str, 0) or 0)
        gp_pct = round(gp / rev * 100, 1) if rev > 0 else 0.0
        net_cash = r2(rec - out)

        months.append({
            'month':      f"{year}-{mo_str}",
            'label':      f"{MONTH_LABELS[m-1]} {year}",
            'short':      MONTH_LABELS[m-1],
            'revenue':    r2(rev),
            'cogs':       r2(cogs),
            'gross_profit': r2(gp),
            'gp_pct':     gp_pct,
            'tx_count':   cnt,
            'receipts':   r2(rec),
            'paid_out':   r2(out),
            'net_cash':   net_cash,
        })
        ytd['revenue']   += rev
        ytd['cogs']      += cogs
        ytd['gp']        += gp
        ytd['tx_count']  += cnt
        ytd['receipts']  += rec
        ytd['paid_out']  += out

    ytd_gp_pct = round(ytd['gp'] / ytd['revenue'] * 100, 1) if ytd['revenue'] > 0 else 0.0

    return {
        'year': year,
        'months': months,
        'ytd': {
            'revenue':      r2(ytd['revenue']),
            'cogs':         r2(ytd['cogs']),
            'gross_profit': r2(ytd['gp']),
            'gp_pct':       ytd_gp_pct,
            'tx_count':     ytd['tx_count'],
            'receipts':     r2(ytd['receipts']),
            'paid_out':     r2(ytd['paid_out']),
            'net_cash':     r2(ytd['receipts'] - ytd['paid_out']),
        },
        'available_years': [str(y) for y in sorted(set(
            int(r['yr']) for r in qry(
                "SELECT DISTINCT strftime('%Y', sale_date) AS yr FROM sales WHERE yr IS NOT NULL", ()
            )
        ), reverse=True)] or [str(date.today().year)],
    }


def get_dashboard():
    today_str  = date.today().isoformat()
    today_d    = date.today()
    month_start = today_d.replace(day=1).isoformat()
    # Last month date range
    last_month_end_d   = today_d.replace(day=1) - timedelta(days=1)
    last_month_start_d = last_month_end_d.replace(day=1)
    last_month_start   = last_month_start_d.isoformat()
    last_month_end     = last_month_end_d.isoformat()

    # Sales today
    sales_today = qry1("""
        SELECT COUNT(*) as cnt, COALESCE(SUM(total),0) as revenue,
               COALESCE(SUM(cogs),0) as cogs,
               COALESCE(SUM(gross_profit),0) as gp
        FROM sales WHERE sale_date = ?
    """, (today_str,)) or {}

    # Sales this month
    sales_month = qry1("""
        SELECT COUNT(*) as cnt, COALESCE(SUM(total),0) as revenue,
               COALESCE(SUM(cogs),0) as cogs,
               COALESCE(SUM(gross_profit),0) as gp
        FROM sales WHERE sale_date >= ?
    """, (month_start,)) or {}

    # Sales last month
    sales_last_month = qry1("""
        SELECT COUNT(*) as cnt, COALESCE(SUM(total),0) as revenue,
               COALESCE(SUM(cogs),0) as cogs,
               COALESCE(SUM(gross_profit),0) as gp
        FROM sales WHERE sale_date >= ? AND sale_date <= ?
    """, (last_month_start, last_month_end)) or {}

    # Cash position: total received from customers minus total paid to suppliers
    cash_in  = (qry1("SELECT COALESCE(SUM(amount),0) as v FROM customer_payments",  ()) or {}).get('v', 0)
    cash_out = (qry1("SELECT COALESCE(SUM(amount),0) as v FROM supplier_payments",  ()) or {}).get('v', 0)
    cash_position = r2(cash_in - cash_out)

    # AR summary
    ar_invoices = qry("""
        SELECT inv.id, inv.status, inv.due_date FROM invoices inv
    """)
    ar_unpaid_count = sum(1 for i in ar_invoices if i['status'] in ('UNPAID','PARTIAL'))

    # Compute total AR outstanding + overdue breakdown
    ar_outstanding = 0.0
    overdue_ar_count = 0
    overdue_ar_amount = 0.0
    overdue_ar_max_days = 0
    for inv in ar_invoices:
        if inv['status'] in ('UNPAID','PARTIAL'):
            _, _, total, paid, balance = compute_invoice_balance(inv['id'])
            ar_outstanding += balance
            due_str = inv.get('due_date') or ''
            if due_str and due_str < today_str:
                days_late = (today_d - date.fromisoformat(due_str)).days
                overdue_ar_count += 1
                overdue_ar_amount += balance
                overdue_ar_max_days = max(overdue_ar_max_days, days_late)

    # AP summary
    seven_days_str = (today_d + timedelta(days=7)).isoformat()
    ap_bills = qry("SELECT id, status, due_date FROM supplier_bills")
    ap_unpaid_count = sum(1 for b in ap_bills if b['status'] in ('UNPAID','PARTIAL'))
    ap_outstanding = 0.0
    ap_overdue_count = 0
    ap_overdue_amount = 0.0
    ap_due_soon_count = 0
    ap_due_soon_amount = 0.0
    for bill in ap_bills:
        if bill['status'] in ('UNPAID','PARTIAL'):
            total, paid, balance = compute_bill_balance(bill['id'])
            ap_outstanding += balance
            due_str = bill.get('due_date') or ''
            if due_str and due_str < today_str:
                ap_overdue_count += 1
                ap_overdue_amount += balance
            elif due_str and due_str <= seven_days_str:
                ap_due_soon_count += 1
                ap_due_soon_amount += balance

    # Inventory alerts
    stock_map = get_stock_map()
    alerts = qry("""
        SELECT i.code, i.name, i.reorder_level FROM ingredients i WHERE i.reorder_level > 0
    """)
    low_stock = []
    for a in alerts:
        bal = stock_map.get(
            qry1("SELECT id FROM ingredients WHERE code=?", (a['code'],))['id'], 0
        ) if a['code'] else 0
        if bal <= a['reorder_level']:
            low_stock.append({'code': a['code'], 'name': a['name'],
                               'balance': bal, 'reorder': a['reorder_level']})

    # Sales by customer type this month
    by_type = qry("""
        SELECT customer_type, COUNT(*) as cnt, SUM(total) as revenue
        FROM sales WHERE sale_date >= ? GROUP BY customer_type
    """, (month_start,))

    # Finished goods stock — query all variants (active + inactive) so historical stock shows
    fg_stock = get_finished_stock_map()
    fg_list = []
    if fg_stock:
        var_ids = ','.join(str(i) for i in fg_stock.keys())
        all_vars = {r['id']: r for r in qry(f"""
            SELECT pv.id, pv.sku_code, p.name as product_name, ps.label as pack_size
            FROM product_variants pv
            JOIN products p ON p.id=pv.product_id
            JOIN pack_sizes ps ON ps.id=pv.pack_size_id
            WHERE pv.id IN ({var_ids})
        """)}
        for vid, units in fg_stock.items():
            v = all_vars.get(vid)
            if v and units > 0:
                fg_list.append({'skuCode': v['sku_code'], 'product': v['product_name'],
                                'packSize': v['pack_size'], 'units': units})

    return {
        'salesToday': {
            'count':      int(sales_today.get('cnt', 0)),
            'revenue':    r2(sales_today.get('revenue', 0)),
            'grossProfit':r2(sales_today.get('gp', 0)),
        },
        'salesMonth': {
            'count':   int(sales_month.get('cnt', 0)),
            'revenue': r2(sales_month.get('revenue', 0)),
            'cogs':    r2(sales_month.get('cogs', 0)),
            'grossProfit': r2(sales_month.get('gp', 0)),
        },
        'salesLastMonth': {
            'count':   int(sales_last_month.get('cnt', 0)),
            'revenue': r2(sales_last_month.get('revenue', 0)),
            'cogs':    r2(sales_last_month.get('cogs', 0)),
            'grossProfit': r2(sales_last_month.get('gp', 0)),
        },
        'cashPosition': cash_position,
        'ar': {
            'unpaidCount':  ar_unpaid_count,
            'outstanding':  r2(ar_outstanding),
        },
        'overdueAR': {
            'count':        overdue_ar_count,
            'amount':       r2(overdue_ar_amount),
            'maxDaysOverdue': overdue_ar_max_days,
        },
        'ap': {
            'unpaidCount':  ap_unpaid_count,
            'outstanding':  r2(ap_outstanding),
        },
        'apOverdue': {
            'count':        ap_overdue_count,
            'amount':       r2(ap_overdue_amount),
        },
        'apDueSoon': {
            'count':        ap_due_soon_count,
            'amount':       r2(ap_due_soon_amount),
        },
        'lowStockAlerts':   low_stock,
        'salesByType':      by_type,
        'finishedGoods':    fg_list,
    }

def get_rep_performance_report(period=None):
    """
    Sales rep performance dashboard.
    period = 'YYYY-MM' (defaults to current month).
    Returns per-rep: route_coverage_pct, visit_hit_rate, orders_count,
    revenue_actual, revenue_target, revenue_vs_target_pct, monthly_trend.
    """
    from datetime import date
    if not period:
        period = date.today().strftime('%Y-%m')

    # All active reps
    reps = qry("""
        SELECT id, employee_id, name, phone, designation
        FROM sales_reps
        WHERE status IS NULL OR status='active'
        ORDER BY name
    """)

    result = []
    for rep in reps:
        rid = rep['id']

        # ── Route coverage: stops in all currently-assigned routes ──────
        total_stops = qry1("""
            SELECT COUNT(rc.id) as cnt
            FROM rep_routes rr
            JOIN route_customers rc ON rc.route_id = rr.route_id
            WHERE rr.rep_id=? AND rr.assigned_to IS NULL
        """, (rid,))['cnt'] or 0

        # Unique customers visited this month
        visited = qry1("""
            SELECT COUNT(DISTINCT customer_id) as cnt
            FROM beat_visits
            WHERE rep_id=? AND strftime('%Y-%m', visit_date)=?
        """, (rid, period))['cnt'] or 0

        route_coverage_pct = round(visited / total_stops * 100, 1) if total_stops else 0.0

        # ── Visit hit rate: scheduled stops vs actual visits ────────────
        # Scheduled = total_stops (route_customers); Visited = beat_visits this month
        visit_hit_rate = route_coverage_pct  # same metric for now (unique cust visited / total stops)

        # ── Orders ──────────────────────────────────────────────────────
        orders_count = qry1("""
            SELECT COUNT(id) as cnt FROM field_orders
            WHERE rep_id=? AND strftime('%Y-%m', order_date)=?
        """, (rid, period))['cnt'] or 0

        # ── Revenue actual (sum of confirmed field order items) ─────────
        rev_actual = qry1("""
            SELECT COALESCE(SUM(foi.quantity * foi.unit_price), 0) as total
            FROM field_orders fo
            JOIN field_order_items foi ON foi.order_id = fo.id
            WHERE fo.rep_id=? AND fo.status='confirmed'
              AND strftime('%Y-%m', fo.order_date)=?
        """, (rid, period))['total'] or 0.0

        # ── Revenue target ──────────────────────────────────────────────
        tgt_row = qry1("""
            SELECT revenue_target FROM rep_targets
            WHERE rep_id=? AND month=?
        """, (rid, period))
        rev_target = tgt_row['revenue_target'] if tgt_row else 0.0

        rev_vs_target_pct = round(rev_actual / rev_target * 100, 1) if rev_target else None

        # ── Monthly trend: last 6 months revenue ───────────────────────
        trend_rows = qry("""
            SELECT strftime('%Y-%m', fo.order_date) as month,
                   COALESCE(SUM(foi.quantity * foi.unit_price), 0) as revenue,
                   COUNT(DISTINCT fo.id) as orders
            FROM field_orders fo
            JOIN field_order_items foi ON foi.order_id = fo.id
            WHERE fo.rep_id=? AND fo.status='confirmed'
              AND fo.order_date >= date('now','-6 months')
            GROUP BY 1 ORDER BY 1
        """, (rid,))

        result.append({
            'repId':            rid,
            'employeeId':       rep['employee_id'],
            'name':             rep['name'],
            'designation':      rep['designation'],
            'period':           period,
            'totalStops':       total_stops,
            'visitedCustomers': visited,
            'routeCoveragePct': route_coverage_pct,
            'visitHitRate':     visit_hit_rate,
            'ordersCount':      orders_count,
            'revenueActual':    rev_actual,
            'revenueTarget':    rev_target,
            'revsVsTargetPct':  rev_vs_target_pct,
            'monthlyTrend':     [dict(r) for r in trend_rows],
        })

    return {'period': period, 'reps': result}


def get_margin_report(month=None):
    """Margin report per product, optionally filtered by YYYY-MM."""
    if month:
        where = f"WHERE s.sale_date LIKE '{month}%'"
    else:
        where = ""
    rows = qry(f"""
        SELECT s.product_code, s.product_name, s.pack_size,
               COUNT(*) as orders,
               SUM(s.qty) as units_sold,
               SUM(s.total) as revenue,
               SUM(s.cogs) as cogs,
               SUM(s.gross_profit) as gross_profit
        FROM sales s {where}
        GROUP BY s.product_code, s.product_name, s.pack_size
        ORDER BY gross_profit DESC
    """)
    for r in rows:
        if r['revenue'] and r['revenue'] > 0:
            r['margin_pct'] = r2(r['gross_profit'] / r['revenue'] * 100)
        else:
            r['margin_pct'] = 0.0
    return rows


# ═══════════════════════════════════════════════════════════════════
#  HTTP SERVER
# ═══════════════════════════════════════════════════════════════════

def _add_security_headers(handler):
    """Add security headers to every response."""
    handler.send_header('X-Frame-Options', 'DENY')
    handler.send_header('X-Content-Type-Options', 'nosniff')
    handler.send_header('X-XSS-Protection', '1; mode=block')
    handler.send_header('Referrer-Policy', 'strict-origin-when-cross-origin')
    handler.send_header('Content-Security-Policy',
        "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:;")
    # CORS — always allow localhost; also allow configured CORS_ORIGINS
    origin = handler.headers.get('Origin', '')
    allowed = list(CORS_ORIGINS) + ['http://localhost', 'http://127.0.0.1']
    if any(origin.startswith(o) for o in allowed):
        handler.send_header('Access-Control-Allow-Origin', origin)
        handler.send_header('Vary', 'Origin')

def send_json(handler, data, status=200):
    body = json.dumps(data, default=str).encode()
    handler.send_response(status)
    handler.send_header('Content-Type', 'application/json')
    handler.send_header('Content-Length', len(body))
    _add_security_headers(handler)
    handler.end_headers()
    handler.wfile.write(body)

def send_error(handler, msg, status=400):
    send_json(handler, {'error': str(msg)}, status)

def read_body(handler):
    length = int(handler.headers.get('Content-Length', 0))
    if length == 0:
        return {}
    ct = handler.headers.get('Content-Type', '')
    if ct.startswith('multipart/form-data'):
        return {}  # multipart body consumed later by cgi.FieldStorage
    return json.loads(handler.rfile.read(length))


class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        print(f"  {self.command} {self.path}")

    def do_OPTIONS(self):
        self.send_response(200)
        origin = self.headers.get('Origin', '')
        allowed = list(CORS_ORIGINS) + ['http://localhost', 'http://127.0.0.1']
        if any(origin.startswith(o) for o in allowed):
            self.send_header('Access-Control-Allow-Origin', origin)
            self.send_header('Vary', 'Origin')
        self.send_header('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type,Authorization')
        _add_security_headers(self)
        self.end_headers()

    def handle_error(self, e):
        try:
            import traceback
            traceback.print_exc()
            _log('error', f'Unhandled exception: {e}', path=self.path, exc_info=True)
            send_json(self, {'error': str(e)}, 500)
        except Exception:
            pass

    def do_GET(self):
        try:
            self._do_GET_inner()
        except Exception as e:
            self.handle_error(e)

    def _do_GET_inner(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')
        qs     = parse_qs(parsed.query)

        try:
            # ── Static files ──────────────────────────────────────
            # If accessed via order.spicetopia.food, always serve order.html
            host = (self.headers.get('X-Forwarded-Host') or self.headers.get('Host', '')).split(':')[0].lower()
            if host == 'order.spicetopia.food':
                order_page = PUBLIC_DIR / 'order.html'
                if order_page.exists():
                    self._serve_file(order_page, 'text/html; charset=utf-8')
                else:
                    send_error(self, "Order portal not found", 404)
                return

            if path == '' or path == '/':
                self._serve_file(PUBLIC_DIR / 'index.html', 'text/html')
                return

            # /welcome — personalised splash (no auth required)
            if path == '/welcome':
                html = _build_welcome_html()
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.end_headers()
                self.wfile.write(html.encode('utf-8'))
                return

            # /field — mobile field app (no BMS auth required — uses PIN)
            if path in ('/field', '/field/'):
                field_page = PUBLIC_DIR / 'field.html'
                if field_page.exists():
                    self._serve_file(field_page, 'text/html; charset=utf-8')
                else:
                    send_error(self, "Field app not found", 404)
                return

            # /order — B2B sales rep PWA (no BMS auth required — uses field PIN)
            if path in ('/order', '/order/'):
                order_page = PUBLIC_DIR / 'order.html'
                if order_page.exists():
                    self._serve_file(order_page, 'text/html; charset=utf-8')
                else:
                    send_error(self, "Order portal not found", 404)
                return

            # /order-manifest.json — PWA manifest
            if path == '/order-manifest.json':
                mf = PUBLIC_DIR / 'order-manifest.json'
                if mf.exists():
                    self._serve_file(mf, 'application/manifest+json')
                else:
                    send_error(self, "Manifest not found", 404)
                return

            # /order-sw.js — service worker
            if path == '/order-sw.js':
                sw = PUBLIC_DIR / 'order-sw.js'
                if sw.exists():
                    self._serve_file(sw, 'application/javascript')
                else:
                    send_error(self, "Service worker not found", 404)
                return

            # /db-upload — one-time database upload page (admin only)
            if path == '/db-upload':
                sess = get_session(self)
                if not sess or sess['role'] != 'admin':
                    self.send_response(302)
                    self.send_header('Location', '/')
                    self.end_headers()
                    return
                html = """<!DOCTYPE html><html><head><meta charset=utf-8>
                <title>Upload Database — Spicetopia</title>
                <style>
                  body{font-family:-apple-system,sans-serif;background:#0f0f0f;color:#fff;
                       display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}
                  .box{background:#1a1a1a;border:1px solid #333;border-radius:16px;
                       padding:48px;max-width:480px;width:90%;text-align:center}
                  h2{margin:0 0 8px;font-size:22px}
                  p{color:#888;font-size:13px;margin-bottom:28px;line-height:1.6}
                  input[type=file]{display:block;width:100%;margin-bottom:20px;color:#ccc;
                                   background:#111;border:1px solid #333;border-radius:8px;padding:10px}
                  button{background:#E8901A;color:#000;font-weight:700;font-size:14px;
                         padding:12px 40px;border-radius:10px;border:none;cursor:pointer;width:100%}
                  .msg{margin-top:16px;font-size:13px;color:#4caf50}
                  .err{margin-top:16px;font-size:13px;color:#f44336}
                </style></head><body>
                <div class=box>
                  <h2>Upload Database</h2>
                  <p>Upload your <strong>spicetopia.db</strong> file from your Mac.<br>
                     This will replace the current cloud database.</p>
                  <form id=f enctype="multipart/form-data">
                    <input type=file id=db name=db accept=".db" required>
                    <button type=button onclick=upload()>Upload & Replace Database</button>
                  </form>
                  <div id=msg></div>
                </div>
                <script>
                async function upload(){
                  const f=document.getElementById('db').files[0];
                  if(!f){alert('Select a .db file first');return}
                  const fd=new FormData();fd.append('db',f);
                  document.getElementById('msg').innerHTML='<span style=color:#888>Uploading...</span>';
                  const r=await fetch('/api/admin/db-upload',{method:'POST',body:fd,
                    headers:{Authorization:'Bearer '+localStorage.getItem('sp_token')}});
                  const d=await r.json();
                  if(d.ok){document.getElementById('msg').className='msg';
                            document.getElementById('msg').textContent='✓ Database replaced. Redirecting...'
                            setTimeout(()=>location.href='/',2000)}
                  else{document.getElementById('msg').className='err';
                       document.getElementById('msg').textContent='Error: '+d.error}
                }
                </script></body></html>"""
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                self.end_headers()
                self.wfile.write(html.encode())
                return

            if not path.startswith('/api'):
                file_path = PUBLIC_DIR / path.lstrip('/')
                if file_path.exists() and file_path.is_file():
                    ext = file_path.suffix.lower()
                    ct  = {
                        '.css':  'text/css',
                        '.js':   'application/javascript',
                        '.jpg':  'image/jpeg',
                        '.jpeg': 'image/jpeg',
                        '.png':  'image/png',
                        '.gif':  'image/gif',
                        '.webp': 'image/webp',
                        '.svg':  'image/svg+xml',
                        '.ico':  'image/x-icon',
                    }.get(ext, 'text/html')
                    self._serve_file(file_path, ct)
                else:
                    send_error(self, "Not found", 404)
                return

            # ── GET /health — no auth required, for load balancers / monitoring ──
            if path == '/health' or path == '/api/health':
                db_ok = False
                try:
                    qry1("SELECT 1", ())
                    db_ok = True
                except Exception:
                    pass
                uptime = int(time.time() - _SERVER_START_TIME)
                status = 'ok' if db_ok else 'degraded'
                code   = 200 if db_ok else 503
                send_json(self, {
                    'status':         status,
                    'db':             'ok' if db_ok else 'error',
                    'uptime_seconds': uptime,
                    'version':        '2.0',
                }, code)
                return

            # ── Auth gate (all /api/ except auth endpoints) ──────
            sess = None   # will be set by get_session below
            if path not in ('/api/auth/login', '/api/auth/me'):
                sess = get_session(self)
                if not sess:
                    send_json(self, {'error': 'Unauthorized'}, 401); return

            # GET /api/health — no auth, returns server start time for deploy verification
            if path == '/api/health':
                send_json(self, {'ok': True, 'started_at': SERVER_START_TIME})
                return

            # GET /api/auth/me — session status
            if path == '/api/auth/me':
                sess = get_session(self)
                if sess:
                    send_json(self, {'authenticated': True, **sess})
                else:
                    send_json(self, {'authenticated': False})
                return

            # GET /api/users  (admin only)
            if path == '/api/users':
                sess = get_session(self)
                if not sess or sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                send_json(self, list_users())
                return

            # ── ADMIN BACKUP STATUS ────────────────────────────────────────
            # GET /api/admin/backup  — list backups + next run time (admin only)
            if path == '/api/admin/backup':
                if not sess or sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                backups = []
                if BACKUP_PATH and BACKUP_PATH.exists():
                    for f in sorted(BACKUP_PATH.glob('spicetopia_*.db'), reverse=True):
                        backups.append({
                            'filename': f.name,
                            'size_kb':  f.stat().st_size // 1024,
                            'modified': datetime.fromtimestamp(f.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S'),
                        })
                send_json(self, {
                    'backup_path':       str(BACKUP_PATH) if BACKUP_PATH else None,
                    'backup_keep_days':  BACKUP_KEEP_DAYS,
                    'backup_count':      len(backups),
                    'total_size_kb':     sum(b['size_kb'] for b in backups),
                    'last_backup':       backups[0]['modified'] if backups else None,
                    'backups':           backups,
                })
                return

            # GET /api/admin/settings  — return runtime + DB settings (admin only)
            if path == '/api/admin/settings':
                if not sess or sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                send_json(self, {
                    'whatsapp_enabled':       WA_ENABLED,
                    'whatsapp_admin_phone':   WA_ADMIN_PHONE,
                    'whatsapp_admin_apikey':  WA_ADMIN_APIKEY,
                    'whatsapp_expiry_warn_hours': WA_EXPIRY_WARN_HOURS,
                })
                return

            # ── ADMIN PRICE MASTER ─────────────────────────────────────────
            # GET /api/admin/price-master  — full price matrix (admin only)
            if path == '/api/admin/price-master':
                sess = get_session(self)
                if not sess or sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                rows = qry("""
                    SELECT  p.code  AS product_code,
                            p.name  AS product_name,
                            ps.label AS pack_size,
                            pt.code  AS price_type_code,
                            pt.label AS price_type_label,
                            pt.id    AS price_type_id,
                            pv.id    AS variant_id,
                            pv.sku_code,
                            COALESCE(pp.price, 0)            AS price,
                            COALESCE(pp.effective_from, '')  AS effective_from,
                            COALESCE(pp.active_flag, 0)      AS active_flag,
                            pp.id                            AS price_id
                    FROM    product_variants pv
                    JOIN    products    p  ON p.id  = pv.product_id
                    JOIN    pack_sizes  ps ON ps.id = pv.pack_size_id
                    CROSS JOIN price_types pt
                    LEFT JOIN product_prices pp
                           ON pp.product_variant_id = pv.id
                          AND pp.price_type_id = pt.id
                          AND pp.active_flag = 1
                    WHERE   pv.active_flag = 1
                      AND   pt.code != 'mfg_cost'
                    ORDER   BY p.name, ps.grams, pt.id
                """)
                send_json(self, rows)
                return

            # GET /api/admin/ingredients  — ALL ingredients incl. inactive (admin only)
            if path == '/api/admin/ingredients':
                if not sess or sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                stock_map = get_stock_map()
                rows = qry("SELECT * FROM ingredients ORDER BY code")
                result = []
                for i in rows:
                    iid  = i['id']
                    bal  = stock_map.get(iid, 0)
                    active = i.get('active', 1)
                    if active is None: active = 1
                    result.append({
                        **dict(i),
                        'active':       int(active),
                        'balanceGrams': r2(bal),
                        'status':       'INACTIVE' if not active else (
                                        'OK' if bal > float(i.get('reorder_level') or 0) else
                                        ('LOW' if bal > 0 else 'OUT')),
                    })
                send_json(self, result)
                return

            # GET /api/ingredients/next-code  — peek next ING-xxxSP code (admin)
            if path == '/api/ingredients/next-code':
                if not sess or sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                send_json(self, {'code': peek_next_ingredient_code()})
                return

            # GET /api/products/next-blend-code?prefix=GM  — peek next GM-BC-xxx code (admin)
            if path == '/api/products/next-blend-code':
                if not sess or sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                prefix = qs.get('prefix', [''])[0].strip().upper()
                if not prefix:
                    send_json(self, {'error': 'prefix is required (e.g. GM, CM, RCP)'}, 400); return
                send_json(self, {'code': peek_next_blend_code(prefix)})
                return

            # GET /api/admin/price-master/export  — CSV download (admin only)
            # GET /api/admin/masters/template/{type}  — download CSV template
            if path.startswith('/api/admin/masters/template/'):
                sess = get_session(self, qs)
                if not sess or sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                master_type = path.split('/')[-1]
                csv_bytes = _master_template_csv(master_type)
                if not csv_bytes:
                    send_error(self, f'Unknown master type: {master_type}', 404); return
                fname = f"template_{master_type}.csv"
                self.send_response(200)
                self.send_header('Content-Type', 'text/csv; charset=utf-8')
                self.send_header('Content-Disposition', f'attachment; filename="{fname}"')
                self.send_header('Content-Length', str(len(csv_bytes)))
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(csv_bytes)
                return

            if path == '/api/admin/price-master/export':
                sess = get_session(self, qs)
                if not sess or sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                rows = qry("""
                    SELECT  p.code   AS product_code,
                            p.name   AS product_name,
                            ps.label AS pack_size,
                            pt.code  AS price_type,
                            COALESCE(pp.price, 0) AS price,
                            COALESCE(pp.effective_from, ?) AS effective_from
                    FROM    product_variants pv
                    JOIN    products   p  ON p.id  = pv.product_id
                    JOIN    pack_sizes ps ON ps.id = pv.pack_size_id
                    CROSS JOIN price_types pt
                    LEFT JOIN product_prices pp
                           ON pp.product_variant_id = pv.id
                          AND pp.price_type_id = pt.id
                          AND pp.active_flag = 1
                    WHERE   pv.active_flag = 1
                    ORDER   BY p.name, ps.grams, pt.id
                """, (today(),))
                import io
                buf = io.StringIO()
                writer = csv.writer(buf)
                writer.writerow(['product_code','product_name','pack_size','price_type','price','effective_from'])
                for r in rows:
                    writer.writerow([r['product_code'], r['product_name'], r['pack_size'],
                                     r['price_type'], r['price'], r['effective_from']])
                csv_bytes = buf.getvalue().encode('utf-8')
                fname = f"spicetopia_prices_{today()}.csv"
                self.send_response(200)
                self.send_header('Content-Type', 'text/csv; charset=utf-8')
                self.send_header('Content-Disposition', f'attachment; filename="{fname}"')
                self.send_header('Content-Length', str(len(csv_bytes)))
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(csv_bytes)
                return

            # ── API Routes ────────────────────────────────────────

            # GET /api/ref — all reference data for the UI
            if path == '/api/ref':
                stock_map = get_stock_map()
                fg_stock  = get_finished_stock_map()

                products = []
                for p in ref.get('products', []):
                    pvs = [v for v in ref.get('variants', []) if v['product_id'] == p['id']]
                    for v in pvs:
                        v['stockUnits'] = r2(fg_stock.get(v['id'], 0))
                    p['variants'] = pvs
                    products.append(p)

                ingredients = []
                for i in ref.get('ingredients', []):
                    iid = qry1("SELECT id FROM ingredients WHERE code=?", (i['code'],))
                    bal = stock_map.get(iid['id'], 0) if iid else 0
                    ingredients.append({**i, 'balanceGrams': r2(bal),
                                        'status': ('OK' if bal > i.get('reorder_level',0)
                                                   else ('LOW' if bal > 0 else 'OUT'))})
                send_json(self, {
                    'products':    products,
                    'customers':   ref.get('customers', []),
                    'suppliers':   ref.get('suppliers', []),
                    'ingredients': ingredients,
                    'packSizes':   ref.get('pack_sizes', []),
                    'priceTypes':  ref.get('price_types', []),
                    'roles':       [{'value': r, 'label': ROLE_LABELS.get(r, r)} for r in VALID_ROLES],
                })
                return

            # GET /api/dashboard
            if path == '/api/dashboard':
                send_json(self, get_dashboard())
                return

            # GET /api/products
            if path == '/api/products':
                send_json(self, ref.get('products', []))
                return

            # GET /api/prices  — all active prices (product_code, pack_size, price, price_type_code)
            if path == '/api/prices':
                prices = qry("""
                    SELECT pp.id, pp.price, pp.effective_from, pp.active_flag,
                           pt.code  AS price_type_code,
                           pt.label AS price_type_label,
                           pv.sku_code,
                           ps.label AS pack_size,
                           p.code   AS product_code,
                           p.name   AS product_name
                    FROM   product_prices pp
                    JOIN   price_types    pt ON pt.id = pp.price_type_id
                    JOIN   product_variants pv ON pv.id = pp.product_variant_id
                    JOIN   pack_sizes     ps ON ps.id = pv.pack_size_id
                    JOIN   products       p  ON p.id  = pv.product_id
                    WHERE  pp.active_flag = 1
                    ORDER  BY p.name, ps.grams, pt.id
                """)
                send_json(self, prices)
                return

            # GET /api/products/:code/prices
            if path.startswith('/api/products/') and path.endswith('/prices'):
                code = path.split('/')[3]
                prod = qry1("SELECT id FROM products WHERE code=?", (code,))
                if not prod:
                    send_error(self, "Product not found", 404); return
                prices = qry("""
                    SELECT pp.*, pt.code as price_type_code, pt.label as price_type_label,
                           pv.sku_code, ps.label as pack_size
                    FROM product_prices pp
                    JOIN price_types pt     ON pt.id = pp.price_type_id
                    JOIN product_variants pv ON pv.id = pp.product_variant_id
                    JOIN pack_sizes ps       ON ps.id = pv.pack_size_id
                    WHERE pv.product_id=? AND pp.active_flag=1
                    ORDER BY ps.grams, pt.id
                """, (prod['id'],))
                send_json(self, prices)
                return

            # GET /api/customers
            if path == '/api/customers':
                show_all = qs.get('all', [None])[0] == '1'
                if show_all:
                    rows = qry("SELECT * FROM customers ORDER BY name")
                    send_json(self, rows)
                else:
                    send_json(self, ref.get('customers', []))
                return

            # GET /api/customers/export  — CSV download of all customers
            if path == '/api/customers/export':
                sess = get_session(self, qs)
                if not sess:
                    send_error(self, 'Unauthorized', 401); return
                rows = qry("""
                    SELECT code, account_number, name, customer_type,
                           city, address, phone, email, payment_terms_days,
                           credit_limit, created_at
                    FROM customers
                    WHERE COALESCE(active,1)=1
                    ORDER BY name
                """)
                import io
                buf = io.StringIO()
                writer = csv.writer(buf)
                writer.writerow(['code','account_number','name','customer_type',
                                 'city','address','phone','email','payment_terms_days',
                                 'credit_limit','created_at'])
                for r in rows:
                    writer.writerow([
                        r['code'],
                        r['account_number'] or '',
                        r['name'],
                        r['customer_type'],
                        r['city'] or '',
                        r['address'] or '',
                        r['phone'] or '',
                        r['email'] or '',
                        r['payment_terms_days'],
                        r2(r['credit_limit']),
                        r['created_at']
                    ])
                csv_bytes = buf.getvalue().encode('utf-8')
                fname = f"spicetopia_customers_{today()}.csv"
                self.send_response(200)
                self.send_header('Content-Type', 'text/csv; charset=utf-8')
                self.send_header('Content-Disposition', f'attachment; filename="{fname}"')
                self.send_header('Content-Length', str(len(csv_bytes)))
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(csv_bytes)
                return

            # GET /api/customers/:id/report  — full customer profile report
            if path.startswith('/api/customers/') and path.endswith('/report'):
                cust_id = int(path.split('/')[3])
                cust = qry1("SELECT * FROM customers WHERE id=?", (cust_id,))
                if not cust:
                    send_error(self, "Customer not found", 404); return

                # All invoices with computed balances
                inv_rows = qry("""
                    SELECT id, invoice_number, invoice_date, due_date, status
                    FROM invoices WHERE customer_id=? ORDER BY invoice_date DESC
                """, (cust['id'],))
                today_str = date.today().isoformat()
                invoices = []
                total_invoiced = 0.0; total_paid_alloc = 0.0; total_balance = 0.0
                overdue_count  = 0;   overdue_amount   = 0.0
                aging = {'0_30': 0.0, '31_60': 0.0, '61_90': 0.0, '90plus': 0.0}
                for inv in inv_rows:
                    s, t, total, paid, bal = compute_invoice_balance(inv['id'])
                    inv['subtotal'] = s; inv['tax'] = t; inv['total'] = total
                    inv['paid'] = paid; inv['balance'] = bal
                    total_invoiced    += total
                    total_paid_alloc  += paid
                    total_balance     += bal
                    if bal > 0 and inv['due_date'] and inv['due_date'] < today_str:
                        overdue_count  += 1
                        overdue_amount += bal
                        days_over = (date.today() - date.fromisoformat(inv['due_date'])).days
                        if   days_over <= 30:  aging['0_30']  += bal
                        elif days_over <= 60:  aging['31_60'] += bal
                        elif days_over <= 90:  aging['61_90'] += bal
                        else:                  aging['90plus'] += bal
                    invoices.append(dict(inv))

                # All payments
                payments = qry("""
                    SELECT id, payment_ref, payment_date, payment_mode, amount, notes
                    FROM customer_payments WHERE customer_id=? ORDER BY payment_date DESC
                """, (cust['id'],))

                # Top products purchased (from invoice_items)
                top_products = qry("""
                    SELECT ii.product_name, ii.pack_size,
                           SUM(ii.quantity)  AS total_qty,
                           SUM(ii.line_total) AS total_value,
                           COUNT(DISTINCT ii.invoice_id) AS order_count
                    FROM invoice_items ii
                    JOIN invoices inv ON inv.id = ii.invoice_id
                    WHERE inv.customer_id=?
                    GROUP BY ii.product_code, ii.pack_size
                    ORDER BY total_value DESC LIMIT 10
                """, (cust['id'],))

                # Last order date
                last_inv = invoices[0] if invoices else None
                last_order_date = last_inv['invoice_date'] if last_inv else None
                avg_invoice = r2(total_invoiced / len(invoices)) if invoices else 0

                # Credit limit utilization
                credit_limit = float(cust.get('credit_limit') or 0)
                credit_used_pct = r2((total_balance / credit_limit * 100) if credit_limit > 0 else 0)

                send_json(self, {
                    'customer':       dict(cust),
                    'invoices':       invoices,
                    'payments':       [dict(p) for p in payments],
                    'top_products':   [dict(p) for p in top_products],
                    'summary': {
                        'total_invoiced':   r2(total_invoiced),
                        'total_paid':       r2(total_paid_alloc),
                        'total_balance':    r2(total_balance),
                        'invoice_count':    len(invoices),
                        'overdue_count':    overdue_count,
                        'overdue_amount':   r2(overdue_amount),
                        'last_order_date':  last_order_date,
                        'avg_invoice':      avg_invoice,
                        'credit_limit':     credit_limit,
                        'credit_used_pct':  credit_used_pct,
                    },
                    'aging': {k: r2(v) for k, v in aging.items()},
                })
                return

            # GET /api/customers/:id/statement/pdf
            if path.startswith('/api/customers/') and path.endswith('/statement/pdf'):
                sess = get_session(self, qs)
                if not require(sess, 'admin','accountant','sales'):
                    send_error(self, 'Permission denied', 403); return
                cust_id = int(path.split('/')[3])
                try:
                    pdf_bytes = generate_statement_pdf(cust_id)
                    cust = qry1("SELECT account_number, name FROM customers WHERE id=?", (cust_id,))
                    acct = (cust or {}).get('account_number') or (cust or {}).get('name', f'CUST-{cust_id}')
                    fname = f"Statement-{acct}-{date.today().isoformat()}.pdf"
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/pdf')
                    self.send_header('Content-Disposition', f'attachment; filename="{fname}"')
                    self.send_header('Content-Length', str(len(pdf_bytes)))
                    self.end_headers()
                    self.wfile.write(pdf_bytes)
                except Exception as e:
                    send_error(self, str(e), 500)
                return

            # GET /api/customers/:id/statement — chronological statement
            if path.startswith('/api/customers/') and path.endswith('/statement'):
                cust_id = int(path.split('/')[3])
                cust = qry1("SELECT * FROM customers WHERE id=?", (cust_id,))
                if not cust:
                    send_error(self, "Customer not found", 404); return
                today_str = date.today().isoformat()
                inv_rows = qry("""
                    SELECT id, invoice_number, invoice_date, due_date, status
                    FROM invoices WHERE customer_id=? ORDER BY invoice_date ASC, id ASC
                """, (cust['id'],))
                invoices = []
                for inv in inv_rows:
                    s, t, total, paid, bal = compute_invoice_balance(inv['id'])
                    rec = dict(inv)
                    rec['total'] = total; rec['paid'] = paid; rec['balance'] = bal
                    overdue = bal > 0 and inv['due_date'] and inv['due_date'] < today_str
                    rec['days_overdue'] = (date.today() - date.fromisoformat(inv['due_date'])).days if overdue else 0
                    invoices.append(rec)
                payments = qry("""
                    SELECT id, payment_ref, payment_date, payment_mode, amount, notes
                    FROM customer_payments WHERE customer_id=? ORDER BY payment_date ASC, id ASC
                """, (cust['id'],))
                # Build chronological lines: INVOICE or PAYMENT
                lines = []
                running_balance = 0.0
                events = []
                for inv in invoices:
                    events.append({'date': inv['invoice_date'], 'type': 'INVOICE', 'data': inv})
                for pay in payments:
                    events.append({'date': pay['payment_date'], 'type': 'PAYMENT', 'data': dict(pay)})
                events.sort(key=lambda x: (x['date'], 0 if x['type']=='INVOICE' else 1))
                for ev in events:
                    if ev['type'] == 'INVOICE':
                        running_balance += ev['data']['total']
                        lines.append({
                            'date': ev['date'], 'type': 'INVOICE',
                            'ref': ev['data']['invoice_number'],
                            'due_date': ev['data']['due_date'],
                            'status': ev['data']['status'],
                            'days_overdue': ev['data']['days_overdue'],
                            'debit': ev['data']['total'], 'credit': 0,
                            'balance': r2(running_balance),
                        })
                    else:
                        running_balance -= ev['data']['amount']
                        lines.append({
                            'date': ev['date'], 'type': 'PAYMENT',
                            'ref': ev['data']['payment_ref'],
                            'mode': ev['data']['payment_mode'],
                            'notes': ev['data']['notes'] or '',
                            'debit': 0, 'credit': ev['data']['amount'],
                            'balance': r2(running_balance),
                        })
                total_invoiced  = sum(i['total'] for i in invoices)
                total_paid_alloc= sum(i['paid']  for i in invoices)
                total_balance   = sum(i['balance'] for i in invoices)
                send_json(self, {
                    'customer': dict(cust),
                    'lines': lines,
                    'summary': {
                        'total_invoiced': r2(total_invoiced),
                        'total_paid':     r2(total_paid_alloc),
                        'balance_due':    r2(total_balance),
                        'invoice_count':  len(invoices),
                        'payment_count':  len(payments),
                        'as_of':          today_str,
                    }
                })
                return

            # GET /api/customers/:id/balance
            if path.startswith('/api/customers/') and path.endswith('/balance'):
                cust_id = int(path.split('/')[3])
                cust    = qry1("SELECT id, account_number FROM customers WHERE id=?", (cust_id,))
                if not cust:
                    send_error(self, "Customer not found", 404); return
                invs  = qry("SELECT id FROM invoices WHERE customer_id=?", (cust['id'],))
                total_bal = 0.0
                for inv in invs:
                    _, _, t, p, b = compute_invoice_balance(inv['id'])
                    total_bal += b
                send_json(self, {'customerId': cust_id, 'accountNumber': cust['account_number'], 'balanceDue': r2(total_bal)})
                return

            # GET /api/suppliers  (active only — used by dropdowns)
            if path == '/api/suppliers':
                send_json(self, _suppliers_with_zones())
                return

            # GET /api/suppliers/export  — CSV download of all suppliers
            if path == '/api/suppliers/export':
                sess = get_session(self, qs)
                if not sess:
                    send_error(self, 'Unauthorized', 401); return
                rows = qry("""
                    SELECT s.code, s.name, s.contact, s.phone, s.email,
                           s.city, s.address, z.name as zone_name, s.created_at
                    FROM suppliers s
                    LEFT JOIN zones z ON z.id = s.zone_id
                    WHERE s.active_flag = 1
                    ORDER BY s.name
                """)
                import io
                buf = io.StringIO()
                writer = csv.writer(buf)
                writer.writerow(['code','name','contact','phone','email','city','address','zone','created_at'])
                for r in rows:
                    writer.writerow([r['code'], r['name'], r['contact'], r['phone'],
                                     r['email'], r['city'], r['address'],
                                     r['zone_name'] or '', r['created_at']])
                csv_bytes = buf.getvalue().encode('utf-8')
                fname = f"spicetopia_suppliers_{today()}.csv"
                self.send_response(200)
                self.send_header('Content-Type', 'text/csv; charset=utf-8')
                self.send_header('Content-Disposition', f'attachment; filename="{fname}"')
                self.send_header('Content-Length', str(len(csv_bytes)))
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(csv_bytes)
                return

            # GET /api/admin/suppliers  (all suppliers incl. inactive — admin CRUD view)
            if path == '/api/admin/suppliers':
                if not sess or sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                _ensure_supplier_zone_col()
                rows = qry("""
                    SELECT s.*, z.name as zone_name
                    FROM suppliers s LEFT JOIN zones z ON z.id = s.zone_id
                    ORDER BY s.active_flag DESC, s.name
                """)
                send_json(self, rows)
                return

            # GET /api/suppliers/:id/balance
            if path.startswith('/api/suppliers/') and path.endswith('/balance'):
                sup_id = int(path.split('/')[3])
                bills  = qry("SELECT id FROM supplier_bills WHERE supplier_id=?", (sup_id,))
                total_bal = 0.0
                for b in bills:
                    t, p, bal = compute_bill_balance(b['id'])
                    total_bal += bal
                send_json(self, {'supplierId': sup_id, 'balanceDue': r2(total_bal)})
                return

            # GET /api/ingredient-price-history
            if path == '/api/ingredient-price-history':
                ing_id = qs.get('ingredientId', [None])[0]
                rows = get_ingredient_price_history(int(ing_id) if ing_id else None)
                send_json(self, rows)
                return

            # GET /api/ingredients + stock
            if path == '/api/ingredients':
                stock_map    = get_stock_map()
                reserved_map = get_wo_reserved_stock_map()
                # Build price-history lookup: ingredient_id → latest 2 records
                price_hist = {}
                for row in qry("""
                    SELECT ph.ingredient_id, ph.old_cost_per_kg, ph.new_cost_per_kg,
                           ph.pct_change, ph.changed_at
                    FROM ingredient_price_history ph
                    INNER JOIN (
                        SELECT ingredient_id, MAX(changed_at) as latest
                        FROM ingredient_price_history GROUP BY ingredient_id
                    ) mx ON mx.ingredient_id=ph.ingredient_id AND mx.latest=ph.changed_at
                """):
                    price_hist[row['ingredient_id']] = row
                result = []
                for i in ref.get('ingredients', []):
                    irow = qry1("SELECT id, cost_per_kg FROM ingredients WHERE code=?", (i['code'],))
                    if not irow:
                        continue
                    iid       = irow['id']
                    bal       = stock_map.get(iid, 0)
                    reserved  = reserved_map.get(iid, 0)
                    available = max(0.0, r2(bal - reserved))
                    rl        = r2(i.get('reorder_level', 0))
                    cost      = r2(irow.get('cost_per_kg') or 0)
                    ph        = price_hist.get(iid, {})
                    # old_cost_per_kg is NULL for 'initial' seed records
                    raw_old   = ph.get('old_cost_per_kg')    # may be None
                    old_cost  = r2(raw_old) if raw_old is not None else None
                    pct_chg   = ph.get('pct_change')         # None for initial or 0→price
                    src       = ph.get('source', '')
                    result.append({
                        **i,
                        'id':                iid,
                        'balanceGrams':      r2(bal),
                        'reservedGrams':     r2(reserved),
                        'availableGrams':    available,
                        'status':            'OK' if available > rl else ('LOW' if available > 0 else 'OUT'),
                        'cost_per_kg':       cost,
                        'prev_cost_per_kg':  old_cost,          # None = no previous data
                        'cost_pct_change':   round(pct_chg, 2) if pct_chg is not None else None,
                        'price_source':      src,               # 'initial' | 'master_sync'
                        'last_price_change': ph.get('changed_at'),
                        'has_price_history': bool(ph),
                    })
                send_json(self, result)
                return

            # GET /api/ingredients/export  — CSV download of all ingredients (admin only)
            if path == '/api/ingredients/export':
                sess = get_session(self, qs)
                if not sess or sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                stock_map    = get_stock_map()
                reserved_map = get_wo_reserved_stock_map()
                rows = qry("""
                    SELECT id, code, name, cost_per_kg, reorder_level, unit, created_at
                    FROM ingredients WHERE COALESCE(active,1)=1 ORDER BY code
                """)
                import io
                buf = io.StringIO()
                writer = csv.writer(buf)
                writer.writerow(['code','name','cost_per_kg','unit','stock_grams',
                                 'reserved_grams','available_grams','reorder_level','created_at'])
                for r in rows:
                    iid       = r['id']
                    bal       = round(stock_map.get(iid, 0), 2)
                    reserved  = round(reserved_map.get(iid, 0), 2)
                    available = round(max(0.0, bal - reserved), 2)
                    writer.writerow([r['code'], r['name'], r['cost_per_kg'],
                                     r.get('unit','kg'), bal, reserved, available,
                                     r['reorder_level'], r['created_at']])
                csv_bytes = buf.getvalue().encode('utf-8')
                fname = f"spicetopia_ingredients_{today()}.csv"
                self.send_response(200)
                self.send_header('Content-Type', 'text/csv; charset=utf-8')
                self.send_header('Content-Disposition', f'attachment; filename="{fname}"')
                self.send_header('Content-Length', str(len(csv_bytes)))
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(csv_bytes)
                return

            # GET /api/inventory/ledger?ingredientId=X
            if path == '/api/inventory/ledger':
                ing_id = qs.get('ingredientId', [None])[0]
                if ing_id:
                    rows = qry("""
                        SELECT il.*, i.code, i.name
                        FROM inventory_ledger il JOIN ingredients i ON i.id=il.ingredient_id
                        WHERE il.ingredient_id=?
                        ORDER BY il.created_at DESC LIMIT 100
                    """, (int(ing_id),))
                else:
                    rows = qry("""
                        SELECT il.*, i.code, i.name
                        FROM inventory_ledger il JOIN ingredients i ON i.id=il.ingredient_id
                        ORDER BY il.created_at DESC LIMIT 200
                    """)
                send_json(self, rows)
                return

            # GET /api/work-orders
            if path == '/api/work-orders':
                send_json(self, list_work_orders())
                return

            # GET /api/work-orders/:id  — single WO detail
            if path.startswith('/api/work-orders/') and path.count('/') == 3 and path.split('/')[-1].isdigit():
                wo_id = int(path.split('/')[-1])
                wo = qry1("""
                    SELECT wo.*, p.name as product_name, p.code as product_code,
                           ps.label as pack_size, pv.sku_code,
                           co.order_number as customer_order_number,
                           wo.customer_order_id
                    FROM work_orders wo
                    JOIN product_variants pv ON pv.id = wo.product_variant_id
                    JOIN products p ON p.id = pv.product_id
                    LEFT JOIN pack_sizes ps ON ps.id = pv.pack_size_id
                    LEFT JOIN customer_orders co ON co.id = wo.customer_order_id
                    WHERE wo.id=?
                """, (wo_id,))
                if not wo:
                    send_error(self, "Work order not found", 404); return
                procurement = get_procurement_list(wo_id)
                wo['ingredients'] = procurement.get('lines', [])
                send_json(self, wo)
                return

            # GET /api/work-orders/:id/procurement
            if path.startswith('/api/work-orders/') and path.endswith('/procurement'):
                wo_id = int(path.split('/')[-2])
                send_json(self, get_procurement_list(wo_id))
                return

            # GET /api/work-orders/:id/feasibility  — structured shortfall data
            if path.startswith('/api/work-orders/') and path.endswith('/feasibility'):
                wo_id = int(path.split('/')[-2])
                wo    = qry1("SELECT * FROM work_orders WHERE id=?", (wo_id,))
                if not wo:
                    send_error(self, "Work order not found", 404); return
                result = check_wo_feasibility(wo['product_variant_id'], wo['qty_units'], wo_id=wo_id)
                send_json(self, result)
                return

            # GET /api/work-orders/check?productVariantId=X&qtyUnits=Y
            if path == '/api/work-orders/check':
                vid  = qs.get('productVariantId', [None])[0]
                qty  = qs.get('qtyUnits', ['0'])[0]
                if not vid:
                    send_json(self, {'error': 'productVariantId required'}, 400); return
                send_json(self, check_wo_feasibility(int(vid), int(qty)))
                return

            # GET /api/production/:id  — batch detail with ingredient breakdown
            if path.startswith('/api/production/') and len(path.split('/')) == 4:
                batch_db_id = int(path.split('/')[3])
                batch = qry1("""
                    SELECT pb.*, p.code as product_code, p.name as product_name,
                           ps.label as pack_label, ps.grams as pack_grams
                    FROM production_batches pb
                    JOIN products p ON p.id = pb.product_id
                    LEFT JOIN product_variants pv ON pv.id = pb.product_variant_id
                    LEFT JOIN pack_sizes ps ON ps.id = pv.pack_size_id
                    WHERE pb.id=?
                """, (batch_db_id,))
                if not batch:
                    send_error(self, "Batch not found", 404); return
                consumption = qry("""
                    SELECT pc.qty_grams, i.code as ing_code, i.name as ing_name,
                           i.cost_per_kg,
                           ROUND(pc.qty_grams / 1000.0 * i.cost_per_kg, 2) as line_cost
                    FROM production_consumption pc
                    JOIN ingredients i ON i.id = pc.ingredient_id
                    WHERE pc.batch_id = ?
                    ORDER BY i.code
                """, (batch_db_id,))
                total_cost = r2(sum(c['line_cost'] for c in consumption))
                cost_per_unit = r2(total_cost / batch['qty_units']) if batch['qty_units'] else 0
                send_json(self, {**batch,
                                 'consumption': consumption,
                                 'ingredient_cost': total_cost,
                                 'cost_per_unit': cost_per_unit})
                return

            # GET /api/production
            if path == '/api/production':
                rows = qry("""
                    SELECT pb.*, p.code as product_code, p.name as product_name,
                           ps.label as pack_label,
                           wo.wo_number,
                           COALESCE((
                               SELECT ROUND(SUM(pc.qty_grams / 1000.0 * i.cost_per_kg), 2)
                               FROM production_consumption pc
                               JOIN ingredients i ON i.id = pc.ingredient_id
                               WHERE pc.batch_id = pb.id
                           ), 0) as ingredient_cost
                    FROM production_batches pb
                    JOIN products p ON p.id = pb.product_id
                    LEFT JOIN product_variants pv ON pv.id = pb.product_variant_id
                    LEFT JOIN pack_sizes ps ON ps.id = pv.pack_size_id
                    LEFT JOIN work_orders wo ON wo.batch_id = pb.batch_id
                    ORDER BY pb.batch_date DESC LIMIT 200
                """)
                for row in rows:
                    row['cost_per_unit'] = r2(row['ingredient_cost'] / row['qty_units']) if row.get('qty_units') else 0
                send_json(self, rows)
                return

            # ── Phase 3: Review Queue GET endpoints ──────────────────────

            # GET /api/review-queue
            if path == '/api/review-queue':
                source_f = qs.get('source', [None])[0]
                filters = {}
                if source_f:
                    filters['order_source'] = source_f
                send_json(self, get_review_queue(filters))
                return

            # GET /api/review-queue/:id
            if path.startswith('/api/review-queue/') and len(path.split('/')) == 4:
                order_id = int(path.split('/')[3])
                detail = _order_detail(order_id)
                if not detail:
                    send_error(self, "Order not found", 404); return
                send_json(self, detail)
                return

            # GET /api/stock/available?variantId=X
            if path == '/api/stock/available':
                vid = qs.get('variantId', [None])[0]
                if not vid:
                    send_error(self, "variantId required", 400); return
                send_json(self, get_stock_situation(int(vid)))
                return

            # ── Customer Orders ──────────────────────────────────
            # GET /api/customer-orders
            if path == '/api/customer-orders':
                status_f = qs.get('status', [None])[0]
                send_json(self, list_customer_orders(status_f))
                return

            # GET /api/customer-orders/:id
            if path.startswith('/api/customer-orders/') and len(path.split('/')) == 4:
                order_id = int(path.split('/')[3])
                detail = _order_detail(order_id)
                if not detail:
                    send_error(self, "Order not found", 404); return
                send_json(self, detail)
                return

            # GET /api/sales
            if path == '/api/sales':
                month = qs.get('month', [None])[0]
                if month:
                    rows = qry("SELECT * FROM sales WHERE sale_date LIKE ? ORDER BY sale_date DESC",
                               (f"{month}%",))
                else:
                    rows = qry("SELECT * FROM sales ORDER BY sale_date DESC LIMIT 200")
                send_json(self, rows)
                return

            # GET /api/invoices
            if path == '/api/invoices':
                cust_id = qs.get('customerId', [None])[0]
                status  = qs.get('status', [None])[0]
                sql = """
                    SELECT inv.*, c.name as customer_name, c.customer_type, c.code as cust_code,
                           co.order_number as customer_order_number
                    FROM invoices inv
                    JOIN customers c ON c.id=inv.customer_id
                    LEFT JOIN customer_orders co ON co.id = inv.customer_order_id
                """
                params, where = [], []
                if cust_id:
                    where.append("inv.customer_id=?"); params.append(int(cust_id))
                if status:
                    where.append("inv.status=?"); params.append(status)
                if where:
                    sql += " WHERE " + " AND ".join(where)
                sql += " ORDER BY inv.invoice_date DESC LIMIT 200"
                rows = qry(sql, params)
                # Enrich with computed balances
                for row in rows:
                    s, t, total, paid, bal = compute_invoice_balance(row['id'])
                    row['subtotal'] = s; row['tax'] = t; row['total'] = total
                    row['paid'] = paid; row['balance'] = bal
                send_json(self, rows)
                return

            # GET /api/invoices/:id/pdf
            if path.startswith('/api/invoices/') and path.endswith('/pdf'):
                sess = get_session(self, qs)
                if not require(sess, 'admin','accountant','sales','warehouse'):
                    send_error(self, 'Permission denied', 403); return
                inv_id = int(path.split('/')[3])
                try:
                    pdf_bytes = generate_invoice_pdf(inv_id)
                    inv_num = (qry1("SELECT invoice_number FROM invoices WHERE id=?",
                                    (inv_id,)) or {}).get('invoice_number', f'INV-{inv_id}')
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/pdf')
                    self.send_header('Content-Disposition',
                                     f'attachment; filename="{inv_num}.pdf"')
                    self.send_header('Content-Length', str(len(pdf_bytes)))
                    self.end_headers()
                    self.wfile.write(pdf_bytes)
                except Exception as e:
                    send_error(self, str(e), 500)
                return

            # GET /api/invoices/:id
            if path.startswith('/api/invoices/') and len(path.split('/')) == 4:
                inv_id = int(path.split('/')[3])
                inv = qry1("""
                    SELECT inv.*, c.name as customer_name, c.customer_type, c.code as cust_code,
                           c.email as customer_email, c.phone as customer_phone, c.city as customer_city
                    FROM invoices inv JOIN customers c ON c.id=inv.customer_id
                    WHERE inv.id=?
                """, (inv_id,))
                if not inv:
                    send_error(self, "Invoice not found", 404); return
                items = qry("SELECT * FROM invoice_items WHERE invoice_id=?", (inv_id,))
                allocs = qry("""
                    SELECT pa.*, cp.payment_ref, cp.payment_date, cp.payment_mode
                    FROM payment_allocations pa
                    JOIN customer_payments cp ON cp.id=pa.payment_id
                    WHERE pa.invoice_id=?
                """, (inv_id,))
                s, t, total, paid, bal = compute_invoice_balance(inv_id)
                send_json(self, {**inv, 'items': items, 'allocations': allocs,
                                  'subtotal': s, 'tax': t, 'total': total,
                                  'paid': paid, 'balance': bal})
                return

            # GET /api/customer-payments
            if path == '/api/customer-payments':
                cust_id = qs.get('customerId', [None])[0]
                if cust_id:
                    rows = qry("""
                        SELECT cp.*, c.name as customer_name
                        FROM customer_payments cp JOIN customers c ON c.id=cp.customer_id
                        WHERE cp.customer_id=? ORDER BY cp.payment_date DESC
                    """, (int(cust_id),))
                else:
                    rows = qry("""
                        SELECT cp.*, c.name as customer_name
                        FROM customer_payments cp JOIN customers c ON c.id=cp.customer_id
                        ORDER BY cp.payment_date DESC LIMIT 200
                    """)
                # Enrich with unallocated balance + applied invoice numbers
                for r in rows:
                    alloc = r2(qry1(
                        "SELECT COALESCE(SUM(allocated_amount),0) as s FROM payment_allocations WHERE payment_id=?",
                        (r['id'],)
                    )['s'])
                    r['allocated'] = alloc
                    r['unallocated'] = r2(r['amount'] - alloc)
                    inv_refs = qry("""
                        SELECT i.invoice_number FROM payment_allocations pa
                        JOIN invoices i ON i.id=pa.invoice_id
                        WHERE pa.payment_id=? ORDER BY i.invoice_date
                    """, (r['id'],))
                    r['applied_to'] = [x['invoice_number'] for x in inv_refs]
                send_json(self, rows)
                return

            # GET /api/customer-payments/:id  — single payment detail with allocations
            if path.startswith('/api/customer-payments/') and len(path.split('/')) == 4:
                pay_id = int(path.split('/')[3])
                pay = qry1("""
                    SELECT cp.*, c.name as customer_name, c.code as customer_code,
                           c.phone as customer_phone
                    FROM customer_payments cp JOIN customers c ON c.id=cp.customer_id
                    WHERE cp.id=?
                """, (pay_id,))
                if not pay:
                    send_error(self, "Payment not found", 404); return
                allocs = qry("""
                    SELECT pa.*, i.invoice_number, i.invoice_date
                    FROM payment_allocations pa
                    JOIN invoices i ON i.id=pa.invoice_id
                    WHERE pa.payment_id=?
                    ORDER BY i.invoice_date
                """, (pay_id,))
                total_alloc = r2(sum(a['allocated_amount'] for a in allocs))
                pay['allocated']   = total_alloc
                pay['unallocated'] = r2(pay['amount'] - total_alloc)
                pay['allocations'] = allocs
                send_json(self, pay)
                return

            # GET /api/ar/aging
            if path == '/api/ar/aging':
                send_json(self, get_ar_aging())
                return

            # GET /api/bills
            if path == '/api/bills':
                sup_id = qs.get('supplierId', [None])[0]
                sql = """
                    SELECT sb.*, s.name as supplier_name
                    FROM supplier_bills sb JOIN suppliers s ON s.id=sb.supplier_id
                """
                params = []
                if sup_id:
                    sql += " WHERE sb.supplier_id=?"
                    params.append(int(sup_id))
                sql += " ORDER BY sb.bill_date DESC LIMIT 200"
                rows = qry(sql, params)
                for row in rows:
                    t, p, b = compute_bill_balance(row['id'])
                    row['total'] = t; row['paid'] = p; row['balance'] = b
                    # Self-heal: sync status from actual amounts (catches legacy mismatches)
                    if row.get('status') not in ('VOID',):
                        correct_status = _compute_bill_status(row['id'])
                        if row.get('status') != correct_status:
                            run("UPDATE supplier_bills SET status=? WHERE id=?",
                                (correct_status, row['id']))
                            row['status'] = correct_status
                send_json(self, rows)
                return

            # GET /api/bills/:id
            if path.startswith('/api/bills/') and len(path.split('/')) == 4:
                bill_id = int(path.split('/')[3])
                bill = qry1("""
                    SELECT sb.*, s.name as supplier_name
                    FROM supplier_bills sb JOIN suppliers s ON s.id=sb.supplier_id
                    WHERE sb.id=?
                """, (bill_id,))
                if not bill:
                    send_error(self, "Bill not found", 404); return
                items = qry("""
                    SELECT sbi.*, i.code as ing_code, i.name as ing_name
                    FROM supplier_bill_items sbi JOIN ingredients i ON i.id=sbi.ingredient_id
                    WHERE sbi.bill_id=?
                """, (bill_id,))
                allocs = qry("""
                    SELECT spa.*, sp.payment_ref, sp.payment_date, sp.payment_mode
                    FROM supplier_payment_allocations spa
                    JOIN supplier_payments sp ON sp.id=spa.payment_id
                    WHERE spa.bill_id=?
                """, (bill_id,))
                t, p, b = compute_bill_balance(bill_id)
                # Self-heal: sync status from actual amounts
                if bill.get('status') not in ('VOID',):
                    correct_status = _compute_bill_status(bill_id)
                    if bill.get('status') != correct_status:
                        run("UPDATE supplier_bills SET status=? WHERE id=?",
                            (correct_status, bill_id))
                        bill['status'] = correct_status
                # Attach originating PO info for back-link
                po_link = None
                if bill.get('po_id'):
                    po_row = qry1("SELECT po_number FROM purchase_orders WHERE id=?", (bill['po_id'],))
                    if po_row:
                        po_link = {'id': bill['po_id'], 'po_number': po_row['po_number']}
                send_json(self, {**bill, 'items': items, 'allocations': allocs,
                                  'total': t, 'paid': p, 'balance': b, 'po_link': po_link})
                return

            # GET /api/supplier-payments
            if path == '/api/supplier-payments':
                sup_id = qs.get('supplierId', [None])[0]
                if sup_id:
                    rows = qry("""
                        SELECT sp.*, s.name as supplier_name
                        FROM supplier_payments sp JOIN suppliers s ON s.id=sp.supplier_id
                        WHERE sp.supplier_id=? ORDER BY sp.payment_date DESC
                    """, (int(sup_id),))
                else:
                    rows = qry("""
                        SELECT sp.*, s.name as supplier_name
                        FROM supplier_payments sp JOIN suppliers s ON s.id=sp.supplier_id
                        ORDER BY sp.payment_date DESC LIMIT 200
                    """)
                for r in rows:
                    alloc = r2(qry1(
                        "SELECT COALESCE(SUM(allocated_amount),0) as s FROM supplier_payment_allocations WHERE payment_id=?",
                        (r['id'],)
                    )['s'])
                    r['allocated'] = alloc
                    r['unallocated'] = r2(r['amount'] - alloc)
                    bill_refs = qry("""
                        SELECT sb.bill_number FROM supplier_payment_allocations spa
                        JOIN supplier_bills sb ON sb.id=spa.bill_id
                        WHERE spa.payment_id=? ORDER BY sb.bill_date
                    """, (r['id'],))
                    r['applied_to'] = [x['bill_number'] for x in bill_refs]
                send_json(self, rows)
                return

            # GET /api/supplier-payments/:id  — single payment detail with allocations
            if path.startswith('/api/supplier-payments/') and len(path.split('/')) == 4:
                pay_id = int(path.split('/')[3])
                pay = qry1("""
                    SELECT sp.*, s.name as supplier_name
                    FROM supplier_payments sp JOIN suppliers s ON s.id=sp.supplier_id
                    WHERE sp.id=?
                """, (pay_id,))
                if not pay:
                    send_error(self, "Payment not found", 404); return
                allocs = qry("""
                    SELECT spa.*, sb.bill_number, sb.bill_date
                    FROM supplier_payment_allocations spa
                    JOIN supplier_bills sb ON sb.id=spa.bill_id
                    WHERE spa.payment_id=?
                    ORDER BY sb.bill_date
                """, (pay_id,))
                total_alloc = r2(sum(a['allocated_amount'] for a in allocs))
                pay['allocated']   = total_alloc
                pay['unallocated'] = r2(pay['amount'] - total_alloc)
                pay['allocations'] = allocs
                send_json(self, pay)
                return

            # GET /api/ap/aging
            if path == '/api/ap/aging':
                send_json(self, get_ap_aging())
                return

            # GET /api/costing/config  (admin only)
            if path == '/api/costing/config':
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                send_json(self, get_costing_config())
                return

            # GET /api/costing/standard-costs  (admin only)
            if path == '/api/costing/standard-costs':
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                send_json(self, get_all_standard_costs())
                return

            # GET /api/costing/standard-costs/:productCode/:packSize  (admin only)
            if path.startswith('/api/costing/standard-costs/') and len(path.split('/')) == 6:
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                parts = path.split('/')
                result = compute_standard_cost(parts[4], parts[5])
                if not result:
                    send_error(self, 'SKU not found', 404); return
                send_json(self, result)
                return

            # GET /api/costing/batch-variances  (admin only)
            if path == '/api/costing/batch-variances':
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                days = int(qs.get('days', ['90'])[0])
                send_json(self, get_batch_variances(days))
                return

            # GET /api/costing/price-history  (admin only)
            if path == '/api/costing/price-history':
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                limit = int(qs.get('limit', ['100'])[0])
                change_type = qs.get('type', [None])[0]
                days = qs.get('days', [None])[0]
                days = int(days) if days else None
                send_json(self, get_price_history(limit=limit, change_type=change_type, days=days))
                return

            # GET /api/costing/margin-alerts  (admin only)
            if path == '/api/costing/margin-alerts':
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                include_dismissed = qs.get('dismissed', ['false'])[0].lower() == 'true'
                alerts = get_margin_alerts(include_dismissed=include_dismissed)
                # Optionally trigger email for new unsent alerts
                unsent = [a for a in alerts if not a.get('dismissed') and not a.get('emailSent')]
                if unsent:
                    send_margin_alert_email(unsent)
                send_json(self, alerts)
                return

            # GET /api/reports/pl?year=YYYY
            if path == '/api/reports/pl':
                sess = get_session(self, qs)
                if not require(sess, 'admin', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                year = qs.get('year', [str(date.today().year)])[0]
                send_json(self, get_pl_report(year))
                return

            # GET /api/reports/margins
            if path == '/api/reports/margins':
                month = qs.get('month', [None])[0]
                send_json(self, get_margin_report(month))
                return

            # GET /api/reports/rep-performance?period=YYYY-MM
            if path == '/api/reports/rep-performance':
                period = qs.get('period', [None])[0]
                send_json(self, get_rep_performance_report(period))
                return

            # GET /api/bom/:productCode
            if path.startswith('/api/bom/'):
                code = path.split('/')[3]
                prod = qry1("SELECT id FROM products WHERE code=?", (code,))
                if not prod:
                    send_error(self, "Product not found", 404); return
                bv = qry1("""
                    SELECT * FROM bom_versions WHERE product_id=? AND active_flag=1
                    ORDER BY version_no DESC LIMIT 1
                """, (prod['id'],))
                if not bv:
                    send_json(self, {'version': None, 'items': []}); return
                items = qry("""
                    SELECT bi.*, i.code as ing_code, i.name as ing_name
                    FROM bom_items bi JOIN ingredients i ON i.id=bi.ingredient_id
                    WHERE bi.bom_version_id=?
                """, (bv['id'],))
                send_json(self, {**bv, 'items': items})
                return

            # GET /api/audit?table=X&recordId=Y
            if path == '/api/audit':
                table = qs.get('table', [None])[0]
                rec   = qs.get('recordId', [None])[0]
                sql   = "SELECT * FROM change_log"
                p     = []
                if table:
                    sql += " WHERE table_name=?"; p.append(table)
                    if rec:
                        sql += " AND record_id=?"; p.append(rec)
                sql += " ORDER BY timestamp DESC LIMIT 200"
                send_json(self, qry(sql, p))
                return

            # ── ZONES ──────────────────────────────────────────────────────
            # GET /api/zones
            if path == '/api/zones':
                send_json(self, list_zones())
                return

            # GET /api/zones/:id
            if path.startswith('/api/zones/') and len(path.split('/')) == 4:
                zone_id = int(path.split('/')[3])
                z = qry1("SELECT * FROM zones WHERE id=?", (zone_id,))
                if not z:
                    send_error(self, "Zone not found", 404); return
                send_json(self, z)
                return

            # ── ROUTES ─────────────────────────────────────────────────────
            # GET /api/routes
            if path == '/api/routes':
                zone_id = qs.get('zoneId', [None])[0]
                send_json(self, list_routes(int(zone_id) if zone_id else None))
                return

            # GET /api/routes/:id
            if path.startswith('/api/routes/') and len(path.split('/')) == 4:
                route_id = int(path.split('/')[3])
                r = qry1("""
                    SELECT r.*, z.name as zone_name FROM routes r
                    JOIN zones z ON z.id=r.zone_id WHERE r.id=?
                """, (route_id,))
                if not r:
                    send_error(self, "Route not found", 404); return
                send_json(self, r)
                return

            # GET /api/routes/:id/customers
            if path.startswith('/api/routes/') and path.endswith('/customers'):
                route_id = int(path.split('/')[3])
                send_json(self, list_route_customers(route_id))
                return

            # ── SALES REPS ─────────────────────────────────────────────────
            # GET /api/reps
            if path == '/api/reps':
                show_all = qs.get('all', [None])[0] == '1'
                send_json(self, list_reps(active_only=not show_all))
                return

            # GET /api/reps/:id
            if path.startswith('/api/reps/') and len(path.split('/')) == 4:
                rep_id = int(path.split('/')[3])
                rep = get_rep(rep_id)
                if not rep:
                    send_error(self, "Rep not found", 404); return
                send_json(self, rep)
                return

            # GET /api/reps/:id/payroll-preview?period=YYYY-MM
            if path.startswith('/api/reps/') and path.endswith('/payroll-preview'):
                rep_id = int(path.split('/')[3])
                period = qs.get('period', [None])[0] or date.today().strftime('%Y-%m')
                send_json(self, calculate_payroll(rep_id, period))
                return

            # ── FIELD ORDERS ───────────────────────────────────────────────
            # GET /api/field-orders
            if path == '/api/field-orders':
                rep_id    = qs.get('repId',   [None])[0]
                status    = qs.get('status',  [None])[0]
                date_from = qs.get('from',    [None])[0]
                date_to   = qs.get('to',      [None])[0]
                send_json(self, list_field_orders(
                    int(rep_id) if rep_id else None,
                    status, date_from, date_to
                ))
                return

            # GET /api/field-orders/:id
            if path.startswith('/api/field-orders/') and len(path.split('/')) == 4:
                order_id = int(path.split('/')[3])
                order = get_field_order(order_id)
                if not order:
                    send_error(self, "Order not found", 404); return
                send_json(self, order)
                return

            # ── BEAT VISITS ────────────────────────────────────────────────
            # GET /api/beat-visits?repId=X&date=YYYY-MM-DD
            if path == '/api/beat-visits':
                rep_id    = qs.get('repId',  [None])[0]
                visit_dt  = qs.get('date',   [None])[0]
                route_id  = qs.get('routeId',[None])[0]
                sql = """
                    SELECT bv.*, sr.name as rep_name, c.name as customer_name,
                           r.name as route_name
                    FROM beat_visits bv
                    JOIN sales_reps sr ON sr.id=bv.rep_id
                    JOIN customers c ON c.id=bv.customer_id
                    LEFT JOIN routes r ON r.id=bv.route_id
                """
                p, where = [], []
                if rep_id:
                    where.append("bv.rep_id=?"); p.append(int(rep_id))
                if visit_dt:
                    where.append("bv.visit_date=?"); p.append(visit_dt)
                if route_id:
                    where.append("bv.route_id=?"); p.append(int(route_id))
                if where:
                    sql += " WHERE " + " AND ".join(where)
                sql += " ORDER BY bv.visit_date DESC, bv.id DESC LIMIT 200"
                send_json(self, qry(sql, p))
                return

            # ── PAYROLL ────────────────────────────────────────────────────
            # GET /api/payroll?period=YYYY-MM
            if path == '/api/payroll':
                period = qs.get('period', [None])[0]
                send_json(self, list_payroll_runs(period))
                return

            # ── FIELD APP — today's route ──────────────────────────────────
            # GET /api/field/my-routes  (requires field_rep session)
            if path == '/api/field/my-routes':
                rep_id = sess.get('repId') if sess else None
                if not rep_id:
                    send_error(self, "Unauthorized", 401); return
                send_json(self, get_rep_today_route(rep_id))
                return

            # ── B2B PORTAL — FIELD ENDPOINTS ────────────────────────────────

            # GET /api/field/customers/lookup?q=  (field rep session)
            if path == '/api/field/customers/lookup':
                fsess = _get_field_session(self, qs)
                if not fsess:
                    send_json(self, {'error': 'Field rep session required'}, 401); return
                q = (qs.get('q', [''])[0] or '').strip()
                if len(q) < 2:
                    send_json(self, {'error': 'Query must be at least 2 characters'}, 400); return
                send_json(self, field_lookup_customers(q, fsess['repId']))
                return

            # GET /api/field/products?customerType=RETAIL  (field rep session)
            if path == '/api/field/products':
                fsess = _get_field_session(self, qs)
                if not fsess:
                    send_json(self, {'error': 'Field rep session required'}, 401); return
                ctype = qs.get('customerType', ['RETAIL'])[0].upper()
                send_json(self, field_get_products(ctype))
                return

            # ── PURCHASE ORDERS ─────────────────────────────────────────────
            # GET /api/purchase-orders
            if path == '/api/purchase-orders':
                status_f = qs.get('status', [None])[0]
                send_json(self, list_purchase_orders(status_f))
                return

            # GET /api/purchase-orders/bom-calculate?variantId=X&qty=Y
            if path == '/api/purchase-orders/bom-calculate':
                vid = qs.get('variantId', [None])[0]
                qty = qs.get('qty', [None])[0]
                if not vid or not qty:
                    send_error(self, "variantId and qty required", 400); return
                send_json(self, bom_calculate_ingredients(int(vid), int(qty)))
                return

            # GET /api/purchase-orders/:id/pdf
            if path.startswith('/api/purchase-orders/') and path.endswith('/pdf'):
                if not require(sess, 'admin', 'accountant', 'warehouse'):
                    send_error(self, 'Permission denied', 403); return
                po_id = int(path.split('/')[3])
                try:
                    pdf_bytes = generate_po_pdf(po_id)
                    po_num = get_purchase_order(po_id)['po_number']
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/pdf')
                    self.send_header('Content-Disposition', f'attachment; filename="{po_num}.pdf"')
                    self.send_header('Content-Length', str(len(pdf_bytes)))
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(pdf_bytes)
                except Exception as e:
                    send_error(self, str(e), 500)
                return

            # GET /api/purchase-orders/:id
            if path.startswith('/api/purchase-orders/') and len(path.split('/')) == 4:
                po_id = int(path.split('/')[3])
                send_json(self, get_purchase_order(po_id))
                return

            # GET /api/ingredients/:id/last-cost
            if path.startswith('/api/ingredients/') and path.endswith('/last-cost'):
                ing_id = int(path.split('/')[3])
                row = qry1("""
                    SELECT pi.unit_cost_kg
                    FROM po_items pi
                    JOIN purchase_orders po ON po.id=pi.po_id
                    WHERE pi.ingredient_id=? AND po.status='received' AND pi.unit_cost_kg > 0
                    ORDER BY po.updated_at DESC, po.id DESC
                    LIMIT 1
                """, (ing_id,))
                send_json(self, {'unit_cost_kg': row['unit_cost_kg'] if row else 0})
                return

            send_error(self, "Not found", 404)

        except Exception as e:
            print(f"  ERROR GET {self.path}: {e}")
            send_error(self, str(e), 500)

    def do_POST(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')
        qs     = parse_qs(parsed.query)
        try:
            data = read_body(self)

            # POST /api/dev/reset-all  (DEV_TOOLS only, admin only)
            if path == '/api/dev/reset-all':
                if os.environ.get('DEV_TOOLS', '').lower() not in ('1', 'true', 'yes'):
                    send_error(self, 'Not available in this environment', 403); return
                sess = get_session(self)
                if not sess or sess['role'] != 'admin':
                    send_json(self, {'error': 'Admin only'}, 403); return
                result = dev_reset_all()
                send_json(self, result)
                return

            # POST /api/dev/seed-fg-stock  (DEV_TOOLS only — bypasses BOM for tests)
            if path == '/api/dev/seed-fg-stock':
                if os.environ.get('DEV_TOOLS', '').lower() not in ('1', 'true', 'yes'):
                    send_error(self, 'Not available in this environment', 403); return
                sess = get_session(self)
                if not sess or sess['role'] != 'admin':
                    send_json(self, {'error': 'Admin only'}, 403); return
                _pc   = data.get('productCode', '')
                _ps   = data.get('packSize', '')
                _qty  = int(data.get('qtyUnits', 0))
                _date = data.get('batchDate', today())
                _var  = ref['var_by_sku'].get((_pc, _ps))
                if not _var:
                    send_json(self, {'error': f'Variant not found: {_pc}/{_ps}'}, 400); return
                if _qty <= 0:
                    send_json(self, {'error': 'qtyUnits must be positive'}, 400); return
                _bid = next_id('batch', 'BATCH')
                _c = _conn()
                try:
                    _c.execute("""
                        INSERT INTO production_batches
                            (batch_id, batch_date, product_id, product_variant_id,
                             qty_grams, qty_units, pack_size, notes, unit_cost_at_posting)
                        VALUES (?,?,?,?,?,?,?,?,?)
                    """, (_bid, _date, _var['product_id'], _var['id'],
                          _qty * _var.get('pack_grams', 50), _qty, _var['pack_size'],
                          'DEV seed', 0.0))
                    _c.commit()
                except Exception as _e:
                    _c.rollback(); _c.close()
                    send_json(self, {'error': str(_e)}, 500); return
                finally:
                    try: _c.close()
                    except: pass
                save_db()
                send_json(self, {'ok': True, 'batchId': _bid, 'qtyUnits': _qty,
                                 'productCode': _pc, 'packSize': _ps})
                return

            # POST /api/auth/login  (no auth required)
            if path == '/api/auth/login':
                ip = _get_client_ip(self)
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
                        """, (token, user['id'], user['username'], display, user['role'], user.get('permissions','[]'), now, expires_at, now))
                        _clear_rate_limit(ip)
                        print(f"  ⚠ Admin bypass token used from {ip} — remove ADMIN_BYPASS_TOKEN now!")
                        send_json(self, {'token': token, 'role': user['role'],
                                         'username': user['username'],
                                         'displayName': display,
                                         'userId': user['id'], 'permissions': []})
                        return
                try:
                    _check_rate_limit(ip)
                    result = login_user(data.get('username', ''), data.get('password', ''))
                    _clear_rate_limit(ip)
                    send_json(self, result)
                except ValueError as e:
                    _record_failed_attempt(ip)
                    send_json(self, {'error': str(e)}, 401)
                return

            # POST /api/field/auth  (no BMS session required — PIN login for reps)
            if path == '/api/field/auth':
                ip    = _get_client_ip(self)
                phone = data.get('phone','').strip()
                pin   = str(data.get('pin',''))
                try:
                    _check_rate_limit(ip)
                    result = field_login(phone, pin)
                    _clear_rate_limit(ip)
                    send_field_login_response(self, result)
                except ValueError as e:
                    _record_failed_attempt(ip)
                    send_json(self, {'error': str(e)}, 401)
                return

            # POST /api/field/send-otp  (no session — sends WhatsApp OTP to rep)
            if path == '/api/field/send-otp':
                ip    = _get_client_ip(self)
                phone = data.get('phone', '').strip()
                try:
                    _check_rate_limit(ip)
                    result = send_field_otp(phone)
                    send_json(self, result)
                except ValueError as e:
                    _record_failed_attempt(ip)
                    send_json(self, {'error': str(e)}, 400)
                return

            # POST /api/field/verify-otp  (no session — verifies OTP, returns field session)
            if path == '/api/field/verify-otp':
                ip    = _get_client_ip(self)
                phone = data.get('phone', '').strip()
                code  = str(data.get('code', '')).strip()
                try:
                    _check_rate_limit(ip)
                    rep = verify_field_otp(phone, code)
                    _clear_rate_limit(ip)
                    result = _create_field_session(rep)
                    send_field_login_response(self, result)
                except ValueError as e:
                    _record_failed_attempt(ip)
                    send_json(self, {'error': str(e)}, 401)
                return

            # ── B2B PORTAL — POST /api/field/customers  (field rep session) ──
            if path == '/api/field/customers':
                fsess = _get_field_session(self, qs)
                if not fsess:
                    send_json(self, {'error': 'Field rep session required'}, 401); return
                try:
                    customer = field_create_customer(data, fsess['repId'])
                    send_json(self, customer, 201)
                except ValidationError as e:
                    send_json(self, {'error': 'Validation failed', 'fields': e.errors}, 422)
                except ValueError as e:
                    send_error(self, str(e), 400)
                return

            # Auth gate for all other POST endpoints
            sess = get_session(self)
            if not sess:
                send_json(self, {'error': 'Unauthorized'}, 401); return

            # POST /api/auth/logout
            if path == '/api/auth/logout':
                auth = self.headers.get('Authorization', '')
                if auth.startswith('Bearer '):
                    logout_user(auth[7:])
                send_json(self, {'ok': True})
                return

            # POST /api/admin/reconcile-statuses (admin only — fix any status drift)
            if path == '/api/admin/reconcile-statuses':
                if sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                invoice_ids = [r['id'] for r in qry("SELECT id FROM invoices")]
                fixed = []
                for iid in invoice_ids:
                    inv = qry1("SELECT id, status FROM invoices WHERE id=?", (iid,))
                    correct = _sync_invoice_status(iid)
                    if inv and inv['status'] != correct:
                        fixed.append({'invoice_id': iid, 'was': inv['status'], 'now': correct})
                _log('info', 'reconcile_statuses', fixed_count=len(fixed))
                send_json(self, {'fixed': len(fixed), 'details': fixed})
                return

            # POST /api/admin/ingredients/truncate  (admin only — wipe all ingredients + reset counter)
            if path == '/api/admin/ingredients/truncate':
                if sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                try:
                    c = _conn()
                    count = c.execute("SELECT COUNT(*) FROM ingredients").fetchone()[0]
                    c.execute("PRAGMA foreign_keys = OFF")
                    c.execute("DELETE FROM ingredients")
                    c.execute("DELETE FROM id_counters WHERE entity='ingredient'")
                    c.commit()
                    c.execute("PRAGMA foreign_keys = ON")
                    c.close()
                    load_ref()
                    _log('info', 'ingredients_truncated', deleted=count, by=sess['username'])
                    send_json(self, {'ok': True, 'deleted': count,
                                     'message': f'Deleted {count} ingredients. Counter reset. Safe to reimport.'})
                except Exception as e:
                    send_error(self, str(e), 500)
                return

            # POST /api/admin/suppliers/truncate  (admin only — wipe all suppliers + reset counter)
            if path == '/api/admin/suppliers/truncate':
                if sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                try:
                    c = _conn()
                    count = c.execute("SELECT COUNT(*) FROM suppliers").fetchone()[0]
                    c.execute("PRAGMA foreign_keys = OFF")
                    c.execute("DELETE FROM suppliers")
                    c.execute("DELETE FROM id_counters WHERE entity='supplier'")
                    c.commit()
                    c.execute("PRAGMA foreign_keys = ON")
                    c.close()
                    load_ref()
                    _log('info', 'suppliers_truncated', deleted=count, by=sess['username'])
                    send_json(self, {'ok': True, 'deleted': count,
                                     'message': f'Deleted {count} suppliers. Counter reset. Safe to reimport.'})
                except Exception as e:
                    send_error(self, str(e), 500)
                return

            # POST /api/admin/customers/truncate  (admin only — wipe all customers + reset counter)
            # Use before reimporting clean master data. Safe only when no real orders exist.
            if path == '/api/admin/customers/truncate':
                if sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                try:
                    c = _conn()
                    count = c.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
                    c.execute("PRAGMA foreign_keys = OFF")
                    c.execute("DELETE FROM customers")
                    c.execute("DELETE FROM id_counters WHERE entity='customer'")
                    c.commit()
                    c.execute("PRAGMA foreign_keys = ON")
                    c.close()
                    load_ref()
                    _log('info', 'customers_truncated', deleted=count, by=sess['username'])
                    send_json(self, {'ok': True, 'deleted': count,
                                     'message': f'Deleted {count} customers. Counter reset. Safe to reimport.'})
                except Exception as e:
                    send_error(self, str(e), 500)
                return

            # POST /api/admin/backup  (admin only — manual backup trigger)
            if path == '/api/admin/backup':
                if sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                try:
                    result = run_backup()
                    send_json(self, {'ok': True, **result})
                except Exception as e:
                    send_error(self, str(e), 500)
                return

            # POST /api/admin/db-upload  (admin only — replace database from uploaded file)
            if path == '/api/admin/db-upload':
                if sess['role'] != 'admin':
                    send_json(self, {'ok': False, 'error': 'Permission denied'}, 403); return
                try:
                    import cgi as _cgi
                    ct = self.headers.get('Content-Type', '')
                    length = int(self.headers.get('Content-Length', 0))
                    environ = {'REQUEST_METHOD': 'POST', 'CONTENT_TYPE': ct, 'CONTENT_LENGTH': str(length)}
                    form = _cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ=environ)
                    db_field = form['db']
                    db_bytes = db_field.file.read()
                    if len(db_bytes) < 1024:
                        send_json(self, {'ok': False, 'error': 'File too small — not a valid database'}, 400); return
                    # Write to the source DB path
                    DB_SRC.parent.mkdir(parents=True, exist_ok=True)
                    tmp_upload = DB_SRC.parent / 'spicetopia_upload.db'
                    with open(tmp_upload, 'wb') as f:
                        f.write(db_bytes)
                    # Verify it's a valid SQLite file
                    try:
                        test_conn = sqlite3.connect(str(tmp_upload))
                        tables = test_conn.execute("SELECT count(*) FROM sqlite_master WHERE type='table'").fetchone()[0]
                        test_conn.close()
                        if tables < 5:
                            send_json(self, {'ok': False, 'error': f'Database has only {tables} tables — may be wrong file'}, 400); return
                    except Exception as ve:
                        send_json(self, {'ok': False, 'error': f'Invalid database: {ve}'}, 400); return
                    # Replace live database
                    shutil.copy2(str(tmp_upload), str(DB_SRC))
                    shutil.copy2(str(tmp_upload), str(DB_TMP))
                    tmp_upload.unlink(missing_ok=True)
                    print(f"  ✓ Database replaced via upload ({len(db_bytes)//1024} KB) by {sess['username']}")
                    send_json(self, {'ok': True, 'tables': tables, 'size_kb': len(db_bytes)//1024})
                except Exception as e:
                    send_json(self, {'ok': False, 'error': str(e)}, 500)
                return

            # POST /api/admin/test-whatsapp  (admin only — send a test message)
            if path == '/api/admin/test-whatsapp':
                if sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                if not WA_ENABLED:
                    send_json(self, {'ok': False, 'error': 'WhatsApp notifications are disabled. Enable them in Admin → Settings → WhatsApp.'}); return
                if not WA_ADMIN_PHONE or not WA_ADMIN_APIKEY:
                    send_json(self, {'ok': False, 'error': 'Admin phone and API key must be saved in Admin → Settings → WhatsApp.'}); return
                _wa_admin(
                    f"✅ *Spicetopia BMS*\n"
                    f"Test message — WhatsApp notifications are working!\n"
                    f"Server: {socket.gethostname()}"
                )
                send_json(self, {'ok': True, 'message': f'Test message sent to {WA_ADMIN_PHONE[:5]}****'})
                return

            # POST /api/users  (admin only)
            if path == '/api/users':
                result = create_user(data, sess['role'])
                send_json(self, result, 201)
                return

            # POST /api/ingredients  (admin only — no name stored)
            if path == '/api/ingredients':
                if sess['role'] != 'admin':
                    send_error(self, 'Admin only', 403); return
                result = create_ingredient(data)
                send_json(self, result, 201)
                return

            # POST /api/ingredients/:code/reactivate  (admin only)
            if path.startswith('/api/ingredients/') and path.endswith('/reactivate'):
                if sess['role'] != 'admin':
                    send_error(self, 'Admin only', 403); return
                code = path.split('/')[3]
                result = reactivate_ingredient(code)
                send_json(self, result)
                return

            # POST /api/ingredients/costs/bulk  (admin only — bulk cost update)
            if path == '/api/ingredients/costs/bulk':
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                rows = data.get('rows', [])
                if not rows:
                    send_error(self, 'rows array required', 400); return
                result = bulk_update_ingredient_costs(rows, sess.get('username', 'admin'))
                send_json(self, result)
                return

            # POST /api/costing/margin-alerts/:id/dismiss  (admin only)
            if path.startswith('/api/costing/margin-alerts/') and path.endswith('/dismiss'):
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                try:
                    alert_id = int(path.split('/')[-2])
                    result = dismiss_margin_alert(alert_id, sess.get('username', 'admin'))
                    send_json(self, result)
                except (ValueError, IndexError) as e:
                    send_error(self, str(e), 400)
                return

            # POST /api/customers/:id/reactivate  (admin only)
            if path.startswith('/api/customers/') and path.endswith('/reactivate'):
                if sess['role'] != 'admin':
                    send_error(self, 'Admin only', 403); return
                cust_id = int(path.split('/')[3])
                result  = update_customer(cust_id, {'active': 1})
                send_json(self, result)
                return

            # POST /api/customers  (admin, sales)
            if path == '/api/customers':
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                result = create_customer(data)
                send_json(self, result, 201)
                return

            # POST /api/suppliers/:id/reactivate  (admin only)
            if path.startswith('/api/suppliers/') and path.endswith('/reactivate'):
                if sess['role'] != 'admin':
                    send_error(self, 'Admin only', 403); return
                sup_id = int(path.split('/')[3])
                result = update_supplier(sup_id, {'active_flag': 1})
                send_json(self, result)
                return

            # POST /api/suppliers  (admin only)
            if path == '/api/suppliers':
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                result = create_supplier(data)
                send_json(self, result, 201)
                return

            # POST /api/sales  (admin, sales)
            if path == '/api/sales':
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                result = create_sale(data)
                send_json(self, result, 201)
                return

            # ── Customer Orders ──────────────────────────────────
            # POST /api/customer-orders  (admin, sales)
            if path == '/api/customer-orders':
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                result = create_customer_order(data)
                send_json(self, result, 201)
                return

            # POST /api/customer-orders/:id/confirm  (admin, sales)
            if path.startswith('/api/customer-orders/') and path.endswith('/confirm'):
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                order_id = int(path.split('/')[3])
                send_json(self, confirm_customer_order(order_id), 200)
                return

            # POST /api/customer-orders/:id/items  — add line item (admin, sales)
            if path.startswith('/api/customer-orders/') and path.endswith('/items') and len(path.split('/')) == 5:
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                order_id = int(path.split('/')[3])
                send_json(self, add_customer_order_item(order_id, data), 201)
                return

            # POST /api/customer-orders/:id/cancel  (admin, sales)
            if path.startswith('/api/customer-orders/') and path.endswith('/cancel'):
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                order_id = int(path.split('/')[3])
                send_json(self, cancel_customer_order(order_id), 200)
                return

            # ── Phase 3: Review Queue endpoints ──────────────────────────

            # POST /api/orders/external  — external order intake (consumer/retailer/field rep)
            if path == '/api/orders/external':
                # If caller is a field rep session, tag the order with their rep id
                sess = get_session(self, qs)
                if sess and sess.get('role') == 'field_rep':
                    data['created_by_rep_id'] = sess.get('repId')
                result = create_customer_order_external(data)
                status = 200 if result.pop('_idempotent', False) else 201
                send_json(self, result, status)
                return

            # POST /api/review-queue/:id/approve  (admin, sales)
            if path.startswith('/api/review-queue/') and path.endswith('/approve'):
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                order_id = int(path.split('/')[3])
                send_json(self, approve_order_with_edit(order_id, data), 200)
                return

            # POST /api/review-queue/:id/reject  (admin, sales)
            if path.startswith('/api/review-queue/') and path.endswith('/reject'):
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                order_id = int(path.split('/')[3])
                reason = data.get('reason', '')
                send_json(self, reject_order(order_id, reason), 200)
                return

            # POST /api/review-queue/:id/reopen  (admin, sales)
            if path.startswith('/api/review-queue/') and path.endswith('/reopen'):
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                order_id = int(path.split('/')[3])
                send_json(self, reopen_rejected_order(order_id), 200)
                return

            # POST /api/admin/orders/check-holds  — manual hold expiry trigger
            if path == '/api/admin/orders/check-holds':
                sess = get_session(self)
                if not sess or sess.get('role') != 'admin':
                    send_json(self, {'error': 'Admin only'}, 403)
                    return
                count = check_and_expire_holds()
                send_json(self, {'expired': count}, 200)
                return

            # ── End Phase 3 Review Queue endpoints ───────────────────────

            # POST /api/customer-orders/:id/items/:item_id/work-order  (admin, warehouse)
            if path.startswith('/api/customer-orders/') and path.endswith('/work-order'):
                if not require(sess, 'admin', 'warehouse'):
                    send_error(self, 'Permission denied', 403); return
                parts = path.split('/')
                # /api/customer-orders/3/items/7/work-order  → parts = ['','api','customer-orders','3','items','7','work-order']
                order_id = int(parts[3])
                item_id  = int(parts[5])
                send_json(self, create_wo_from_order_item(order_id, item_id, data), 201)
                return

            # POST /api/customer-orders/:id/invoice  (admin, sales)
            if path.startswith('/api/customer-orders/') and path.endswith('/invoice'):
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                order_id = int(path.split('/')[3])
                send_json(self, generate_invoice_from_order(order_id, data), 201)
                return

            # POST /api/sales/multi  (admin, sales)
            if path == '/api/sales/multi':
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                result = create_multi_sale(data)
                send_json(self, result, 201)
                return

            # POST /api/invoices/:id/pay  (admin, sales, accountant)
            if path.startswith('/api/invoices/') and path.endswith('/pay'):
                if not require(sess, 'admin', 'sales', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                inv_id = int(path.split('/')[3])
                result = pay_invoice_direct(inv_id, data)
                send_json(self, result, 201)
                return

            # POST /api/invoices/:id/adjust  (admin, accountant) — Sprint P1
            if path.startswith('/api/invoices/') and path.endswith('/adjust'):
                if not require(sess, 'admin', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                inv_id = int(path.split('/')[3])
                result = adjust_invoice(inv_id, data)
                send_json(self, result, 201)
                return

            # POST /api/invoices/:id/void  (admin, accountant only)
            if path.startswith('/api/invoices/') and path.endswith('/void'):
                if not require(sess, 'admin', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                inv_id = int(path.split('/')[3])
                result = void_invoice(inv_id, data.get('note',''), sess['username'])
                send_json(self, result)
                return

            # POST /api/supplier-bills/:id/void  (admin, accountant only)
            if path.startswith('/api/supplier-bills/') and path.endswith('/void'):
                if not require(sess, 'admin', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                bill_id = int(path.split('/')[3])
                result = void_supplier_bill(bill_id, data.get('note',''), sess['username'])
                send_json(self, result)
                return

            # POST /api/invoices/:id/items  (admin, sales)
            if path.startswith('/api/invoices/') and path.endswith('/items'):
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                inv_id = int(path.split('/')[3])
                result = add_invoice_item(inv_id, data)
                send_json(self, result)
                return

            # POST /api/customer-payments  (admin, sales, accountant)
            if path == '/api/customer-payments':
                if not require(sess, 'admin', 'sales', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                result = record_customer_payment(data)
                send_json(self, result, 201)
                return

            # POST /api/customer-payments/:id/allocate  (admin, sales, accountant)
            if path.startswith('/api/customer-payments/') and path.endswith('/allocate'):
                if not require(sess, 'admin', 'sales', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                pay_id = int(path.split('/')[3])
                result = allocate_customer_payment(
                    pay_id, int(data['invoiceId']), r2(data['amount'])
                )
                send_json(self, result)
                return

            # POST /api/bills  (admin, accountant)
            if path == '/api/bills':
                if not require(sess, 'admin', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                result = create_supplier_bill(data)
                send_json(self, result, 201)
                return

            # POST /api/bills/:id/pay  (admin, accountant)
            if path.startswith('/api/bills/') and path.endswith('/pay'):
                if not require(sess, 'admin', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                bill_id = int(path.split('/')[3])
                result  = pay_bill_direct(bill_id, data)
                send_json(self, result, 201)
                return

            # POST /api/bills/:id/adjust  (admin, accountant) — Sprint P1
            if path.startswith('/api/bills/') and path.endswith('/adjust'):
                if not require(sess, 'admin', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                bill_id = int(path.split('/')[3])
                result  = adjust_bill(bill_id, data)
                send_json(self, result, 201)
                return

            # POST /api/supplier-payments  (admin, accountant)
            if path == '/api/supplier-payments':
                if not require(sess, 'admin', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                result = record_supplier_payment(data)
                send_json(self, result, 201)
                return

            # POST /api/supplier-payments/:id/allocate  (admin, accountant)
            if path.startswith('/api/supplier-payments/') and path.endswith('/allocate'):
                if not require(sess, 'admin', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                pay_id = int(path.split('/')[3])
                result = allocate_supplier_payment(
                    pay_id, int(data['billId']), r2(data['amount'])
                )
                send_json(self, result)
                return

            # POST /api/work-orders  (admin, warehouse)
            if path == '/api/work-orders':
                if not require(sess, 'admin', 'warehouse'):
                    send_error(self, 'Permission denied', 403); return
                result = create_work_order(data)
                send_json(self, result, 201)
                return

            # POST /api/work-orders/:id/convert  (admin, warehouse)
            if path.startswith('/api/work-orders/') and path.endswith('/convert'):
                if not require(sess, 'admin', 'warehouse'):
                    send_error(self, 'Permission denied', 403); return
                wo_id = int(path.split('/')[-2])
                result = convert_wo_to_batch(wo_id)
                send_json(self, result, 201)
                return

            # POST /api/work-orders/:id/status  (admin, warehouse)
            if path.startswith('/api/work-orders/') and path.endswith('/status'):
                if not require(sess, 'admin', 'warehouse'):
                    send_error(self, 'Permission denied', 403); return
                wo_id = int(path.split('/')[-2])
                result = update_work_order_status(wo_id, data.get('status',''))
                send_json(self, result)
                return

            # POST /api/bom  (admin only — create / replace active BOM for a product)
            if path == '/api/bom':
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                result = create_or_update_bom(data)
                send_json(self, result, 201)
                return

            # POST /api/production  (admin, warehouse)
            if path == '/api/production':
                if not require(sess, 'admin', 'warehouse'):
                    send_error(self, 'Permission denied', 403); return
                result = create_production_batch(data)
                send_json(self, result, 201)
                return

            # POST /api/inventory/adjustment  (admin, warehouse)
            if path == '/api/inventory/adjustment':
                if not require(sess, 'admin', 'warehouse'):
                    send_error(self, 'Permission denied', 403); return
                result = create_adjustment(data)
                send_json(self, result)
                return

            # POST /api/prices  (admin only)
            if path == '/api/prices':
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                result = set_product_price(data)
                send_json(self, result)
                return

            # POST /api/admin/masters/upload/{type}  — upload CSV or XLSX master file
            if path.startswith('/api/admin/masters/upload/'):
                if sess['role'] != 'admin':
                    send_json(self, {'ok': False, 'error': 'Permission denied'}, 403); return
                master_type = path.split('/')[-1]
                if master_type not in ('customers', 'suppliers', 'products', 'prices', 'ingredients', 'bom'):
                    send_json(self, {'ok': False, 'error': f'Unknown master type: {master_type}'}, 400); return
                try:
                    import cgi as _cgi
                    ct = self.headers.get('Content-Type', '')
                    length = int(self.headers.get('Content-Length', 0))
                    environ = {'REQUEST_METHOD': 'POST', 'CONTENT_TYPE': ct, 'CONTENT_LENGTH': str(length)}
                    form = _cgi.FieldStorage(fp=self.rfile, headers=self.headers, environ=environ)
                    if 'file' not in form:
                        send_json(self, {'ok': False, 'error': 'No file uploaded'}, 400); return
                    f = form['file']
                    raw_bytes = f.file.read()
                    filename  = f.filename or ''
                    rows, err = _parse_upload_file(raw_bytes, filename)
                    if err:
                        send_json(self, {'ok': False, 'error': err}, 400); return
                    if not rows:
                        send_json(self, {'ok': False, 'error': 'File is empty or has no data rows'}, 400); return
                    if master_type == 'customers':
                        result = import_customers_master(rows)
                        backfill_customer_account_numbers()  # assign account numbers immediately
                    elif master_type == 'suppliers':
                        result = import_suppliers_master(rows)
                    elif master_type == 'products':
                        result = import_products_master(rows)
                    elif master_type == 'prices':
                        result = import_prices_master(rows)
                    elif master_type == 'ingredients':
                        result = import_ingredients_master(rows)
                    elif master_type == 'bom':
                        result = import_bom_master(rows)
                    send_json(self, {'ok': True, 'rows_processed': len(rows), **result})
                except Exception as e:
                    send_json(self, {'ok': False, 'error': str(e)}, 500)
                return

            # POST /api/admin/price-master/import  — bulk CSV import (admin only)
            if path == '/api/admin/price-master/import':
                sess = get_session(self)
                if not sess or sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                rows = data.get('rows', [])   # [{product_code, pack_size, price_type, price, effective_from}]
                if not rows:
                    send_error(self, 'No rows provided', 400); return
                # Validate and build ops
                price_types_map = {pt['code']: pt['id'] for pt in qry("SELECT id, code FROM price_types")}
                results = []
                errors  = []
                eff_default = today()
                for i, row in enumerate(rows):
                    pcode = str(row.get('product_code','')).strip()
                    psize = str(row.get('pack_size','')).strip()
                    ptype = str(row.get('price_type','')).strip().lower()
                    try:
                        price_val = float(str(row.get('price',0)).replace(',',''))
                    except ValueError:
                        errors.append(f"Row {i+1}: invalid price '{row.get('price')}'")
                        continue
                    eff = str(row.get('effective_from', eff_default)).strip() or eff_default
                    if not pcode:
                        errors.append(f"Row {i+1}: missing product_code"); continue
                    if not psize:
                        errors.append(f"Row {i+1}: missing pack_size"); continue
                    if ptype not in price_types_map:
                        errors.append(f"Row {i+1}: unknown price_type '{ptype}' (valid: {', '.join(price_types_map.keys())})"); continue
                    if price_val < 0:
                        errors.append(f"Row {i+1}: price cannot be negative"); continue
                    # Look up variant
                    var = qry1("""
                        SELECT pv.id FROM product_variants pv
                        JOIN products   p  ON p.id  = pv.product_id
                        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
                        WHERE p.code = ? AND ps.label = ? AND pv.active_flag = 1
                    """, (pcode, psize))
                    if not var:
                        errors.append(f"Row {i+1}: product '{pcode}' / pack '{psize}' not found"); continue
                    pt_id = price_types_map[ptype]
                    set_product_price({
                        'productVariantId': var['id'],
                        'priceTypeId':      pt_id,
                        'price':            price_val,
                        'effectiveFrom':    eff
                    })
                    results.append({'product_code': pcode, 'pack_size': psize,
                                    'price_type': ptype, 'price': price_val})
                send_json(self, {'updated': len(results), 'errors': errors, 'rows': results})
                return

            # POST /api/products/generate-blend-code  — atomically assign next code for prefix
            if path == '/api/products/generate-blend-code':
                if sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                prefix = data.get('prefix', '').strip().upper()
                if not prefix:
                    send_json(self, {'error': 'prefix required'}, 400); return
                code = next_blend_code(prefix)
                send_json(self, {'code': code})
                return

            # POST /api/products  (admin only)
            if path == '/api/products':
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                result = create_product(data)
                send_json(self, result, 201)
                return

            # ── FIELD AUTH (no BMS session required) ─────────────────────
            # ── ZONES ─────────────────────────────────────────────────────
            # POST /api/zones  (admin only)
            if path == '/api/zones':
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                result = create_zone(data)
                send_json(self, result, 201)
                return

            # ── ROUTES ─────────────────────────────────────────────────────
            # POST /api/routes  (admin only)
            if path == '/api/routes':
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                result = create_route(data)
                send_json(self, result, 201)
                return

            # ── SALES REPS ─────────────────────────────────────────────────
            # POST /api/reps  (admin only)
            if path == '/api/reps':
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                result = create_rep(data)
                send_json(self, result, 201)
                return

            # POST /api/reps/:id/routes  (admin only)
            if path.startswith('/api/reps/') and path.endswith('/routes'):
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                rep_id   = int(path.split('/')[3])
                route_id = int(data.get('routeId'))
                result   = assign_rep_route(rep_id, route_id)
                send_json(self, result)
                return

            # POST /api/reps/:id/routes/:assign_id/unassign  (admin only)
            if path.startswith('/api/reps/') and path.endswith('/unassign'):
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                parts     = path.split('/')
                assign_id = int(parts[5])
                result    = unassign_rep_route(assign_id)
                send_json(self, result)
                return

            # POST /api/reps/:id/target  (admin only)
            if path.startswith('/api/reps/') and path.endswith('/target'):
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                rep_id = int(path.split('/')[3])
                result = set_rep_target(rep_id, data)
                send_json(self, result)
                return

            # POST /api/reps/:id/advance  (admin, accountant)
            if path.startswith('/api/reps/') and path.endswith('/advance'):
                if not require(sess, 'admin', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                rep_id = int(path.split('/')[3])
                result = record_advance(rep_id, data)
                send_json(self, result, 201)
                return

            # ── ROUTE CUSTOMERS ────────────────────────────────────────────
            # POST /api/routes/:id/customers  (admin only)
            if path.startswith('/api/routes/') and path.endswith('/customers'):
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                route_id  = int(path.split('/')[3])
                cust_id   = int(data.get('customerId'))
                result    = assign_customer_route(
                    cust_id, route_id,
                    data.get('shopName',''), data.get('address','')
                )
                send_json(self, result)
                return

            # ── BEAT VISITS ────────────────────────────────────────────────
            # POST /api/beat-visits  (admin, sales, field_rep)
            if path == '/api/beat-visits':
                if not require(sess, 'admin', 'sales', 'field_rep'):
                    send_error(self, 'Permission denied', 403); return
                # Inject repId from session if not provided (field app flow)
                if not data.get('repId') and sess and sess.get('repId'):
                    data['repId'] = sess['repId']
                result = record_beat_visit(data)
                send_json(self, result, 201)
                return

            # ── FIELD ORDERS ───────────────────────────────────────────────
            # POST /api/field-orders  (admin, sales, field_rep)
            if path == '/api/field-orders':
                if not require(sess, 'admin', 'sales', 'field_rep'):
                    send_error(self, 'Permission denied', 403); return
                # Inject repId from session if not provided (field app flow)
                if not data.get('repId') and sess and sess.get('repId'):
                    data['repId'] = sess['repId']
                result = create_field_order(data)
                send_json(self, result, 201)
                return

            # POST /api/field-orders/:id/confirm  (admin, sales)
            if path.startswith('/api/field-orders/') and path.endswith('/confirm'):
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                order_id = int(path.split('/')[3])
                result   = confirm_field_order(order_id, data)
                send_json(self, result)
                return

            # ── PAYROLL ────────────────────────────────────────────────────
            # POST /api/payroll/run  (admin, accountant)
            if path == '/api/payroll/run':
                if not require(sess, 'admin', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                period   = data.get('period','')
                rep_ids  = data.get('repIds')
                result   = run_payroll(period, rep_ids)
                send_json(self, result)
                return

            # POST /api/payroll/finalize  (admin, accountant)
            if path == '/api/payroll/finalize':
                if not require(sess, 'admin', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                rep_id = int(data.get('repId'))
                period = data.get('period','')
                result = finalize_payroll(rep_id, period)
                send_json(self, result)
                return

            # ── PURCHASE ORDERS ──────────────────────────────────────────
            # POST /api/purchase-orders  (admin, accountant, warehouse)
            if path == '/api/purchase-orders':
                if not require(sess, 'admin', 'accountant', 'warehouse'):
                    send_error(self, 'Permission denied', 403); return
                result = create_purchase_order(data)
                send_json(self, result, 201)
                return

            # POST /api/purchase-orders/:id/status  (admin, accountant, warehouse)
            if path.startswith('/api/purchase-orders/') and path.endswith('/status'):
                if not require(sess, 'admin', 'accountant', 'warehouse'):
                    send_error(self, 'Permission denied', 403); return
                po_id  = int(path.split('/')[3])
                status = data.get('status', '')
                result = update_purchase_order_status(po_id, status, data)
                send_json(self, result)
                return

            send_error(self, "Not found", 404)

        except ValidationError as e:
            send_json(self, {'error': 'Validation failed', 'fields': e.errors}, 422)
        except ValueError as e:
            send_error(self, str(e), 400)
        except Exception as e:
            print(f"  ERROR POST {self.path}: {e}")
            import traceback; traceback.print_exc()
            _log('error', f'POST error: {e}', path=self.path, exc_info=True)
            send_error(self, str(e), 500)

    def do_PATCH(self):
        """PATCH is treated identically to PUT — partial updates are handled at the function level."""
        return self.do_PUT()

    def do_PUT(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')
        try:
            data = read_body(self)

            # Auth gate
            sess = get_session(self)
            if not sess:
                send_json(self, {'error': 'Unauthorized'}, 401); return

            # PUT /api/admin/settings  (admin only — save WA config to DB + hot-reload)
            if path == '/api/admin/settings':
                if sess['role'] != 'admin':
                    send_error(self, 'Permission denied', 403); return
                if 'whatsapp_enabled' in data:
                    set_setting('whatsapp_enabled', '1' if data['whatsapp_enabled'] else '0')
                if 'whatsapp_admin_phone' in data:
                    set_setting('whatsapp_admin_phone', data['whatsapp_admin_phone'].strip())
                if 'whatsapp_admin_apikey' in data:
                    set_setting('whatsapp_admin_apikey', data['whatsapp_admin_apikey'].strip())
                if 'whatsapp_expiry_warn_hours' in data:
                    set_setting('whatsapp_expiry_warn_hours', str(int(data['whatsapp_expiry_warn_hours'])))
                    global WA_EXPIRY_WARN_HOURS
                    WA_EXPIRY_WARN_HOURS = int(data['whatsapp_expiry_warn_hours'])
                _reload_wa_from_db()
                send_json(self, {
                    'ok': True,
                    'whatsapp_enabled':     WA_ENABLED,
                    'whatsapp_admin_phone': WA_ADMIN_PHONE,
                })
                return

            # PUT /api/products/variants/:id/wastage  (admin only)
            parts = path.split('/')
            if (path.startswith('/api/products/variants/') and
                    len(parts) == 6 and parts[5] == 'wastage'):
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                variant_id  = int(parts[4])
                wastage_pct = data.get('wastage_pct')
                if wastage_pct is None:
                    send_error(self, 'wastage_pct required', 400); return
                wpct = float(wastage_pct)
                if not (0 <= wpct < 1):
                    send_error(self, 'wastage_pct must be between 0 and 0.99', 400); return
                c = _conn()
                try:
                    c.execute(
                        "UPDATE product_variants SET wastage_pct=? WHERE id=?",
                        (wpct, variant_id)
                    )
                    c.commit()
                finally:
                    c.close()
                send_json(self, {'ok': True, 'variant_id': variant_id, 'wastage_pct': wpct})
                return

            # PUT /api/products/variants/:id/gtin  (admin only)
            if (path.startswith('/api/products/variants/') and
                    len(parts) == 6 and parts[5] == 'gtin'):
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                variant_id = int(parts[4])
                gtin_val   = data.get('gtin')  # None = clear, string = set
                if gtin_val is not None:
                    gtin_val = str(gtin_val).strip()
                    if gtin_val == '':
                        gtin_val = None  # treat empty string as clear
                    elif not gtin_val.isdigit() or not (8 <= len(gtin_val) <= 14):
                        send_error(self, 'GTIN must be 8–14 digits', 400); return
                c = _conn()
                try:
                    c.execute(
                        "UPDATE product_variants SET gtin=? WHERE id=?",
                        (gtin_val, variant_id)
                    )
                    c.commit()
                finally:
                    c.close()
                send_json(self, {'ok': True, 'variant_id': variant_id, 'gtin': gtin_val})
                return

            # PUT /api/costing/config  (admin only)
            if path == '/api/costing/config':
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                key   = data.get('key')
                value = data.get('value')
                if not key or value is None:
                    send_error(self, 'key and value required', 400); return
                try:
                    result = update_costing_config(key, str(value), sess.get('username', 'admin'))
                    send_json(self, result)
                except ValueError as e:
                    send_error(self, str(e), 400)
                return

            # PUT /api/products/variants/:id/sku  (admin only)
            if (path.startswith('/api/products/variants/') and
                    len(parts) == 6 and parts[5] == 'sku'):
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                variant_id = int(parts[4])
                new_sku = (data.get('sku_code') or '').strip().upper()
                if not new_sku:
                    send_error(self, 'sku_code required', 400); return
                if not re.match(r'^[A-Z0-9][A-Z0-9\-]{1,19}$', new_sku):
                    send_error(self, 'SKU code must be 2–20 alphanumeric/dash characters', 400); return
                c = _conn()
                try:
                    existing = c.execute(
                        "SELECT id FROM product_variants WHERE sku_code=? AND id!=?",
                        (new_sku, variant_id)).fetchone()
                    if existing:
                        send_error(self, f"SKU code '{new_sku}' is already in use", 409); return
                    c.execute("UPDATE product_variants SET sku_code=? WHERE id=?", (new_sku, variant_id))
                    c.commit()
                finally:
                    c.close()
                load_ref()
                send_json(self, {'ok': True, 'variant_id': variant_id, 'sku_code': new_sku})
                return

            # PUT /api/users/:id
            if path.startswith('/api/users/') and len(path.split('/')) == 4:
                uid    = int(path.split('/')[3])
                result = update_user(uid, data, sess['role'], sess['userId'])
                send_json(self, result)
                return

            # PUT /api/customers/:id  (admin, sales)
            if path.startswith('/api/customers/') and len(path.split('/')) == 4:
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                cust_id = int(path.split('/')[3])
                result  = update_customer(cust_id, data)
                send_json(self, result)
                return

            # PUT /api/suppliers/:id  (admin only)
            if path.startswith('/api/suppliers/') and len(path.split('/')) == 4:
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                sup_id = int(path.split('/')[3])
                result = update_supplier(sup_id, data)
                send_json(self, result)
                return

            # PUT /api/products/:code  (edit name / urdu name / blend_code — admin only)
            if path.startswith('/api/products/') and len(path.split('/')) == 4:
                if sess['role'] != 'admin':
                    send_error(self, 'Admin only', 403); return
                code   = path.split('/')[3]
                result = update_product(code, data)
                send_json(self, result)
                return

            # PUT /api/ingredients/:code  (edit cost / unit / reorder — admin only)
            if path.startswith('/api/ingredients/') and len(path.split('/')) == 4:
                if sess['role'] != 'admin':
                    send_error(self, 'Admin only', 403); return
                code   = path.split('/')[3]
                result = update_ingredient(code, data)
                send_json(self, result)
                return

            # PUT /api/prices/:id  (admin only)
            if path.startswith('/api/prices/') and len(path.split('/')) == 4:
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                result = set_product_price(data)
                send_json(self, result)
                return

            # PUT /api/customer-orders/:id/items/:item_id  (admin, sales)
            if path.startswith('/api/customer-orders/') and '/items/' in path and len(path.split('/')) == 6:
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                parts    = path.split('/')
                order_id = int(parts[3])
                item_id  = int(parts[5])
                new_qty  = data.get('qty')
                if new_qty is None:
                    send_error(self, 'qty is required', 400); return
                result = update_order_item_qty(order_id, item_id, new_qty)
                send_json(self, result)
                return

            # PUT /api/customer-orders/:id  (admin, sales)
            if path.startswith('/api/customer-orders/') and len(path.split('/')) == 4:
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                order_id = int(path.split('/')[3])
                result   = update_customer_order(order_id, data)
                send_json(self, result)
                return

            # PUT /api/work-orders/:id  (admin, warehouse)
            if path.startswith('/api/work-orders/') and len(path.split('/')) == 4:
                if not require(sess, 'admin', 'warehouse'):
                    send_error(self, 'Permission denied', 403); return
                wo_id  = int(path.split('/')[3])
                result = update_work_order(wo_id, data)
                send_json(self, result)
                return

            # PUT /api/purchase-orders/:id  (admin, accountant, warehouse — edit header fields)
            if path.startswith('/api/purchase-orders/') and len(path.split('/')) == 4:
                if not require(sess, 'admin', 'accountant', 'warehouse'):
                    send_error(self, 'Permission denied', 403); return
                po_id  = int(path.split('/')[3])
                result = update_purchase_order(po_id, data)
                send_json(self, result)
                return

            # PUT /api/bills/:id  (admin, accountant — edit header fields on UNPAID/PARTIAL bills)
            if path.startswith('/api/bills/') and len(path.split('/')) == 4:
                if not require(sess, 'admin', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                bill_id = int(path.split('/')[3])
                result  = update_supplier_bill(bill_id, data)
                send_json(self, result)
                return

            # PUT /api/zones/:id  (admin only)
            if path.startswith('/api/zones/') and len(path.split('/')) == 4:
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                zone_id = int(path.split('/')[3])
                result = update_zone(zone_id, data)
                send_json(self, result)
                return

            # PUT /api/routes/:id  (admin only)
            if path.startswith('/api/routes/') and len(path.split('/')) == 4:
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                route_id = int(path.split('/')[3])
                result = update_route(route_id, data)
                send_json(self, result)
                return

            # PUT /api/reps/:id  (admin only)
            if path.startswith('/api/reps/') and len(path.split('/')) == 4:
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                rep_id = int(path.split('/')[3])
                result = update_rep(rep_id, data)
                send_json(self, result)
                return

            send_error(self, "Not found", 404)

        except ValidationError as e:
            send_json(self, {'error': 'Validation failed', 'fields': e.errors}, 422)
        except ValueError as e:
            send_error(self, str(e), 400)
        except Exception as e:
            print(f"  ERROR PUT {self.path}: {e}")
            import traceback; traceback.print_exc()
            _log('error', f'PUT error: {e}', path=self.path, exc_info=True)
            send_error(self, str(e), 500)

    def do_DELETE(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')
        try:
            sess = get_session(self)
            if not sess:
                send_json(self, {'error': 'Unauthorized'}, 401); return

            # DELETE /api/customers/:id  → soft deactivate (admin only)
            if path.startswith('/api/customers/') and len(path.split('/')) == 4:
                if sess['role'] != 'admin':
                    send_error(self, 'Admin only', 403); return
                cust_id = int(path.split('/')[3])
                result  = update_customer(cust_id, {'active': 0})
                send_json(self, result)
                return

            # DELETE /api/suppliers/:id  → soft deactivate (admin only)
            if path.startswith('/api/suppliers/') and len(path.split('/')) == 4:
                if sess['role'] != 'admin':
                    send_error(self, 'Admin only', 403); return
                sup_id = int(path.split('/')[3])
                result = update_supplier(sup_id, {'active_flag': 0})
                send_json(self, result)
                return

            # DELETE /api/users/:id  → deactivate (admin only)
            if path.startswith('/api/users/') and len(path.split('/')) == 4:
                if sess['role'] != 'admin':
                    send_error(self, 'Admin only', 403); return
                uid    = int(path.split('/')[3])
                if uid == sess['userId']:
                    send_error(self, 'Cannot deactivate your own account', 400); return
                result = update_user(uid, {'active': 0}, sess['role'], sess['userId'])
                send_json(self, result)
                return

            # DELETE /api/ingredients/:code  → soft deactivate (admin only)
            if path.startswith('/api/ingredients/') and len(path.split('/')) == 4:
                if sess['role'] != 'admin':
                    send_error(self, 'Admin only', 403); return
                code   = path.split('/')[3]
                result = deactivate_ingredient(code)
                send_json(self, result)
                return

            # DELETE /api/invoice-items/:id  (admin, sales)
            if path.startswith('/api/invoice-items/') and len(path.split('/')) == 4:
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                item_id = int(path.split('/')[3])
                result  = remove_invoice_item(item_id)
                send_json(self, result)
                return

            # DELETE /api/products/:code  (admin only)
            if path.startswith('/api/products/') and len(path.split('/')) == 4:
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                code   = path.split('/')[3]
                result = deactivate_product(code)
                send_json(self, result)
                return

            # DELETE /api/variants/:id  (admin only)
            if path.startswith('/api/variants/') and len(path.split('/')) == 4:
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                vid    = int(path.split('/')[3])
                result = deactivate_variant(vid)
                send_json(self, result)
                return

            # DELETE /api/payment-allocations/:id  (admin, accountant) — Sprint P1
            if path.startswith('/api/payment-allocations/') and len(path.split('/')) == 4:
                if not require(sess, 'admin', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                alloc_id = int(path.split('/')[3])
                result   = deallocate_payment(alloc_id)
                send_json(self, result)
                return

            # DELETE /api/supplier-payment-allocations/:id  (admin, accountant) — Sprint P1
            if path.startswith('/api/supplier-payment-allocations/') and len(path.split('/')) == 4:
                if not require(sess, 'admin', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                alloc_id = int(path.split('/')[3])
                result   = deallocate_supplier_payment(alloc_id)
                send_json(self, result)
                return

            send_error(self, "Not found", 404)

        except ValidationError as e:
            send_json(self, {'error': 'Validation failed', 'fields': e.errors}, 422)
        except ValueError as e:
            send_error(self, str(e), 400)
        except Exception as e:
            print(f"  ERROR DELETE {self.path}: {e}")
            import traceback; traceback.print_exc()
            _log('error', f'DELETE error: {e}', path=self.path, exc_info=True)
            send_error(self, str(e), 500)

    def _serve_file(self, path, content_type):
        try:
            content = path.read_bytes()
            self.send_response(200)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', len(content))
            # Prevent browsers from caching HTML — always fetch fresh after deploy
            if 'html' in content_type:
                self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
                self.send_header('Pragma', 'no-cache')
                self.send_header('Expires', '0')
            _add_security_headers(self)
            self.end_headers()
            self.wfile.write(content)
        except FileNotFoundError:
            send_error(self, "File not found", 404)


# ═══════════════════════════════════════════════════════════════════
#  MASTER FILE SYNC
# ═══════════════════════════════════════════════════════════════════

MASTER_INGREDIENT_PRICING = MASTERS_DIR / 'ingredient_pricing.csv'
MASTER_SUPPLIERS          = MASTERS_DIR / 'suppliers.csv'
MASTER_CUSTOMERS          = MASTERS_DIR / 'customers.csv'


def ensure_master_schema():
    """Add cost_per_kg, active to ingredients; credit_limit to customers; create price_history table."""
    c = _conn()
    try:
        # ingredients.cost_per_kg (original)
        try:
            c.execute("ALTER TABLE ingredients ADD COLUMN cost_per_kg REAL NOT NULL DEFAULT 0")
            c.commit()
            print("  ✓ Masters: added cost_per_kg column to ingredients")
        except Exception:
            pass  # column already exists
        # ingredients.active (new)
        try:
            c.execute("ALTER TABLE ingredients ADD COLUMN active INTEGER NOT NULL DEFAULT 1")
            c.commit()
            print("  ✓ Masters: added active column to ingredients")
        except Exception:
            pass
        # customers.credit_limit (new)
        try:
            c.execute("ALTER TABLE customers ADD COLUMN credit_limit REAL DEFAULT 0")
            c.commit()
            print("  ✓ Masters: added credit_limit column to customers")
        except Exception:
            pass
        # customers.account_number (new)
        try:
            c.execute("ALTER TABLE customers ADD COLUMN account_number TEXT DEFAULT NULL")
            c.commit()
            print("  ✓ Masters: added account_number column to customers")
        except Exception:
            pass
        # customers.address (new)
        try:
            c.execute("ALTER TABLE customers ADD COLUMN address TEXT DEFAULT ''")
            c.commit()
            print("  ✓ Masters: added address column to customers")
        except Exception:
            pass

        # customers.zone_id — territory zone for sales rep out-of-zone detection
        try:
            c.execute("ALTER TABLE customers ADD COLUMN zone_id INTEGER DEFAULT NULL")
            c.commit()
            print("  ✓ Masters: added zone_id column to customers")
        except Exception:
            pass  # column already exists

        # ── P2.5 void/cancel migrations ─────────────────────────────────
        # invoices: add voided_at / voided_by / void_note columns
        for col, dflt in [('voided_at', 'NULL'), ('voided_by', "''"), ('void_note', "''")]:
            try:
                c.execute(f"ALTER TABLE invoices ADD COLUMN {col} TEXT DEFAULT {dflt}")
                c.commit()
                print(f"  ✓ Masters: added invoices.{col}")
            except Exception:
                pass

        # sales: add voided flag so voided invoices restore finished-goods stock
        try:
            c.execute("ALTER TABLE sales ADD COLUMN voided INTEGER DEFAULT 0")
            c.commit()
            print("  ✓ Masters: added sales.voided column")
        except Exception:
            pass

        # supplier_bills: add VOID to the status CHECK constraint via writable_schema,
        # then add voided_at / voided_by / void_note columns
        try:
            sb_schema = c.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='supplier_bills'"
            ).fetchone()
            if sb_schema and 'VOID' not in (sb_schema[0] or ''):
                new_sql = (sb_schema[0] or '').replace(
                    "CHECK(status IN ('UNPAID','PARTIAL','PAID'))",
                    "CHECK(status IN ('UNPAID','PARTIAL','PAID','VOID'))"
                )
                c.execute("PRAGMA writable_schema = ON")
                c.execute(
                    "UPDATE sqlite_master SET sql=? WHERE type='table' AND name='supplier_bills'",
                    (new_sql,)
                )
                c.execute("PRAGMA writable_schema = OFF")
                c.commit()
                print("  ✓ Masters: added VOID to supplier_bills.status CHECK constraint")
        except Exception as e:
            print(f"  ⚠ supplier_bills VOID constraint migration: {e}")
        for col, dflt in [('voided_at', 'NULL'), ('voided_by', "''"), ('void_note', "''")]:
            try:
                c.execute(f"ALTER TABLE supplier_bills ADD COLUMN {col} TEXT DEFAULT {dflt}")
                c.commit()
                print(f"  ✓ Masters: added supplier_bills.{col}")
            except Exception:
                pass

        c.execute("""
            CREATE TABLE IF NOT EXISTS ingredient_price_history (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                ingredient_id  INTEGER NOT NULL,
                old_cost_per_kg REAL,
                new_cost_per_kg REAL NOT NULL,
                pct_change     REAL,
                changed_at     TEXT DEFAULT (datetime('now')),
                source         TEXT DEFAULT 'master_sync',
                FOREIGN KEY (ingredient_id) REFERENCES ingredients(id)
            )
        """)
        c.commit()

        # ── P1 Sprint: add ADJUSTMENT to payment_mode CHECK constraints ──────
        for tbl in ('customer_payments', 'supplier_payments'):
            try:
                tbl_schema = c.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (tbl,)
                ).fetchone()
                if tbl_schema and 'ADJUSTMENT' not in (tbl_schema[0] or ''):
                    old_check = "CHECK(payment_mode IN ('CASH','BANK_TRANSFER','CHEQUE','OTHER'))"
                    new_check = "CHECK(payment_mode IN ('CASH','BANK_TRANSFER','CHEQUE','OTHER','ADJUSTMENT'))"
                    new_sql   = (tbl_schema[0] or '').replace(old_check, new_check)
                    if new_sql != tbl_schema[0]:
                        c.execute("PRAGMA writable_schema = ON")
                        c.execute(
                            "UPDATE sqlite_master SET sql=? WHERE type='table' AND name=?",
                            (new_sql, tbl)
                        )
                        c.execute("PRAGMA writable_schema = OFF")
                        c.commit()
                        print(f"  ✓ Migration: added ADJUSTMENT to {tbl}.payment_mode CHECK constraint")
            except Exception as e:
                print(f"  ⚠ {tbl} ADJUSTMENT constraint migration: {e}")

    finally:
        c.close()
    save_db()


def generate_master_templates():
    """Create master CSV files from current DB data if they don't exist."""
    MASTERS_DIR.mkdir(exist_ok=True)

    if not MASTER_INGREDIENT_PRICING.exists():
        ings = qry("SELECT code, name, cost_per_kg FROM ingredients ORDER BY code")
        with open(MASTER_INGREDIENT_PRICING, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=['code', 'name', 'cost_per_kg'])
            w.writeheader()
            w.writerows(ings)
        print(f"  ✓ Masters: created {MASTER_INGREDIENT_PRICING.name} ({len(ings)} ingredients)")

    if not MASTER_SUPPLIERS.exists():
        sups = qry("SELECT code, name, contact, phone, email, city FROM suppliers ORDER BY code")
        with open(MASTER_SUPPLIERS, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=['code', 'name', 'contact', 'phone', 'email', 'city'])
            w.writeheader()
            w.writerows(sups)
        print(f"  ✓ Masters: created {MASTER_SUPPLIERS.name} ({len(sups)} suppliers)")

    if not MASTER_CUSTOMERS.exists():
        custs = qry("SELECT code, name, customer_type, city, phone, email, payment_terms_days, default_pack FROM customers ORDER BY code")
        with open(MASTER_CUSTOMERS, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=['code','name','customer_type','city','phone','email','payment_terms_days','default_pack'])
            w.writeheader()
            w.writerows(custs)
        print(f"  ✓ Masters: created {MASTER_CUSTOMERS.name} ({len(custs)} customers)")


def sync_master_files():
    """
    Compare master CSV files with DB. Apply changes, log price history.
    Called on every startup.
    """
    MASTERS_DIR.mkdir(exist_ok=True)
    changed_total = 0

    # ── Ingredient Pricing ─────────────────────────────────────────
    if MASTER_INGREDIENT_PRICING.exists():
        c = _conn()
        try:
            changes = 0
            with open(MASTER_INGREDIENT_PRICING, newline='', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    code     = (row.get('code') or '').strip().upper()
                    new_cost = r2(row.get('cost_per_kg', 0))
                    if not code:
                        continue
                    ing = qry1("SELECT id, name, cost_per_kg FROM ingredients WHERE code=?", (code,))
                    if ing:
                        old_cost = r2(ing['cost_per_kg'] or 0)
                        if abs(new_cost - old_cost) > 0.001:
                            pct = round((new_cost - old_cost) / old_cost * 100, 2) if old_cost else None
                            c.execute("UPDATE ingredients SET cost_per_kg=? WHERE code=?", (new_cost, code))
                            c.execute("""
                                INSERT INTO ingredient_price_history
                                    (ingredient_id, old_cost_per_kg, new_cost_per_kg, pct_change, source)
                                VALUES (?,?,?,?,'master_sync')
                            """, (ing['id'], old_cost if old_cost else None, new_cost, pct))
                            changes += 1
                    else:
                        # New ingredient in master — insert it
                        name = (row.get('name') or code).strip()
                        c.execute("""
                            INSERT OR IGNORE INTO ingredients (code, name, cost_per_kg)
                            VALUES (?,?,?)
                        """, (code, name, new_cost))
                        changes += 1
            c.commit()
            if changes:
                print(f"  ✓ Masters: ingredient_pricing — {changes} price(s) updated")
                changed_total += changes
        except Exception as e:
            print(f"  ⚠ Masters: ingredient_pricing sync error — {e}")
            c.rollback()
        finally:
            c.close()

    # ── Suppliers ─────────────────────────────────────────────────
    if MASTER_SUPPLIERS.exists():
        c = _conn()
        try:
            changes = 0
            with open(MASTER_SUPPLIERS, newline='', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    code = (row.get('code') or '').strip().upper()
                    if not code:
                        continue
                    existing = qry1("SELECT * FROM suppliers WHERE code=?", (code,))
                    fields = {
                        'name':    (row.get('name') or '').strip(),
                        'contact': (row.get('contact') or '').strip(),
                        'phone':   (row.get('phone') or '').strip(),
                        'email':   (row.get('email') or '').strip(),
                        'city':    (row.get('city') or '').strip(),
                    }
                    if existing:
                        diff = {k: v for k, v in fields.items() if str(existing.get(k, '') or '') != v and v}
                        if diff:
                            sets = ', '.join(f"{k}=?" for k in diff)
                            c.execute(f"UPDATE suppliers SET {sets} WHERE code=?", [*diff.values(), code])
                            changes += 1
                    else:
                        c.execute("""
                            INSERT OR IGNORE INTO suppliers (code, name, contact, phone, email, city)
                            VALUES (?,?,?,?,?,?)
                        """, (code, fields['name'], fields['contact'], fields['phone'], fields['email'], fields['city']))
                        changes += 1
            c.commit()
            if changes:
                print(f"  ✓ Masters: suppliers — {changes} record(s) updated")
                changed_total += changes
        except Exception as e:
            print(f"  ⚠ Masters: suppliers sync error — {e}")
            c.rollback()
        finally:
            c.close()

    # ── Customers ─────────────────────────────────────────────────
    if MASTER_CUSTOMERS.exists():
        c = _conn()
        try:
            changes = 0
            with open(MASTER_CUSTOMERS, newline='', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    code = (row.get('code') or '').strip().upper()
                    if not code:
                        continue
                    existing = qry1("SELECT * FROM customers WHERE code=?", (code,))
                    fields = {
                        'name':               (row.get('name') or '').strip(),
                        'customer_type':      (row.get('customer_type') or 'RETAIL').strip().upper(),
                        'city':               (row.get('city') or '').strip(),
                        'phone':              (row.get('phone') or '').strip(),
                        'email':              (row.get('email') or '').strip(),
                        'payment_terms_days': int(row.get('payment_terms_days') or 30),
                        'default_pack':       (row.get('default_pack') or '').strip(),
                    }
                    if existing:
                        diff = {k: v for k, v in fields.items()
                                if str(existing.get(k, '') or '') != str(v) and v != '' and v != 0}
                        if diff:
                            sets = ', '.join(f"{k}=?" for k in diff)
                            c.execute(f"UPDATE customers SET {sets} WHERE code=?", [*diff.values(), code])
                            changes += 1
                    else:
                        c.execute("""
                            INSERT OR IGNORE INTO customers
                                (code, name, customer_type, city, phone, email,
                                 payment_terms_days, default_pack)
                            VALUES (?,?,?,?,?,?,?,?)
                        """, (code, fields['name'], fields['customer_type'],
                              fields['city'], fields['phone'], fields['email'],
                              fields['payment_terms_days'], fields['default_pack']))
                        changes += 1
            c.commit()
            if changes:
                print(f"  ✓ Masters: customers — {changes} record(s) updated")
                changed_total += changes
        except Exception as e:
            print(f"  ⚠ Masters: customers sync error — {e}")
            c.rollback()
        finally:
            c.close()

    if changed_total:
        save_db()
        load_ref()
    else:
        print("  ✓ Masters: all files in sync — no changes")


def seed_zones_routes():
    """
    Idempotent: seeds Karachi and Hyderabad sales zones and their area routes.
    Skips gracefully if zones already exist (matches by name + city).
    """
    ZONE_DATA = [
        # ── Karachi ──────────────────────────────────────────────────────────
        {
            'city': 'Karachi',
            'name': 'KHI-Z1 South Karachi',
            'description': 'Premium + Dense Retail — High-end retail, supermarkets, branded spice demand',
            'routes': ['Saddar', 'Clifton', 'Defence (DHA)', 'Tariq Road'],
            'visit_days': 'Wed',
        },
        {
            'city': 'Karachi',
            'name': 'KHI-Z2 Central Karachi',
            'description': 'High Volume Markets — Dense population, kiryana stores, core FMCG zone',
            'routes': ['Gulshan-e-Iqbal', 'Gulistan-e-Johar', 'Federal B Area', 'Liaquatabad'],
            'visit_days': 'Mon,Tue',
        },
        {
            'city': 'Karachi',
            'name': 'KHI-Z3 East / Industrial Belt',
            'description': 'Mixed income + industrial workforce — Economy packs, distributor network',
            'routes': ['Korangi', 'Landhi', 'Shah Faisal Colony'],
            'visit_days': '',
        },
        {
            'city': 'Karachi',
            'name': 'KHI-Z4 West Karachi',
            'description': 'Wholesale + low to mid-income — Bulk sales, loose spices, weekly routes',
            'routes': ['Orangi Town', 'Baldia Town', 'SITE Area (KHI)'],
            'visit_days': 'Thu',
        },
        {
            'city': 'Karachi',
            'name': 'KHI-Z5 North Karachi',
            'description': 'Large residential clusters — Stable repeat consumption, route-based',
            'routes': ['North Karachi', 'North Nazimabad', 'Buffer Zone', 'Surjani Town'],
            'visit_days': 'Fri',
        },
        {
            'city': 'Karachi',
            'name': 'KHI-Z6 Wholesale Markets',
            'description': 'Bulk buyers, traders, distributors — Dedicated wholesale team',
            'routes': ['Jodia Bazaar', 'Bolton Market', 'Empress Market'],
            'visit_days': 'Sat',
        },
        # ── Hyderabad ─────────────────────────────────────────────────────────
        {
            'city': 'Hyderabad',
            'name': 'HYD-Z1 City Core',
            'description': 'Main Market — High footfall, dense retail, daily coverage',
            'routes': ['Saddar (HYD)', 'Resham Gali', 'Shahi Bazaar', 'Market Tower', 'Heera-Baad'],
            'visit_days': 'Thu',
        },
        {
            'city': 'Hyderabad',
            'name': 'HYD-Z2 Latifabad',
            'description': 'Residential + retail mix — Strong FMCG demand, distributor + retailer focus',
            'routes': ['Latifabad Units 1-6', 'Latifabad Units 7-12', 'Auto Bhan Road'],
            'visit_days': 'Mon,Tue',
        },
        {
            'city': 'Hyderabad',
            'name': 'HYD-Z3 Qasimabad',
            'description': 'Growing urban area — High middle-income households, grocery + push',
            'routes': ['Main Qasimabad', 'Wadhu Wah Road', 'Citizen Colony'],
            'visit_days': 'Wed',
        },
        {
            'city': 'Hyderabad',
            'name': 'HYD-Z4 Industrial & Peripheral',
            'description': 'Wholesale, warehouses, bulk buyers — Distributor relationship focus',
            'routes': ['SITE Area (HYD)', 'Kohsar', 'Hali Road', 'Tando Jam Road'],
            'visit_days': '',
        },
        {
            'city': 'Hyderabad',
            'name': 'HYD-Z5 Outskirts',
            'description': 'Semi-urban / rural mix — Weekly visits, sub-distributors if volume grows',
            'routes': ['Kotri', 'Tando Jam', 'Hussainabad'],
            'visit_days': 'Fri',
        },
    ]

    c = _conn()
    zones_added = 0
    routes_added = 0
    try:
        for zd in ZONE_DATA:
            existing = c.execute(
                "SELECT id FROM zones WHERE name=? AND city=?", (zd['name'], zd['city'])
            ).fetchone()
            if existing:
                zone_id = existing[0]
            else:
                c.execute(
                    "INSERT INTO zones (name, city, active) VALUES (?,?,1)",
                    (zd['name'], zd['city'])
                )
                zone_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
                zones_added += 1
            for route_name in zd['routes']:
                exists = c.execute(
                    "SELECT id FROM routes WHERE zone_id=? AND name=?", (zone_id, route_name)
                ).fetchone()
                if not exists:
                    c.execute(
                        "INSERT INTO routes (zone_id, name, visit_days, active) VALUES (?,?,?,1)",
                        (zone_id, route_name, zd['visit_days'])
                    )
                    routes_added += 1
        c.commit()
        if zones_added or routes_added:
            print(f"  ✓ Zones/Routes seeded: {zones_added} zones, {routes_added} routes (KHI + HYD)")
        else:
            print("  ✓ Zones/Routes: already seeded — skipped")
    except Exception as e:
        print(f"  ⚠ seed_zones_routes error: {e}")
    finally:
        c.close()


def seed_price_history():
    """
    If an ingredient has cost_per_kg > 0 but no history record, seed one
    with old_cost=NULL and source='initial'. This makes the current price
    visible in the inventory change column from day one.
    """
    ings = qry("""
        SELECT i.id, i.code, i.cost_per_kg
        FROM ingredients i
        LEFT JOIN ingredient_price_history ph ON ph.ingredient_id = i.id
        WHERE i.cost_per_kg > 0 AND ph.id IS NULL
    """)
    if not ings:
        return
    c = _conn()
    try:
        for i in ings:
            c.execute("""
                INSERT INTO ingredient_price_history
                    (ingredient_id, old_cost_per_kg, new_cost_per_kg, pct_change, source)
                VALUES (?, NULL, ?, NULL, 'initial')
            """, (i['id'], i['cost_per_kg']))
        c.commit()
        print(f"  ✓ Price history: seeded {len(ings)} initial price record(s)")
    except Exception as e:
        print(f"  ⚠ Price history seed error: {e}")
        c.rollback()
    finally:
        c.close()
    save_db()


def get_ingredient_price_history(ingredient_id=None, limit=50):
    sql = """
        SELECT ph.*, i.code, i.name
        FROM ingredient_price_history ph
        JOIN ingredients i ON i.id = ph.ingredient_id
        {}
        ORDER BY ph.changed_at DESC
        LIMIT ?
    """.format("WHERE ph.ingredient_id=?" if ingredient_id else "")
    params = (ingredient_id, limit) if ingredient_id else (limit,)
    return qry(sql, params)


# ═══════════════════════════════════════════════════════════════════
#  COSTING CONFIG — table, seed, get, update
# ═══════════════════════════════════════════════════════════════════

def ensure_costing_config():
    """Create costing_config + costing_config_history tables and seed defaults. Idempotent."""
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS costing_config (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL,
                label      TEXT,
                updated_at TEXT,
                updated_by TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS costing_config_history (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                config_key TEXT NOT NULL,
                old_value  TEXT,
                new_value  TEXT NOT NULL,
                pct_change REAL,
                changed_by TEXT,
                changed_at TEXT DEFAULT (datetime('now')),
                note       TEXT
            )
        """)
        defaults = [
            ('packaging_cost_per_unit', '15.00', 'Packaging Cost per Unit (Rs)'),
            ('overhead_pct',            '0.10',  'Overhead % of RM Cost'),
            ('margin_mfr',              '1.30',  'Direct Sale Margin Multiplier'),
            ('margin_dist',             '1.10',  'Distributor Margin Multiplier'),
            ('margin_mrp',              '1.22',  'MRP Margin Multiplier'),
            ('margin_floor_pct',        '30.00', 'Minimum Profit Margin % (alert threshold)'),
            ('labour_cost_per_unit',    '5.00',  'Labour Cost per Unit (Rs)'),
        ]
        for key, value, label in defaults:
            c.execute(
                "INSERT OR IGNORE INTO costing_config (key, value, label) VALUES (?,?,?)",
                (key, value, label)
            )
        # Update overhead if still at old placeholder 0.29
        c.execute("UPDATE costing_config SET value='0.10' WHERE key='overhead_pct' AND value='0.29'")
        # Update stale labels
        c.execute("UPDATE costing_config SET label='Direct Sale Margin Multiplier' WHERE key='margin_mfr'")
        c.execute("UPDATE costing_config SET label='Minimum Profit Margin % (alert threshold)' WHERE key='margin_floor_pct'")
        c.commit()
        print("  \u2713 Costing config table ready")
    finally:
        c.close()


def ensure_variant_wastage_pct():
    """Add wastage_pct column to product_variants. Idempotent."""
    c = _conn()
    try:
        existing = {r[1] for r in c.execute("PRAGMA table_info(product_variants)").fetchall()}
        if 'wastage_pct' not in existing:
            c.execute("ALTER TABLE product_variants ADD COLUMN wastage_pct REAL DEFAULT 0")
            print("  \u2713 product_variants: added wastage_pct")
        c.commit()
    finally:
        c.close()


def ensure_variant_gtin():
    """Add gtin column to product_variants and seed known GTINs. Idempotent."""
    c = _conn()
    try:
        existing = {r[1] for r in c.execute("PRAGMA table_info(product_variants)").fetchall()}
        if 'gtin' not in existing:
            c.execute("ALTER TABLE product_variants ADD COLUMN gtin TEXT DEFAULT NULL")
            print("  ✓ product_variants: added gtin")

        # Seed GTINs — match by product name + pack size grams (robust across any sku_code format)
        # Only seeds if gtin is currently NULL (never overwrites manually-entered values)
        seeds = [
            ('Chaat Masala', 50,   '8966000086913'),
            ('Garam Masala', 50,   '8966000086920'),
        ]
        for prod_name, grams, gtin_val in seeds:
            cur = c.execute("""
                UPDATE product_variants
                SET gtin = ?
                WHERE gtin IS NULL
                  AND product_id IN (SELECT id FROM products WHERE name = ?)
                  AND pack_size_id IN (SELECT id FROM pack_sizes WHERE grams = ?)
            """, (gtin_val, prod_name, grams))
            if cur.rowcount:
                print(f"  ✓ gtin seeded: {prod_name} {grams}g -> {gtin_val}")
        c.commit()
    finally:
        c.close()


def ensure_clean_customer_codes():
    """One-time migration: fix SP-SP-CUST-XXXX double-prefix → SP-CUST-XXXX.
    Updates customers table + denormalized cust_code in sales + customer_orders.
    Idempotent — safe to run on every startup."""
    c = _conn()
    try:
        bad = c.execute(
            "SELECT id, code FROM customers WHERE code LIKE 'SP-SP-CUST-%'"
        ).fetchall()
        if not bad:
            c.close()
            return
        # Check which tables have a cust_code column before touching them
        sales_cols = {r['name'] for r in c.execute("PRAGMA table_info(sales)")}
        co_cols    = {r['name'] for r in c.execute("PRAGMA table_info(customer_orders)")}
        for row in bad:
            old_code = row['code']
            new_code = old_code.replace('SP-SP-CUST-', 'SP-CUST-', 1)
            c.execute("UPDATE customers SET code=? WHERE id=?", (new_code, row['id']))
            if 'cust_code' in sales_cols:
                c.execute("UPDATE sales SET cust_code=? WHERE cust_code=?", (new_code, old_code))
            if 'cust_code' in co_cols:
                c.execute("UPDATE customer_orders SET cust_code=? WHERE cust_code=?", (new_code, old_code))
            print(f"  ✓ customer code fixed: {old_code} → {new_code}")
        c.commit()
        print(f"  ✓ Fixed {len(bad)} customer code(s) — double-prefix removed")
    finally:
        c.close()


def ensure_clean_supplier_codes():
    """Assign clean SUP-NNN codes to any SP-SUP-* suppliers.
    Finds the current max SUP-NNN and assigns next sequential codes.
    Idempotent — safe to run on every startup. Never crashes server."""
    c = _conn()
    try:
        bad = c.execute(
            "SELECT id, code FROM suppliers WHERE code LIKE 'SP-SUP-%' ORDER BY code"
        ).fetchall()
        if not bad:
            c.close()
            return
        # Find current max SUP-NNN number
        max_row = c.execute(
            "SELECT code FROM suppliers WHERE code LIKE 'SUP-%' ORDER BY code DESC LIMIT 1"
        ).fetchone()
        try:
            next_num = int(max_row['code'].split('-')[1]) + 1 if max_row else 1
        except Exception:
            next_num = 100
        fixed = 0
        for row in bad:
            old_code = row['code']
            new_code = f"SUP-{next_num:03d}"
            next_num += 1
            c.execute("UPDATE suppliers SET code=? WHERE id=?", (new_code, row['id']))
            print(f"  ✓ supplier code: {old_code} → {new_code}")
            fixed += 1
        c.commit()
        print(f"  ✓ Normalized {fixed} supplier code(s) to SUP-NNN format")
    except Exception as e:
        print(f"  ⚠ ensure_clean_supplier_codes error (non-fatal): {e}")
        try: c.rollback()
        except: pass
    finally:
        c.close()


def _reset_admin_pw_if_requested():
    """If RESET_ADMIN_PW env var is set, reset admin password (SHA-256) and clear all rate limits."""
    new_pw = os.environ.get('RESET_ADMIN_PW', '').strip()
    if not new_pw:
        return
    # Use SHA-256 with empty salt — no argon2 dependency, guaranteed to verify
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


def ensure_price_types_sprint6():
    """Update price_type labels for Sprint 6 terminology + add bulk. Idempotent."""
    c = _conn()
    try:
        updates = [
            ('mfg_cost',    'Cost to Make'),
            ('ex_factory',  'Direct Sale'),
            ('retail_mrp',  'MRP'),
            ('distributor', 'Distributor'),
        ]
        for code, label in updates:
            c.execute("UPDATE price_types SET label=? WHERE code=?", (label, code))
        c.execute("INSERT OR IGNORE INTO price_types (code, label) VALUES ('bulk', 'Bulk')")
        c.commit()
        print("  \u2713 price_types: Sprint 6 labels updated + bulk added")
    finally:
        c.close()


def ensure_price_history_extended():
    """Add change_type, config_key, changed_by, note columns to ingredient_price_history. Idempotent."""
    c = _conn()
    try:
        existing = {r[1] for r in c.execute("PRAGMA table_info(ingredient_price_history)").fetchall()}
        additions = [
            ('change_type', "TEXT DEFAULT 'ingredient'"),
            ('config_key',  'TEXT'),
            ('changed_by',  "TEXT DEFAULT 'system'"),
            ('note',        'TEXT'),
        ]
        for col, typedef in additions:
            if col not in existing:
                c.execute("ALTER TABLE ingredient_price_history ADD COLUMN {} {}".format(col, typedef))
                print("  \u2713 price_history: added {}".format(col))
        c.commit()
    finally:
        c.close()


def get_costing_config():
    """Return all costing config values as a dict keyed by config key."""
    rows = qry("SELECT key, value, label, updated_at, updated_by FROM costing_config ORDER BY key")
    return {r['key']: dict(r) for r in rows}


def _get_config_val(cfg, key, default):
    """Helper — extract float from costing_config dict."""
    try:
        return float(cfg[key]['value'])
    except (KeyError, TypeError, ValueError):
        return default


def compute_standard_cost(product_code, pack_size_label, cfg=None):
    """
    Compute standard cost for one SKU using current BOM + ingredient costs + costing_config.
    Returns dict with full cost breakdown, or None if no active BOM.
    pack_size_label: e.g. '50g', '100g', '1000g'
    """
    if cfg is None:
        cfg = get_costing_config()

    packaging    = _get_config_val(cfg, 'packaging_cost_per_unit', 15.0)
    overhead_pct = _get_config_val(cfg, 'overhead_pct', 0.10)
    margin_mfr   = _get_config_val(cfg, 'margin_mfr', 1.30)
    margin_dist  = _get_config_val(cfg, 'margin_dist', 1.10)
    margin_mrp   = _get_config_val(cfg, 'margin_mrp', 1.22)
    floor_pct    = _get_config_val(cfg, 'margin_floor_pct', 30.0)
    labour       = _get_config_val(cfg, 'labour_cost_per_unit', 5.0)

    # Fetch variant (include wastage_pct)
    variant = qry1("""
        SELECT pv.id, pv.sku_code, p.code as product_code, p.name as product_name,
               ps.label as pack_size, ps.grams as pack_grams, pv.product_id,
               COALESCE(pv.wastage_pct, 0) as wastage_pct
        FROM product_variants pv
        JOIN products p   ON p.id = pv.product_id
        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE p.code=? AND ps.label=? AND pv.active_flag=1
    """, (product_code, pack_size_label))
    if not variant:
        return None

    wastage_pct = float(variant['wastage_pct'] or 0)

    # Fetch active BOM
    bom_ver = qry1("""
        SELECT * FROM bom_versions WHERE product_id=? AND active_flag=1
        ORDER BY version_no DESC LIMIT 1
    """, (variant['product_id'],))
    if not bom_ver:
        return {
            'productCode': product_code, 'productName': variant['product_name'],
            'packSize': pack_size_label, 'skuCode': variant['sku_code'],
            'variantId': variant['id'],
            'has_bom': False, 'ingredients': [],
            'rm_cost': 0, 'wastage_adj': 0, 'overhead': 0,
            'packaging': packaging, 'labour': labour,
            'cost_to_make': round(packaging + labour, 2),
            'direct_sale': 0, 'distributor': 0, 'mrp': 0,
            'gross_margin_pct': 0, 'below_floor': True,
            'wastage_pct': wastage_pct,
        }

    # Scale BOM to 1 unit of this pack size
    pack_grams = float(variant['pack_grams'] or 0)
    scale = pack_grams / float(bom_ver['batch_size_grams']) if bom_ver['batch_size_grams'] else 0

    bom_items = qry("""
        SELECT bi.quantity_grams, i.id as ing_id, i.code as ing_code, i.name as ing_name,
               i.cost_per_kg
        FROM bom_items bi
        JOIN ingredients i ON i.id = bi.ingredient_id
        WHERE bi.bom_version_id=?
        ORDER BY i.code
    """, (bom_ver['id'],))

    ingredients = []
    rm_cost_raw = 0.0
    for b in bom_items:
        qty_kg   = round(b['quantity_grams'] * scale / 1000.0, 6)
        cpkg     = float(b['cost_per_kg'] or 0)
        line_cost= round(qty_kg * cpkg, 4)
        rm_cost_raw += line_cost
        ingredients.append({
            'code':       b['ing_code'],
            'name':       b['ing_name'],
            'qty_kg':     qty_kg,
            'cost_per_kg':cpkg,
            'line_cost':  line_cost,
        })

    # Apply product-level wastage to total RM cost
    rm_cost_raw = round(rm_cost_raw, 2)
    if wastage_pct > 0 and wastage_pct < 1:
        rm_cost_adjusted = round(rm_cost_raw / (1 - wastage_pct), 2)
    else:
        rm_cost_adjusted = rm_cost_raw
    wastage_adj  = round(rm_cost_adjusted - rm_cost_raw, 2)

    overhead     = round(rm_cost_adjusted * overhead_pct, 2)
    cost_to_make = round(rm_cost_adjusted + overhead + packaging + labour, 2)
    direct_sale  = round(cost_to_make * margin_mfr, 2)
    distributor  = round(direct_sale * margin_dist, 2)
    mrp          = round(distributor * margin_mrp, 2)
    margin_pct   = round((mrp - cost_to_make) / mrp * 100, 1) if mrp > 0 else 0

    return {
        'productCode':      product_code,
        'productName':      variant['product_name'],
        'packSize':         pack_size_label,
        'skuCode':          variant['sku_code'],
        'variantId':        variant['id'],
        'has_bom':          True,
        'ingredients':      ingredients,
        'rm_cost':          rm_cost_adjusted,
        'wastage_adj':      wastage_adj,
        'wastage_pct':      wastage_pct,
        'overhead':         overhead,
        'packaging':        packaging,
        'labour':           labour,
        'cost_to_make':     cost_to_make,
        'direct_sale':      direct_sale,
        'distributor':      distributor,
        'mrp':              mrp,
        'gross_margin_pct': margin_pct,
        'below_floor':      margin_pct < floor_pct,
    }


def get_all_standard_costs():
    """Return standard costs for all active SKUs."""
    cfg = get_costing_config()
    variants = qry("""
        SELECT p.code as product_code, ps.label as pack_size
        FROM product_variants pv
        JOIN products p   ON p.id = pv.product_id
        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE pv.active_flag=1 AND p.active=1
        ORDER BY p.code, ps.grams
    """)
    results = []
    for v in variants:
        cost = compute_standard_cost(v['product_code'], v['pack_size'], cfg)
        if cost:
            results.append(cost)
    return results


def get_batch_variances(days=90):
    """Compare unit_cost_at_posting vs computed standard cost for recent batches."""
    cfg = get_costing_config()
    batches = qry("""
        SELECT pb.id, pb.batch_id, pb.batch_date, pb.qty_units,
               pb.unit_cost_at_posting,
               p.code as product_code, p.name as product_name,
               ps.label as pack_size
        FROM production_batches pb
        JOIN products p ON p.id = pb.product_id
        LEFT JOIN product_variants pv ON pv.id = pb.product_variant_id
        LEFT JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE pb.batch_date >= date('now', ? || ' days')
        ORDER BY pb.batch_date DESC
        LIMIT 200
    """, ('-' + str(days),))

    results = []
    for b in batches:
        actual = float(b['unit_cost_at_posting'] or 0)
        std_data = compute_standard_cost(b['product_code'], b['pack_size'] or '', cfg) if b['pack_size'] else None
        standard = std_data['cost_to_make'] if std_data and std_data.get('has_bom') else None
        variance = round(actual - standard, 2) if standard is not None else None
        variance_pct = round((variance / standard) * 100, 1) if standard and variance is not None else None
        results.append({
            'batchId':       b['batch_id'],
            'batchDate':     b['batch_date'],
            'productCode':   b['product_code'],
            'productName':   b['product_name'],
            'packSize':      b['pack_size'],
            'qtyUnits':      b['qty_units'],
            'actual_mfg':    actual,
            'standard_mfg':  standard,
            'variance':      variance,
            'variance_pct':  variance_pct,
            'favourable':    variance_pct is not None and variance_pct < 0,
            'flag':          variance_pct is not None and variance_pct > 5,
        })
    return results


def update_costing_config(key, value, username):
    """Update a single costing config key. Logs change to price_history. Returns full config."""
    row = qry1("SELECT * FROM costing_config WHERE key=?", (key,))
    if not row:
        raise ValueError("Unknown config key: {}".format(key))
    old_val = float(row['value'])
    new_val = float(value)
    pct     = round((new_val - old_val) / old_val * 100, 2) if old_val != 0 else 0
    c = _conn()
    try:
        # Log to costing_config_history
        c.execute("""
            INSERT INTO costing_config_history
                (config_key, old_value, new_value, pct_change, changed_by)
            VALUES (?, ?, ?, ?, ?)
        """, (key, str(old_val), str(new_val), pct, username))
        c.execute("""
            UPDATE costing_config
            SET value=?, updated_at=datetime('now'), updated_by=?
            WHERE key=?
        """, (str(new_val), username, key))
        c.commit()
    finally:
        c.close()
    save_db()
    return get_costing_config()


def ensure_margin_alerts_table():
    """Create margin_alerts table if not present (idempotent)."""
    c = _conn()
    try:
        c.execute("""
            CREATE TABLE IF NOT EXISTS margin_alerts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                product_code TEXT NOT NULL,
                pack_size    TEXT NOT NULL,
                sku_code     TEXT,
                margin_pct   REAL NOT NULL,
                floor_pct    REAL NOT NULL,
                detected_at  TEXT DEFAULT (datetime('now')),
                dismissed_at TEXT,
                dismissed_by TEXT,
                email_sent   INTEGER DEFAULT 0
            )
        """)
        c.commit()
    finally:
        c.close()


def get_price_history(limit=100, change_type=None, days=None):
    """
    Return unified price history: ingredient cost changes + config changes.
    Merges ingredient_price_history (change_type='ingredient') and
    costing_config_history (change_type='config').
    """
    rows = []

    # --- Ingredient price history ---
    ing_where = "WHERE 1=1"
    ing_params = []
    if change_type and change_type != 'config':
        ing_where += " AND COALESCE(iph.change_type, 'ingredient') = ?"
        ing_params.append(change_type)
    if days:
        ing_where += " AND iph.changed_at >= datetime('now', ? || ' days')"
        ing_params.append('-' + str(days))

    ing_rows = qry("""
        SELECT
            'ingredient'           AS change_type,
            iph.id                 AS id,
            i.code                 AS entity_code,
            i.name                 AS entity_name,
            iph.old_cost_per_kg    AS old_value,
            iph.new_cost_per_kg    AS new_value,
            iph.pct_change,
            COALESCE(iph.changed_by, 'system') AS changed_by,
            iph.changed_at,
            COALESCE(iph.source, 'manual') AS source,
            iph.note
        FROM ingredient_price_history iph
        JOIN ingredients i ON i.id = iph.ingredient_id
        {where}
        ORDER BY iph.changed_at DESC
        LIMIT ?
    """.format(where=ing_where), ing_params + [limit])

    for r in ing_rows:
        rows.append(dict(r))

    # --- Config history ---
    if change_type in (None, 'config'):
        cfg_where = "WHERE 1=1"
        cfg_params = []
        if days:
            cfg_where += " AND changed_at >= datetime('now', ? || ' days')"
            cfg_params.append('-' + str(days))

        cfg_rows = qry("""
            SELECT
                'config'     AS change_type,
                cch.id       AS id,
                cch.config_key   AS entity_code,
                cc.label     AS entity_name,
                cch.old_value,
                cch.new_value,
                cch.pct_change,
                COALESCE(cch.changed_by, 'system') AS changed_by,
                cch.changed_at,
                'manual'     AS source,
                cch.note
            FROM costing_config_history cch
            LEFT JOIN costing_config cc ON cc.key = cch.config_key
            {where}
            ORDER BY cch.changed_at DESC
            LIMIT ?
        """.format(where=cfg_where), cfg_params + [limit])

        for r in cfg_rows:
            rows.append(dict(r))

    # Merge + sort by changed_at desc, return top `limit`
    rows.sort(key=lambda x: x.get('changed_at') or '', reverse=True)
    return rows[:limit]


def get_margin_alerts(include_dismissed=False):
    """
    Compute current margin alerts by checking all active SKUs against floor.
    Also checks margin_alerts table for already-logged/dismissed alerts.
    Returns list of active (undismissed) alerts.
    """
    ensure_margin_alerts_table()
    all_costs = get_all_standard_costs()
    cfg = get_costing_config()
    floor_pct = _get_config_val(cfg, 'margin_floor_pct', 30.0)

    alerts = []
    for sku in all_costs:
        if not sku.get('has_bom'):
            continue
        gm = sku.get('gross_margin_pct', 0)
        if gm < floor_pct:
            # Check if already dismissed
            existing = qry1("""
                SELECT * FROM margin_alerts
                WHERE product_code=? AND pack_size=? AND dismissed_at IS NULL
                ORDER BY detected_at DESC LIMIT 1
            """, (sku['productCode'], sku['packSize']))

            if not existing:
                # Log new alert
                c = _conn()
                try:
                    c.execute("""
                        INSERT INTO margin_alerts
                            (product_code, pack_size, sku_code, margin_pct, floor_pct)
                        VALUES (?, ?, ?, ?, ?)
                    """, (sku['productCode'], sku['packSize'],
                          sku.get('skuCode'), round(gm, 2), floor_pct))
                    c.commit()
                    existing = qry1("""
                        SELECT * FROM margin_alerts
                        WHERE product_code=? AND pack_size=? AND dismissed_at IS NULL
                        ORDER BY id DESC LIMIT 1
                    """, (sku['productCode'], sku['packSize']))
                finally:
                    c.close()

            if existing:
                alerts.append({
                    'alertId':      existing['id'],
                    'productCode':  sku['productCode'],
                    'productName':  sku['productName'],
                    'packSize':     sku['packSize'],
                    'skuCode':      sku.get('skuCode'),
                    'margin_pct':   round(gm, 2),
                    'floor_pct':    floor_pct,
                    'gap':          round(floor_pct - gm, 2),
                    'detectedAt':   existing['detected_at'],
                    'emailSent':    bool(existing['email_sent']),
                    'exFactory':    sku.get('ex_factory'),
                })

    if include_dismissed:
        dismissed = qry("""
            SELECT * FROM margin_alerts WHERE dismissed_at IS NOT NULL
            ORDER BY dismissed_at DESC LIMIT 50
        """)
        for d in dismissed:
            cost_data = compute_standard_cost(d['product_code'], d['pack_size'])
            alerts.append({
                'alertId':      d['id'],
                'productCode':  d['product_code'],
                'packSize':     d['pack_size'],
                'skuCode':      d['sku_code'],
                'margin_pct':   d['margin_pct'],
                'floor_pct':    d['floor_pct'],
                'detectedAt':   d['detected_at'],
                'dismissedAt':  d['dismissed_at'],
                'dismissedBy':  d['dismissed_by'],
                'dismissed':    True,
            })

    return alerts


def dismiss_margin_alert(alert_id, username):
    """Mark a margin alert as dismissed."""
    ensure_margin_alerts_table()
    row = qry1("SELECT * FROM margin_alerts WHERE id=?", (alert_id,))
    if not row:
        raise ValueError("Alert not found: {}".format(alert_id))
    run("""
        UPDATE margin_alerts
        SET dismissed_at=datetime('now'), dismissed_by=?
        WHERE id=?
    """, (username, alert_id))
    save_db()
    return {'ok': True, 'alertId': alert_id}


def send_margin_alert_email(alerts):
    """
    Send email notification for margin alerts.
    Reads ALERT_EMAIL env var. Returns True if sent, False if not configured.
    """
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    to_addr  = os.environ.get('ALERT_EMAIL', '')
    smtp_host= os.environ.get('SMTP_HOST', '')
    smtp_port= int(os.environ.get('SMTP_PORT', '587'))
    smtp_user= os.environ.get('SMTP_USER', '')
    smtp_pass= os.environ.get('SMTP_PASS', '')

    if not to_addr or not smtp_host:
        return False  # not configured

    subject = 'Spicetopia: %d Margin Alert(s) Below Floor' % len(alerts)
    lines = ['The following SKUs are below the margin floor:\n']
    for a in alerts:
        lines.append('  • %s %s — margin %.1f%% (floor %.1f%%, gap %.1f%%)' % (
            a['productCode'], a['packSize'],
            a['margin_pct'], a['floor_pct'], a['gap']))
    lines.append('\nLog in to review: Prices & Costs → Margin Alerts')

    msg = MIMEMultipart()
    msg['From']    = smtp_user or 'noreply@spicetopia.com'
    msg['To']      = to_addr
    msg['Subject'] = subject
    msg.attach(MIMEText('\n'.join(lines), 'plain'))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as s:
            s.starttls()
            if smtp_user:
                s.login(smtp_user, smtp_pass)
            s.send_message(msg)

        # Mark alerts as email_sent
        for a in alerts:
            run("UPDATE margin_alerts SET email_sent=1 WHERE id=?", (a['alertId'],))

        _log('info', 'margin_alert_email_sent', count=len(alerts), to=to_addr)
        return True
    except Exception as e:
        _log('error', 'margin_alert_email_failed', error=str(e))
        return False


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — ZONES & ROUTES
# ═══════════════════════════════════════════════════════════════════

def list_zones():
    return qry("SELECT * FROM zones WHERE active=1 ORDER BY name")

def create_zone(data):
    name = data.get('name','').strip()
    if not name:
        raise ValueError("Zone name is required")
    c = _conn()
    try:
        c.execute("INSERT INTO zones (name, city) VALUES (?,?)",
                  (name, data.get('city', data.get('description',''))))
        c.commit()
        zone_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        c.close()
    save_db()
    return qry1("SELECT * FROM zones WHERE id=?", (zone_id,))

def update_zone(zone_id, data):
    z = qry1("SELECT * FROM zones WHERE id=?", (zone_id,))
    if not z:
        raise ValueError(f"Zone not found: {zone_id}")
    mapping = {'name':'name','city':'city','description':'city','active':'active'}
    set_parts, vals = [], []
    seen_cols = set()
    for k, col in mapping.items():
        if k in data and col not in seen_cols:
            set_parts.append(f"{col}=?")
            vals.append(data[k])
            seen_cols.add(col)
    if set_parts:
        vals.append(zone_id)
        c = _conn()
        try:
            c.execute(f"UPDATE zones SET {', '.join(set_parts)} WHERE id=?", vals)
            c.commit()
        finally:
            c.close()
        save_db()
    return qry1("SELECT * FROM zones WHERE id=?", (zone_id,))

def list_routes(zone_id=None):
    if zone_id:
        return qry("""
            SELECT r.*, z.name as zone_name
            FROM routes r JOIN zones z ON z.id=r.zone_id
            WHERE r.zone_id=? AND r.active=1 ORDER BY r.name
        """, (zone_id,))
    return qry("""
        SELECT r.*, z.name as zone_name
        FROM routes r JOIN zones z ON z.id=r.zone_id
        WHERE r.active=1 ORDER BY z.name, r.name
    """)

def create_route(data):
    zone_id = data.get('zoneId') or data.get('zone_id')
    name = data.get('name','').strip()
    if not zone_id or not name:
        raise ValueError("zoneId and name are required")
    z = qry1("SELECT id FROM zones WHERE id=?", (int(zone_id),))
    if not z:
        raise ValueError(f"Zone not found: {zone_id}")
    c = _conn()
    try:
        c.execute("""
            INSERT INTO routes (zone_id, name, visit_days)
            VALUES (?,?,?)
        """, (int(zone_id), name, data.get('visitDays', data.get('visit_days',''))))
        c.commit()
        route_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        c.close()
    save_db()
    return qry1("""
        SELECT r.*, z.name as zone_name FROM routes r JOIN zones z ON z.id=r.zone_id
        WHERE r.id=?
    """, (route_id,))

def update_route(route_id, data):
    r = qry1("SELECT * FROM routes WHERE id=?", (route_id,))
    if not r:
        raise ValueError(f"Route not found: {route_id}")
    mapping = {'name':'name','visitDays':'visit_days','visit_days':'visit_days','active':'active'}
    set_parts, vals = [], []
    for k, col in mapping.items():
        if k in data:
            set_parts.append(f"{col}=?")
            vals.append(data[k])
    if set_parts:
        vals.append(route_id)
        c = _conn()
        try:
            c.execute(f"UPDATE routes SET {', '.join(set_parts)} WHERE id=?", vals)
            c.commit()
        finally:
            c.close()
        save_db()
    return qry1("""
        SELECT r.*, z.name as zone_name FROM routes r JOIN zones z ON z.id=r.zone_id
        WHERE r.id=?
    """, (route_id,))


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — SALES REPS
# ═══════════════════════════════════════════════════════════════════

def list_reps(active_only=True):
    # status column: 'active' | 'inactive' | NULL
    # primary_zone_id: the zone assigned to the rep
    sql = """
        SELECT sr.*,
               z.name as zone_name
        FROM sales_reps sr
        LEFT JOIN zones z ON z.id=sr.primary_zone_id
        {}
        ORDER BY sr.name
    """.format("WHERE (sr.status IS NULL OR sr.status='active')" if active_only else "")
    return qry(sql)

def get_rep(rep_id):
    rep = qry1("SELECT * FROM sales_reps WHERE id=?", (rep_id,))
    if not rep:
        return None
    rep['routes'] = qry("""
        SELECT rr.id as assign_id, r.id as route_id, r.name as route_name,
               z.name as zone_name, rr.assigned_from
        FROM rep_routes rr
        JOIN routes r ON r.id=rr.route_id
        JOIN zones z ON z.id=r.zone_id
        WHERE rr.rep_id=? AND rr.assigned_to IS NULL
    """, (rep_id,))
    rep['salary_components'] = qry(
        "SELECT * FROM rep_salary_components WHERE rep_id=? ORDER BY effective_from DESC LIMIT 1",
        (rep_id,))
    rep['commission_rules'] = qry(
        "SELECT * FROM rep_commission_rules WHERE rep_id=? AND active=1",
        (rep_id,))
    rep['targets'] = qry(
        "SELECT * FROM rep_targets WHERE rep_id=? ORDER BY month DESC LIMIT 6",
        (rep_id,))
    rep['advances'] = qry(
        "SELECT * FROM rep_advances WHERE rep_id=? ORDER BY advance_date DESC LIMIT 12",
        (rep_id,))
    return rep

def create_rep(data):
    name  = data.get('name','').strip()
    phone = data.get('phone','').strip()
    if not name:
        raise ValueError("Rep name is required")
    if not phone:
        raise ValueError("Phone number is required")
    # Check duplicate phone
    existing = qry1(
        "SELECT id FROM sales_reps WHERE phone=? AND (status IS NULL OR status='active')",
        (phone,))
    if existing:
        raise ValueError(f"Phone {phone} already registered to another rep")
    # Hash PIN if provided
    pin = data.get('pin','')
    pin_hash = hashlib.sha256(pin.encode()).hexdigest() if pin else ''
    c = _conn()
    try:
        # Auto-generate employee_id: SR-0001, SR-0002 ...
        last_id = c.execute(
            "SELECT COUNT(*) FROM sales_reps").fetchone()[0]
        emp_id = f"SR-{(last_id + 1):04d}"
        c.execute("""
            INSERT INTO sales_reps
                (employee_id, name, phone, pin_hash, email, notes,
                 joining_date, status, designation)
            VALUES (?,?,?,?,?,?,?,'active',?)
        """, (emp_id, name, phone, pin_hash,
              data.get('email',''),
              data.get('notes',''),
              data.get('joinDate', str(date.today())),
              data.get('designation','SR')))
        c.commit()
        rep_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        c.close()
    # Seed salary components if provided
    base_salary = float(data.get('baseSalary', 0) or 0)
    if base_salary:
        c2 = _conn()
        try:
            c2.execute("""
                INSERT INTO rep_salary_components
                    (rep_id, basic_salary, fuel_allowance, mobile_allowance,
                     other_allowance, effective_from, active)
                VALUES (?,?,?,?,?,?,1)
            """, (rep_id, base_salary,
                  float(data.get('fuelAllowance', 0) or 0),
                  float(data.get('mobileAllowance', 0) or 0),
                  float(data.get('otherAllowance', 0) or 0),
                  str(date.today())))
            c2.commit()
        finally:
            c2.close()
    # Seed commission rule if provided
    comm_pct = float(data.get('commissionPct', 0) or 0)
    if comm_pct:
        c3 = _conn()
        try:
            c3.execute("""
                INSERT INTO rep_commission_rules
                    (rep_id, base_commission_pct, accelerator_pct, target_bonus, effective_from, active)
                VALUES (?,?,?,?,?,1)
            """, (rep_id, comm_pct,
                  float(data.get('acceleratorPct', 0) or 0),
                  float(data.get('flatTargetBonus', 0) or 0),
                  str(date.today())))
            c3.commit()
        finally:
            c3.close()
    save_db()
    return get_rep(rep_id)

def update_rep(rep_id, data):
    rep = qry1("SELECT * FROM sales_reps WHERE id=?", (rep_id,))
    if not rep:
        raise ValueError(f"Rep not found: {rep_id}")
    mapping = {
        'name':'name','phone':'phone','email':'email',
        'joinDate':'joining_date','notes':'notes',
        'status':'status','designation':'designation',
        'whatsapp_apikey':'whatsapp_apikey',
        'zoneId':'primary_zone_id',
    }
    set_parts, vals = [], []
    for k, col in mapping.items():
        if k in data:
            set_parts.append(f"{col}=?")
            vals.append(data[k])
    # Handle PIN reset
    if 'pin' in data and data['pin']:
        set_parts.append("pin_hash=?")
        vals.append(hashlib.sha256(str(data['pin']).encode()).hexdigest())
    if set_parts:
        vals.append(rep_id)
        c = _conn()
        try:
            c.execute(f"UPDATE sales_reps SET {', '.join(set_parts)} WHERE id=?", vals)
            c.commit()
        finally:
            c.close()
    # Update salary component if baseSalary sent
    if 'baseSalary' in data:
        c = _conn()
        try:
            # Deactivate old
            c.execute("UPDATE rep_salary_components SET active=0 WHERE rep_id=?", (rep_id,))
            c.execute("""
                INSERT INTO rep_salary_components
                    (rep_id, basic_salary, fuel_allowance, mobile_allowance,
                     other_allowance, effective_from, active)
                VALUES (?,?,?,?,?,?,1)
            """, (rep_id, float(data['baseSalary']),
                  float(data.get('fuelAllowance', 0) or 0),
                  float(data.get('mobileAllowance', 0) or 0),
                  float(data.get('otherAllowance', 0) or 0),
                  str(date.today())))
            c.commit()
        finally:
            c.close()
    # Update commission rule if sent
    if 'commissionPct' in data:
        c = _conn()
        try:
            c.execute("UPDATE rep_commission_rules SET active=0 WHERE rep_id=?", (rep_id,))
            c.execute("""
                INSERT INTO rep_commission_rules
                    (rep_id, base_commission_pct, accelerator_pct, target_bonus, effective_from, active)
                VALUES (?,?,?,?,?,1)
            """, (rep_id, float(data['commissionPct']),
                  float(data.get('acceleratorPct', 0) or 0),
                  float(data.get('flatTargetBonus', 0) or 0),
                  str(date.today())))
            c.commit()
        finally:
            c.close()
    save_db()
    return get_rep(rep_id)

def assign_rep_route(rep_id, route_id):
    rep = qry1("SELECT id FROM sales_reps WHERE id=?", (rep_id,))
    if not rep:
        raise ValueError(f"Rep not found: {rep_id}")
    route = qry1("SELECT id FROM routes WHERE id=? AND active=1", (route_id,))
    if not route:
        raise ValueError(f"Route not found: {route_id}")
    # Check not already assigned (assigned_to IS NULL = currently active)
    existing = qry1(
        "SELECT id FROM rep_routes WHERE rep_id=? AND route_id=? AND assigned_to IS NULL",
        (rep_id, route_id))
    if existing:
        return {'ok': True, 'message': 'Already assigned'}
    c = _conn()
    try:
        c.execute("""
            INSERT INTO rep_routes (rep_id, route_id, assigned_from)
            VALUES (?,?,?)
        """, (rep_id, route_id, str(date.today())))
        c.commit()
    finally:
        c.close()
    save_db()
    return {'ok': True, 'repId': rep_id, 'routeId': route_id}

def unassign_rep_route(assign_id):
    c = _conn()
    try:
        c.execute("UPDATE rep_routes SET assigned_to=? WHERE id=?",
                  (str(date.today()), assign_id))
        c.commit()
    finally:
        c.close()
    save_db()
    return {'ok': True}

def assign_customer_route(customer_id, route_id, shop_name='', address=''):
    existing = qry1(
        "SELECT id FROM route_customers WHERE customer_id=? AND route_id=? AND active=1",
        (customer_id, route_id))
    if existing:
        return {'ok': True, 'message': 'Already assigned'}
    c = _conn()
    try:
        c.execute("""
            INSERT INTO route_customers (route_id, customer_id, shop_name, address)
            VALUES (?,?,?,?)
        """, (route_id, customer_id, shop_name, address))
        c.commit()
    finally:
        c.close()
    save_db()
    return {'ok': True}

def list_route_customers(route_id):
    return qry("""
        SELECT rc.*, c.name as customer_name, c.code as customer_code,
               c.phone as customer_phone
        FROM route_customers rc
        JOIN customers c ON c.id=rc.customer_id
        WHERE rc.route_id=?
        ORDER BY rc.stop_sequence, c.name
    """, (route_id,))


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — REP TARGETS
# ═══════════════════════════════════════════════════════════════════

def set_rep_target(rep_id, data):
    # month field in DB (YYYY-MM format)
    period = (data.get('period') or data.get('month','') or '').strip()
    if not period:
        raise ValueError("period (YYYY-MM) is required")
    revenue_target = float(data.get('targetAmount') or data.get('revenueTarget', 0))
    visit_target   = int(data.get('visitTarget', 0) or 0)
    # Upsert
    existing = qry1("SELECT id FROM rep_targets WHERE rep_id=? AND month=?", (rep_id, period))
    c = _conn()
    try:
        if existing:
            c.execute("UPDATE rep_targets SET revenue_target=?, visit_target=? WHERE id=?",
                      (revenue_target, visit_target, existing['id']))
        else:
            c.execute("""
                INSERT INTO rep_targets (rep_id, month, revenue_target, visit_target)
                VALUES (?,?,?,?)
            """, (rep_id, period, revenue_target, visit_target))
        c.commit()
    finally:
        c.close()
    save_db()
    return qry1("SELECT * FROM rep_targets WHERE rep_id=? AND month=?", (rep_id, period))


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — REP ADVANCES
# ═══════════════════════════════════════════════════════════════════

def record_advance(rep_id, data):
    amount = float(data.get('amount', 0) or 0)
    if amount <= 0:
        raise ValueError("Amount must be positive")
    monthly_recovery = float(data.get('monthlyRecovery', 0) or 0)
    c = _conn()
    try:
        c.execute("""
            INSERT INTO rep_advances
                (rep_id, advance_date, amount, monthly_recovery, outstanding, notes)
            VALUES (?,?,?,?,?,?)
        """, (rep_id, data.get('advanceDate', str(date.today())),
              amount, monthly_recovery, amount, data.get('notes','')))
        c.commit()
        adv_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        c.close()
    save_db()
    return qry1("SELECT * FROM rep_advances WHERE id=?", (adv_id,))


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — BEAT VISITS & FIELD ORDERS
# ═══════════════════════════════════════════════════════════════════

def record_beat_visit(data):
    rep_id     = data.get('repId')
    route_id   = data.get('routeId')
    cust_id    = data.get('customerId')
    visit_date = data.get('visitDate', str(date.today()))
    outcome    = data.get('outcome','visited')   # visited | no_order | closed
    notes      = data.get('notes','')
    if not rep_id or not route_id or not cust_id:
        raise ValueError("repId, routeId, customerId are required")
    c = _conn()
    try:
        c.execute("""
            INSERT INTO beat_visits (rep_id, route_id, customer_id, visit_date, outcome, notes)
            VALUES (?,?,?,?,?,?)
        """, (int(rep_id), int(route_id), int(cust_id), visit_date, outcome, notes))
        c.commit()
        visit_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
    finally:
        c.close()
    save_db()
    return qry1("SELECT * FROM beat_visits WHERE id=?", (visit_id,))

def create_field_order(data):
    rep_id     = data.get('repId')
    route_id   = data.get('routeId')
    cust_id    = data.get('customerId')
    items      = data.get('items', [])
    if not rep_id or not cust_id or not items:
        raise ValueError("repId, customerId, items[] are required")
    order_date    = data.get('orderDate', str(date.today()))
    notes         = data.get('notes','')
    cash_collected = float(data.get('cashCollected', 0))
    # Generate order_ref
    _sync_counter_to_max('field_order', 'field_orders', 'order_ref', 'SP-FO-')
    order_ref = next_id('field_order', 'FO')
    c = _conn()
    try:
        c.execute("""
            INSERT INTO field_orders
                (order_ref, rep_id, route_id, customer_id, order_date, status, notes, cash_collected)
            VALUES (?,?,?,?,?,'pending',?,?)
        """, (order_ref, int(rep_id), int(route_id) if route_id else None,
              int(cust_id), order_date, notes, cash_collected))
        c.commit()
        order_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        for item in items:
            # Accept both naming conventions from frontend
            variant_id = item.get('productVariantId') or item.get('variantId')
            qty        = float(item.get('quantity', item.get('qty', 0)))
            unit_price = float(item.get('unitPrice', 0))
            if qty <= 0 or not variant_id:
                continue
            c.execute("""
                INSERT INTO field_order_items (order_id, product_variant_id, quantity, unit_price)
                VALUES (?,?,?,?)
            """, (order_id, int(variant_id), qty, unit_price))
        c.commit()
    finally:
        c.close()
    save_db()
    return get_field_order(order_id)

def get_field_order(order_id):
    order = qry1("""
        SELECT fo.*, sr.name as rep_name, c.name as customer_name,
               r.name as route_name
        FROM field_orders fo
        JOIN sales_reps sr ON sr.id=fo.rep_id
        JOIN customers c ON c.id=fo.customer_id
        LEFT JOIN routes r ON r.id=fo.route_id
        WHERE fo.id=?
    """, (order_id,))
    if not order:
        return None
    order['items'] = qry("""
        SELECT foi.*, pv.sku_code, p.name as product_name, ps.label as pack_size
        FROM field_order_items foi
        JOIN product_variants pv ON pv.id=foi.product_variant_id
        JOIN products p ON p.id=pv.product_id
        JOIN pack_sizes ps ON ps.id=pv.pack_size_id
        WHERE foi.order_id=?
    """, (order_id,))
    return order

def list_field_orders(rep_id=None, status=None, date_from=None, date_to=None):
    """
    Returns all orders created through the sales rep portal (order.html) PLUS
    legacy field_orders records. Portal orders come from customer_orders where
    order_source='rep_assisted'. Status 'draft' is normalised to 'pending' so
    the BMS Confirm button appears.
    """
    # ── Portal orders (order.html PWA) ──────────────────────────────────────
    portal_wheres = ["co.order_source='rep_assisted'"]
    portal_params = []
    if rep_id:
        portal_wheres.append("co.created_by_rep_id=?")
        portal_params.append(int(rep_id))
    # status filter: 'pending' maps to draft in customer_orders
    if status == 'pending':
        portal_wheres.append("co.status IN ('draft','pending_review')")
    elif status == 'confirmed':
        portal_wheres.append("co.status IN ('confirmed','invoiced','partially_invoiced')")
    elif status == 'cancelled':
        portal_wheres.append("co.status='cancelled'")
    if date_from:
        portal_wheres.append("co.order_date>=?"); portal_params.append(date_from)
    if date_to:
        portal_wheres.append("co.order_date<=?"); portal_params.append(date_to)

    portal_sql = """
        SELECT
            co.id,
            co.order_number                              AS order_ref,
            co.order_date,
            CASE
                WHEN co.status IN ('draft','pending_review') THEN 'pending'
                WHEN co.status IN ('confirmed','invoiced','partially_invoiced') THEN 'confirmed'
                ELSE co.status
            END                                          AS status,
            sr.name                                      AS rep_name,
            c.name                                       AS customer_name,
            NULL                                         AS route_name,
            COALESCE((
                SELECT SUM(coi.qty_ordered * coi.unit_price)
                FROM customer_order_items coi
                WHERE coi.order_id = co.id
            ), 0)                                        AS order_total,
            (
                SELECT inv.id FROM invoices inv
                WHERE inv.customer_order_id = co.id
                ORDER BY inv.id DESC LIMIT 1
            )                                            AS invoice_id,
            co.notes,
            'portal'                                     AS _source
        FROM customer_orders co
        LEFT JOIN sales_reps sr ON sr.id = co.created_by_rep_id
        LEFT JOIN customers  c  ON c.id  = co.customer_id
        WHERE {portal_where}
    """.format(portal_where=' AND '.join(portal_wheres))

    # ── Legacy field_orders ──────────────────────────────────────────────────
    legacy_wheres = []
    legacy_params = []
    if rep_id:
        legacy_wheres.append("fo.rep_id=?"); legacy_params.append(int(rep_id))
    if status:
        legacy_wheres.append("fo.status=?"); legacy_params.append(status)
    if date_from:
        legacy_wheres.append("fo.order_date>=?"); legacy_params.append(date_from)
    if date_to:
        legacy_wheres.append("fo.order_date<=?"); legacy_params.append(date_to)

    legacy_where_str = ("WHERE " + " AND ".join(legacy_wheres)) if legacy_wheres else ""

    legacy_sql = """
        SELECT
            fo.id,
            fo.order_ref,
            fo.order_date,
            fo.status,
            sr.name                                      AS rep_name,
            c.name                                       AS customer_name,
            r.name                                       AS route_name,
            COALESCE((
                SELECT SUM(quantity * unit_price)
                FROM field_order_items
                WHERE order_id = fo.id
            ), 0)                                        AS order_total,
            COALESCE(fo.confirmed_invoice_id, fo.invoice_id) AS invoice_id,
            fo.notes,
            'legacy'                                     AS _source
        FROM field_orders fo
        JOIN sales_reps sr ON sr.id = fo.rep_id
        JOIN customers  c  ON c.id  = fo.customer_id
        LEFT JOIN routes r ON r.id  = fo.route_id
        {legacy_where}
    """.format(legacy_where=legacy_where_str)

    # ── UNION + sort ─────────────────────────────────────────────────────────
    union_sql = f"""
        SELECT * FROM (
            {portal_sql}
            UNION ALL
            {legacy_sql}
        )
        ORDER BY order_date DESC, id DESC
        LIMIT 200
    """
    all_params = portal_params + legacy_params
    return qry(union_sql, all_params)

def create_invoice(inv_data):
    """
    Create an invoice directly from structured data.
    inv_data: {custCode, invoiceDate, notes, items: [{skuCode, qty, unitPrice}]}
    Returns: {id, invoiceNumber, total}
    """
    cust = qry1("SELECT * FROM customers WHERE code=?", (inv_data.get('custCode',''),))
    if not cust:
        raise ValueError(f"Customer not found: {inv_data.get('custCode')}")
    items = inv_data.get('items', [])
    if not items:
        raise ValueError("Invoice must have at least one item")
    _sync_counter_to_max('invoice', 'invoices', 'invoice_number', 'SP-INV-')
    inv_num  = next_id('invoice', 'INV')
    inv_date = inv_data.get('invoiceDate', str(date.today()))
    terms    = int(cust.get('payment_terms_days', 30))
    try:
        due_date = (date.fromisoformat(inv_date) + timedelta(days=terms)).isoformat()
    except Exception:
        due_date = inv_date
    c = _conn()
    try:
        c.execute("""
            INSERT INTO invoices (invoice_number, customer_id, invoice_date, due_date, status, notes)
            VALUES (?,?,?,?,'UNPAID',?)
        """, (inv_num, cust['id'], inv_date, due_date, inv_data.get('notes','')))
        c.commit()
        inv_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        subtotal = 0.0
        for item in items:
            # Support skuCode OR (productCode + packSize)
            sku = item.get('skuCode') or item.get('productCode')
            var = qry1("SELECT * FROM product_variants WHERE sku_code=?", (sku,))
            if not var:
                # Try looking up by product_code + pack approach via sku directly
                var = qry1("SELECT pv.* FROM product_variants pv JOIN products p ON p.id=pv.product_id WHERE pv.sku_code=? OR p.code=?", (sku, sku))
            qty  = float(item.get('qty', item.get('quantity', 0)))
            uprice = float(item.get('unitPrice', 0))
            if qty <= 0:
                continue
            line = round(qty * uprice, 2)
            subtotal += line
            prod_code = ''
            prod_name = ''
            pack_size = ''
            if var:
                pv_row = qry1("""
                    SELECT p.code as prod_code, p.name as prod_name, ps.label as pack_size
                    FROM product_variants pv
                    JOIN products p ON p.id=pv.product_id
                    JOIN pack_sizes ps ON ps.id=pv.pack_size_id
                    WHERE pv.id=?
                """, (var['id'],))
                if pv_row:
                    prod_code = pv_row['prod_code']
                    prod_name = pv_row['prod_name']
                    pack_size = pv_row['pack_size']
            c.execute("""
                INSERT INTO invoice_items
                    (invoice_id, product_variant_id, product_code, product_name, pack_size, quantity, unit_price, line_total)
                VALUES (?,?,?,?,?,?,?,?)
            """, (inv_id, var['id'] if var else None, prod_code, prod_name, pack_size, qty, uprice, line))
        # Apply 18% GST
        gst   = round(subtotal * 0.18, 2)
        total = round(subtotal + gst, 2)
        c.commit()
    finally:
        c.close()
    save_db()
    return {'id': inv_id, 'invoiceNumber': inv_num, 'subtotal': subtotal, 'gst': gst, 'total': total}


def confirm_field_order(order_id, data=None):
    """Confirm field order → create invoice automatically."""
    order = get_field_order(order_id)
    if not order:
        raise ValueError(f"Field order not found: {order_id}")
    if order['status'] == 'confirmed':
        raise ValueError("Order already confirmed")
    if not order['items']:
        raise ValueError("Cannot confirm an order with no items")
    # Build invoice data
    cust_row = qry1("SELECT * FROM customers WHERE id=?", (order['customer_id'],))
    if not cust_row:
        raise ValueError("Customer not found")
    # Create invoice
    inv_data = {
        'custCode':    cust_row['code'],
        'invoiceDate': order['order_date'],
        'notes':       f"Field order #{order_id}",
        'items': [
            {
                'skuCode':   item['sku_code'],
                'qty':       item['quantity'],
                'unitPrice': item['unit_price'],
            }
            for item in order['items']
        ]
    }
    inv_result = create_invoice(inv_data)
    # Mark field order confirmed + link invoice
    c = _conn()
    try:
        c.execute("""
            UPDATE field_orders SET status='confirmed', confirmed_invoice_id=?
            WHERE id=?
        """, (inv_result.get('id'), order_id))
        # Handle cash collected
        cash = float((data or {}).get('cashCollected', 0))
        if cash > 0:
            c.execute("UPDATE field_orders SET cash_collected=? WHERE id=?", (cash, order_id))
        c.commit()
    finally:
        c.close()
    save_db()
    result = get_field_order(order_id)
    # Inject camelCase invoice fields for frontend convenience
    result['invoiceId']     = inv_result.get('id')
    result['invoiceNumber'] = inv_result.get('invoiceNumber')
    return result


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — PAYROLL ENGINE
# ═══════════════════════════════════════════════════════════════════

def calculate_payroll(rep_id, period):
    """
    Calculate payroll for a rep for a given period (YYYY-MM).
    Returns breakdown dict — does NOT save until run_payroll() called.
    """
    rep = qry1("SELECT * FROM sales_reps WHERE id=?", (rep_id,))
    if not rep:
        raise ValueError(f"Rep not found: {rep_id}")

    # Fixed salary — use basic_salary column
    salary_comp = qry1("""
        SELECT * FROM rep_salary_components
        WHERE rep_id=? AND active=1 AND effective_from<=?
        ORDER BY effective_from DESC LIMIT 1
    """, (rep_id, period + '-31'))
    base_salary      = float(salary_comp['basic_salary']      or 0) if salary_comp else 0.0
    fuel_allowance   = float(salary_comp['fuel_allowance']    or 0) if salary_comp else 0.0
    mobile_allowance = float(salary_comp['mobile_allowance']  or 0) if salary_comp else 0.0
    other_allowance  = float(salary_comp['other_allowance']   or 0) if salary_comp else 0.0
    total_fixed = base_salary + fuel_allowance + mobile_allowance + other_allowance

    # Commission rule
    comm_rule = qry1("""
        SELECT * FROM rep_commission_rules
        WHERE rep_id=? AND active=1
        ORDER BY effective_from DESC LIMIT 1
    """, (rep_id,))
    base_comm_pct    = float(comm_rule['base_commission_pct'] or 0) if comm_rule else 0.0
    accelerator_pct  = float(comm_rule.get('accelerator_pct') or 0) if comm_rule else 0.0
    flat_bonus       = float(comm_rule.get('target_bonus')    or 0) if comm_rule else 0.0

    # Target for this period — uses 'month' column in rep_targets
    target_row = qry1("SELECT * FROM rep_targets WHERE rep_id=? AND month=?", (rep_id, period))
    target_amount = float(target_row['revenue_target'] or 0) if target_row else 0.0

    # Actual sales — sum confirmed field orders in the period
    sales_row = qry1("""
        SELECT COALESCE(SUM(foi.quantity * foi.unit_price), 0) as total_sales
        FROM field_orders fo
        JOIN field_order_items foi ON foi.order_id=fo.id
        WHERE fo.rep_id=? AND fo.order_date LIKE ? AND fo.status='confirmed'
    """, (rep_id, period + '%'))
    actual_sales = float(sales_row['total_sales'] or 0) if sales_row else 0.0

    # Commission calc
    base_commission   = r2(actual_sales * base_comm_pct / 100.0)
    accelerator_bonus = 0.0
    target_bonus      = 0.0
    if target_amount > 0 and actual_sales >= target_amount:
        above_target      = actual_sales - target_amount
        accelerator_bonus = r2(above_target * accelerator_pct / 100.0)
        target_bonus      = flat_bonus
    total_commission = r2(base_commission + accelerator_bonus + target_bonus)

    # Advances for the period (unrecovered)
    advances_row = qry1("""
        SELECT COALESCE(SUM(amount),0) as total_advances
        FROM rep_advances
        WHERE rep_id=? AND advance_date LIKE ? AND recovered=0
    """, (rep_id, period + '%'))
    total_advances = float(advances_row['total_advances'] or 0) if advances_row else 0.0

    gross = r2(total_fixed + total_commission)
    net   = r2(gross - total_advances)

    return {
        'repId':             rep_id,
        'repName':           rep['name'],
        'period':            period,
        'baseSalary':        base_salary,
        'fuelAllowance':     fuel_allowance,
        'mobileAllowance':   mobile_allowance,
        'otherAllowance':    other_allowance,
        'totalFixed':        total_fixed,
        'actualSales':       actual_sales,
        'targetAmount':      target_amount,
        'baseCommission':    base_commission,
        'acceleratorBonus':  accelerator_bonus,
        'targetBonus':       target_bonus,
        'totalCommission':   total_commission,
        'totalAdvances':   total_advances,
        'grossPay':        gross,
        'netPay':          net,
    }

def run_payroll(period, rep_ids=None):
    """
    Calculate and save payroll run for all reps (or specified rep_ids).
    """
    all_reps = qry(
        "SELECT id FROM sales_reps WHERE (status IS NULL OR status='active')")
    if rep_ids:
        all_reps = [r for r in all_reps if r['id'] in rep_ids]

    results = []
    c = _conn()
    try:
        for rep_row in all_reps:
            calc = calculate_payroll(rep_row['id'], period)
            # Check not already finalized
            existing = c.execute(
                "SELECT id FROM payroll_runs WHERE rep_id=? AND period=? AND status='final'",
                (rep_row['id'], period)
            ).fetchone()
            if existing:
                calc['status'] = 'already_finalized'
                results.append(calc)
                continue
            # Upsert draft — using new columns added to payroll_runs
            prev = c.execute(
                "SELECT id FROM payroll_runs WHERE rep_id=? AND period=?",
                (rep_row['id'], period)
            ).fetchone()
            if prev:
                c.execute("""
                    UPDATE payroll_runs SET
                        base_salary=?, actual_sales=?, target_amount=?,
                        base_commission=?, accelerator_bonus=?, target_bonus=?,
                        total_commission=?, total_advances=?, gross_pay=?, net_pay=?,
                        status='draft', run_at=datetime('now')
                    WHERE id=?
                """, (calc['baseSalary'], calc['actualSales'], calc['targetAmount'],
                      calc['baseCommission'], calc['acceleratorBonus'], calc['targetBonus'],
                      calc['totalCommission'], calc['totalAdvances'],
                      calc['grossPay'], calc['netPay'], prev[0]))
            else:
                c.execute("""
                    INSERT INTO payroll_runs
                        (rep_id, month, period, base_salary, actual_sales, target_amount,
                         base_commission, accelerator_bonus, target_bonus,
                         total_commission, total_advances, gross_pay, net_pay, status,
                         run_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'draft',datetime('now'))
                """, (rep_row['id'], period, period,
                      calc['baseSalary'], calc['actualSales'], calc['targetAmount'],
                      calc['baseCommission'], calc['acceleratorBonus'], calc['targetBonus'],
                      calc['totalCommission'], calc['totalAdvances'],
                      calc['grossPay'], calc['netPay']))
            calc['status'] = 'draft'
            results.append(calc)
        c.commit()
    finally:
        c.close()
    save_db()
    return results

def finalize_payroll(rep_id, period):
    run = qry1("SELECT * FROM payroll_runs WHERE rep_id=? AND period=?", (rep_id, period))
    if not run:
        raise ValueError("No payroll run found for this rep/period")
    if run['status'] == 'final':
        raise ValueError("Already finalized")
    # Mark advances as recovered
    c = _conn()
    try:
        c.execute("""
            UPDATE rep_advances SET recovered=1
            WHERE rep_id=? AND advance_date LIKE ? AND recovered=0
        """, (rep_id, period + '%'))
        c.execute("UPDATE payroll_runs SET status='final' WHERE rep_id=? AND period=?",
                  (rep_id, period))
        c.commit()
    finally:
        c.close()
    save_db()
    return qry1("SELECT * FROM payroll_runs WHERE rep_id=? AND period=?", (rep_id, period))

def list_payroll_runs(period=None):
    if period:
        rows = qry("""
            SELECT pr.*, sr.name as rep_name
            FROM payroll_runs pr JOIN sales_reps sr ON sr.id=pr.rep_id
            WHERE pr.period=? ORDER BY sr.name
        """, (period,))
    else:
        rows = qry("""
            SELECT pr.*, sr.name as rep_name
            FROM payroll_runs pr JOIN sales_reps sr ON sr.id=pr.rep_id
            ORDER BY pr.period DESC, sr.name LIMIT 200
        """)
    return rows


# ═══════════════════════════════════════════════════════════════════
#  FIELD APP AUTH
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

# ═══════════════════════════════════════════════════════════════════
#  B2B PORTAL — FIELD REP BUSINESS LOGIC
# ═══════════════════════════════════════════════════════════════════

def _get_field_session(handler, qs=None):
    """Get session and verify it is a field rep. Returns session dict or None."""
    sess = get_session(handler, qs)
    if not sess or sess.get('role') != 'field_rep':
        return None
    return sess


def _is_out_of_route(rep_id, customer_id):
    """Return True if customer's zone does not match the rep's assigned zone."""
    rep = qry1("SELECT primary_zone_id FROM sales_reps WHERE id=?", (rep_id,))
    if not rep or not rep.get('primary_zone_id'):
        return False  # rep has no zone assigned — don't flag as out-of-zone
    customer = qry1("SELECT zone_id FROM customers WHERE id=?", (customer_id,))
    if not customer or not customer.get('zone_id'):
        return False  # customer has no zone — don't flag
    return int(rep['primary_zone_id']) != int(customer['zone_id'])


def _wa_notify_out_of_route(order_id, rep_id):
    """WhatsApp admin + rep's manager when a rep places an order outside their assigned route."""
    order    = qry1("SELECT * FROM customer_orders WHERE id=?", (order_id,))
    rep      = qry1("SELECT * FROM sales_reps WHERE id=?", (rep_id,))
    customer = qry1("SELECT * FROM customers WHERE id=?", (order['customer_id'],))
    if not order or not rep or not customer:
        return
    msg = (
        f"⚠️ OUT-OF-ROUTE ORDER ALERT\n"
        f"Rep: {rep['name']} ({rep['phone']})\n"
        f"Customer: {customer['name']} ({customer.get('account_number','')})\n"
        f"Order: {order['order_number']}\n"
        f"City: {customer.get('city','')}\n"
        f"This customer is NOT on the rep's assigned routes."
    )
    _wa_admin(msg)
    if rep.get('reporting_to'):
        manager = qry1("SELECT * FROM sales_reps WHERE id=?", (rep['reporting_to'],))
        if manager and manager.get('whatsapp_apikey') and manager.get('phone'):
            _wa_send(manager['phone'], manager['whatsapp_apikey'], msg)


def field_lookup_customers(query, rep_id):
    """Search customers by account number, name, or phone.
    Returns match list with onRoute flag for each customer."""
    q  = f'%{query}%'
    customers = qry("""
        SELECT id, code, account_number, name, customer_type, city, phone
        FROM customers
        WHERE active=1 AND (account_number LIKE ? OR name LIKE ? OR phone LIKE ?)
        ORDER BY name LIMIT 20
    """, (q, q, q))

    # Determine which customers are on this rep's routes
    rep_routes = qry("""
        SELECT route_id FROM rep_routes
        WHERE rep_id=? AND (assigned_to IS NULL OR assigned_to >= date('now'))
    """, (rep_id,))
    on_route_ids = set()
    if rep_routes:
        rids = [r['route_id'] for r in rep_routes]
        placeholders = ','.join('?' * len(rids))
        on_route = qry(
            f"SELECT customer_id FROM route_customers WHERE route_id IN ({placeholders})",
            rids
        )
        on_route_ids = {r['customer_id'] for r in on_route}

    return [{
        'id':            c['id'],
        'code':          c['code'],
        'accountNumber': c['account_number'],
        'name':          c['name'],
        'customerType':  c['customer_type'],
        'city':          c['city'],
        'phone':         c['phone'],
        'onRoute':       c['id'] in on_route_ids,
    } for c in customers]


def field_create_customer(data, rep_id):
    """Create a customer from the B2B field portal. Uses field rep session.
    Identical to create_customer() but accessible via field token."""
    return create_customer(data)


def field_get_products(customer_type='RETAIL'):
    """Return active products + variants as a FLAT list — one entry per variant.
    Maps: RETAIL→retail_mrp, DIRECT→distributor, WHOLESALE→distributor.
    Never exposes mfg_cost or ex_factory prices.
    Frontend (order.html) expects: variant_id, product_name, sku_code, pack_size, grams, price."""
    type_map      = {'RETAIL': 'retail_mrp', 'DIRECT': 'distributor', 'WHOLESALE': 'distributor'}
    price_type_cd = type_map.get((customer_type or 'RETAIL').upper(), 'retail_mrp')

    products = qry("SELECT * FROM products WHERE active=1 ORDER BY name")
    result   = []
    for prod in products:
        variants = qry("""
            SELECT pv.id, pv.sku_code, ps.label AS pack_size, ps.grams,
                   pp.price
            FROM product_variants pv
            JOIN pack_sizes ps ON ps.id = pv.pack_size_id
            LEFT JOIN product_prices pp
                   ON pp.product_variant_id = pv.id
                  AND pp.active_flag = 1
                  AND pp.price_type_id = (SELECT id FROM price_types WHERE code=?)
            WHERE pv.product_id=? AND pv.active_flag=1
            ORDER BY ps.grams
        """, (price_type_cd, prod['id']))
        for v in variants:
            result.append({
                'variant_id':   v['id'],
                'product_code': prod['code'],
                'product_name': prod['name'],
                'sku_code':     v['sku_code'],
                'pack_size':    v['pack_size'],
                'grams':        v['grams'],
                'price':        v['price'],
            })
    return result


def send_field_login_response(handler, result):
    """Send field login response.
    Sets httpOnly cookie (field.html) AND includes token in body (order.html PWA / localStorage)."""
    token = result.pop('token')
    result.pop('setcookie', None)
    result['token'] = token   # PWA stores this in localStorage for Bearer auth
    body = json.dumps(result, default=str).encode()
    handler.send_response(200)
    handler.send_header('Content-Type', 'application/json')
    handler.send_header('Content-Length', len(body))
    handler.send_header('Set-Cookie',
        f'field_token={token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=86400')
    _add_security_headers(handler)
    handler.end_headers()
    handler.wfile.write(body)

def get_rep_today_route(rep_id):
    """Get today's route info and stop list for the field app."""
    today_day = date.today().strftime('%A')[:3].lower()  # mon, tue, etc.
    routes = qry("""
        SELECT r.*, z.name as zone_name
        FROM rep_routes rr
        JOIN routes r ON r.id=rr.route_id
        JOIN zones z ON z.id=r.zone_id
        WHERE rr.rep_id=? AND rr.assigned_to IS NULL
          AND (r.visit_days LIKE ? OR r.visit_days LIKE '%all%')
    """, (rep_id, f'%{today_day}%'))
    result = []
    for route in routes:
        stops = list_route_customers(route['id'])
        result.append({**route, 'stops': stops})
    return result


# ═══════════════════════════════════════════════════════════════════
#  SPRINT 1: FOUNDATION MODULES
#  These imports override the equivalent local definitions above.
#  Python resolves global names at call-time, so all server.py functions
#  automatically use the module versions once this block runs.
# ═══════════════════════════════════════════════════════════════════
from modules.utils  import *   # r2, fmtpkr, today, VALID_ROLES, ROLE_LABELS, require, ValidationError, validate_fields, CITY_CODE_MAP, _city_to_code, _logger, _log
from modules.db     import *   # _conn, qry, qry1, run, run_many, save_db, audit_log
from modules.id_gen import *   # next_id, _sync_counter_to_max, next_ingredient_code, generate_account_number, ...
from modules.auth   import *   # _hash_pw, _hash_pw_new, _verify_pw, login_user, logout_user, get_session, _session_cleanup, field_login, _get_field_session, ensure_sessions_table, ensure_rate_limit_table, _check_rate_limit, _record_failed_attempt, _clear_rate_limit, _get_client_ip, _ARGON2_AVAILABLE, _argon2
from modules.users      import *   # ensure_users_table, list_users, create_user, update_user, _reset_admin_pw_if_requested
from modules.customers  import *   # create_customer, update_customer, import_customers_master, ensure_clean_customer_codes, assign_customer_route, list_route_customers, field_lookup_customers, field_create_customer
from modules.suppliers  import *   # create_supplier, update_supplier, import_suppliers_master, _ensure_supplier_zone_col, ensure_clean_supplier_codes, _suppliers_with_zones
from modules.products   import *   # create_product, update_product, deactivate_product, deactivate_variant, import_products_master, ensure_variant_wastage_pct, ensure_variant_gtin
from modules.inventory   import *   # get_stock_map, get_wo_reserved_stock_map, get_finished_stock_map, get_soft_hold_qty, get_hard_reserved_qty, get_available_for_soft_hold, get_stock_situation, create_adjustment, create_ingredient, update_ingredient, bulk_update_ingredient_costs, deactivate_ingredient, reactivate_ingredient, import_ingredients_master
from modules.migrations  import *   # ensure_full_schema, ensure_system_settings_schema, _migrate_invoice_items_line_total, ensure_work_orders_table, ensure_customer_orders_schema, ensure_review_queue_schema, _migrate_supplier_bills_void, _migrate_change_log_void_action, _migrate_customer_type_wholesale, _ensure_b2b_order_columns, ensure_supplier_bills_schema, ensure_purchase_orders_schema, ensure_batch_cost_column, ensure_master_schema, ensure_costing_config, ensure_price_types_sprint6, ensure_price_history_extended, ensure_margin_alerts_table
from modules.orders      import *   # _enforce_credit_limit, _wa_send, _wa_admin, _wa_rep, _wa_notify_order_approved, _wa_notify_order_rejected, _wa_notify_order_received, _wa_notify_hold_expiring, _wa_notify_out_of_route, place_soft_hold, release_soft_hold, convert_soft_hold_to_hard_reservation, check_and_expire_holds, create_customer_order_external, get_review_queue, approve_order_with_edit, update_order_item_qty, reject_order, reopen_rejected_order, _order_status, _order_detail, list_customer_orders, _check_order_stock_warnings, create_customer_order, update_customer_order, add_customer_order_item, confirm_customer_order, cancel_customer_order, create_wo_from_order_item, generate_invoice_from_order
from modules.invoices    import *   # compute_invoice_balance, _compute_invoice_status, _sync_invoice_status, get_ar_aging, create_invoice, add_invoice_item, remove_invoice_item, record_customer_payment, allocate_customer_payment, pay_invoice_direct, deallocate_payment, adjust_invoice, void_invoice, generate_invoice_pdf, generate_statement_pdf, _pdf_colors, _pkr
from modules.purchasing  import *   # compute_bill_balance, _compute_bill_status, _sync_bill_status, get_ap_aging, create_supplier_bill, update_supplier_bill, record_supplier_payment, allocate_supplier_payment, pay_bill_direct, deallocate_supplier_payment, adjust_bill, void_supplier_bill, list_purchase_orders, get_purchase_order, create_purchase_order, update_purchase_order, update_purchase_order_status, bom_calculate_ingredients, generate_po_pdf
from modules.production  import *   # check_wo_feasibility, get_procurement_list, list_work_orders, create_work_order, convert_wo_to_batch, update_work_order, update_work_order_status, create_production_batch, create_or_update_bom, import_bom_master
from modules.costing     import *   # get_costing_config, compute_standard_cost, get_all_standard_costs, get_batch_variances, update_costing_config, set_product_price, import_prices_master, get_price_history, get_ingredient_price_history, seed_price_history, get_margin_alerts, dismiss_margin_alert, send_margin_alert_email
from modules.reports     import *   # get_dashboard, get_pl_report, get_rep_performance_report, get_margin_report
from modules.field       import *   # list_zones, create_zone, update_zone, list_routes, create_route, update_route, list_reps, get_rep, create_rep, update_rep, assign_rep_route, unassign_rep_route, set_rep_target, record_advance, record_beat_visit, create_field_order, get_field_order, list_field_orders, confirm_field_order, calculate_payroll, run_payroll, finalize_payroll, list_payroll_runs, get_rep_today_route, field_get_products, _is_out_of_route, _wa_notify_out_of_route, create_sale, create_multi_sale


# ═══════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════

if __name__ == '__main__':

    # ── Restore mode ──────────────────────────────────────────────
    if '--restore' in sys.argv:
        restore_db_interactive()
        sys.exit(0)

    print()
    print('╔══════════════════════════════════════════╗')
    print('║   SPICETOPIA BMS — STARTING               ║')
    print(f'║     OS: {OS:<33}║')
    print('╚══════════════════════════════════════════╝')
    print()

    # ── Step 0: Apply config + env overrides, set up logging ─────
    _apply_startup_config()
    _setup_logging()

    # ── Step 1: Resolve database location ────────────────────────
    resolve_db_path()

    # ── Step 2: Bootstrap DB to /tmp ─────────────────────────────
    bootstrap_db()
    ensure_full_schema()   # creates ALL tables on fresh DB (idempotent)
    _migrate_invoice_items_line_total()  # rename old 'total' col → 'line_total' if needed
    ensure_users_table()
    ensure_sessions_table()
    ensure_rate_limit_table()
    ensure_work_orders_table()
    ensure_customer_orders_schema()
    ensure_supplier_bills_schema()
    ensure_purchase_orders_schema()
    ensure_batch_cost_column()
    ensure_costing_config()              # costing_config table + seeds (overhead 10%, labour 5)
    ensure_variant_wastage_pct()         # wastage_pct column on product_variants
    ensure_variant_gtin()                # gtin column on product_variants + seed known GTINs
    ensure_clean_customer_codes()        # fix SP-SP-CUST-* double-prefix → SP-CUST-*
    ensure_clean_supplier_codes()        # normalize SUP-001/SP-SUP-0001 → SP-SUP-XXXX
    _reset_admin_pw_if_requested()       # one-shot reset via RESET_ADMIN_PW env var
    _migrate_supplier_bills_void()       # adds VOID status + voided columns to supplier_bills
    _migrate_change_log_void_action()    # widens change_log CHECK to include 'VOID'
    _migrate_customer_type_wholesale()   # adds WHOLESALE to customer_type CHECK
    _ensure_b2b_order_columns()          # adds out_of_route + idempotency_key to customer_orders
    ensure_system_settings_schema()  # must be early — _reload_wa_from_db reads it
    _reload_wa_from_db()             # overlay DB-saved WA config on top of config.json
    ensure_review_queue_schema()
    ensure_master_schema()  # must run before load_ref() — adds active, credit_limit cols
    ensure_price_types_sprint6()         # update price_type labels + add bulk
    ensure_price_history_extended()      # adds change_type, config_key, changed_by, note to price_history
    ensure_margin_alerts_table()         # margin_alerts table for floor breach tracking
    ensure_field_otp_table()             # field_otp table for WhatsApp OTP login
    backfill_customer_account_numbers()   # assigns account_number to existing customers, deletes test rows
    load_ref()
    import modules.customers  as _cust_mod; _cust_mod._refresh_ref = load_ref   # wire ref refresh
    import modules.suppliers  as _sup_mod;  _sup_mod._refresh_ref = load_ref   # wire ref refresh
    import modules.products   as _prod_mod; _prod_mod._refresh_ref = load_ref  # wire ref refresh
    import modules.inventory  as _inv_mod;  _inv_mod._refresh_ref = load_ref   # wire ref refresh
    import modules.orders     as _ord_mod2                                      # wire orders callbacks
    _ord_mod2._refresh_ref               = load_ref
    _ord_mod2._is_out_of_route_fn        = _is_out_of_route
    _ord_mod2._wa_notify_out_of_route_fn = _wa_notify_out_of_route
    _ord_mod2._check_wo_feasibility_fn   = check_wo_feasibility
    import modules.invoices   as _inv_mod3                                      # wire invoices callbacks
    _inv_mod3._order_status_fn           = _order_status
    generate_master_templates()
    sync_master_files()
    seed_price_history()
    seed_zones_routes()              # seeds KHI (6 zones) + HYD (5 zones) + all area routes

    # ── Step 2d: Auto-seed staging environment ────────────────────
    if os.environ.get('AUTO_SEED', '').lower() in ('1', 'true', 'yes'):
        try:
            inv_count = _conn().execute("SELECT COUNT(*) FROM invoices").fetchone()[0]
            if inv_count == 0:
                print("  🌱 AUTO_SEED detected — running staging seed script...")
                seed_script = BASE_DIR / 'seed_staging.py'
                if seed_script.exists():
                    result = subprocess.run(
                        [sys.executable, str(seed_script), str(DB_TMP)],
                        capture_output=True, text=True, timeout=120
                    )
                    for line in result.stdout.splitlines():
                        print(f"     {line}")
                    if result.returncode != 0:
                        print(f"  ⚠️  Seed error: {result.stderr[:500]}")
                    else:
                        print("  ✓ Staging seed complete.")
                else:
                    print("  ⚠️  AUTO_SEED set but seed_staging.py not found — skipping.")
            else:
                print(f"  ℹ️  AUTO_SEED skipped — DB already has {inv_count} invoice(s).")
        except Exception as _seed_err:
            print(f"  ⚠️  AUTO_SEED failed: {_seed_err}")

    # ── Step 2b: Acquire OneDrive lock ───────────────────────────
    acquire_db_lock()
    threading.Thread(target=_heartbeat_lock, daemon=True).start()

    # ── Step 2c: Start background maintenance threads ─────────────
    threading.Thread(target=_session_cleanup, daemon=True).start()
    threading.Thread(target=_backup_thread, daemon=True).start()
    threading.Thread(target=_hold_expiry_thread, daemon=True).start()

    # ── Step 3: Print welcome in terminal ─────────────────────────
    print()
    print(f"  ✓ Spicetopia BMS is ready.")
    print()

    # ── Security check: warn if default credentials are still set ──
    try:
        admin = qry1("SELECT password_hash, salt, auth_scheme FROM users WHERE username='admin'", ())
        if admin and _verify_pw('admin123', admin['password_hash'], admin.get('salt',''), admin.get('auth_scheme','sha256')):
            print('  ⚠️  SECURITY WARNING: Default admin password (admin123) is still set!')
            print('     Change it immediately in Admin → Users before going live.')
            print()
    except Exception:
        pass

    # ── Step 4: Bind server first so it's ready before browser opens
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        lan_ip = s.getsockname()[0]
        s.close()
    except Exception:
        lan_ip = "YOUR-PC-IP"
    url = f"http://localhost:{PORT}"
    server = ThreadingHTTPServer(('0.0.0.0', PORT), Handler)
    print(f"  ✓ Server running at {url}")
    print(f"  ✓ Field app for reps:  http://{lan_ip}:{PORT}/field.html")
    print(f"  ✓ B2B Order portal:    http://{lan_ip}:{PORT}/order.html")
    print(f"  Press Ctrl+C to stop")
    print()

    # Open browser in background thread (webbrowser has no thread restriction)
    # Skipped when NO_BROWSER=1 (e.g. automated test runs)
    if os.environ.get('NO_BROWSER', '').lower() not in ('1', 'true', 'yes'):
        def _open_browser():
            import time; time.sleep(1.2)
            webbrowser.open(url)   # open the login page directly
        threading.Thread(target=_open_browser, daemon=True).start()

    # ── Step 5: Welcome popup — MUST run on main thread on macOS ──
    # Show BEFORE serve_forever() blocks the main thread.
    # osascript on macOS is blocking (user clicks OK) then server runs.
    # On Windows tkinter is also blocking but safe on main thread.
    _show_welcome_gui()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        release_db_lock()
        print(f"\n  Stopped. Goodbye, {USER_NAME.replace('_',' ')}! 👋")
