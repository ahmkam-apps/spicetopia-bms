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

# Route modules — HTTP dispatch extracted out of do_GET/do_POST by domain.
# (routes.auth_routes imports `send_json` back from this file, but only inside
# its function bodies, so there's no circular-import problem at load time.)
from routes.auth_routes import (
    handle_get_pre_gate as _auth_routes_get_pre_gate,
    handle_get_post_gate as _auth_routes_get_post_gate,
    handle_post_login as _auth_routes_post_login,
    handle_post_logout as _auth_routes_post_logout,
)

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

    # Backup recipe images — zip the recipe-images folder alongside the DB backup
    try:
        import zipfile as _zf
        img_dir = Path(os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', '/data')) / 'recipe-images'
        if img_dir.is_dir() and any(img_dir.iterdir()):
            img_zip = BACKUP_PATH / f"recipe_images_{ts}.zip"
            with _zf.ZipFile(img_zip, 'w', _zf.ZIP_DEFLATED) as zf:
                for img_file in img_dir.iterdir():
                    if img_file.is_file():
                        zf.write(img_file, img_file.name)
            # Prune old image zips matching backup retention
            for f in BACKUP_PATH.glob("recipe_images_*.zip"):
                try:
                    if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
                        f.unlink()
                except Exception:
                    pass
            print(f"  ✓ Recipe images backed up: {img_zip.name}")
    except Exception as e:
        print(f"  ⚠ Recipe image backup skipped: {e}")

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


# ═══════════════════════════════════════════════════════════════════
#  RBAC — ROLES AND PERMISSION HELPER
# ═══════════════════════════════════════════════════════════════════

VALID_ROLES = ('super_user', 'admin', 'sales', 'warehouse', 'accountant', 'field_rep', 'user')

# Role hierarchy for display / UI
ROLE_LABELS = {
    'super_user':  'Owner (Super User)',
    'admin':       'Administrator',
    'sales':       'Sales',
    'warehouse':   'Warehouse',
    'accountant':  'Accountant',
    'field_rep':   'Sales Rep',
    'user':        'Viewer (read-only)',
}


def has_permission(sess, key):
    """Granular permission check. super_user has everything; otherwise the named
    permission key must be in the session's permissions list. The session carries
    'permissions' as a parsed list (set at login; refreshed on the user's next login),
    and uses the key 'userId' (camelCase). Used to gate delegated areas (planning,
    costs.*, recipe.*, …) without touching role gates."""
    if not sess:
        return False
    if sess.get('role') == 'super_user':
        return True
    perms = sess.get('permissions')
    if isinstance(perms, list):
        return key in perms
    # Fallback for any session shape that didn't carry the parsed list.
    try:
        uid = sess.get('userId') or sess.get('user_id')
        row = qry1("SELECT permissions FROM users WHERE id=?", (uid,))
        plist = json.loads(row['permissions']) if row and row.get('permissions') else []
        return key in plist
    except Exception:
        return False


def _can_plan(sess):
    """Planning access gate: admins/super_user always; other roles only if granted
    the 'planning' permission. (require() already lets super_user pass everything.)"""
    return require(sess, 'admin') or has_permission(sess, 'planning')


def _can_costs(sess):
    """Costing module gate (additive): admins/super_user, or anyone granted 'costs'.
    Costs are not secret — this just lets you delegate costing (e.g. to an accountant)."""
    return require(sess, 'admin') or has_permission(sess, 'costs')


def _can_recipe(sess):
    """Recipe / BOM gate — the SECRET (ingredient quantities). super_user OR a user
    explicitly granted the 'recipe' permission ONLY. A plain admin does NOT qualify.
    (has_permission already returns True for super_user.)"""
    return has_permission(sess, 'recipe')


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


# ═══════════════════════════════════════════════════════════════════
#  ID GENERATION
# ═══════════════════════════════════════════════════════════════════


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


# ═══════════════════════════════════════════════════════════════════
#  AUDIT TRAIL
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  USER MANAGEMENT + AUTH
# ═══════════════════════════════════════════════════════════════════


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
               pv.gtin, COALESCE(pv.show_online, 0) as show_online,
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


# ═══════════════════════════════════════════════════════════════════
#  STOCK HOLD — SOFT HOLDS FOR REVIEW QUEUE (Phase 3)
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  WHATSAPP NOTIFICATIONS (CallMeBot)
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  WHATSAPP ORDER PARSER — Claude API powered
# ═══════════════════════════════════════════════════════════════════

def _normalise_phone(raw):
    """Normalise Pakistani phone numbers to 03xxxxxxxxx format."""
    if not raw:
        return None
    p = re.sub(r'[\s\-\(\)\+]', '', str(raw))
    if p.startswith('923') and len(p) == 12:
        p = '0' + p[2:]
    if p.startswith('92') and len(p) == 12:
        p = '0' + p[2:]
    if p.startswith('3') and len(p) == 10:
        p = '0' + p
    return p if len(p) >= 10 else raw


ERP_ASSISTANT_KB = """Spicetopia ERP — what each area is for (the flow is Buy → Make → Sell):

SELL (Sales & AR):
- orders-invoices: take a customer order (reserves stock), then raise/track its invoice. Start here to "take an order" or "make an invoice".
- field-orders: orders taken by sales reps out in the field.
- review-queue: approve or reject pending orders (from the website or reps) before they're confirmed.
- receipts-aging: record a customer's payment against an invoice, and see who owes money (AR aging).

BUY (Procurement & AP):
- purchase-orders: order ingredients/materials from a supplier.
- bills: record the supplier's bill/invoice for what you bought.
- ap-payments: pay a supplier and allocate the payment to bills.
- ap-aging: see what you owe suppliers, by age.

MAKE (Operations):
- inventory: check stock levels, make stock adjustments (in kg), view the movement ledger. Stock only moves via orders/production, never edited directly.
- production: two-step manufacturing — create a Work Order, then complete it to record a Batch (which produces finished stock). Procurement of ingredients for a work order is calculated here from the BOM. To estimate the ingredient cost to make a specific quantity, create a Work Order and open its procurement list (shows ingredients needed + estimated cost from the BOM).

REPORTS:
- dashboard: KPIs and charts overview.
- pl-report: profit & loss by period. margins: gross margin by product. rep-performance: sales by rep.
- planning: forecasting and scenario planning (opens the Planning tool).

MASTER DATA / ADMIN:
- customers, suppliers, products, sales-reps, zones-routes: reference data.
- price-master: manual price grid per SKU. prices-costs: STANDARD COSTING — the Standard Costs tab shows what each product costs to make (called "cost to make", "BOM cost", "standard cost", or "recipe cost"): raw-material cost computed from the BOM/recipe, plus packaging, conversion and overhead, plus the margins and selling prices. This is where you estimate or check a product's cost; the cost line items are set under Cost Parameters here.
- bom: the secret recipe (Bill of Materials) — restricted.
- users: user accounts & permissions. payroll: rep payroll. master-data: bulk CSV/XLSX import. ingredients-admin: ingredient master & costs. audit: change history. system: settings & backup.

Common tasks: "record a payment" → receipts-aging. "take/create an order" → orders-invoices. "make a batch / produce stock" → production. "buy ingredients / raise a PO" → purchase-orders. "approve an order" → review-queue. "add a customer" → customers. "check stock" → inventory."""


def erp_assistant_answer(question, allowed=None) -> dict:
    """In-app help guide. Free-text question → JSON {answer, steps[], navTarget} grounded
    in the ERP. Read-only guidance only — never performs actions."""
    import os
    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    if not api_key:
        return {'answer': 'The help assistant isn’t configured yet. Use the ? Help guide for now.',
                'steps': [], 'navTarget': None, 'error': 'no_api_key'}
    nav_lines, valid_ids = '', set()
    if isinstance(allowed, list):
        for a in allowed:
            try:
                _id = a.get('id'); _lbl = a.get('label', '')
                if _id:
                    valid_ids.add(_id); nav_lines += f"  - {_id}: {_lbl}\n"
            except Exception:
                pass
    system_prompt = f"""You are the in-app help guide for the Spicetopia ERP. A manager (not a developer) asks how to do a task. Answer with short, concrete steps that match THIS app's screens.

{ERP_ASSISTANT_KB}

Screens THIS user can open (use ONLY these ids for navTarget):
{nav_lines or '  (no list provided — set navTarget to null)'}

Return ONLY valid JSON, no other text:
{{"answer": "one short direct sentence", "steps": ["step 1", "step 2"], "navTarget": "<one screen id from the list above, or null>"}}

Rules:
- Steps must be specific to this ERP and use the real screen names above.
- navTarget = the single best screen to START the task (an id from the list), or null if unclear / not in the list.
- Always map the question to the closest relevant screen above and give your best step-by-step guidance. Only say the ERP can't do it (answer says so, steps=[], navTarget=null) when NOTHING above is even related.
- You only GUIDE; never say you performed an action. Max ~6 steps."""
    try:
        import urllib.request, urllib.error
        payload = json.dumps({
            'model': 'claude-haiku-4-5-20251001',
            'max_tokens': 700,
            'system': system_prompt,
            'messages': [{'role': 'user', 'content': str(question)[:1000]}],
        }).encode('utf-8')
        req = urllib.request.Request(
            'https://api.anthropic.com/v1/messages', data=payload,
            headers={'x-api-key': api_key, 'anthropic-version': '2023-06-01', 'content-type': 'application/json'},
            method='POST')
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                body = json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as he:
            detail = ''
            try: detail = he.read().decode('utf-8')[:400]
            except Exception: pass
            print(f"  ⚠ assistant_failed HTTP {he.code}: {detail}")
            _log('error', 'assistant_failed', error=f'HTTP {he.code} {detail}')
            reason = detail
            try:
                reason = (json.loads(detail).get('error', {}) or {}).get('message', detail)
            except Exception:
                pass
            return {'answer': "Sorry — the assistant hit an error reaching the AI service.",
                    'steps': [], 'navTarget': None, 'error': f'HTTP {he.code}: {str(reason)[:200]}'}
        raw = body['content'][0]['text'].strip()
        if '{' in raw:
            raw = raw[raw.find('{'):raw.rfind('}') + 1]
        out = json.loads(raw)
    except Exception as e:
        print(f"  ⚠ assistant_failed: {type(e).__name__}: {e}")
        _log('error', 'assistant_failed', error=str(e))
        return {'answer': "Sorry — I couldn’t answer that just now. Try the ? Help guide.",
                'steps': [], 'navTarget': None, 'error': str(e)}
    nt = out.get('navTarget')
    if nt and valid_ids and nt not in valid_ids:
        nt = None
    return {'answer': out.get('answer', ''), 'steps': out.get('steps') or [], 'navTarget': nt}


def parse_whatsapp_order(message: str) -> dict:
    """
    Use Claude API (Haiku) to extract order details from a WhatsApp message.
    Returns pre-fill payload for the ERP order form.
    """
    import os
    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    if not api_key:
        return {'error': 'ANTHROPIC_API_KEY not configured', 'parsed': False}

    # ── Load all active SKUs from DB for the prompt ──────────────
    variants = qry("""
        SELECT pv.id as variant_id, pv.sku_code,
               p.name as product_name, ps.label as pack_size,
               pv.active_flag
        FROM product_variants pv
        JOIN products p    ON p.id  = pv.product_id
        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
        WHERE pv.active_flag = 1
        ORDER BY p.name, ps.grams
    """)
    sku_list = '\n'.join(
        f"  - {v['product_name']} {v['pack_size']} → sku_code: {v['sku_code']}, variant_id: {v['variant_id']}"
        for v in variants
    )

    system_prompt = f"""You are an order parser for Chacha's Masala, a Pakistani spice brand.
Extract order details from a WhatsApp message and return ONLY valid JSON.

Available products:
{sku_list}

Return this exact JSON structure (use null for missing fields, 0 for missing quantities):
{{
  "name": "customer name or null",
  "phone": "phone number or null",
  "address": "delivery address or null",
  "items": [
    {{"variant_id": 1, "sku_code": "SPCM-50", "product_name": "Chaat Masala", "pack_size": "50g", "qty": 2}}
  ]
}}

Rules:
- Only include items with qty > 0
- Match product names tolerantly (e.g. "chaat" → Chaat Masala, "garam" → Garam Masala, "fish" → Fish Masala)
- If no items found return empty items array
- Return ONLY the JSON object, no other text"""

    try:
        import urllib.request, urllib.error
        payload = json.dumps({
            'model': 'claude-haiku-4-5-20251001',
            'max_tokens': 512,
            'system': system_prompt,
            'messages': [{'role': 'user', 'content': message}]
        }).encode('utf-8')
        req = urllib.request.Request(
            'https://api.anthropic.com/v1/messages',
            data=payload,
            headers={
                'x-api-key': api_key,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json',
            },
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = json.loads(resp.read().decode('utf-8'))
        raw_text = body['content'][0]['text'].strip()
        parsed = json.loads(raw_text)
    except Exception as e:
        _log('error', 'wa_parse_failed', error=str(e))
        return {'error': f'Parse failed: {str(e)}', 'parsed': False}

    # ── Normalise phone ───────────────────────────────────────────
    if parsed.get('phone'):
        parsed['phone'] = _normalise_phone(parsed['phone'])

    # ── Look up existing customer by phone ────────────────────────
    existing_customer = None
    if parsed.get('phone'):
        existing_customer = qry1(
            "SELECT id, code, name, address FROM customers WHERE phone=? LIMIT 1",
            (parsed['phone'],)
        )

    # ── Enrich items with prices ──────────────────────────────────
    items = parsed.get('items', [])
    for item in items:
        vid = item.get('variant_id')
        if vid:
            price_row = qry1("""
                SELECT p.price FROM prices p
                JOIN price_types pt ON pt.id = p.price_type_id
                WHERE p.variant_id=? AND pt.code='web' AND p.active_flag=1
                LIMIT 1
            """, (vid,))
            if not price_row:
                price_row = qry1("""
                    SELECT p.price FROM prices p
                    JOIN price_types pt ON pt.id = p.price_type_id
                    WHERE p.variant_id=? AND pt.code='standard' AND p.active_flag=1
                    LIMIT 1
                """, (vid,))
            item['unit_price'] = float(price_row['price']) if price_row else 0.0
        else:
            item['unit_price'] = 0.0
        item['line_total'] = item['unit_price'] * int(item.get('qty', 0))

    raw_total = sum(i['line_total'] for i in items)

    return {
        'parsed': True,
        'name':     parsed.get('name'),
        'phone':    parsed.get('phone'),
        'address':  parsed.get('address'),
        'items':    items,
        'raw_total': raw_total,
        'existing_customer': existing_customer,
    }


# ═══════════════════════════════════════════════════════════════════
#  REVIEW QUEUE — ORDER INTAKE & MANAGEMENT
# ═══════════════════════════════════════════════════════════════════


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


# ═══════════════════════════════════════════════════════════════════
#  ACCOUNTS PAYABLE HELPERS
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  VOID TRANSACTIONS  (P2.5)
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — CUSTOMERS
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — PRODUCTS
# ═══════════════════════════════════════════════════════════════════

def _save_recipe_sub(c, rid, data):
    """Replace steps and ingredients for a recipe (call inside open transaction)."""
    steps = data.get('steps') or []
    ingredients = data.get('ingredients') or []
    c.execute("DELETE FROM recipe_steps WHERE recipe_id=?", (rid,))
    for i, s in enumerate(steps):
        instr = (s.get('instruction') or '').strip()
        if instr:
            c.execute("INSERT INTO recipe_steps (recipe_id, step_number, instruction) VALUES (?,?,?)",
                      (rid, i + 1, instr))
    c.execute("DELETE FROM recipe_ingredients WHERE recipe_id=?", (rid,))
    for i, ing in enumerate(ingredients):
        item = (ing.get('item') or '').strip()
        if item:
            c.execute("INSERT INTO recipe_ingredients (recipe_id, sort_order, item) VALUES (?,?,?)",
                      (rid, i, item))


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — SUPPLIERS
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — SALES + INVOICES (AR)
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  CUSTOMER ORDERS  (Sales Order → Production → Invoice flow)
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — PURCHASE ORDERS
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — BOM MANAGEMENT
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — ACCOUNTS PAYABLE
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — PAYMENT SIMPLIFICATION (Sprint P1)
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — PRODUCTION PLANNING (WORK ORDERS)
# ═══════════════════════════════════════════════════════════════════


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


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — PRODUCTION
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — INVENTORY ADJUSTMENT
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  PRODUCT PRICES
# ═══════════════════════════════════════════════════════════════════


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


# ═══════════════════════════════════════════════════════════════════
#  DASHBOARD & REPORTS
# ═══════════════════════════════════════════════════════════════════


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

            # ── Consumer website — chachamasala.com ──────────────────
            if host in ('chachamasala.com', 'www.chachamasala.com'):
                chacha_page = PUBLIC_DIR / 'chacha.html'
                if chacha_page.exists():
                    self._serve_file(chacha_page, 'text/html; charset=utf-8')
                else:
                    send_error(self, "Page not found", 404)
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

            # /db-upload — one-time database upload page.
            # NOTE: no session gate on the PAGE — browser navigations carry no
            # Authorization header (token lives in localStorage), so a header
            # check here always bounced admins to '/'. The page is inert; the
            # actual POST /api/admin/db-upload endpoint enforces admin.
            if path == '/db-upload':
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
                    headers:{Authorization:'Bearer '+(localStorage.getItem('erp_token')||localStorage.getItem('sp_token'))}});
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
            # (routes/auth_routes.py handle_get_pre_gate — moved, not rewritten)
            if _auth_routes_get_pre_gate(self, path, _SERVER_START_TIME):
                return

            # ── GET /api/public/prices — no auth, consumer website price feed ──
            if path == '/api/public/prices':
                rows = qry("""
                    SELECT
                        p.code          AS product_code,
                        p.name          AS product_name,
                        pv.id           AS variant_id,
                        pv.sku_code,
                        ps.label        AS pack_size,
                        ps.grams,
                        pp.price,
                        pp.effective_from,
                        COALESCE(pv.show_online, 0) AS show_online
                    FROM product_prices pp
                    JOIN price_types pt      ON pt.id = pp.price_type_id AND pt.code = 'web'
                    JOIN product_variants pv ON pv.id = pp.product_variant_id AND pv.active_flag = 1
                                            AND COALESCE(pv.show_online, 0) = 1
                    JOIN products p          ON p.id  = pv.product_id AND p.active = 1
                    JOIN pack_sizes ps       ON ps.id = pv.pack_size_id
                    WHERE pp.active_flag = 1
                    ORDER BY p.code, ps.grams
                """)
                send_json(self, {
                    'currency':   'PKR',
                    'prices':     rows,
                    'updated_at': today(),
                })
                return

            # ── GET /api/public/recipes — no auth, recipe listing ──
            if path == '/api/public/recipes':
                rows = qry("""
                    SELECT r.id, r.title, r.slug, r.masala_code, r.description,
                           r.prep_mins, r.cook_mins, r.serves, r.image_path, r.sort_order
                    FROM recipes r WHERE r.active=1 ORDER BY r.sort_order ASC, r.id ASC
                """)
                for row in rows:
                    if row.get('image_path'):
                        row['image_url'] = '/api/public/recipe-images/' + row['image_path']
                    else:
                        row['image_url'] = None
                send_json(self, rows)
                return

            # ── GET /api/public/recipes/:slug — no auth, single recipe ──
            if path.startswith('/api/public/recipes/') and len(path.split('/')) == 5:
                slug = path.split('/')[-1]
                recipe = qry1("SELECT * FROM recipes WHERE slug=? AND active=1", (slug,))
                if not recipe:
                    send_error(self, 'Recipe not found', 404); return
                recipe['steps']       = qry("SELECT * FROM recipe_steps WHERE recipe_id=? ORDER BY step_number", (recipe['id'],))
                recipe['ingredients'] = qry("SELECT * FROM recipe_ingredients WHERE recipe_id=? ORDER BY sort_order", (recipe['id'],))
                if recipe.get('image_path'):
                    recipe['image_url'] = '/api/public/recipe-images/' + recipe['image_path']
                else:
                    recipe['image_url'] = None
                send_json(self, recipe)
                return

            # ── GET /api/public/recipe-images/:filename — serve uploaded image ──
            if path.startswith('/api/public/recipe-images/'):
                import mimetypes
                filename = path.split('/')[-1]
                # Sanitise — no path traversal
                filename = os.path.basename(filename)
                img_dir  = os.path.join(os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', '/data'), 'recipe-images')
                img_path = os.path.join(img_dir, filename)
                if not os.path.isfile(img_path):
                    send_error(self, 'Image not found', 404); return
                mime = mimetypes.guess_type(img_path)[0] or 'application/octet-stream'
                with open(img_path, 'rb') as f:
                    data = f.read()
                self.send_response(200)
                self.send_header('Content-Type', mime)
                self.send_header('Content-Length', str(len(data)))
                self.send_header('Cache-Control', 'public, max-age=86400')
                self.end_headers()
                self.wfile.write(data)
                return

            # ── Auth gate (all /api/ except auth endpoints) ──────
            sess = None   # will be set by get_session below
            if path not in ('/api/auth/login', '/api/auth/me'):
                sess = get_session(self)
                if not sess:
                    send_json(self, {'error': 'Unauthorized'}, 401); return

            # GET /api/health (again) + GET /api/auth/me — session status
            # (routes/auth_routes.py handle_get_post_gate — moved, not rewritten)
            if _auth_routes_get_post_gate(self, path, SERVER_START_TIME):
                return

            # GET /api/users  (admin only)
            if path == '/api/users':
                sess = get_session(self)
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                _users = list_users()
                if sess.get('role') != 'super_user':
                    _users = [u for u in _users if u.get('role') != 'super_user']  # only the owner sees the owner
                send_json(self, _users)
                return

            # ── ADMIN BACKUP STATUS ────────────────────────────────────────
            # GET /api/admin/backup  — list backups + next run time (admin only)
            if path == '/api/admin/backup':
                if not require(sess, 'admin'):
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

            # GET /api/admin/backup/download  — stream a fresh consistent DB snapshot (admin only)
            # Auth via query token so a plain browser link works.
            if path == '/api/admin/backup/download':
                dsess = get_session(self, qs)
                if not require(dsess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                import tempfile as _tf
                _ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                _tmp = Path(_tf.gettempdir()) / f"spicetopia_dl_{_ts}.db"
                try:
                    _src = sqlite3.connect(str(DB_TMP)); _dst = sqlite3.connect(str(_tmp))
                    try:
                        _src.backup(_dst)          # online backup — consistent, no write lock
                    finally:
                        _dst.close(); _src.close()
                    _data = _tmp.read_bytes()
                finally:
                    try: _tmp.unlink()
                    except Exception: pass
                _fname = f"spicetopia_backup_{_ts}.db"
                self.send_response(200)
                self.send_header('Content-Type', 'application/octet-stream')
                self.send_header('Content-Disposition', f'attachment; filename="{_fname}"')
                self.send_header('Content-Length', str(len(_data)))
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(_data)
                return

            # GET /api/admin/settings  — return runtime + DB settings (admin only)
            if path == '/api/admin/settings':
                if not require(sess, 'admin'):
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
                if not require(sess, 'admin'):
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
                if not require(sess, 'admin'):
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
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                send_json(self, {'code': peek_next_ingredient_code()})
                return

            # GET /api/ingredients/duplicates  — same-name ingredients under different codes (admin, read-only)
            if path == '/api/ingredients/duplicates':
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                import threading as _th, time as _t
                print("  [duplicates] request received", flush=True)
                _box = {}
                def _work():
                    try:
                        _box['data'] = find_duplicate_ingredients()
                    except Exception as _e:
                        import traceback; traceback.print_exc()
                        _box['err'] = str(_e)
                _t0 = _t.time()
                _wt = _th.Thread(target=_work, daemon=True); _wt.start(); _wt.join(15)
                if _wt.is_alive():
                    print(f"  [duplicates] TIMED OUT after {_t.time()-_t0:.1f}s", flush=True)
                    send_error(self, 'Duplicate scan timed out (>15s) — likely a DB lock or data-volume issue', 504)
                elif 'err' in _box:
                    send_error(self, 'Duplicate scan failed: ' + _box['err'], 500)
                else:
                    print(f"  [duplicates] returned {len(_box.get('data', []))} group(s) in {_t.time()-_t0:.2f}s", flush=True)
                    send_json(self, _box.get('data', []))
                return

            # GET /api/products/next-blend-code?prefix=GM  — peek next GM-BC-xxx code (admin)
            if path == '/api/products/next-blend-code':
                if not require(sess, 'admin'):
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
                if not require(sess, 'admin'):
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
                if not require(sess, 'admin'):
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

            # GET /api/recipes  (admin)
            if path == '/api/recipes':
                if not require(sess, 'admin', 'sales', 'warehouse', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                rows = qry("""
                    SELECT r.id, r.title, r.slug, r.masala_code, r.description,
                           r.prep_mins, r.cook_mins, r.serves, r.image_path,
                           r.active, r.sort_order, r.created_at
                    FROM recipes r ORDER BY r.sort_order ASC, r.id ASC
                """)
                for row in rows:
                    row['image_url'] = ('/api/public/recipe-images/' + row['image_path']) if row.get('image_path') else None
                send_json(self, rows)
                return

            # GET /api/recipes/:id  (admin — full detail with steps + ingredients)
            if path.startswith('/api/recipes/') and len(path.split('/')) == 4 and path.split('/')[3].isdigit():
                if not require(sess, 'admin', 'sales', 'warehouse', 'accountant'):
                    send_error(self, 'Permission denied', 403); return
                rid = int(path.split('/')[3])
                recipe = qry1("SELECT * FROM recipes WHERE id=?", (rid,))
                if not recipe:
                    send_error(self, 'Recipe not found', 404); return
                recipe['steps']       = qry("SELECT * FROM recipe_steps WHERE recipe_id=? ORDER BY step_number", (rid,))
                recipe['ingredients'] = qry("SELECT * FROM recipe_ingredients WHERE recipe_id=? ORDER BY sort_order", (rid,))
                recipe['image_url']   = ('/api/public/recipe-images/' + recipe['image_path']) if recipe.get('image_path') else None
                send_json(self, recipe)
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
                if not require(sess, 'admin'):
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
                if not require(sess, 'admin'):
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

            # GET /api/costing/config  (admin or 'costs' permission)
            if path == '/api/costing/config':
                if not _can_costs(sess):
                    send_error(self, 'Costing access required', 403); return
                send_json(self, get_costing_config())
                return

            # GET /api/costing/standard-costs  (admin or 'costs' permission)
            if path == '/api/costing/standard-costs':
                if not _can_costs(sess):
                    send_error(self, 'Costing access required', 403); return
                send_json(self, get_all_standard_costs())
                return

            # GET /api/costing/standard-costs/:productCode/:packSize  (admin or 'costs' permission)
            if path.startswith('/api/costing/standard-costs/') and len(path.split('/')) == 6:
                if not _can_costs(sess):
                    send_error(self, 'Costing access required', 403); return
                parts = path.split('/')
                result = compute_standard_cost(parts[4], parts[5])
                if not result:
                    send_error(self, 'SKU not found', 404); return
                send_json(self, result)
                return

            # GET /api/costing/batch-variances  (admin or 'costs' permission)
            if path == '/api/costing/batch-variances':
                if not _can_costs(sess):
                    send_error(self, 'Costing access required', 403); return
                days = int(qs.get('days', ['90'])[0])
                send_json(self, get_batch_variances(days))
                return

            # GET /api/costing/price-history  (admin or 'costs' permission)
            if path == '/api/costing/price-history':
                if not _can_costs(sess):
                    send_error(self, 'Costing access required', 403); return
                limit = int(qs.get('limit', ['100'])[0])
                change_type = qs.get('type', [None])[0]
                days = qs.get('days', [None])[0]
                days = int(days) if days else None
                send_json(self, get_price_history(limit=limit, change_type=change_type, days=days))
                return

            # GET /api/costing/margin-alerts  (admin or 'costs' permission)
            if path == '/api/costing/margin-alerts':
                if not _can_costs(sess):
                    send_error(self, 'Costing access required', 403); return
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

            # GET /api/costing/operating-costs  (admin or 'costs')
            if path == '/api/costing/operating-costs':
                if not _can_costs(sess):
                    send_error(self, 'Costing access required', 403); return
                send_json(self, list_operating_costs())
                return

            # GET /api/bom/:productCode  (recipe owner / 'recipe' permission only — the secret)
            if path.startswith('/api/bom/'):
                if not _can_recipe(sess):
                    send_error(self, 'Recipe access required', 403); return
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

            # ── Planning Input System (admin only) ──────────────────
            if path == '/api/planning/versions':
                if not _can_plan(get_session(self, qs)):
                    send_error(self, 'Planning access required', 403); return
                send_json(self, list_plan_versions())
                return
            if path == '/api/planning/manufacturers':
                if not _can_plan(get_session(self, qs)):
                    send_error(self, 'Planning access required', 403); return
                send_json(self, list_manufacturers())
                return
            if path == '/api/planning/variants':
                if not _can_plan(get_session(self, qs)):
                    send_error(self, 'Planning access required', 403); return
                send_json(self, list_active_variants())
                return
            if path == '/api/planning/zones':
                if not _can_plan(get_session(self, qs)):
                    send_error(self, 'Planning access required', 403); return
                send_json(self, list_zones())
                return
            if path == '/api/planning/compare':
                if not _can_plan(get_session(self, qs)):
                    send_error(self, 'Planning access required', 403); return
                _raw  = qs.get('versions', [''])[0]
                _vids = [int(x) for x in _raw.split(',') if x.strip().isdigit()]
                send_json(self, compare_scenarios(_vids))
                return
            if path.startswith('/api/planning/versions/') and path.split('/')[4].isdigit():
                if not _can_plan(get_session(self, qs)):
                    send_error(self, 'Planning access required', 403); return
                parts = path.split('/')
                vid   = int(parts[4])
                if len(parts) == 5:
                    send_json(self, get_plan_version(vid)); return
                if len(parts) == 6 and parts[5] == 'forecast':
                    send_json(self, list_sales_forecast(vid)); return
                if len(parts) == 6 and parts[5] == 'targets':
                    send_json(self, list_sales_targets(vid)); return
                if len(parts) == 6 and parts[5] == 'manufacturing':
                    send_json(self, list_manufacturing(vid)); return
                if len(parts) == 6 and parts[5] == 'financial':
                    send_json(self, get_financial(vid)); return
                if len(parts) == 6 and parts[5] == 'pricing':
                    send_json(self, list_pricing(vid)); return
                if len(parts) == 6 and parts[5] == 'projected-sales':
                    _m = qs.get('months', [None])[0]
                    send_json(self, projected_sales(vid, int(_m) if _m else None)); return
                if len(parts) == 6 and parts[5] == 'capacity-vs-demand':
                    send_json(self, capacity_vs_demand(vid)); return
                if len(parts) == 6 and parts[5] == 'production-required':
                    send_json(self, production_required(vid)); return
                if len(parts) == 6 and parts[5] == 'cash-flow':
                    send_json(self, cash_flow(vid)); return
                if len(parts) == 6 and parts[5] == 'risk':
                    send_json(self, risk_assessment(vid)); return
                if len(parts) == 6 and parts[5] == 'ingredients':
                    send_json(self, ingredient_requirements(vid)); return
                if len(parts) == 6 and parts[5] == 'releases':
                    send_json(self, list_releases(vid)); return

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
                if not require(sess, 'admin'):
                    send_json(self, {'error': 'Admin only'}, 403); return
                result = dev_reset_all()
                send_json(self, result)
                return

            # POST /api/dev/seed-fg-stock  (DEV_TOOLS only — bypasses BOM for tests)
            if path == '/api/dev/seed-fg-stock':
                if os.environ.get('DEV_TOOLS', '').lower() not in ('1', 'true', 'yes'):
                    send_error(self, 'Not available in this environment', 403); return
                sess = get_session(self)
                if not require(sess, 'admin'):
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
            # (routes/auth_routes.py handle_post_login — moved, not rewritten)
            if _auth_routes_post_login(self, path, data):
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

            # POST /api/public/orders  — no-auth consumer order intake from chacha.html
            if path == '/api/public/orders':
                # Validate required fields
                name   = (data.get('name') or '').strip()
                phone  = _normalise_phone(data.get('phone') or '')
                area   = (data.get('area') or '').strip()
                email  = (data.get('email') or '').strip() or None
                items  = data.get('items', [])
                if not name:
                    send_error(self, 'name is required', 400); return
                if not phone or len(phone) < 10:
                    send_error(self, 'valid phone number required', 400); return
                if not items:
                    send_error(self, 'at least one item required', 400); return
                # Validate items: each must have variant_id (int) and qty > 0
                clean_items = []
                for it in items:
                    try:
                        vid = int(it.get('variant_id') or it.get('variantId') or 0)
                        qty = float(it.get('qty', 0))
                    except (TypeError, ValueError):
                        send_error(self, 'invalid item format', 400); return
                    if vid <= 0 or qty <= 0:
                        send_error(self, 'invalid item: variant_id and qty required', 400); return
                    # Confirm variant exists and is show_online=1
                    var_row = qry1("""
                        SELECT pv.id, p.code as productCode, ps.label as packSize, pp.price
                        FROM product_variants pv
                        JOIN products p    ON p.id  = pv.product_id AND p.active=1
                        JOIN pack_sizes ps ON ps.id = pv.pack_size_id
                        LEFT JOIN (
                            SELECT pp2.product_variant_id, pp2.price
                            FROM product_prices pp2
                            JOIN price_types pt2 ON pt2.id = pp2.price_type_id AND pt2.code='web'
                            WHERE pp2.active_flag=1
                            ORDER BY pp2.effective_from DESC
                        ) pp ON pp.product_variant_id = pv.id
                        WHERE pv.id=? AND pv.active_flag=1 AND COALESCE(pv.show_online,0)=1
                    """, (vid,))
                    if not var_row:
                        send_error(self, f'Product {vid} not available for online orders', 400); return
                    clean_items.append({
                        'variantId': vid,
                        'qty': qty,
                        'unitPrice': var_row['price'] or 0,
                    })
                # Find or create consumer customer by phone
                cust_row = qry1("SELECT code FROM customers WHERE phone=?", (phone,))
                if not cust_row:
                    # Create a DIRECT customer (consumer)
                    new_cust = create_customer({
                        'name':         name,
                        'city':         area or 'Karachi',
                        'phone':        phone,
                        'email':        email or '',
                        'customerType': 'DIRECT',
                        'address':      area or '',
                    })
                    cust_code = new_cust['code']
                else:
                    cust_code = cust_row['code']
                # Submit order via standard external intake
                order_data = {
                    'custCode':     cust_code,
                    'order_source': 'consumer_website',
                    'notes':        ('Online order. Area: ' + area) if area else 'Online order',
                    'items':        clean_items,
                }
                try:
                    result = create_customer_order_external(order_data)
                except ValueError as ve:
                    send_error(self, str(ve), 400); return
                except Exception as ex:
                    _log('error', 'public_order_failed', error=str(ex))
                    send_error(self, 'Could not place order. Please try again.', 500); return
                send_json(self, {
                    'ok':      True,
                    'orderId': result.get('orderId'),
                    'ref':     result.get('orderCode') or result.get('orderId'),
                    'message': 'Order received! We will contact you to confirm delivery.',
                }, 201)
                return

            # Auth gate for all other POST endpoints
            sess = get_session(self)
            if not sess:
                send_json(self, {'error': 'Unauthorized'}, 401); return

            # POST /api/auth/logout
            # (routes/auth_routes.py handle_post_logout — moved, not rewritten)
            if _auth_routes_post_logout(self, path):
                return

            # POST /api/admin/reconcile-statuses (admin only — fix any status drift)
            if path == '/api/admin/reconcile-statuses':
                if not require(sess, 'admin'):
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
                if not require(sess, 'admin'):
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
                if not require(sess, 'admin'):
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
                if not require(sess, 'admin'):
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
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                try:
                    result = run_backup()
                    send_json(self, {'ok': True, **result})
                except Exception as e:
                    send_error(self, str(e), 500)
                return

            # POST /api/admin/db-upload  (admin only — replace database from uploaded file)
            if path == '/api/admin/db-upload':
                if not require(sess, 'admin'):
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
                if not require(sess, 'admin'):
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
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                result = create_ingredient(data)
                send_json(self, result, 201)
                return

            # POST /api/ingredients/:code/reactivate  (admin only)
            if path.startswith('/api/ingredients/') and path.endswith('/reactivate'):
                if not require(sess, 'admin'):
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

            # POST /api/costing/margin-alerts/:id/dismiss  (admin or 'costs' permission)
            if path.startswith('/api/costing/margin-alerts/') and path.endswith('/dismiss'):
                if not _can_costs(sess):
                    send_error(self, 'Costing access required', 403); return
                try:
                    alert_id = int(path.split('/')[-2])
                    result = dismiss_margin_alert(alert_id, sess.get('username', 'admin'))
                    send_json(self, result)
                except (ValueError, IndexError) as e:
                    send_error(self, str(e), 400)
                return

            # POST /api/costing/operating-costs  (admin or 'costs') — record a month's actual cost
            if path == '/api/costing/operating-costs':
                if not _can_costs(sess):
                    send_error(self, 'Costing access required', 403); return
                try:
                    result = upsert_operating_cost(data.get('month'), data.get('category'),
                                                   data.get('amount'), sess.get('username', 'admin'))
                    send_json(self, result)
                except ValueError as e:
                    send_error(self, str(e), 400)
                return

            # POST /api/customers/:id/reactivate  (admin only)
            if path.startswith('/api/customers/') and path.endswith('/reactivate'):
                if not require(sess, 'admin'):
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
                if not require(sess, 'admin'):
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

            # POST /api/orders/parse  — parse WhatsApp message via Claude API
            if path == '/api/orders/parse':
                if not require(sess, 'admin', 'sales'):
                    send_error(self, 'Permission denied', 403); return
                msg = data.get('message', '').strip()
                if not msg:
                    send_error(self, 'message is required', 400); return
                send_json(self, parse_whatsapp_order(msg), 200)
                return

            # POST /api/assistant/ask  — in-app help guide (any logged-in user)
            if path == '/api/assistant/ask':
                if not sess:
                    send_json(self, {'error': 'Unauthorized'}, 401); return
                q = (data.get('question') or '').strip()
                if not q:
                    send_error(self, 'question is required', 400); return
                send_json(self, erp_assistant_answer(q, data.get('allowed')), 200)
                return

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
                if not require(sess, 'admin'):
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

            # POST /api/bom  (recipe owner / 'recipe' permission only — the secret)
            if path == '/api/bom':
                if not _can_recipe(sess):
                    send_error(self, 'Recipe access required', 403); return
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
                if not _can_costs(sess):
                    send_error(self, 'Costing access required', 403); return
                result = set_product_price(data)
                send_json(self, result)
                return

            # POST /api/admin/masters/upload/{type}  — upload CSV or XLSX master file
            if path.startswith('/api/admin/masters/upload/'):
                if not require(sess, 'admin'):
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
                if not require(sess, 'admin'):
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
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                prefix = data.get('prefix', '').strip().upper()
                if not prefix:
                    send_json(self, {'error': 'prefix required'}, 400); return
                code = next_blend_code(prefix)
                send_json(self, {'code': code})
                return

            # ── RECIPES ───────────────────────────────────────────────────

            # POST /api/recipes  (admin only — create recipe)
            if path == '/api/recipes':
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                title       = (data.get('title') or '').strip()
                masala_code = (data.get('masala_code') or '').strip()
                if not title:
                    send_error(self, 'Title is required', 400); return
                if masala_code not in ('SPCM', 'SPGM', 'SPFM'):
                    send_error(self, 'masala_code must be SPCM, SPGM or SPFM', 400); return
                import re as _re
                slug = _re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')
                description = (data.get('description') or '').strip()
                prep_mins   = int(data.get('prep_mins') or 0)
                cook_mins   = int(data.get('cook_mins') or 0)
                serves      = int(data.get('serves') or 4)
                sort_order  = int(data.get('sort_order') or 0)
                image_path  = None
                # Handle base64 image upload
                if data.get('image_b64') and data.get('image_ext'):
                    import base64, uuid as _uuid
                    img_dir = os.path.join(os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', '/data'), 'recipe-images')
                    os.makedirs(img_dir, exist_ok=True)
                    ext = data['image_ext'].lower().lstrip('.')
                    filename = _uuid.uuid4().hex + '.' + ext
                    with open(os.path.join(img_dir, filename), 'wb') as f:
                        f.write(base64.b64decode(data['image_b64']))
                    image_path = filename
                c = _conn()
                try:
                    c.execute("""
                        INSERT INTO recipes (title, slug, masala_code, description,
                            prep_mins, cook_mins, serves, image_path, sort_order)
                        VALUES (?,?,?,?,?,?,?,?,?)
                    """, (title, slug, masala_code, description, prep_mins, cook_mins, serves, image_path, sort_order))
                    rid = c.execute("SELECT last_insert_rowid()").fetchone()[0]
                    _save_recipe_sub(c, rid, data)
                    c.commit()
                finally:
                    c.close()
                send_json(self, {'ok': True, 'id': rid, 'slug': slug}, 201)
                return

            # POST /api/recipes/:id/image  (admin only — replace image)
            if path.startswith('/api/recipes/') and path.endswith('/image'):
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                rid = int(path.split('/')[3])
                if not data.get('image_b64') or not data.get('image_ext'):
                    send_error(self, 'image_b64 and image_ext required', 400); return
                import base64, uuid as _uuid
                img_dir = os.path.join(os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', '/data'), 'recipe-images')
                os.makedirs(img_dir, exist_ok=True)
                ext = data['image_ext'].lower().lstrip('.')
                filename = _uuid.uuid4().hex + '.' + ext
                with open(os.path.join(img_dir, filename), 'wb') as f:
                    f.write(base64.b64decode(data['image_b64']))
                run("UPDATE recipes SET image_path=? WHERE id=?", (filename, rid))
                send_json(self, {'ok': True, 'image_path': filename,
                                 'image_url': '/api/public/recipe-images/' + filename})
                return

            # POST /api/products  (admin only)
            if path == '/api/products':
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                result = create_product(data)
                send_json(self, result, 201)
                return

            # POST /api/products/:code/variants  — add pack size to existing product (admin only)
            if path.startswith('/api/products/') and path.endswith('/variants') and len(path.split('/')) == 5:
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                prod_code = path.split('/')[3]
                result = create_product_variant(prod_code, data)
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

            # ── Planning Input System (admin only) ──────────────────
            if path == '/api/planning/versions':
                if not _can_plan(sess):
                    send_error(self, 'Planning access required', 403); return
                send_json(self, create_plan_version(data, sess['username']), 201)
                return
            if path == '/api/planning/manufacturers':
                if not _can_plan(sess):
                    send_error(self, 'Planning access required', 403); return
                send_json(self, create_manufacturer(data, sess['username']), 201)
                return
            if path.startswith('/api/planning/versions/') and path.split('/')[4].isdigit():
                if not _can_plan(sess):
                    send_error(self, 'Planning access required', 403); return
                parts = path.split('/')
                vid   = int(parts[4])
                if len(parts) == 6 and parts[5] == 'forecast':
                    send_json(self, upsert_sales_forecast(vid, data, sess['username']), 201); return
                if len(parts) == 6 and parts[5] == 'targets':
                    send_json(self, upsert_sales_target(vid, data, sess['username']), 201); return
                if len(parts) == 6 and parts[5] == 'manufacturing':
                    send_json(self, upsert_manufacturing(vid, data, sess['username']), 201); return
                if len(parts) == 6 and parts[5] == 'financial':
                    send_json(self, upsert_financial(vid, data, sess['username']), 201); return
                if len(parts) == 6 and parts[5] == 'pricing':
                    send_json(self, upsert_pricing(vid, data, sess['username']), 201); return
                if len(parts) == 6 and parts[5] == 'release':
                    send_json(self, release_to_manufacturing(vid, data.get('period_month'), sess['username']), 201); return
                if len(parts) == 6 and parts[5] == 'approve':
                    send_json(self, approve_plan_version(vid, sess['username'])); return
                if len(parts) == 6 and parts[5] == 'unapprove':
                    send_json(self, unapprove_plan_version(vid, sess['username'])); return

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
                if not require(sess, 'admin'):
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

            # PUT /api/recipes/:id  (admin only — update metadata + steps + ingredients)
            if path.startswith('/api/recipes/') and len(path.split('/')) == 4 and path.split('/')[3].isdigit():
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                rid = int(path.split('/')[3])
                import re as _re
                title       = (data.get('title') or '').strip()
                masala_code = (data.get('masala_code') or '').strip()
                if not title:
                    send_error(self, 'Title is required', 400); return
                if masala_code not in ('SPCM', 'SPGM', 'SPFM'):
                    send_error(self, 'masala_code must be SPCM, SPGM or SPFM', 400); return
                slug        = _re.sub(r'[^a-z0-9]+', '-', title.lower()).strip('-')
                description = (data.get('description') or '').strip()
                prep_mins   = int(data.get('prep_mins') or 0)
                cook_mins   = int(data.get('cook_mins') or 0)
                serves      = int(data.get('serves') or 4)
                sort_order  = int(data.get('sort_order') or 0)
                active      = 1 if data.get('active', True) else 0
                c = _conn()
                try:
                    c.execute("""UPDATE recipes SET title=?, slug=?, masala_code=?, description=?,
                                 prep_mins=?, cook_mins=?, serves=?, sort_order=?, active=?
                                 WHERE id=?""",
                              (title, slug, masala_code, description, prep_mins, cook_mins,
                               serves, sort_order, active, rid))
                    _save_recipe_sub(c, rid, data)
                    c.commit()
                finally:
                    c.close()
                send_json(self, {'ok': True, 'id': rid, 'slug': slug})
                return

            # PUT /api/recipes/:id/active  (admin only — toggle publish)
            if path.startswith('/api/recipes/') and path.endswith('/active'):
                if not require(sess, 'admin'):
                    send_error(self, 'Permission denied', 403); return
                rid    = int(path.split('/')[3])
                active = 1 if data.get('active') else 0
                run("UPDATE recipes SET active=? WHERE id=?", (active, rid))
                send_json(self, {'ok': True, 'id': rid, 'active': active})
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

            # PUT /api/products/variants/:id/show-online  (admin only)
            if (path.startswith('/api/products/variants/') and
                    len(parts) == 6 and parts[5] == 'show-online'):
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                variant_id  = int(parts[4])
                show_online = 1 if data.get('show_online') else 0
                c = _conn()
                try:
                    c.execute(
                        "UPDATE product_variants SET show_online=? WHERE id=?",
                        (show_online, variant_id)
                    )
                    c.commit()
                finally:
                    c.close()
                load_ref()
                send_json(self, {'ok': True, 'variant_id': variant_id, 'show_online': show_online})
                return

            # PUT /api/costing/config  (admin or 'costs' permission)
            if path == '/api/costing/config':
                if not _can_costs(sess):
                    send_error(self, 'Costing access required', 403); return
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
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                code   = path.split('/')[3]
                result = update_product(code, data)
                send_json(self, result)
                return

            # PUT /api/ingredients/:code  (edit cost / unit / reorder — admin only)
            if path.startswith('/api/ingredients/') and len(path.split('/')) == 4:
                if not require(sess, 'admin'):
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

            # ── Planning Input System (admin only) ──────────────────
            if (path.startswith('/api/planning/versions/') and len(path.split('/')) == 5
                    and path.split('/')[4].isdigit()):
                if not _can_plan(sess):
                    send_error(self, 'Planning access required', 403); return
                vid = int(path.split('/')[4])
                send_json(self, update_plan_version(vid, data, sess['username']))
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

            # DELETE /api/recipes/:id  → soft deactivate (admin only)
            if path.startswith('/api/recipes/') and len(path.split('/')) == 4:
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                rid = int(path.split('/')[3])
                run("UPDATE recipes SET active=0 WHERE id=?", (rid,))
                send_json(self, {'ok': True, 'id': rid})
                return

            # DELETE /api/customers/:id  → soft deactivate (admin only)
            if path.startswith('/api/customers/') and len(path.split('/')) == 4:
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                cust_id = int(path.split('/')[3])
                result  = update_customer(cust_id, {'active': 0})
                send_json(self, result)
                return

            # DELETE /api/suppliers/:id  → soft deactivate (admin only)
            if path.startswith('/api/suppliers/') and len(path.split('/')) == 4:
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                sup_id = int(path.split('/')[3])
                result = update_supplier(sup_id, {'active_flag': 0})
                send_json(self, result)
                return

            # DELETE /api/users/:id  → deactivate (admin only)
            if path.startswith('/api/users/') and len(path.split('/')) == 4:
                if not require(sess, 'admin'):
                    send_error(self, 'Admin only', 403); return
                uid    = int(path.split('/')[3])
                if uid == sess['userId']:
                    send_error(self, 'Cannot deactivate your own account', 400); return
                result = update_user(uid, {'active': 0}, sess['role'], sess['userId'])
                send_json(self, result)
                return

            # DELETE /api/ingredients/:code  → soft deactivate (admin only)
            if path.startswith('/api/ingredients/') and len(path.split('/')) == 4:
                if not require(sess, 'admin'):
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

            # ── Planning Input System (admin only) ──────────────────
            if (path.startswith('/api/planning/forecast/') and len(path.split('/')) == 5
                    and path.split('/')[4].isdigit()):
                if not _can_plan(sess):
                    send_error(self, 'Planning access required', 403); return
                send_json(self, delete_sales_forecast(int(path.split('/')[4]), sess['username']))
                return
            if (path.startswith('/api/planning/targets/') and len(path.split('/')) == 5
                    and path.split('/')[4].isdigit()):
                if not _can_plan(sess):
                    send_error(self, 'Planning access required', 403); return
                send_json(self, delete_sales_target(int(path.split('/')[4]), sess['username']))
                return
            if (path.startswith('/api/planning/manufacturing/') and len(path.split('/')) == 5
                    and path.split('/')[4].isdigit()):
                if not _can_plan(sess):
                    send_error(self, 'Planning access required', 403); return
                send_json(self, delete_manufacturing(int(path.split('/')[4]), sess['username']))
                return
            if (path.startswith('/api/planning/pricing/') and len(path.split('/')) == 5
                    and path.split('/')[4].isdigit()):
                if not _can_plan(sess):
                    send_error(self, 'Planning access required', 403); return
                send_json(self, delete_pricing(int(path.split('/')[4]), sess['username']))
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


# ═══════════════════════════════════════════════════════════════════
#  COSTING CONFIG — table, seed, get, update
# ═══════════════════════════════════════════════════════════════════


def _get_config_val(cfg, key, default):
    """Helper — extract float from costing_config dict."""
    try:
        return float(cfg[key]['value'])
    except (KeyError, TypeError, ValueError):
        return default


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — ZONES & ROUTES
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — SALES REPS
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — REP TARGETS
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — REP ADVANCES
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — BEAT VISITS & FIELD ORDERS
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  BUSINESS LOGIC — PAYROLL ENGINE
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  FIELD APP AUTH
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
#  B2B PORTAL — FIELD REP BUSINESS LOGIC
# ═══════════════════════════════════════════════════════════════════


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
from modules.products   import *   # create_product, create_product_variant, update_product, deactivate_product, deactivate_variant, import_products_master, ensure_variant_wastage_pct, ensure_variant_gtin
from modules.inventory   import *   # get_stock_map, get_wo_reserved_stock_map, get_finished_stock_map, get_soft_hold_qty, get_hard_reserved_qty, get_available_for_soft_hold, get_stock_situation, create_adjustment, create_ingredient, update_ingredient, bulk_update_ingredient_costs, deactivate_ingredient, reactivate_ingredient, import_ingredients_master
from modules.migrations  import *   # ensure_full_schema, ensure_system_settings_schema, _migrate_invoice_items_line_total, ensure_work_orders_table, ensure_customer_orders_schema, ensure_review_queue_schema, _migrate_supplier_bills_void, _migrate_change_log_void_action, _migrate_customer_type_wholesale, _ensure_b2b_order_columns, ensure_supplier_bills_schema, ensure_purchase_orders_schema, ensure_batch_cost_column, ensure_master_schema, ensure_costing_config, ensure_price_types_sprint6, ensure_price_history_extended, ensure_margin_alerts_table, ensure_web_price_type, ensure_25g_pack_and_spgm25, ensure_web_prices
from modules.orders      import *   # _enforce_credit_limit, _wa_send, _wa_admin, _wa_rep, _wa_notify_order_approved, _wa_notify_order_rejected, _wa_notify_order_received, _wa_notify_hold_expiring, _wa_notify_out_of_route, place_soft_hold, release_soft_hold, convert_soft_hold_to_hard_reservation, check_and_expire_holds, create_customer_order_external, get_review_queue, approve_order_with_edit, update_order_item_qty, reject_order, reopen_rejected_order, _order_status, _order_detail, list_customer_orders, _check_order_stock_warnings, create_customer_order, update_customer_order, add_customer_order_item, confirm_customer_order, cancel_customer_order, create_wo_from_order_item, generate_invoice_from_order
from modules.invoices    import *   # compute_invoice_balance, _compute_invoice_status, _sync_invoice_status, get_ar_aging, create_invoice, add_invoice_item, remove_invoice_item, record_customer_payment, allocate_customer_payment, pay_invoice_direct, deallocate_payment, adjust_invoice, void_invoice, generate_invoice_pdf, generate_statement_pdf, _pdf_colors, _pkr
from modules.purchasing  import *   # compute_bill_balance, _compute_bill_status, _sync_bill_status, get_ap_aging, create_supplier_bill, update_supplier_bill, record_supplier_payment, allocate_supplier_payment, pay_bill_direct, deallocate_supplier_payment, adjust_bill, void_supplier_bill, list_purchase_orders, get_purchase_order, create_purchase_order, update_purchase_order, update_purchase_order_status, bom_calculate_ingredients, generate_po_pdf
from modules.production  import *   # check_wo_feasibility, get_procurement_list, list_work_orders, create_work_order, convert_wo_to_batch, update_work_order, update_work_order_status, create_production_batch, create_or_update_bom, import_bom_master
from modules.costing     import *   # get_costing_config, compute_standard_cost, get_all_standard_costs, get_batch_variances, update_costing_config, set_product_price, import_prices_master, get_price_history, get_ingredient_price_history, seed_price_history, get_margin_alerts, dismiss_margin_alert, send_margin_alert_email
from modules.reports     import *   # get_dashboard, get_pl_report, get_rep_performance_report, get_margin_report
from modules.planning    import *   # plan_version + sales forecast/target CRUD, projected_sales (Planning Input Module)
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
    # Run all startup migrations/seeds with PER-STEP GUARDS so a single failure on a
    # given environment's data can never crash the boot — the server must always reach
    # serve_forever (Railway treats a startup exit as a crash and rolls back).
    for _step in (
        ensure_full_schema, _migrate_invoice_items_line_total, ensure_users_table,
        ensure_sessions_table, ensure_rate_limit_table, ensure_work_orders_table,
        ensure_customer_orders_schema, ensure_supplier_bills_schema, ensure_purchase_orders_schema,
        ensure_batch_cost_column, ensure_costing_config, ensure_variant_wastage_pct,
        ensure_variant_gtin, ensure_variant_show_online, ensure_clean_product_codes,
        ensure_clean_customer_codes, ensure_clean_supplier_codes, _reset_admin_pw_if_requested,
        _migrate_supplier_bills_void, _migrate_change_log_void_action, _migrate_customer_type_wholesale,
        _ensure_b2b_order_columns, ensure_system_settings_schema, _reload_wa_from_db,
        ensure_review_queue_schema, ensure_master_schema, ensure_price_types_sprint6,
        ensure_price_history_extended, ensure_margin_alerts_table, ensure_field_otp_table,
        ensure_ingredient_price_volatile, ensure_ingredient_unit_kg, ensure_web_price_type, ensure_25g_pack_and_spgm25,
        ensure_web_prices, ensure_recipe_tables, ensure_change_log_reason,
        ensure_planning_foundations, ensure_plan_version_horizon, ensure_plan_sales_tables,
        ensure_plan_m2_tables, ensure_plan_code, ensure_plan_release,
        ensure_scenario_type_cleanup, ensure_plan_forecast_zone, ensure_operating_costs,
        backfill_customer_account_numbers,
    ):
        try:
            _step()
        except Exception as _se:
            import traceback; traceback.print_exc()
            print(f"  ⚠ startup step {getattr(_step, '__name__', _step)} FAILED (continuing): {_se}")
    try:
        load_ref()
    except Exception as _se:
        import traceback; traceback.print_exc()
        print(f"  ⚠ load_ref FAILED (continuing): {_se}")
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
    for _step in (generate_master_templates, sync_master_files, seed_price_history, seed_zones_routes):
        try:
            _step()
        except Exception as _se:
            import traceback; traceback.print_exc()
            print(f"  ⚠ startup step {getattr(_step, '__name__', _step)} FAILED (continuing): {_se}")

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
