"""
modules/utils.py — Pure stateless utilities for Spicetopia BMS.

Zero external dependencies (no DB, no globals, no config).
Safe to import from any module without circular-import risk.

Exported via `from modules.utils import *` in server.py.
"""
import json
import logging
from datetime import date

__all__ = [
    'r2', 'fmtpkr', 'today',
    'VALID_ROLES', 'ROLE_LABELS', 'CITY_CODE_MAP',
    'require', '_city_to_code',
    'ValidationError', 'validate_fields',
    '_logger', '_log',
]

# ── Logging ───────────────────────────────────────────────────────────────────
# _logger is None until _setup_logging() runs at startup and syncs the instance
# via: import modules.utils as _utils_mod; _utils_mod._logger = _logger

_logger: logging.Logger = None


def _log(level: str, msg: str, **fields):
    """Convenience wrapper — logs to _logger if available, else prints.
    Pass exc_info=True to capture the current exception's stack trace."""
    if _logger is None:
        return
    exc_info = fields.pop('exc_info', False)
    extra = {k: v for k, v in fields.items()}
    getattr(_logger, level)(msg, extra=extra, exc_info=exc_info)

# ── Numeric helpers ────────────────────────────────────────────────────────────

def r2(n):
    try:    return round(float(n or 0), 2)
    except: return 0.0


def fmtpkr(n):
    """Format a number as PKR string for server-side warning messages."""
    try:    return f"PKR {float(n or 0):,.0f}"
    except: return "PKR 0"


def today():
    return date.today().isoformat()


# ── RBAC ──────────────────────────────────────────────────────────────────────

VALID_ROLES = ('admin', 'sales', 'warehouse', 'accountant', 'field_rep', 'user')

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


# ── Input validation ───────────────────────────────────────────────────────────

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


# ── Geography helpers ──────────────────────────────────────────────────────────

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
