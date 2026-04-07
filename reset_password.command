#!/bin/bash
# ─────────────────────────────────────────────
#  Spicetopia BMS — Password Reset Tool
#  Double-click to run (right-click → Open first time)
# ─────────────────────────────────────────────
cd "$(dirname "$0")"

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║    SPICETOPIA — PASSWORD RESET TOOL      ║"
echo "╚══════════════════════════════════════════╝"
echo ""

python3 - << 'PYEOF'
import sqlite3, hashlib, uuid, os

db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "spicetopia.db")

if not os.path.exists(db_path):
    print(f"ERROR: Database not found at:\n  {db_path}")
    input("\nPress Enter to close...")
    exit(1)

conn = sqlite3.connect(db_path)

NEW_PASS = "spice1"
users = conn.execute("SELECT id, username FROM users").fetchall()

if not users:
    print("ERROR: No users found in database.")
    input("\nPress Enter to close...")
    exit(1)

for uid, uname in users:
    salt = uuid.uuid4().hex[:8]
    pw_hash = hashlib.sha256((salt + NEW_PASS).encode()).hexdigest()
    conn.execute(
        "UPDATE users SET password_hash=?, salt=?, auth_scheme='sha256' WHERE id=?",
        (pw_hash, salt, uid)
    )
    print(f"  ✓ {uname}  →  password reset to: {NEW_PASS}")

conn.commit()
conn.close()

print("")
print("Done! You can now log in with:")
print(f"  Username: admin")
print(f"  Password: {NEW_PASS}")
print("")
PYEOF

read -p "Press Enter to close..."
