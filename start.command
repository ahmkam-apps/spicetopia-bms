#!/bin/bash
# ────────────────────────────────────────────────────────────────
#  Spicetopia ERP v2 — macOS Launcher
#  Double-click to start. First time: right-click → Open
# ────────────────────────────────────────────────────────────────

cd "$(dirname "$0")"

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║     SPICETOPIA ERP v3 — STARTING         ║"
echo "╚══════════════════════════════════════════╝"
echo ""

# Check Python 3
if ! command -v python3 &>/dev/null; then
  echo "ERROR: Python 3 is not installed."
  echo "Download from https://www.python.org/downloads/"
  read -p "Press Enter to close..."; exit 1
fi

# Check / install reportlab
python3 -c "import reportlab" 2>/dev/null
if [ $? -ne 0 ]; then
  echo "Installing reportlab (one-time setup)..."
  pip3 install reportlab --quiet
  if [ $? -ne 0 ]; then
    echo "ERROR: Could not install reportlab. Try: pip3 install reportlab"
    read -p "Press Enter to close..."; exit 1
  fi
  echo "reportlab installed."
  echo ""
fi

# Check database exists
if [ ! -f "data/spicetopia.db" ]; then
  echo "ERROR: Database not found at data/spicetopia.db"
  echo "Run the migration script first:"
  echo "  cd ../migration-plan && python3 migrate.py"
  read -p "Press Enter to close..."; exit 1
fi

# Kill any existing server on port 3001
EXISTING=$(lsof -ti :3001 2>/dev/null)
if [ -n "$EXISTING" ]; then
  echo "Stopping previous server instance..."
  kill "$EXISTING" 2>/dev/null
  sleep 1
fi

echo "Starting Spicetopia ERP v2..."
python3 server.py
