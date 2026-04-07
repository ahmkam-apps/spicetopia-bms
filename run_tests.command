#!/bin/bash
# ────────────────────────────────────────────────────────────────
#  Spicetopia ERP — Test Runner
#  Double-click to run. First time: right-click → Open
# ────────────────────────────────────────────────────────────────

cd "$(dirname "$0")"

GREEN='\033[92m'
RED='\033[91m'
YELLOW='\033[93m'
CYAN='\033[96m'
BOLD='\033[1m'
RESET='\033[0m'

clear
echo ""
echo "╔══════════════════════════════════════════╗"
echo "║     SPICETOPIA ERP — TEST RUNNER         ║"
echo "╚══════════════════════════════════════════╝"
echo ""

# ── 1. Check Python 3 ─────────────────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
  echo -e "${RED}ERROR: Python 3 is not installed.${RESET}"
  read -p "Press Enter to close..."; exit 1
fi

# ── 2. Install requests if missing ────────────────────────────────────────────
python3 -c "import requests" 2>/dev/null
if [ $? -ne 0 ]; then
  echo "Installing 'requests' package (one-time setup)..."
  INSTALLED=0

  # Try 1: pip3 with --break-system-packages (Linux/newer macOS)
  pip3 install requests --quiet --break-system-packages 2>/dev/null && INSTALLED=1

  # Try 2: plain pip3 (older macOS)
  if [ $INSTALLED -eq 0 ]; then
    pip3 install requests --quiet 2>/dev/null && INSTALLED=1
  fi

  # Try 3: python3 -m pip (fallback)
  if [ $INSTALLED -eq 0 ]; then
    python3 -m pip install requests --quiet 2>/dev/null && INSTALLED=1
  fi

  # Try 4: python3 -m pip with --user (no sudo needed)
  if [ $INSTALLED -eq 0 ]; then
    python3 -m pip install requests --quiet --user 2>/dev/null && INSTALLED=1
  fi

  if [ $INSTALLED -eq 0 ]; then
    echo -e "${RED}ERROR: Could not install 'requests' automatically.${RESET}"
    echo ""
    echo "Please run this manually in Terminal, then try again:"
    echo "  pip3 install requests"
    echo "  OR"
    echo "  python3 -m pip install requests --user"
    echo ""
    read -p "Press Enter to close..."; exit 1
  fi

  echo "Done."
  echo ""
fi

# ── 3. Check if server is running on port 3001 ────────────────────────────────
echo -e "${CYAN}Checking server on port 3001...${RESET}"
if ! curl -s --max-time 3 http://localhost:3001/ -o /dev/null 2>/dev/null; then
  echo ""
  echo -e "${YELLOW}⚠ Server is not running on port 3001.${RESET}"
  echo ""
  echo "Options:"
  echo "  [1] Start the server automatically (in background)"
  echo "  [2] I will start it myself — wait 15 seconds"
  echo "  [3] Cancel"
  echo ""
  read -p "Choose [1/2/3]: " choice

  case "$choice" in
    1)
      echo "Starting server in background..."
      python3 server.py &>/tmp/spicetopia_server.log &
      SERVER_PID=$!
      echo "Server PID: $SERVER_PID"
      echo "Waiting for server to come up..."
      for i in {1..10}; do
        sleep 1
        if curl -s --max-time 2 http://localhost:3001/ -o /dev/null 2>/dev/null; then
          echo -e "${GREEN}✓ Server is up.${RESET}"
          break
        fi
        echo -n "."
      done
      if ! curl -s --max-time 2 http://localhost:3001/ -o /dev/null 2>/dev/null; then
        echo -e "${RED}ERROR: Server did not start. Check /tmp/spicetopia_server.log${RESET}"
        read -p "Press Enter to close..."; exit 1
      fi
      ;;
    2)
      echo "Waiting 15 seconds for you to start the server..."
      sleep 15
      if ! curl -s --max-time 3 http://localhost:3001/ -o /dev/null 2>/dev/null; then
        echo -e "${RED}ERROR: Server still not reachable. Aborting.${RESET}"
        read -p "Press Enter to close..."; exit 1
      fi
      echo -e "${GREEN}✓ Server detected.${RESET}"
      ;;
    *)
      echo "Cancelled."
      exit 0
      ;;
  esac
else
  echo -e "${GREEN}✓ Server is running.${RESET}"
fi

echo ""

# ── 4. Choose which module to run ─────────────────────────────────────────────
echo "Which tests do you want to run?"
echo ""
echo "  [1] Full suite (all 13 modules)         ← recommended"
echo "  [2] Integration flow only               (cross-module)"
echo "  [3] Customer Orders only"
echo "  [4] Sales only"
echo "  [5] Production only"
echo "  [6] Custom module name"
echo ""
read -p "Choose [1-6, default=1]: " run_choice
run_choice="${run_choice:-1}"

case "$run_choice" in
  1) MODULE_ARG="" ;;
  2) MODULE_ARG="--module integration" ;;
  3) MODULE_ARG="--module orders" ;;
  4) MODULE_ARG="--module sales" ;;
  5) MODULE_ARG="--module production" ;;
  6)
    read -p "Module name: " custom_mod
    MODULE_ARG="--module $custom_mod"
    ;;
  *) MODULE_ARG="" ;;
esac

echo ""
echo -e "${BOLD}Running tests...${RESET}"
echo "────────────────────────────────────────────────────────────"
echo ""

# ── 5. Run the tests ──────────────────────────────────────────────────────────
python3 test_erp.py $MODULE_ARG
EXIT_CODE=$?

echo ""
echo "────────────────────────────────────────────────────────────"

# ── 6. Show report location and open it ──────────────────────────────────────
REPORT_FILE="test_reports/latest.txt"
if [ -f "$REPORT_FILE" ]; then
  echo ""
  echo -e "${CYAN}Report saved → test_reports/latest.txt${RESET}"
  echo ""
  read -p "Open report in TextEdit? [y/N]: " open_choice
  if [[ "$open_choice" =~ ^[Yy]$ ]]; then
    open -a TextEdit "$REPORT_FILE" 2>/dev/null || open "$REPORT_FILE"
  fi
fi

echo ""
if [ $EXIT_CODE -eq 0 ]; then
  echo -e "${GREEN}${BOLD}All tests passed ✓${RESET}"
else
  echo -e "${RED}${BOLD}Some tests failed — see report above.${RESET}"
fi

echo ""
read -p "Press Enter to close..."
