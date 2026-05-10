#!/usr/bin/env bash
# sprint_prod_push.sh — Push current master to PROD after Claude gives go-ahead
# Usage: ./sprint_prod_push.sh
# Must be run from inside spicetopia-erv-v2/

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

ok()   { echo -e "${GREEN}  ✓ $*${RESET}"; }
fail() { echo -e "${RED}  ✗ $*${RESET}"; exit 1; }
info() { echo -e "${CYAN}  → $*${RESET}"; }
hr()   { echo -e "${CYAN}────────────────────────────────────────────────${RESET}"; }

hr
echo -e "${BOLD}  Spicetopia BMS — PROD Push${RESET}"
hr

info "Pushing master to PROD..."
git push origin master
ok "Pushed — Railway PROD auto-deploy triggered (~60-90s)"
echo ""
echo -e "${GREEN}  ✅ Sprint shipped to PROD. Paste this output to Claude.${RESET}"
echo ""
