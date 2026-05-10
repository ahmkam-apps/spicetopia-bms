#!/usr/bin/env bash
# sprint_deploy.sh — Spicetopia BMS sprint deployment script
# Usage: ./sprint_deploy.sh "Sprint 3: users module"
# Must be run from inside spicetopia-erp-v2/

set -euo pipefail

COMMIT_MSG="${1:-}"
DEV_URL="https://dev-spicetopia-bms-production.up.railway.app"
BMS_PASS="${BMS_PASS:-Gido2dad\$72!2026}"
BOOT_WAIT=90   # seconds to wait for Railway DEV to boot

# ── Colours ───────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

ok()   { echo -e "${GREEN}  ✓ $*${RESET}"; }
fail() { echo -e "${RED}  ✗ $*${RESET}"; exit 1; }
info() { echo -e "${CYAN}  → $*${RESET}"; }
warn() { echo -e "${YELLOW}  ⚠ $*${RESET}"; }
hr()   { echo -e "${CYAN}────────────────────────────────────────────────${RESET}"; }

hr
echo -e "${BOLD}  Spicetopia BMS — Sprint Deploy${RESET}"
hr

# ── Step 0: Commit message required ──────────────────────────────
if [[ -z "$COMMIT_MSG" ]]; then
  fail "Usage: ./sprint_deploy.sh \"Sprint N: description\""
fi

# ── Step 1: Overseer code-only check ─────────────────────────────
info "Running overseer --code-only..."
if ! python3 ../overseer.py --code-only > /tmp/overseer_out.txt 2>&1; then
  cat /tmp/overseer_out.txt
  fail "Overseer failed — fix issues before deploying"
fi
ok "Overseer clean"

# ── Step 2: Git commit ────────────────────────────────────────────
info "Staging and committing..."
git add -A
if git diff --cached --quiet; then
  warn "Nothing to commit — already up to date"
else
  git commit -m "$COMMIT_MSG"
  ok "Committed: $COMMIT_MSG"
fi

# ── Step 3: Deploy to DEV ─────────────────────────────────────────
hr
echo -e "${BOLD}  PHASE 1 — DEV Deploy${RESET}"
hr
info "Deploying to Railway DEV (this uploads + builds)..."
railway up --detach
ok "Upload complete — waiting ${BOOT_WAIT}s for DEV to boot..."
sleep "$BOOT_WAIT"

# ── Step 4: Health check ──────────────────────────────────────────
info "Health check on DEV..."
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${DEV_URL}/api/health" || echo "000")
if [[ "$HTTP_STATUS" != "200" ]]; then
  warn "Health check returned ${HTTP_STATUS} — DEV may still be booting"
  info "Waiting another 30s..."
  sleep 30
  HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${DEV_URL}/api/health" || echo "000")
  if [[ "$HTTP_STATUS" != "200" ]]; then
    fail "DEV health check failed (HTTP ${HTTP_STATUS}) — check Railway build log"
  fi
fi
ok "DEV is live (HTTP ${HTTP_STATUS})"

# ── Step 5: Run test suite ────────────────────────────────────────
info "Running baseline compare against DEV..."
TEST_OUTPUT=$(BMS_URL="$DEV_URL" BMS_PASS="$BMS_PASS" python3 tests/run_all.py --compare 2>&1)
echo "$TEST_OUTPUT"

if echo "$TEST_OUTPUT" | grep -q "No regression vs baseline"; then
  ok "Tests passed — no regression"
  TESTS_PASSED=true
else
  warn "Test regression detected"
  TESTS_PASSED=false
fi

# ── Step 6: Checkpoint ────────────────────────────────────────────
hr
echo -e "${BOLD}  PHASE 1 COMPLETE — DEV Results${RESET}"
hr
if $TESTS_PASSED; then
  echo -e "${GREEN}  ✅ DEV is green — ready for PROD${RESET}"
else
  echo -e "${RED}  ❌ DEV has regressions — DO NOT push to PROD${RESET}"
  echo ""
  echo "  Fix the issues and re-run this script."
  exit 1
fi

echo ""
echo -e "${YELLOW}  ┌─────────────────────────────────────────┐${RESET}"
echo -e "${YELLOW}  │  Review DEV, then paste output to Claude │${RESET}"
echo -e "${YELLOW}  │  Claude will say GO → run Phase 2 below  │${RESET}"
echo -e "${YELLOW}  └─────────────────────────────────────────┘${RESET}"
echo ""
echo "  When Claude gives the go-ahead, run:"
echo -e "  ${BOLD}./sprint_deploy.sh --prod-push${RESET}"
echo ""
