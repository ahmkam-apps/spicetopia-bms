#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# Spicetopia — one-click PROD deploy.
#   • Double-click this file in Finder, OR run:  ./deploy.command "your message"
#   • Clears stale git locks, commits everything, pushes, deploys to PROD.
# ─────────────────────────────────────────────────────────────
set -uo pipefail
cd "$(dirname "$0")" || { echo "Cannot cd to repo"; exit 1; }

echo "──────────────────────────────────────────────"
echo "  Spicetopia → PROD deploy"
echo "  $(pwd)"
echo "──────────────────────────────────────────────"

# 1) Clear any stale git locks (the recurring mounted-volume issue)
rm -f .git/index.lock .git/HEAD.lock 2>/dev/null || true

# 2) Commit message: use the argument, else prompt, else timestamp
MSG="${1:-}"
if [ -z "$MSG" ]; then
  read -r -p "Commit message (Enter for timestamp): " MSG
fi
[ -z "$MSG" ] && MSG="deploy $(date '+%Y-%m-%d %H:%M')"

# 3) Stage + commit (don't abort if there's nothing new to commit)
git add -A
if git commit -m "$MSG"; then
  echo "✓ committed: $MSG"
else
  echo "• nothing new to commit — deploying current code"
fi

# 4) Push to GitHub (runs the overseer pre-push gate)
echo "→ pushing to GitHub (master)…"
if ! git push origin master; then
  echo "✗ git push failed (overseer gate or auth). Fix the above, then re-run."
  read -r -p "Press Enter to close…" _; exit 1
fi

# 5) Target the PROD service and deploy
echo "→ deploying to PROD-SPICETOPIA-BMS…"
railway service PROD-SPICETOPIA-BMS
if ! railway up; then
  echo "✗ railway up failed. Check the output above."
  read -r -p "Press Enter to close…" _; exit 1
fi

echo ""
echo "✓ Deploy started. Verify in ~90s:"
echo "  https://spicetopia-bms-production.up.railway.app/version.txt"
read -r -p "Press Enter to close…" _
