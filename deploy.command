#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
# Spicetopia — one-click deploy to PROD + DEV.
#   • Double-click this file in Finder, OR run:  ./deploy.command "your message"
#   • Clears stale git locks, commits everything, pushes, deploys to PROD then DEV,
#     and re-links the CLI to PROD at the end.
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

# 5) Deploy to PROD (the important one — abort if this fails)
echo "→ deploying to PROD-SPICETOPIA-BMS…"
railway service PROD-SPICETOPIA-BMS
if ! railway up; then
  echo "✗ railway up (PROD) failed. Check the output above."
  read -r -p "Press Enter to close…" _; exit 1
fi

# 6) Also deploy to DEV (best-effort — a DEV hiccup must NOT fail the run; PROD is already done)
#    ⚠ DEV's Watch Paths must be EMPTY in the Railway dashboard, or 'railway up' silently
#      no-ops ("No changes to watched files"). Dashboard → DEV → Settings → Build → Watch Paths.
echo "→ deploying to DEV-SPICETOPIA-BMS…"
railway service DEV-SPICETOPIA-BMS
if railway up; then
  echo "✓ DEV deploy started."
else
  echo "⚠ DEV deploy skipped/failed (PROD is fine). Check that DEV Watch Paths are empty."
fi

# 7) Re-link to PROD so any later plain 'railway up' targets PROD, never DEV
railway service PROD-SPICETOPIA-BMS

echo ""
echo "✓ Deploy started. Verify in ~90s:"
echo "  PROD: https://spicetopia-bms-production.up.railway.app/version.txt"
echo "  DEV:  https://dev-spicetopia-bms-production.up.railway.app/version.txt"
read -r -p "Press Enter to close…" _
