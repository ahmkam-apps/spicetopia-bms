#!/usr/bin/env bash
# R0 regression runner — boots a throwaway server on a COPY of the DB, runs the
# full test suite against it, compares to tests/baseline.json, then tears down.
#
# Usage:
#   tests/run_local.sh                 # compare against baseline (regression gate)
#   tests/run_local.sh --baseline      # re-capture baseline.json (after intended changes)
#   tests/run_local.sh --module phase0 # run a single module
#
# Exit code: 0 if the run passes, non-zero on any regression/failure.
set -u
HERE="$(cd "$(dirname "$0")/.." && pwd)"          # spicetopia-bms/
SRC_DB="${BMS_SRC_DB:-$HERE/data/spicetopia.db}"   # DB to copy (never mutated)
PORT="${BMS_TEST_PORT:-3001}"
WORK="$(mktemp -d)"
DB="$WORK/test.db"
MODE="${1:---compare}"

cleanup() { pkill -9 -f "server.py" >/dev/null 2>&1; rm -rf "$WORK"; }
trap cleanup EXIT

if [ ! -f "$SRC_DB" ]; then echo "✗ source DB not found: $SRC_DB"; exit 2; fi
cp -f "$SRC_DB" "$DB"

echo "→ booting test server on :$PORT (DB copy: $DB)"
DB_PATH="$DB" PORT="$PORT" BACKUP_PATH="$WORK/backups" WA_ENABLED=0 DEV_TOOLS=0 \
    setsid python3 "$HERE/server.py" >"$WORK/server.log" 2>&1 </dev/null &

# wait for health (max ~20s)
for i in $(seq 1 20); do
  if curl -s -m 3 "http://localhost:$PORT/api/health" >/dev/null 2>&1; then break; fi
  sleep 1
done
if ! curl -s -m 3 "http://localhost:$PORT/api/health" >/dev/null 2>&1; then
  echo "✗ server did not come up — log:"; tail -20 "$WORK/server.log"; exit 3
fi

echo "→ running suite ($MODE)"
BMS_URL="http://localhost:$PORT" python3 "$HERE/tests/run_all.py" "$@"
RC=$?
echo "→ suite exit code: $RC"
exit $RC
