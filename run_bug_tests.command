#!/bin/bash
# Spicetopia ERP — Bug Regression Tests
# Double-click this file on Mac to run all BUG-001 to BUG-008 tests

# Change to the directory this script lives in
cd "$(dirname "$0")"

echo ""
echo "╔══════════════════════════════════════════╗"
echo "║   SPICETOPIA — BUG REGRESSION TESTS      ║"
echo "╚══════════════════════════════════════════╝"
echo ""

# Kill any previous test server on port 8770
echo "  Cleaning up port 8770..."
lsof -ti tcp:8770 | xargs kill -9 2>/dev/null
sleep 1

# Fresh test DB
mkdir -p /tmp/testmount
rm -f /tmp/testmount/spicetopia.db
echo "  Starting test server..."

PORT=8770 DEV_TOOLS=1 NO_BROWSER=1 RAILWAY_VOLUME_MOUNT_PATH=/tmp/testmount \
  python3 server.py > /tmp/spicetopia_test_server.log 2>&1 &
SERVER_PID=$!

# Wait for server to be ready (up to 20s)
for i in $(seq 1 20); do
  sleep 1
  if curl -sf http://localhost:8770/api/health > /dev/null 2>&1; then
    echo "  Server ready (${i}s)"
    break
  fi
  if [ $i -eq 20 ]; then
    echo ""
    echo "  ✗ Server failed to start. Check log: /tmp/spicetopia_test_server.log"
    echo ""
    read -p "  Press Enter to close..."
    exit 1
  fi
done

echo ""

# Run tests
python3 test_bugs.py
TEST_EXIT=$?

echo ""

# Shut down test server
kill $SERVER_PID 2>/dev/null

if [ $TEST_EXIT -eq 0 ]; then
  echo "  ✓ All tests passed. Server stopped."
else
  echo "  ✗ Some tests failed. See output above."
fi

echo ""
read -p "  Press Enter to close..."
