#!/bin/bash
set -e

# Setup the DB URL explicitly
export DATABASE_URL=postgres://usr:pwd@localhost:5432/name-postgres
export PORT=8082

echo "Creating dummy excel file for testing..."
# We will use the existing test file
TEST_EXCEL="tests/test_data.xlsx"

# Clear the database table before starting
echo "Clearing objects table..."
psql "$DATABASE_URL" -c "TRUNCATE TABLE objects;" || true

echo "Starting REST API server in background..."
cargo run --bin rest_api > server_output.log 2>&1 &
SERVER_PID=$!

# Wait for server to start, it has to compile so it takes longer
sleep 25

echo "Uploading file via curl..."
curl -X POST "http://127.0.0.1:8082/upload" \
  -F "f=@${TEST_EXCEL}" \
  -F "t=Sheet1" \
  -F "p=" \
  -F "r=5"

echo -e "\nWaiting for database inserts to complete..."
sleep 2

echo "Querying database for top 5 rows in 'objects' table:"
psql "$DATABASE_URL" -c "SELECT id, d, t, p, s, c FROM objects ORDER BY id ASC LIMIT 5;"

echo "Cleaning up..."
kill $SERVER_PID || true
wait $SERVER_PID 2>/dev/null || true
echo "Integration test completed successfully."
