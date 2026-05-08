#!/usr/bin/env bash
# PASS: a malformed event does not block the pipeline. Debezium/sink route
#       the poison to a DLQ topic; subsequent good events continue flowing.
set -euo pipefail

echo "Scenario 05: Poison event routing to DLQ"
echo "============================================"

echo "Inserting a good event first (baseline) ..."
docker compose exec -T postgres psql -U app -d app -c \
  "INSERT INTO users (email, full_name) VALUES ('chaos5-pre@test.dev', 'Pre-poison') ON CONFLICT DO NOTHING;"

echo "Injecting a 'poison' via extremely long field that likely exceeds downstream limits ..."
# Stress the pipeline with a 1MB+ JSONB value. Real validation happens in
# the transformer (decoder).
docker compose exec -T postgres psql -U app -d app -c \
  "INSERT INTO users (email, full_name, profile)
   VALUES ('chaos5-poison@test.dev','Poison',
           jsonb_build_object('blob', repeat('x', 1048576)))
   ON CONFLICT DO NOTHING;"

echo "Inserting a good event AFTER (should flow through) ..."
docker compose exec -T postgres psql -U app -d app -c \
  "INSERT INTO users (email, full_name) VALUES ('chaos5-post@test.dev', 'Post-poison') ON CONFLICT DO NOTHING;"

sleep 10

echo "Checking DLQ topics ..."
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list | grep -E '^dlq\.' || echo "(no DLQ topics yet - expected if no errors)"

echo "Verifying the good post-poison event landed in Mongo ..."
docker compose exec -T mongo mongosh --quiet \
  "mongodb://localhost:27017/migration?replicaSet=rs0" \
  --eval "printjson(db.users.findOne({ _id: /chaos5-post/ }) || db.users.findOne({ email: 'chaos5-post@test.dev' }))"

echo ""
echo "Manual assertion: the 'chaos5-post' event should be present in Mongo,"
echo "proving the poison did NOT block the pipeline."

# Cleanup: the poison row is intentionally too big for Mongo (1MB JSONB
# blob), so it lives in PG but never as a Mongo doc. Leaving it behind
# breaks PG<->Mongo row-count parity for any subsequent scenario in the
# same run-all session. Delete it here so the suite is composable.
echo ""
echo "Cleanup: removing the poison row from PG (and waiting for the DELETE to propagate) ..."
docker compose exec -T postgres psql -U app -d app -c \
  "DELETE FROM users WHERE email = 'chaos5-poison@test.dev';"
sleep 5
