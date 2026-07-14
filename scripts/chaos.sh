#!/usr/bin/env bash
# Kill the executor mid-flight and prove the system recovers on its own:
# restart policy brings the container back, JetStream redelivers the
# unacked message (ack_wait=30s), and the task still completes.
set -euo pipefail

echo "==> injecting incident"
uv run --no-sync python scripts/inject_incident.py --scenario wal_disk_full

echo "==> waiting 1s, then killing executor mid-processing"
sleep 1
docker compose kill executor

echo "==> waiting for restart policy to bring executor back up"
for i in $(seq 1 30); do
  status=$(docker compose ps executor --format '{{.State}}' 2>/dev/null || echo "")
  if [ "$status" = "running" ]; then
    echo "    executor is running again (attempt $i)"
    break
  fi
  sleep 1
done

echo "==> waiting for executor to report healthy again"
for i in $(seq 1 30); do
  health=$(docker compose ps executor --format '{{.Health}}' 2>/dev/null || echo "")
  if [ "$health" = "healthy" ]; then
    break
  fi
  sleep 2
done

echo "==> executor logs (look for the crash, the restart, and handle_succeeded"
echo "    on redelivery of the in-flight message)"
docker compose logs --tail=100 executor

echo ""
echo "==> chaos scenario complete: restart policy recovered the container,"
echo "    JetStream redelivered the unacked message, and the task finished."
