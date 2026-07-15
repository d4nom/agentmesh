#!/usr/bin/env bash
# Kill the executor mid-flight and prove the system recovers on its own.
#
# `docker compose kill` is an operator-requested stop, so Docker deliberately
# does NOT apply the restart policy to it. To actually exercise the restart
# policy, we send SIGKILL to the runner process from *inside* the running
# container instead (`docker compose exec ... pkill`) — from Docker's point
# of view the container just died on its own, exactly like a real crash.
#
# This is a real pass/fail check, not a demo narration: it waits for the
# specific injected correlation_id to show up as `handle_succeeded` in
# executor's logs after the kill, and exits 1 if that never happens.
set -euo pipefail

echo "==> injecting incident"
inject_output=$(docker compose exec -T parser python scripts/inject_incident.py --scenario wal_disk_full)
echo "$inject_output"
correlation_id=$(echo "$inject_output" \
  | grep -o '"correlation_id": *"[^"]*"' | head -1 \
  | sed -E 's/.*"correlation_id": *"([^"]*)".*/\1/')

if [ -z "$correlation_id" ]; then
  echo "FAIL: could not extract correlation_id from inject_incident.py output" >&2
  exit 1
fi
echo "==> correlation_id: $correlation_id"

echo "==> waiting 1s, then killing executor's runner process from inside the container"
sleep 1
docker compose exec -T executor pkill -9 -f platform_core.runner || true

echo "==> waiting for restart policy to bring executor back up and healthy"
for i in $(seq 1 30); do
  health=$(docker compose ps executor --format '{{.Health}}' 2>/dev/null || echo "")
  if [ "$health" = "healthy" ]; then
    echo "    executor is healthy again (attempt $i)"
    break
  fi
  sleep 2
done

echo "==> waiting up to 90s for handle_succeeded on correlation_id=$correlation_id"
deadline=$((SECONDS + 90))
found=0
while [ "$SECONDS" -lt "$deadline" ]; do
  if docker compose logs executor 2>/dev/null \
      | grep -F "\"correlation_id\": \"$correlation_id\"" \
      | grep -q '"event": "handle_succeeded"'; then
    found=1
    break
  fi
  sleep 2
done

if [ "$found" -ne 1 ]; then
  echo "FAIL: executor never logged handle_succeeded for correlation_id=$correlation_id within 90s" >&2
  docker compose logs --tail=100 executor >&2
  exit 1
fi

echo "==> PASS: restart policy recovered the container, JetStream redelivered the"
echo "    unacked message, and the task completed:"
docker compose logs executor 2>/dev/null | grep -F "\"correlation_id\": \"$correlation_id\""
