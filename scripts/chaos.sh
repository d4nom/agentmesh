#!/usr/bin/env bash
# Kill the executor mid-flight and prove the system recovers on its own.
#
# `docker compose kill` is an operator-requested stop, so Docker deliberately
# does NOT apply the restart policy to it. To actually exercise the restart
# policy, we send SIGKILL to the runner process from *inside* the running
# container instead (`docker compose exec ... pkill`) — from Docker's point
# of view the container just died on its own, exactly like a real crash.
#
# The chaos config gives executor a 10-second processing delay. This script
# first observes `execution_started` for the injected correlation_id, records
# Docker's RestartCount, kills the runner, and only passes after RestartCount
# increases and the same message succeeds with num_delivered >= 2.
set -euo pipefail

echo "==> waiting for parser/rag/executor to become healthy"
healthy=0
for i in $(seq 1 40); do
  healthy_count=$(docker compose ps parser rag executor --format '{{.Health}}' 2>/dev/null \
    | grep -c '^healthy$' || true)
  if [ "$healthy_count" = "3" ]; then
    healthy=1
    break
  fi
  sleep 2
done
if [ "$healthy" -ne 1 ]; then
  echo "FAIL: parser/rag/executor did not all become healthy" >&2
  docker compose ps parser rag executor >&2
  exit 1
fi

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

echo "==> waiting until executor is inside handle()"
deadline=$((SECONDS + 30))
started=0
while [ "$SECONDS" -lt "$deadline" ]; do
  matching_logs=$(docker compose logs executor 2>/dev/null \
    | grep -F "\"correlation_id\": \"$correlation_id\"" || true)
  if grep -q '"event": "execution_started"' <<< "$matching_logs"; then
    started=1
    break
  fi
  sleep 1
done
if [ "$started" -ne 1 ]; then
  echo "FAIL: executor never entered handle() for correlation_id=$correlation_id" >&2
  docker compose logs --tail=100 executor >&2
  exit 1
fi

container_id=$(docker compose ps -q executor)
if [ -z "$container_id" ]; then
  echo "FAIL: could not resolve executor container ID" >&2
  exit 1
fi
restart_before=$(docker inspect --format '{{.RestartCount}}' "$container_id")

echo "==> executor is mid-flight; killing its runner process"
set +e
docker compose exec -T executor pkill -9 -f platform_core.runner
kill_status=$?
set -e
echo "    kill command exit status: $kill_status (restart verification is authoritative)"

echo "==> waiting for restart policy to bring executor back up and healthy"
restarted=0
for i in $(seq 1 30); do
  current_container_id=$(docker compose ps -q executor)
  health=$(docker compose ps executor --format '{{.Health}}' 2>/dev/null || echo "")
  restart_after=$(docker inspect --format '{{.RestartCount}}' "$container_id" 2>/dev/null \
    || echo "$restart_before")
  if [ "$current_container_id" = "$container_id" ] \
      && [ "$restart_after" -gt "$restart_before" ] \
      && [ "$health" = "healthy" ]; then
    restarted=1
    echo "    executor restarted and is healthy (RestartCount $restart_before -> $restart_after)"
    break
  fi
  sleep 2
done
if [ "$restarted" -ne 1 ]; then
  echo "FAIL: executor did not restart and return healthy" >&2
  docker compose ps executor >&2
  docker inspect --format '{{json .State}}' "$container_id" >&2 || true
  exit 1
fi

echo "==> waiting up to 90s for redelivered handle_succeeded on correlation_id=$correlation_id"
deadline=$((SECONDS + 90))
found=0
while [ "$SECONDS" -lt "$deadline" ]; do
  matching_logs=$(docker compose logs executor 2>/dev/null \
    | grep -F "\"correlation_id\": \"$correlation_id\"" || true)
  succeeded_logs=$(grep '"event": "handle_succeeded"' <<< "$matching_logs" || true)
  if grep -Eq '"num_delivered": ([2-9]|[1-9][0-9]+)' <<< "$succeeded_logs"; then
    found=1
    break
  fi
  sleep 2
done

if [ "$found" -ne 1 ]; then
  echo "FAIL: executor never logged a redelivered handle_succeeded within 90s" >&2
  docker compose logs --tail=100 executor >&2
  exit 1
fi

echo "==> PASS: restart policy recovered the container, JetStream redelivered the"
echo "    unacked message, and the task completed:"
printf '%s\n' "$matching_logs"
