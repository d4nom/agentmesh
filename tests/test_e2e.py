"""Full pipeline test against a live docker compose stack: `make up` first,
then `uv run pytest -m e2e`."""

from __future__ import annotations

import os
import time

import pytest

from platform_core.bus import connect, ensure_streams, publish, pull_subscribe
from platform_core.envelope import Envelope, new_id

pytestmark = pytest.mark.e2e

CONNECTION_EXHAUSTION_LOG = (
    "FATAL: remaining connection slots are reserved for non-replication superuser connections"
)


async def test_incident_triage_pipeline_completes():
    nats_url = os.environ.get("NATS_URL", "nats://localhost:4222")
    nc, js = await connect(nats_url)
    await ensure_streams(js)

    sub = await pull_subscribe(js, "events.task.completed", durable="test-e2e-completed")

    correlation_id = new_id()
    envelope = Envelope(
        sender="test",
        subject="tasks.parse",
        type="task",
        correlation_id=correlation_id,
        payload={"raw_log": CONNECTION_EXHAUSTION_LOG, "host": "pg-test-01"},
    )
    await publish(js, "tasks.parse", envelope)

    result: Envelope | None = None
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline and result is None:
        try:
            msgs = await sub.fetch(1, timeout=2)
        except TimeoutError:
            continue
        for msg in msgs:
            candidate = Envelope.model_validate_json(msg.data)
            await msg.ack()
            if candidate.correlation_id == correlation_id:
                result = candidate

    await nc.drain()

    assert result is not None, "did not observe events.task.completed for the injected incident"
    assert result.payload["incident"]["error_class"] == "connection_exhaustion"
    assert result.payload["plan"]["action"]
    assert result.payload["plan"]["commands"]


async def test_unprocessable_message_is_dead_lettered_after_max_deliver():
    """A payload that fails the executor's own pydantic validation raises on
    every delivery attempt, so the SDK must exhaust max_deliver=5 and publish
    to dlq.execute instead of redelivering forever."""
    nats_url = os.environ.get("NATS_URL", "nats://localhost:4222")
    nc, js = await connect(nats_url)
    await ensure_streams(js)

    sub = await pull_subscribe(js, "dlq.execute", durable="test-e2e-dlq")

    correlation_id = new_id()
    envelope = Envelope(
        sender="test",
        subject="tasks.execute",
        type="task",
        correlation_id=correlation_id,
        payload={"this_payload": "does_not_match_ExecutionInput"},
    )
    await publish(js, "tasks.execute", envelope)

    result: Envelope | None = None
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline and result is None:
        try:
            msgs = await sub.fetch(1, timeout=2)
        except TimeoutError:
            continue
        for msg in msgs:
            candidate = Envelope.model_validate_json(msg.data)
            await msg.ack()
            if candidate.correlation_id == correlation_id:
                result = candidate

    await nc.drain()

    assert result is not None, "message never reached dlq.execute after max_deliver"
    assert result.type == "error"
    assert "error" in result.payload
