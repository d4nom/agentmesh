"""Second-domain pipeline test against a live docker compose stack:
`make build && docker compose --profile maintenance up -d` first, then
`uv run pytest -m e2e`."""

from __future__ import annotations

import os
import time

import pytest

from platform_core.bus import connect, ensure_streams, publish, pull_subscribe
from platform_core.envelope import Envelope, new_id

pytestmark = pytest.mark.e2e

VALID_REQUEST = {
    "request_id": "REQ-2024-0117",
    "priority": "critical",
    "object": "cluster-123",
    "object_type": "patroni_cluster",
    "purpose": "risk_mitigation",
    "subtasks": [
        {"order": 1, "action": "update_os", "constraints": "rolling, node-by-node"},
        {"order": 2, "action": "renew_certificates", "constraints": None},
        {"order": 3, "action": "run_compliance_check", "constraints": "CIS baseline"},
    ],
    "sla_minutes": 30,
    "is_downtime": False,
}

INVALID_REQUEST = {
    **VALID_REQUEST,
    "request_id": "REQ-2024-0301",
    "object_type": "mongodb_cluster",
}


async def test_valid_request_completes_with_a_validated_plan():
    nats_url = os.environ.get("NATS_URL", "nats://localhost:4222")
    nc, js = await connect(nats_url)
    await ensure_streams(js)

    sub = await pull_subscribe(
        js, "events.maintenance.completed", durable="test-e2e-maintenance-completed"
    )

    correlation_id = new_id()
    envelope = Envelope(
        sender="test",
        subject="tasks.parse_request",
        type="task",
        correlation_id=correlation_id,
        payload=VALID_REQUEST,
    )
    await publish(js, "tasks.parse_request", envelope)

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

    await nc.close()

    assert result is not None, "did not observe events.maintenance.completed for the request"
    assert result.payload["request"]["request_id"] == "REQ-2024-0117"
    plan = result.payload["plan"]
    assert len(plan["plan"]) == 3
    assert plan["total_estimated_minutes"] == 25
    assert plan["sla_verdict"] == "at_risk"


async def test_invalid_request_is_dead_lettered_after_max_deliver():
    nats_url = os.environ.get("NATS_URL", "nats://localhost:4222")
    nc, js = await connect(nats_url)
    await ensure_streams(js)

    sub = await pull_subscribe(js, "dlq.parse_request", durable="test-e2e-maintenance-dlq")

    correlation_id = new_id()
    envelope = Envelope(
        sender="test",
        subject="tasks.parse_request",
        type="task",
        correlation_id=correlation_id,
        payload=INVALID_REQUEST,
    )
    await publish(js, "tasks.parse_request", envelope)

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

    await nc.close()

    assert result is not None, "message never reached dlq.parse_request after max_deliver"
    assert result.type == "error"
    assert "object_type" in result.payload["error"]
