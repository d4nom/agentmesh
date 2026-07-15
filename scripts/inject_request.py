"""Publish a maintenance request onto tasks.parse_request to kick off the pipeline."""

from __future__ import annotations

import argparse
import asyncio
import os

from platform_core.bus import connect, ensure_streams, publish
from platform_core.envelope import Envelope, new_id
from platform_core.observability import (
    get_logger,
    get_tracer,
    init_observability,
    inject_traceparent,
)

REQUEST_SCENARIOS: dict[str, dict] = {
    "os_update": {
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
    },
    "cert_renewal_tight_sla": {
        "request_id": "REQ-2024-0212",
        "priority": "medium",
        "object": "cluster-456",
        "object_type": "patroni_cluster",
        "purpose": "scheduled_maintenance",
        "subtasks": [
            {"order": 1, "action": "renew_certificates", "constraints": "standby before leader"},
        ],
        "sla_minutes": 30,
        "is_downtime": False,
    },
    "invalid_object_type": {
        "request_id": "REQ-2024-0301",
        "priority": "high",
        "object": "cluster-789",
        "object_type": "mongodb_cluster",
        "purpose": "risk_mitigation",
        "subtasks": [
            {"order": 1, "action": "update_os", "constraints": None},
        ],
        "sla_minutes": 30,
        "is_downtime": False,
    },
    "invalid_duplicate_order": {
        "request_id": "REQ-2024-0305",
        "priority": "low",
        "object": "cluster-999",
        "object_type": "postgres_standalone",
        "purpose": "compliance",
        "subtasks": [
            {"order": 1, "action": "run_compliance_check", "constraints": None},
            {"order": 1, "action": "check_access", "constraints": None},
        ],
        "sla_minutes": 60,
        "is_downtime": False,
    },
}


async def main() -> None:
    arg_parser = argparse.ArgumentParser(description=__doc__)
    arg_parser.add_argument("--scenario", choices=sorted(REQUEST_SCENARIOS), default="os_update")
    args = arg_parser.parse_args()

    nats_url = os.environ.get("NATS_URL", "nats://localhost:4222")
    otel_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    init_observability(service_name="inject_request", otel_endpoint=otel_endpoint)
    log = get_logger(agent="inject_request")

    nc, js = await connect(nats_url)
    await ensure_streams(js)

    tracer = get_tracer("agentmesh.inject_request")
    correlation_id = new_id()
    with tracer.start_as_current_span("inject_request"):
        envelope = Envelope(
            sender="inject_request",
            subject="tasks.parse_request",
            type="task",
            correlation_id=correlation_id,
            traceparent=inject_traceparent(),
            payload=REQUEST_SCENARIOS[args.scenario],
        )
        await publish(js, "tasks.parse_request", envelope)

    log.info("request_injected", scenario=args.scenario, correlation_id=correlation_id)
    await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
