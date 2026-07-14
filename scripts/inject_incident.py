"""Publish a synthetic PostgreSQL incident onto tasks.parse to kick off the pipeline."""

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

INCIDENT_SCENARIOS: dict[str, str] = {
    "connection_exhaustion": (
        "2026-07-14 03:12:01 UTC [21044] FATAL: remaining connection slots are reserved "
        "for non-replication superuser connections\n"
        "2026-07-14 03:12:01 UTC [21044] DETAIL: 97 of 100 max_connections already used."
    ),
    "wal_disk_full": (
        '2026-07-14 04:41:09 UTC [8821] PANIC: could not write to file '
        '"pg_wal/000000010000000A000000F3": No space left on device\n'
        "2026-07-14 04:41:09 UTC [8821] LOG: pg_wal disk usage at 100%, replication slot "
        "'analytics_sub' has been inactive for 6 hours."
    ),
    "replication_lag": (
        "2026-07-14 02:05:33 UTC [5510] WARNING: streaming replication delay to replica "
        "10.0.4.22 exceeds 300s, replay_lag=00:08:12"
    ),
    "bloat_vacuum": (
        '2026-07-14 01:00:00 UTC [3390] LOG: autovacuum: found 1834021 dead tuples in '
        'table "orders", autovacuum is falling behind'
    ),
}


async def main() -> None:
    arg_parser = argparse.ArgumentParser(description=__doc__)
    arg_parser.add_argument(
        "--scenario", choices=sorted(INCIDENT_SCENARIOS), default="connection_exhaustion"
    )
    arg_parser.add_argument("--host", default="pg-primary-01")
    args = arg_parser.parse_args()

    nats_url = os.environ.get("NATS_URL", "nats://localhost:4222")
    otel_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    init_observability(service_name="inject_incident", otel_endpoint=otel_endpoint)
    log = get_logger(agent="inject_incident")

    nc, js = await connect(nats_url)
    await ensure_streams(js)

    tracer = get_tracer("agentmesh.inject_incident")
    correlation_id = new_id()
    with tracer.start_as_current_span("inject_incident"):
        envelope = Envelope(
            sender="inject_incident",
            subject="tasks.parse",
            type="task",
            correlation_id=correlation_id,
            traceparent=inject_traceparent(),
            payload={"raw_log": INCIDENT_SCENARIOS[args.scenario], "host": args.host},
        )
        await publish(js, "tasks.parse", envelope)

    log.info("incident_injected", scenario=args.scenario, correlation_id=correlation_id)
    await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
