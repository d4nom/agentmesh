"""Wait for one exact terminal envelope in a JetStream limits-retention stream.

The waiter creates a uniquely named consumer for each invocation and deletes it
before exiting.  It is intentionally limited to exact subjects on
limits-retention streams: using a diagnostic consumer on a work-queue stream
could compete with an agent for task delivery.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import re
import sys
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from nats.js.api import ConsumerConfig, DeliverPolicy, RetentionPolicy
from nats.js.errors import NotFoundError
from pydantic import ValidationError

from platform_core.bus import connect
from platform_core.envelope import Envelope

DEFAULT_WAIT_SECONDS = 60.0
DEFAULT_LOOKBACK_SECONDS = 300.0
FETCH_BATCH_SIZE = 32
FETCH_SLICE_SECONDS = 2.0
WAITER_INACTIVE_THRESHOLD_SECONDS = 300.0


def non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def _consumer_name(subject: str) -> str:
    safe_subject = re.sub(r"[^a-zA-Z0-9_-]+", "-", subject).strip("-")[:32] or "message"
    return f"agentmesh-wait-{safe_subject}-{uuid.uuid4().hex[:12]}"


def _matches(
    envelope: Envelope,
    *,
    subject: str,
    correlation_id: str | None,
    sender: str | None,
    message_type: str | None,
) -> bool:
    return (
        envelope.subject == subject
        and (correlation_id is None or envelope.correlation_id == correlation_id)
        and (sender is None or envelope.sender == sender)
        and (message_type is None or envelope.type == message_type)
    )


async def wait_for_envelope(
    js: Any,
    *,
    subject: str,
    correlation_id: str | None,
    wait_seconds: float,
    lookback_seconds: float,
    sender: str | None = None,
    message_type: str | None = None,
) -> Envelope | None:
    """Return the matching envelope, or ``None`` when the deadline expires."""
    if "*" in subject or ">" in subject:
        raise ValueError("subject must be exact; wildcards are intentionally unsupported")

    stream = await js.find_stream_name_by_subject(subject)
    stream_info = await js.stream_info(stream)
    if stream_info.config.retention == RetentionPolicy.WORK_QUEUE:
        raise ValueError(f"refusing to attach a diagnostic consumer to work-queue stream {stream}")

    consumer = _consumer_name(subject)
    config = ConsumerConfig(
        deliver_policy=DeliverPolicy.BY_START_TIME,
        opt_start_time=datetime.now(UTC) - timedelta(seconds=lookback_seconds),
        inactive_threshold=WAITER_INACTIVE_THRESHOLD_SECONDS,
    )
    subscription = await js.pull_subscribe(
        subject,
        durable=consumer,
        stream=stream,
        config=config,
    )

    deadline = time.monotonic() + wait_seconds
    first_fetch = True
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining < 0 and not first_fetch:
                return None

            try:
                first_fetch = False
                messages = await subscription.fetch(
                    FETCH_BATCH_SIZE,
                    timeout=max(0.05, min(FETCH_SLICE_SECONDS, remaining)),
                )
            except TimeoutError:
                continue

            match: Envelope | None = None
            for message in messages:
                try:
                    candidate = Envelope.model_validate_json(message.data)
                except (ValidationError, ValueError):
                    candidate = None
                await message.ack()
                if candidate is not None and _matches(
                    candidate,
                    subject=subject,
                    correlation_id=correlation_id,
                    sender=sender,
                    message_type=message_type,
                ):
                    match = candidate

            if match is not None:
                return match
    finally:
        with contextlib.suppress(Exception):
            await subscription.unsubscribe()
        try:
            await js.delete_consumer(stream, consumer)
        except NotFoundError:
            pass


async def find_envelope(
    *,
    nats_url: str,
    subject: str,
    correlation_id: str | None,
    wait_seconds: float,
    lookback_seconds: float,
    sender: str | None = None,
    message_type: str | None = None,
) -> Envelope | None:
    nc, js = await connect(nats_url)
    try:
        return await wait_for_envelope(
            js,
            subject=subject,
            correlation_id=correlation_id,
            wait_seconds=wait_seconds,
            lookback_seconds=lookback_seconds,
            sender=sender,
            message_type=message_type,
        )
    finally:
        await nc.close()


def print_result(
    result: Envelope | None,
    *,
    subject: str,
    correlation_id: str | None,
    wait_seconds: float,
) -> None:
    if result is None:
        target = (
            f"correlation_id={correlation_id} on {subject}"
            if correlation_id is not None
            else subject
        )
        print(f"ERROR: nothing found for {target} within {wait_seconds:g}s", file=sys.stderr)
        raise SystemExit(1)
    print(result.model_dump_json(indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subject", required=True, help="exact terminal subject")
    parser.add_argument("--correlation-id", required=True)
    parser.add_argument("--sender", default=None)
    parser.add_argument("--type", choices=["task", "event", "error"], default=None)
    parser.add_argument(
        "--wait-seconds",
        type=non_negative_float,
        default=DEFAULT_WAIT_SECONDS,
    )
    parser.add_argument(
        "--lookback-seconds",
        type=non_negative_float,
        default=DEFAULT_LOOKBACK_SECONDS,
        help="include messages published shortly before this process started",
    )
    args = parser.parse_args()

    nats_url = os.environ.get("NATS_URL", "nats://localhost:4222")
    try:
        result = asyncio.run(
            find_envelope(
                nats_url=nats_url,
                subject=args.subject,
                correlation_id=args.correlation_id,
                wait_seconds=args.wait_seconds,
                lookback_seconds=args.lookback_seconds,
                sender=args.sender,
                message_type=args.type,
            )
        )
    except Exception as exc:
        print(f"ERROR: could not wait for {args.subject}: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc

    print_result(
        result,
        subject=args.subject,
        correlation_id=args.correlation_id,
        wait_seconds=args.wait_seconds,
    )


if __name__ == "__main__":
    main()
