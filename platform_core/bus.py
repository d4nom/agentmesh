from __future__ import annotations

import hashlib
import re

import nats
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, RetentionPolicy, StreamConfig
from nats.js.errors import APIError, NotFoundError

from platform_core.envelope import Envelope

STREAM_ALREADY_EXISTS = 10058
MAX_DURABLE_NAME_LENGTH = 128
EVENTS_MAX_AGE_SECONDS = 7 * 24 * 60 * 60
EVENTS_MAX_BYTES = 256 * 1024 * 1024
DLQ_MAX_AGE_SECONDS = 30 * 24 * 60 * 60
DLQ_MAX_BYTES = 512 * 1024 * 1024
_SAFE_DURABLE_PART = re.compile(r"[^A-Za-z0-9_-]+")

STREAMS: dict[str, tuple[list[str], RetentionPolicy, float | None, int | None]] = {
    "TASKS": (["tasks.>"], RetentionPolicy.WORK_QUEUE, None, None),
    "EVENTS": (
        ["events.>"],
        RetentionPolicy.LIMITS,
        EVENTS_MAX_AGE_SECONDS,
        EVENTS_MAX_BYTES,
    ),
    "DLQ": (
        ["dlq.>"],
        RetentionPolicy.LIMITS,
        DLQ_MAX_AGE_SECONDS,
        DLQ_MAX_BYTES,
    ),
}


class DurableConsumerSubjectMismatchError(RuntimeError):
    """An existing durable consumer is bound to a different subject."""


class DurableConsumerConfigurationError(RuntimeError):
    """An existing durable consumer could not be reconciled with runtime policy."""


def expected_envelope_type(subject: str) -> str | None:
    if subject.startswith("tasks."):
        return "task"
    if subject.startswith("events."):
        return "event"
    if subject.startswith("dlq."):
        return "error"
    return None


def _durable_part(value: str, *, max_length: int = 40) -> str:
    sanitized = _SAFE_DURABLE_PART.sub("-", value).strip("-_")
    return (sanitized or "unnamed")[:max_length]


def durable_consumer_name(agent: str, subject: str) -> str:
    """Return a stable, NATS-safe durable name for an agent subscription.

    A system's display name is intentionally not part of the identity: changing
    between compatible YAML configurations must reconnect to the same durable
    rather than leave an overlapping consumer behind on a work-queue stream.
    Agent plus subject still keeps independent event listeners isolated.
    """
    identity = f"{agent}\0{subject}"
    digest = hashlib.sha256(identity.encode()).hexdigest()[:12]
    name = f"agentmesh-{_durable_part(agent)}-{_durable_part(subject)}-{digest}"
    return name[:MAX_DURABLE_NAME_LENGTH]


def _validate_durable_name(durable: str) -> None:
    if not durable:
        raise ValueError("durable consumer name must not be empty")
    if len(durable) > MAX_DURABLE_NAME_LENGTH:
        raise ValueError(f"durable consumer name exceeds {MAX_DURABLE_NAME_LENGTH} characters")
    if _SAFE_DURABLE_PART.search(durable):
        raise ValueError(
            "durable consumer name may contain only ASCII letters, digits, '-' and '_'"
        )


def _consumer_filter_subjects(config: ConsumerConfig) -> tuple[str, ...]:
    if config.filter_subjects:
        return tuple(config.filter_subjects)
    if config.filter_subject:
        return (config.filter_subject,)
    return ()


async def connect(nats_url: str) -> tuple[nats.NATS, JetStreamContext]:
    nc = await nats.connect(nats_url)
    js = nc.jetstream()
    return nc, js


async def ensure_streams(js: JetStreamContext) -> None:
    for name, (subjects, retention, max_age, max_bytes) in STREAMS.items():
        config = StreamConfig(
            name=name,
            subjects=subjects,
            retention=retention,
            max_age=max_age,
            max_bytes=max_bytes,
        )
        try:
            await js.add_stream(config)
        except APIError as exc:
            if exc.err_code == STREAM_ALREADY_EXISTS:
                await js.update_stream(config)
            else:
                raise


async def publish(js: JetStreamContext, subject: str, envelope: Envelope) -> None:
    if envelope.subject != subject:
        raise ValueError(
            f"envelope subject '{envelope.subject}' does not match publish subject '{subject}'"
        )
    expected_type = expected_envelope_type(subject)
    if expected_type is not None and envelope.type != expected_type:
        raise ValueError(
            f"envelope type '{envelope.type}' does not match publish subject "
            f"'{subject}' (expected '{expected_type}')"
        )
    await js.publish(subject, envelope.model_dump_json().encode())


async def pull_subscribe(
    js: JetStreamContext,
    subject: str,
    durable: str,
    max_deliver: int = 5,
    ack_wait: int = 30,
):
    _validate_durable_name(durable)
    stream = await js.find_stream_name_by_subject(subject)
    config = ConsumerConfig(
        durable_name=durable,
        max_deliver=max_deliver,
        ack_wait=ack_wait,
        filter_subject=subject,
    )
    try:
        existing = await js.consumer_info(stream, durable)
    except NotFoundError:
        pass
    else:
        actual_subjects = _consumer_filter_subjects(existing.config)
        if actual_subjects != (subject,):
            raise DurableConsumerSubjectMismatchError(
                f"durable consumer '{durable}' on stream '{stream}' is configured "
                f"for {actual_subjects or ('<all subjects>',)}, not '{subject}'; "
                "delete or migrate the existing consumer before changing subscribes"
            )
        runtime_policy_changed = (
            existing.config.max_deliver != max_deliver or existing.config.ack_wait != ack_wait
        )
        if runtime_policy_changed:
            try:
                await js.add_consumer(stream, config=config)
                existing = await js.consumer_info(stream, durable)
            except Exception as exc:
                raise DurableConsumerConfigurationError(
                    f"could not update durable consumer '{durable}' on stream "
                    f"'{stream}' to max_deliver={max_deliver}, ack_wait={ack_wait}s"
                ) from exc
            if existing.config.max_deliver != max_deliver or existing.config.ack_wait != ack_wait:
                raise DurableConsumerConfigurationError(
                    f"durable consumer '{durable}' on stream '{stream}' kept "
                    f"max_deliver={existing.config.max_deliver}, "
                    f"ack_wait={existing.config.ack_wait}s after update"
                )

    return await js.pull_subscribe(
        subject,
        durable=durable,
        stream=stream,
        config=config,
    )
