from __future__ import annotations

import nats
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, RetentionPolicy, StreamConfig
from nats.js.errors import APIError

from platform_core.envelope import Envelope

STREAM_ALREADY_EXISTS = 10058

STREAMS: dict[str, tuple[list[str], RetentionPolicy]] = {
    "TASKS": (["tasks.>"], RetentionPolicy.WORK_QUEUE),
    "EVENTS": (["events.>"], RetentionPolicy.LIMITS),
    "DLQ": (["dlq.>"], RetentionPolicy.LIMITS),
}


async def connect(nats_url: str) -> tuple[nats.NATS, JetStreamContext]:
    nc = await nats.connect(nats_url)
    js = nc.jetstream()
    return nc, js


async def ensure_streams(js: JetStreamContext) -> None:
    for name, (subjects, retention) in STREAMS.items():
        config = StreamConfig(name=name, subjects=subjects, retention=retention)
        try:
            await js.add_stream(config)
        except APIError as exc:
            if exc.err_code == STREAM_ALREADY_EXISTS:
                await js.update_stream(config)
            else:
                raise


async def publish(js: JetStreamContext, subject: str, envelope: Envelope) -> None:
    await js.publish(subject, envelope.model_dump_json().encode())


async def pull_subscribe(
    js: JetStreamContext,
    subject: str,
    durable: str,
    max_deliver: int = 5,
    ack_wait: int = 30,
):
    config = ConsumerConfig(
        durable_name=durable,
        max_deliver=max_deliver,
        ack_wait=ack_wait,
    )
    return await js.pull_subscribe(subject, durable=durable, config=config)
