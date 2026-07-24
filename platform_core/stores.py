from __future__ import annotations

import redis.asyncio as redis
from qdrant_client import AsyncQdrantClient

IDEMPOTENCY_TTL_SECONDS = 3600
IDEMPOTENCY_KEY_PREFIX = "processed:"
REDIS_CONNECT_TIMEOUT_SECONDS = 5.0
REDIS_SOCKET_TIMEOUT_SECONDS = 5.0


def make_redis(url: str) -> redis.Redis:
    return redis.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=REDIS_CONNECT_TIMEOUT_SECONDS,
        socket_timeout=REDIS_SOCKET_TIMEOUT_SECONDS,
    )


def make_qdrant(url: str) -> AsyncQdrantClient:
    return AsyncQdrantClient(url=url)


def idempotency_key(consumer_id: str, message_id: str) -> str:
    """Build a per-consumer idempotency key.

    A message can legitimately be delivered to multiple event consumers.  A
    global ``message_id`` key would let the first consumer suppress all of the
    others, so the durable consumer identity is part of the key.
    """
    if not consumer_id:
        raise ValueError("consumer_id must not be empty")
    if not message_id:
        raise ValueError("message_id must not be empty")
    return f"{IDEMPOTENCY_KEY_PREFIX}{consumer_id}:{message_id}"


async def is_already_processed(
    client: redis.Redis,
    consumer_id: str,
    message_id: str,
) -> bool:
    """Check before calling handle() so a message that already succeeded once
    (e.g. redelivered after its ack was lost) isn't handled twice."""
    return bool(await client.exists(idempotency_key(consumer_id, message_id)))


async def mark_processed(client: redis.Redis, consumer_id: str, message_id: str) -> None:
    """Record a message_id as done. Call only after handle() succeeds — recording
    it earlier would make a failing handler's redeliveries look like duplicates
    and get ack'd without ever reaching max_deliver/DLQ."""
    key = idempotency_key(consumer_id, message_id)
    await client.set(key, "1", ex=IDEMPOTENCY_TTL_SECONDS)
