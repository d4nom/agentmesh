from __future__ import annotations

import redis.asyncio as redis
from qdrant_client import AsyncQdrantClient

IDEMPOTENCY_TTL_SECONDS = 3600
IDEMPOTENCY_KEY_PREFIX = "processed:"


def make_redis(url: str) -> redis.Redis:
    return redis.from_url(url, decode_responses=True)


def make_qdrant(url: str) -> AsyncQdrantClient:
    return AsyncQdrantClient(url=url)


async def is_already_processed(client: redis.Redis, message_id: str) -> bool:
    """Check before calling handle() so a message that already succeeded once
    (e.g. redelivered after its ack was lost) isn't handled twice."""
    return bool(await client.exists(f"{IDEMPOTENCY_KEY_PREFIX}{message_id}"))


async def mark_processed(client: redis.Redis, message_id: str) -> None:
    """Record a message_id as done. Call only after handle() succeeds — recording
    it earlier would make a failing handler's redeliveries look like duplicates
    and get ack'd without ever reaching max_deliver/DLQ."""
    key = f"{IDEMPOTENCY_KEY_PREFIX}{message_id}"
    await client.set(key, "1", ex=IDEMPOTENCY_TTL_SECONDS)
