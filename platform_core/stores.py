from __future__ import annotations

import redis.asyncio as redis
from qdrant_client import AsyncQdrantClient

IDEMPOTENCY_TTL_SECONDS = 3600
IDEMPOTENCY_KEY_PREFIX = "processed:"


def make_redis(url: str) -> redis.Redis:
    return redis.from_url(url, decode_responses=True)


def make_qdrant(url: str) -> AsyncQdrantClient:
    return AsyncQdrantClient(url=url)


async def mark_processed_if_new(client: redis.Redis, message_id: str) -> bool:
    """Atomically claim a message_id. Returns True the first time it's seen,
    False if it was already processed (redelivery)."""
    key = f"{IDEMPOTENCY_KEY_PREFIX}{message_id}"
    return bool(await client.set(key, "1", nx=True, ex=IDEMPOTENCY_TTL_SECONDS))
