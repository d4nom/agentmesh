from platform_core.stores import (
    IDEMPOTENCY_KEY_PREFIX,
    IDEMPOTENCY_TTL_SECONDS,
    mark_processed_if_new,
)


class FakeRedis:
    """Mimics the subset of redis.asyncio.Redis.set used for idempotency claims."""

    def __init__(self):
        self.store: dict[str, str] = {}
        self.set_calls: list[dict] = []

    async def set(self, key, value, nx=False, ex=None):
        self.set_calls.append({"key": key, "value": value, "nx": nx, "ex": ex})
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True


async def test_first_delivery_claims_the_message():
    redis = FakeRedis()
    assert await mark_processed_if_new(redis, "msg-1") is True
    assert redis.store[f"{IDEMPOTENCY_KEY_PREFIX}msg-1"] == "1"


async def test_redelivery_of_same_message_id_is_rejected():
    redis = FakeRedis()
    assert await mark_processed_if_new(redis, "msg-1") is True
    assert await mark_processed_if_new(redis, "msg-1") is False


async def test_different_message_ids_are_independent():
    redis = FakeRedis()
    assert await mark_processed_if_new(redis, "msg-1") is True
    assert await mark_processed_if_new(redis, "msg-2") is True


async def test_uses_nx_and_ttl():
    redis = FakeRedis()
    await mark_processed_if_new(redis, "msg-1")
    call = redis.set_calls[0]
    assert call["nx"] is True
    assert call["ex"] == IDEMPOTENCY_TTL_SECONDS
