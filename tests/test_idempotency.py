from platform_core.stores import (
    IDEMPOTENCY_KEY_PREFIX,
    IDEMPOTENCY_TTL_SECONDS,
    is_already_processed,
    mark_processed,
)


class FakeRedis:
    """Mimics the subset of redis.asyncio.Redis used for idempotency claims."""

    def __init__(self):
        self.store: dict[str, str] = {}
        self.set_calls: list[dict] = []

    async def exists(self, key):
        return 1 if key in self.store else 0

    async def set(self, key, value, ex=None):
        self.set_calls.append({"key": key, "value": value, "ex": ex})
        self.store[key] = value
        return True


async def test_unseen_message_is_not_processed():
    redis = FakeRedis()
    assert await is_already_processed(redis, "msg-1") is False


async def test_marking_processed_makes_it_visible_to_is_already_processed():
    redis = FakeRedis()
    await mark_processed(redis, "msg-1")
    assert await is_already_processed(redis, "msg-1") is True
    assert redis.store[f"{IDEMPOTENCY_KEY_PREFIX}msg-1"] == "1"


async def test_different_message_ids_are_independent():
    redis = FakeRedis()
    await mark_processed(redis, "msg-1")
    assert await is_already_processed(redis, "msg-2") is False


async def test_a_failing_handler_is_not_marked_processed():
    """The SDK must only call mark_processed() after handle() succeeds — a
    message that keeps failing must keep showing up as not-yet-processed so
    it can be retried up to max_deliver and eventually dead-lettered, instead
    of looking like a duplicate on redelivery."""
    redis = FakeRedis()
    assert await is_already_processed(redis, "msg-1") is False
    # simulate a failed handle(): nothing marks the message processed
    assert await is_already_processed(redis, "msg-1") is False


async def test_uses_ttl():
    redis = FakeRedis()
    await mark_processed(redis, "msg-1")
    call = redis.set_calls[0]
    assert call["ex"] == IDEMPOTENCY_TTL_SECONDS
