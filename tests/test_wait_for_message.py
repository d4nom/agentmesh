from __future__ import annotations

from types import SimpleNamespace

import pytest
from nats.js.api import DeliverPolicy, RetentionPolicy

from platform_core.envelope import Envelope
from scripts.wait_for_message import WAITER_INACTIVE_THRESHOLD_SECONDS, wait_for_envelope


class FakeMessage:
    def __init__(self, envelope: Envelope) -> None:
        self.data = envelope.model_dump_json().encode()
        self.acked = False

    async def ack(self) -> None:
        self.acked = True


class FakeSubscription:
    def __init__(self, batches: list[list[FakeMessage]]) -> None:
        self.batches = batches
        self.unsubscribed = False

    async def fetch(self, _batch_size: int, **kwargs: float) -> list[FakeMessage]:
        del kwargs
        if self.batches:
            return self.batches.pop(0)
        raise TimeoutError

    async def unsubscribe(self) -> None:
        self.unsubscribed = True


class FakeJetStream:
    def __init__(
        self,
        subscription: FakeSubscription,
        retention: RetentionPolicy = RetentionPolicy.LIMITS,
    ) -> None:
        self.subscription = subscription
        self.retention = retention
        self.deleted: tuple[str, str] | None = None
        self.subscribe_args: tuple[str, str, str, object] | None = None

    async def find_stream_name_by_subject(self, _subject: str) -> str:
        return "EVENTS"

    async def stream_info(self, _stream: str) -> SimpleNamespace:
        return SimpleNamespace(config=SimpleNamespace(retention=self.retention))

    async def pull_subscribe(
        self,
        subject: str,
        *,
        durable: str,
        stream: str,
        config: object,
    ) -> FakeSubscription:
        self.subscribe_args = (subject, durable, stream, config)
        return self.subscription

    async def delete_consumer(self, stream: str, consumer: str) -> bool:
        self.deleted = (stream, consumer)
        return True


def envelope(correlation_id: str, *, sender: str = "executor") -> Envelope:
    return Envelope(
        sender=sender,
        subject="events.incident.completed",
        type="event",
        correlation_id=correlation_id,
        payload={"ok": True},
    )


async def test_waiter_matches_exact_envelope_and_cleans_up_unique_consumer():
    unrelated = FakeMessage(envelope("other"))
    expected = FakeMessage(envelope("wanted"))
    subscription = FakeSubscription([[unrelated, expected]])
    js = FakeJetStream(subscription)

    result = await wait_for_envelope(
        js,
        subject="events.incident.completed",
        correlation_id="wanted",
        sender="executor",
        message_type="event",
        wait_seconds=1,
        lookback_seconds=30,
    )

    assert result is not None
    assert result.correlation_id == "wanted"
    assert unrelated.acked and expected.acked
    assert subscription.unsubscribed
    assert js.subscribe_args is not None
    _, consumer, stream, config = js.subscribe_args
    assert consumer.startswith("agentmesh-wait-events-incident-completed-")
    assert config.deliver_policy == DeliverPolicy.BY_START_TIME
    assert config.inactive_threshold == WAITER_INACTIVE_THRESHOLD_SECONDS
    assert js.deleted == (stream, consumer)


async def test_waiter_cleans_up_after_timeout():
    subscription = FakeSubscription([])
    js = FakeJetStream(subscription)

    result = await wait_for_envelope(
        js,
        subject="events.incident.completed",
        correlation_id="missing",
        wait_seconds=0,
        lookback_seconds=30,
    )

    assert result is None
    assert subscription.unsubscribed
    assert js.deleted is not None


async def test_zero_wait_still_performs_one_immediate_fetch():
    expected = FakeMessage(envelope("already-there"))
    subscription = FakeSubscription([[expected]])
    js = FakeJetStream(subscription)

    result = await wait_for_envelope(
        js,
        subject="events.incident.completed",
        correlation_id="already-there",
        wait_seconds=0,
        lookback_seconds=30,
    )

    assert result is not None
    assert result.correlation_id == "already-there"
    assert expected.acked
    assert js.deleted is not None


async def test_waiter_refuses_work_queue_stream_without_creating_consumer():
    js = FakeJetStream(
        FakeSubscription([]),
        retention=RetentionPolicy.WORK_QUEUE,
    )

    with pytest.raises(ValueError, match="work-queue"):
        await wait_for_envelope(
            js,
            subject="tasks.parse",
            correlation_id="unsafe",
            wait_seconds=1,
            lookback_seconds=30,
        )

    assert js.subscribe_args is None
    assert js.deleted is None


async def test_waiter_rejects_wildcard_subject():
    js = FakeJetStream(FakeSubscription([]))

    with pytest.raises(ValueError, match="wildcards"):
        await wait_for_envelope(
            js,
            subject="events.>",
            correlation_id="too-broad",
            wait_seconds=1,
            lookback_seconds=30,
        )
