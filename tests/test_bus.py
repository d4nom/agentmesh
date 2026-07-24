from types import SimpleNamespace

import pytest
from nats.js.api import RetentionPolicy

from platform_core.bus import (
    DLQ_MAX_AGE_SECONDS,
    DLQ_MAX_BYTES,
    EVENTS_MAX_AGE_SECONDS,
    EVENTS_MAX_BYTES,
    STREAMS,
    publish,
    pull_subscribe,
)
from platform_core.envelope import Envelope


def test_task_stream_preserves_unacked_work_without_platform_expiry():
    subjects, retention, max_age, max_bytes = STREAMS["TASKS"]

    assert subjects == ["tasks.>"]
    assert retention == RetentionPolicy.WORK_QUEUE
    assert max_age is None
    assert max_bytes is None


def test_service_event_stream_has_bounded_retention():
    subjects, retention, max_age, max_bytes = STREAMS["EVENTS"]

    assert subjects == ["events.>"]
    assert retention == RetentionPolicy.LIMITS
    assert max_age == EVENTS_MAX_AGE_SECONDS
    assert max_bytes == EVENTS_MAX_BYTES


def test_dead_letters_are_retained_longer_but_remain_bounded():
    subjects, retention, max_age, max_bytes = STREAMS["DLQ"]

    assert subjects == ["dlq.>"]
    assert retention == RetentionPolicy.LIMITS
    assert max_age == DLQ_MAX_AGE_SECONDS
    assert max_bytes == DLQ_MAX_BYTES


async def test_publish_rejects_subject_mismatch_before_touching_jetstream():
    class FakeJetStream:
        async def publish(self, subject: str, payload: bytes) -> None:
            raise AssertionError("JetStream must not be called")

    envelope = Envelope(
        sender="test",
        subject="events.expected",
        type="event",
        correlation_id="correlation-1",
        payload={},
    )

    with pytest.raises(ValueError, match="does not match"):
        await publish(FakeJetStream(), "events.wrong", envelope)


async def test_publish_rejects_type_mismatch_before_touching_jetstream():
    class FakeJetStream:
        async def publish(self, subject: str, payload: bytes) -> None:
            raise AssertionError("JetStream must not be called")

    envelope = Envelope(
        sender="test",
        subject="tasks.work",
        type="event",
        correlation_id="correlation-1",
        payload={},
    )

    with pytest.raises(ValueError, match="expected 'task'"):
        await publish(FakeJetStream(), "tasks.work", envelope)


async def test_existing_durable_runtime_policy_is_updated():
    class FakeJetStream:
        def __init__(self) -> None:
            self.config = SimpleNamespace(
                filter_subject="tasks.work",
                filter_subjects=None,
                max_deliver=5,
                ack_wait=15,
            )
            self.updated_config = None
            self.subscribed_config = None

        async def find_stream_name_by_subject(self, subject: str) -> str:
            return "TASKS"

        async def consumer_info(self, stream: str, durable: str):
            return SimpleNamespace(config=self.config)

        async def add_consumer(self, stream: str, *, config):
            self.updated_config = config
            self.config = config

        async def pull_subscribe(self, subject: str, **kwargs):
            self.subscribed_config = kwargs["config"]
            return object()

    js = FakeJetStream()

    await pull_subscribe(
        js,
        "tasks.work",
        durable="safe-durable",
        max_deliver=-1,
        ack_wait=30,
    )

    assert js.updated_config is not None
    assert js.updated_config.max_deliver == -1
    assert js.updated_config.ack_wait == 30
    assert js.subscribed_config is js.updated_config
