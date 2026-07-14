import uuid

import pytest
from pydantic import ValidationError

from platform_core.envelope import Envelope


def test_defaults_are_populated():
    env = Envelope(
        correlation_id=str(uuid.uuid4()),
        sender="parser",
        subject="tasks.retrieve",
        type="task",
        payload={"foo": "bar"},
    )
    assert env.spec_version == "1.0"
    assert env.ttl_ms == 60000
    assert env.reply_to is None
    assert env.traceparent is None
    uuid.UUID(env.message_id)


def test_message_id_unique_per_instance():
    kwargs = dict(
        correlation_id=str(uuid.uuid4()),
        sender="parser",
        subject="tasks.retrieve",
        type="task",
        payload={},
    )
    assert Envelope(**kwargs).message_id != Envelope(**kwargs).message_id


def test_round_trips_through_json():
    original = Envelope(
        correlation_id=str(uuid.uuid4()),
        sender="rag",
        subject="tasks.execute",
        type="task",
        traceparent="00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
        payload={"chunks": ["a", "b"]},
    )
    restored = Envelope.model_validate_json(original.model_dump_json())
    assert restored == original


def test_rejects_invalid_type():
    with pytest.raises(ValidationError):
        Envelope(
            correlation_id=str(uuid.uuid4()),
            sender="parser",
            subject="tasks.parse",
            type="not-a-valid-type",
            payload={},
        )


def test_requires_payload_and_correlation_id():
    with pytest.raises(ValidationError):
        Envelope(sender="parser", subject="tasks.parse", type="task")
