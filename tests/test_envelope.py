import uuid

import pytest
from pydantic import ValidationError

from platform_core.envelope import MAX_TTL_MS, Envelope


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


def test_rejects_non_uuid4_message_id():
    with pytest.raises(ValidationError, match="message_id"):
        Envelope(
            message_id="not-a-uuid",
            correlation_id=str(uuid.uuid4()),
            sender="parser",
            subject="tasks.parse",
            type="task",
            payload={},
        )


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


def test_rejects_unroutable_result_type():
    with pytest.raises(ValidationError, match="type"):
        Envelope(
            correlation_id=str(uuid.uuid4()),
            sender="executor",
            subject="events.incident.completed",
            type="result",
            payload={},
        )


def test_rejects_unsupported_protocol_version():
    with pytest.raises(ValidationError, match="spec_version"):
        Envelope(
            spec_version="2.0",
            correlation_id=str(uuid.uuid4()),
            sender="parser",
            subject="tasks.parse",
            type="task",
            payload={},
        )


def test_rejects_unknown_protocol_fields():
    data = {
        "correlation_id": str(uuid.uuid4()),
        "sender": "parser",
        "subject": "tasks.parse",
        "type": "task",
        "payload": {},
        "unexpected": True,
    }

    with pytest.raises(ValidationError, match="unexpected"):
        Envelope.model_validate(data)


def test_requires_payload_and_correlation_id():
    with pytest.raises(ValidationError):
        Envelope(sender="parser", subject="tasks.parse", type="task")


def test_ttl_must_be_positive():
    with pytest.raises(ValidationError, match="ttl_ms"):
        Envelope(
            correlation_id=str(uuid.uuid4()),
            sender="parser",
            subject="tasks.parse",
            type="task",
            ttl_ms=0,
            payload={},
        )


def test_ttl_has_a_bounded_maximum():
    with pytest.raises(ValidationError, match="ttl_ms"):
        Envelope(
            correlation_id=str(uuid.uuid4()),
            sender="parser",
            subject="tasks.parse",
            type="task",
            ttl_ms=MAX_TTL_MS + 1,
            payload={},
        )
