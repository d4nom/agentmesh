from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from opentelemetry.trace import StatusCode

import platform_core.agent as agent_module
import platform_core.bus as bus_module
from platform_core.agent import (
    DEFAULT_MAX_DELIVER,
    DLQ_HANDOFF_RETRY_DELAY_SECONDS,
    JETSTREAM_MAX_DELIVER,
    MAX_CONSECUTIVE_FETCH_FAILURES,
    MAX_CONSECUTIVE_HEARTBEAT_FAILURES,
    MAX_DLQ_ERROR_CHARS,
    BaseAgent,
)
from platform_core.bus import (
    DurableConsumerSubjectMismatchError,
    durable_consumer_name,
)
from platform_core.config import AgentConfig, LLMConfig
from platform_core.envelope import Envelope


def make_config() -> AgentConfig:
    return AgentConfig(
        name="test-agent",
        module="tests.test_agent:TestAgent",
        subscribes="tasks.test",
        publishes=["events.test.completed"],
        stores=["redis"],
        params={},
        system="test-system",
        llm=LLMConfig(),
        nats_url="nats://test",
        redis_url="redis://test",
        qdrant_url="http://qdrant",
        otel_endpoint="http://jaeger:4317",
    )


class FakeMessage:
    def __init__(
        self,
        data: bytes,
        *,
        num_delivered: int = 1,
        subject: str = "tasks.test",
        ack_error: Exception | None = None,
    ) -> None:
        self.data = data
        self.subject = subject
        self.metadata = SimpleNamespace(num_delivered=num_delivered)
        self.ack_error = ack_error
        self.ack_calls = 0
        self.acked = False
        self.termed = False
        self.nak_delays: list[int] = []
        self.in_progress_calls = 0

    async def ack(self) -> None:
        self.ack_calls += 1
        if self.ack_error is not None:
            raise self.ack_error
        self.acked = True

    async def term(self) -> None:
        self.termed = True

    async def nak(self, *, delay: int) -> None:
        self.nak_delays.append(delay)

    async def in_progress(self) -> None:
        self.in_progress_calls += 1


class TestAgent(BaseAgent):
    __test__ = False

    def __init__(self, config: AgentConfig | None = None) -> None:
        super().__init__(config or make_config())
        self.handled = 0
        self._js = object()
        self._redis = object()

    async def handle(self, env: Envelope) -> None:
        self.handled += 1


async def test_invalid_envelope_is_retried_without_killing_consumer() -> None:
    agent = TestAgent()
    msg = FakeMessage(b"not-json")

    await agent._process(msg)

    assert msg.nak_delays == [2]
    assert msg.termed is False
    assert agent.handled == 0


async def test_invalid_envelope_is_dead_lettered_after_max_deliver(monkeypatch) -> None:
    agent = TestAgent()
    msg = FakeMessage(b"not-json", num_delivered=DEFAULT_MAX_DELIVER)
    published: list[tuple[str, Envelope]] = []

    async def capture_publish(js, subject: str, envelope: Envelope) -> None:
        published.append((subject, envelope))

    monkeypatch.setattr(agent_module.bus, "publish", capture_publish)

    await agent._process(msg)

    assert msg.termed is True
    assert len(published) == 1
    subject, envelope = published[0]
    assert subject == "dlq.test"
    assert envelope.type == "error"
    assert envelope.payload["error_stage"] == "envelope_validation"
    assert envelope.payload["raw_message"] == "not-json"


async def test_envelope_subject_mismatch_uses_protocol_retry_path() -> None:
    agent = TestAgent()
    envelope = Envelope(
        sender="test",
        subject="tasks.somewhere-else",
        type="task",
        correlation_id="correlation-1",
        payload={"value": 1},
    )
    msg = FakeMessage(
        envelope.model_dump_json().encode(),
        subject="tasks.test",
    )

    await agent._process(msg)

    assert msg.nak_delays == [2]
    assert agent.handled == 0


async def test_envelope_type_mismatch_uses_protocol_retry_path() -> None:
    agent = TestAgent()
    envelope = Envelope(
        sender="test",
        subject="tasks.test",
        type="event",
        correlation_id="correlation-1",
        payload={"value": 1},
    )
    msg = FakeMessage(envelope.model_dump_json().encode())

    await agent._process(msg)

    assert msg.nak_delays == [2]
    assert agent.handled == 0


async def test_infrastructure_failure_uses_common_retry_path(monkeypatch) -> None:
    agent = TestAgent()
    envelope = Envelope(
        sender="test",
        subject="tasks.test",
        type="task",
        correlation_id="correlation-1",
        payload={"value": 1},
    )
    msg = FakeMessage(envelope.model_dump_json().encode())

    async def redis_is_down(client, consumer_id: str, message_id: str) -> bool:
        raise ConnectionError("redis unavailable")

    monkeypatch.setattr(agent_module, "is_already_processed", redis_is_down)

    await agent._process(msg)

    assert msg.nak_delays == [2]
    assert agent.handled == 0


async def test_consumer_failure_exits_service_instead_of_leaving_heartbeat_zombie(
    monkeypatch,
) -> None:
    class FakeNats:
        def __init__(self) -> None:
            self.drained = False

        async def drain(self) -> None:
            self.drained = True

    class CrashingAgent(TestAgent):
        def __init__(self) -> None:
            super().__init__()
            self.agent_resources_closed = False

        async def _consume_loop(self) -> None:
            raise RuntimeError("consumer crashed")

        async def _heartbeat_loop(self) -> None:
            await self._stop_event.wait()

        async def _publish_event(self, subject: str, payload: dict) -> None:
            return None

        async def close(self) -> None:
            self.agent_resources_closed = True

    class FakeRedis:
        def __init__(self) -> None:
            self.closed = False

        async def aclose(self) -> None:
            self.closed = True

    nc = FakeNats()
    redis = FakeRedis()
    durable_names: list[str] = []
    max_deliver_values: list[int] = []

    async def fake_connect(url: str):
        return nc, object()

    async def fake_ensure_streams(js) -> None:
        return None

    async def fake_pull_subscribe(js, subject: str, durable: str, max_deliver: int, ack_wait: int):
        durable_names.append(durable)
        max_deliver_values.append(max_deliver)
        return object()

    monkeypatch.setattr(agent_module, "connect", fake_connect)
    monkeypatch.setattr(agent_module, "ensure_streams", fake_ensure_streams)
    monkeypatch.setattr(agent_module, "pull_subscribe", fake_pull_subscribe)
    monkeypatch.setattr(agent_module, "make_redis", lambda url: redis)

    agent = CrashingAgent()
    with pytest.raises(RuntimeError, match="consumer crashed"):
        await agent.run()

    assert nc.drained is True
    assert redis.closed is True
    assert agent.agent_resources_closed is True
    assert durable_names == [durable_consumer_name("test-agent", "tasks.test")]
    assert max_deliver_values == [JETSTREAM_MAX_DELIVER]


async def test_startup_failure_releases_partially_initialized_resources(
    monkeypatch,
) -> None:
    class FakeNats:
        def __init__(self) -> None:
            self.drained = False

        async def drain(self) -> None:
            self.drained = True

    class FakeRedis:
        def __init__(self) -> None:
            self.closed = False

        async def aclose(self) -> None:
            self.closed = True

    class StartupAgent(TestAgent):
        def __init__(self) -> None:
            super().__init__()
            self.agent_resources_closed = False

        async def close(self) -> None:
            self.agent_resources_closed = True

    nc = FakeNats()
    redis = FakeRedis()

    async def fake_connect(url: str):
        return nc, object()

    async def fail_ensure_streams(js) -> None:
        raise ConnectionError("JetStream unavailable")

    monkeypatch.setattr(agent_module, "connect", fake_connect)
    monkeypatch.setattr(agent_module, "ensure_streams", fail_ensure_streams)
    monkeypatch.setattr(agent_module, "make_redis", lambda url: redis)

    agent = StartupAgent()
    with pytest.raises(ConnectionError, match="JetStream unavailable"):
        await agent.run()

    assert nc.drained is True
    assert redis.closed is True
    assert agent.agent_resources_closed is True


async def test_long_handler_extends_ack_deadline(monkeypatch) -> None:
    class SlowAgent(TestAgent):
        async def handle(self, env: Envelope) -> None:
            await asyncio.sleep(0.03)
            self.handled += 1

    async def not_processed(client, consumer_id: str, message_id: str) -> bool:
        return False

    marked: list[tuple[str, str]] = []

    async def capture_mark(client, consumer_id: str, message_id: str) -> None:
        marked.append((consumer_id, message_id))

    monkeypatch.setattr(agent_module, "ACK_PROGRESS_INTERVAL_SECONDS", 0.005)
    monkeypatch.setattr(agent_module, "is_already_processed", not_processed)
    monkeypatch.setattr(agent_module, "mark_processed", capture_mark)

    agent = SlowAgent()
    envelope = Envelope(
        sender="test",
        subject="tasks.test",
        type="task",
        correlation_id="correlation-1",
        payload={"value": 1},
    )
    msg = FakeMessage(envelope.model_dump_json().encode())

    await agent._process(msg)

    assert agent.handled == 1
    assert msg.in_progress_calls >= 1
    assert msg.acked is True
    assert marked == [(agent._consumer_id, envelope.message_id)]


async def test_handler_ttl_cancels_work_and_uses_retry_path(monkeypatch) -> None:
    class BlockingAgent(TestAgent):
        def __init__(self) -> None:
            super().__init__()
            self.cancelled = False

        async def handle(self, env: Envelope) -> None:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                self.cancelled = True
                raise

    async def not_processed(client, consumer_id: str, message_id: str) -> bool:
        return False

    monkeypatch.setattr(agent_module, "is_already_processed", not_processed)
    agent = BlockingAgent()
    envelope = Envelope(
        sender="test",
        subject="tasks.test",
        type="task",
        correlation_id="correlation-1",
        ttl_ms=1,
        payload={},
    )
    msg = FakeMessage(envelope.model_dump_json().encode())

    await agent._process(msg)

    assert agent.cancelled is True
    assert msg.acked is False
    assert msg.nak_delays == [2]


async def test_ack_progress_failure_cancels_handler_and_exits_consumer(monkeypatch) -> None:
    class BlockingAgent(TestAgent):
        def __init__(self) -> None:
            super().__init__()
            self.cancelled = False
            self.started = asyncio.Event()

        async def handle(self, env: Envelope) -> None:
            self.started.set()
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                self.cancelled = True
                raise

    class BrokenProgressMessage(FakeMessage):
        async def in_progress(self) -> None:
            await agent.started.wait()
            raise ConnectionError("ack progress unavailable")

    async def not_processed(client, consumer_id: str, message_id: str) -> bool:
        return False

    monkeypatch.setattr(agent_module, "ACK_PROGRESS_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(agent_module, "is_already_processed", not_processed)
    agent = BlockingAgent()
    envelope = Envelope(
        sender="test",
        subject="tasks.test",
        type="task",
        correlation_id="correlation-1",
        payload={},
    )
    msg = BrokenProgressMessage(envelope.model_dump_json().encode())

    with pytest.raises(RuntimeError, match="extend message ack deadline"):
        await agent._process(msg)

    assert agent.cancelled is True
    assert msg.acked is False
    assert msg.nak_delays == []
    assert msg.termed is False


async def test_ack_failure_after_success_is_not_dead_lettered(monkeypatch) -> None:
    async def not_processed(client, consumer_id: str, message_id: str) -> bool:
        return False

    marked: list[tuple[str, str]] = []

    async def capture_mark(client, consumer_id: str, message_id: str) -> None:
        marked.append((consumer_id, message_id))

    async def reject_publish(js, subject: str, envelope: Envelope) -> None:
        raise AssertionError("successful work must not be dead-lettered")

    monkeypatch.setattr(agent_module, "is_already_processed", not_processed)
    monkeypatch.setattr(agent_module, "mark_processed", capture_mark)
    monkeypatch.setattr(agent_module.bus, "publish", reject_publish)

    agent = TestAgent()
    envelope = Envelope(
        sender="test",
        subject="tasks.test",
        type="task",
        correlation_id="correlation-1",
        payload={},
    )
    msg = FakeMessage(
        envelope.model_dump_json().encode(),
        num_delivered=DEFAULT_MAX_DELIVER,
        ack_error=ConnectionError("ack unavailable"),
    )

    await agent._process(msg)

    assert agent.handled == 1
    assert marked == [(agent._consumer_id, envelope.message_id)]
    assert msg.ack_calls == 1
    assert msg.acked is False
    assert msg.termed is False
    assert msg.nak_delays == []


async def test_dlq_publish_failure_leaves_message_unsettled(monkeypatch) -> None:
    class FailingAgent(TestAgent):
        async def handle(self, env: Envelope) -> None:
            raise ValueError("poison")

    async def not_processed(client, consumer_id: str, message_id: str) -> bool:
        return False

    async def fail_publish(js, subject: str, envelope: Envelope) -> None:
        raise ConnectionError("DLQ unavailable")

    monkeypatch.setattr(agent_module, "is_already_processed", not_processed)
    monkeypatch.setattr(agent_module.bus, "publish", fail_publish)

    agent = FailingAgent()
    envelope = Envelope(
        sender="test",
        subject="tasks.test",
        type="task",
        correlation_id="correlation-1",
        payload={},
    )
    msg = FakeMessage(
        envelope.model_dump_json().encode(),
        num_delivered=DEFAULT_MAX_DELIVER,
    )

    await agent._process(msg)

    assert msg.acked is False
    assert msg.termed is False
    assert msg.nak_delays == [DLQ_HANDOFF_RETRY_DELAY_SECONDS]


async def test_valid_poison_message_gets_a_fresh_dlq_envelope(monkeypatch) -> None:
    class FailingAgent(TestAgent):
        async def handle(self, env: Envelope) -> None:
            raise ValueError("poison")

    async def not_processed(client, consumer_id: str, message_id: str) -> bool:
        return False

    published: list[Envelope] = []

    async def capture_publish(js, subject: str, envelope: Envelope) -> None:
        published.append(envelope)

    monkeypatch.setattr(agent_module, "is_already_processed", not_processed)
    monkeypatch.setattr(agent_module.bus, "publish", capture_publish)

    agent = FailingAgent()
    source = Envelope(
        sender="test",
        subject="tasks.test",
        type="task",
        correlation_id="correlation-1",
        payload={},
    )
    msg = FakeMessage(
        source.model_dump_json().encode(),
        num_delivered=DEFAULT_MAX_DELIVER,
    )

    await agent._process(msg)

    assert msg.termed is True
    assert len(published) == 1
    dead_letter = published[0]
    assert dead_letter.message_id != source.message_id
    assert dead_letter.created_at >= source.created_at
    assert dead_letter.correlation_id == source.correlation_id
    assert dead_letter.subject == "dlq.test"
    assert dead_letter.type == "error"


async def test_large_poison_message_gets_a_bounded_dlq_envelope(monkeypatch) -> None:
    class FailingAgent(TestAgent):
        async def handle(self, env: Envelope) -> None:
            raise ValueError("e" * 300_000)

    async def not_processed(client, consumer_id: str, message_id: str) -> bool:
        return False

    published: list[Envelope] = []

    async def capture_publish(js, subject: str, envelope: Envelope) -> None:
        published.append(envelope)

    monkeypatch.setattr(agent_module, "is_already_processed", not_processed)
    monkeypatch.setattr(agent_module.bus, "publish", capture_publish)

    agent = FailingAgent()
    source = Envelope(
        sender="test",
        subject="tasks.test",
        type="task",
        correlation_id="correlation-1",
        payload={"blob": "x" * 900_000},
    )
    msg = FakeMessage(
        source.model_dump_json().encode(),
        num_delivered=DEFAULT_MAX_DELIVER,
    )

    await agent._process(msg)

    assert msg.termed is True
    assert len(published) == 1
    dead_letter = published[0]
    assert len(dead_letter.model_dump_json().encode()) < 64 * 1024
    assert dead_letter.payload["payload_truncated"] is True
    assert dead_letter.payload["payload_size_bytes"] > 900_000
    assert len(dead_letter.payload["error"]) == MAX_DLQ_ERROR_CHARS


async def test_failing_dlq_consumer_does_not_publish_back_into_same_dlq(monkeypatch) -> None:
    class FailingAgent(TestAgent):
        async def handle(self, env: Envelope) -> None:
            raise ValueError("poisoned dead letter")

    async def not_processed(client, consumer_id: str, message_id: str) -> bool:
        return False

    async def reject_publish(js, subject: str, envelope: Envelope) -> None:
        raise AssertionError("a DLQ consumer must not recursively dead-letter")

    monkeypatch.setattr(agent_module, "is_already_processed", not_processed)
    monkeypatch.setattr(agent_module.bus, "publish", reject_publish)

    config = make_config().model_copy(update={"subscribes": "dlq.test"})
    agent = FailingAgent(config)
    source = Envelope(
        sender="producer",
        subject="dlq.test",
        type="error",
        correlation_id="correlation-1",
        payload={"error": "original failure"},
    )
    msg = FakeMessage(
        source.model_dump_json().encode(),
        subject="dlq.test",
        num_delivered=DEFAULT_MAX_DELIVER,
    )

    await agent._process(msg)

    assert msg.termed is True
    assert msg.nak_delays == []


async def test_ack_progress_covers_slow_redis_operations(monkeypatch) -> None:
    async def slow_not_processed(client, consumer_id: str, message_id: str) -> bool:
        await asyncio.sleep(0.012)
        return False

    async def slow_mark(client, consumer_id: str, message_id: str) -> None:
        await asyncio.sleep(0.012)

    monkeypatch.setattr(agent_module, "ACK_PROGRESS_INTERVAL_SECONDS", 0.003)
    monkeypatch.setattr(agent_module, "is_already_processed", slow_not_processed)
    monkeypatch.setattr(agent_module, "mark_processed", slow_mark)

    agent = TestAgent()
    envelope = Envelope(
        sender="test",
        subject="tasks.test",
        type="task",
        correlation_id="correlation-1",
        payload={},
    )
    msg = FakeMessage(envelope.model_dump_json().encode())

    await agent._process(msg)

    assert msg.acked is True
    assert msg.in_progress_calls >= 4


async def test_handler_exception_is_recorded_on_span(monkeypatch) -> None:
    class FailingAgent(TestAgent):
        async def handle(self, env: Envelope) -> None:
            raise ValueError("handler exploded")

    class RecordingSpan:
        def __init__(self) -> None:
            self.exceptions: list[Exception] = []
            self.status = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def set_attribute(self, name: str, value: str) -> None:
            return None

        def record_exception(self, exc: Exception) -> None:
            self.exceptions.append(exc)

        def set_status(self, status) -> None:
            self.status = status

    class RecordingTracer:
        def __init__(self, span: RecordingSpan) -> None:
            self.span = span

        def start_as_current_span(self, name: str) -> RecordingSpan:
            return self.span

    async def not_processed(client, consumer_id: str, message_id: str) -> bool:
        return False

    span = RecordingSpan()
    monkeypatch.setattr(agent_module, "get_tracer", lambda name: RecordingTracer(span))
    monkeypatch.setattr(agent_module, "is_already_processed", not_processed)

    agent = FailingAgent()
    envelope = Envelope(
        sender="test",
        subject="tasks.test",
        type="task",
        correlation_id="correlation-1",
        payload={},
    )
    msg = FakeMessage(envelope.model_dump_json().encode())

    await agent._process(msg)

    assert len(span.exceptions) == 1
    assert str(span.exceptions[0]) == "handler exploded"
    assert span.status.status_code is StatusCode.ERROR
    assert msg.nak_delays == [2]


async def test_repeated_fetch_failures_terminate_consumer() -> None:
    class FailingSubscription:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch(self, batch: int, **kwargs):
            self.calls += 1
            raise ConnectionError("nats unavailable")

    class NoDelayAgent(TestAgent):
        async def _wait_before_fetch_retry(self, consecutive_failures: int) -> None:
            return None

    agent = NoDelayAgent()
    subscription = FailingSubscription()
    agent._sub = subscription

    with pytest.raises(RuntimeError, match="consecutive"):
        await agent._consume_loop()

    assert subscription.calls == MAX_CONSECUTIVE_FETCH_FAILURES


async def test_transient_fetch_failure_is_retried() -> None:
    agent = TestAgent()

    class RecoveringSubscription:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch(self, batch: int, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise ConnectionError("temporary failure")
            agent._stop_event.set()
            return []

    async def skip_delay(consecutive_failures: int) -> None:
        return None

    subscription = RecoveringSubscription()
    agent._sub = subscription
    agent._wait_before_fetch_retry = skip_delay

    await agent._consume_loop()

    assert subscription.calls == 2


async def test_repeated_heartbeat_failures_terminate_background_task(
    monkeypatch,
    tmp_path,
) -> None:
    class FailingHeartbeatAgent(TestAgent):
        def __init__(self) -> None:
            super().__init__()
            self.publish_calls = 0

        async def _publish_event(self, subject: str, payload: dict) -> None:
            self.publish_calls += 1
            raise ConnectionError("nats unavailable")

    marker = tmp_path / "healthy"
    monkeypatch.setattr(agent_module, "HEALTHY_MARKER", marker)
    monkeypatch.setattr(agent_module, "HEARTBEAT_INTERVAL_SECONDS", 0)
    agent = FailingHeartbeatAgent()

    with pytest.raises(RuntimeError, match="heartbeat publish failed"):
        await agent._heartbeat_loop()

    assert agent.publish_calls == MAX_CONSECUTIVE_HEARTBEAT_FAILURES
    assert marker.exists() is False


async def test_transient_heartbeat_failure_recovers(monkeypatch, tmp_path) -> None:
    class RecoveringHeartbeatAgent(TestAgent):
        def __init__(self) -> None:
            super().__init__()
            self.publish_calls = 0

        async def _publish_event(self, subject: str, payload: dict) -> None:
            self.publish_calls += 1
            if self.publish_calls == 1:
                raise ConnectionError("temporary failure")
            self._stop_event.set()

    marker = tmp_path / "healthy"
    monkeypatch.setattr(agent_module, "HEALTHY_MARKER", marker)
    monkeypatch.setattr(agent_module, "HEARTBEAT_INTERVAL_SECONDS", 0)
    agent = RecoveringHeartbeatAgent()

    await agent._heartbeat_loop()

    assert agent.publish_calls == 2
    assert marker.exists() is True


async def test_heartbeat_task_failure_is_supervised(monkeypatch) -> None:
    class FakeNats:
        async def drain(self) -> None:
            return None

    class FakeRedis:
        async def aclose(self) -> None:
            return None

    class BrokenHeartbeatAgent(TestAgent):
        async def _consume_loop(self) -> None:
            await self._stop_event.wait()

        async def _heartbeat_loop(self) -> None:
            raise RuntimeError("heartbeat crashed")

        async def _publish_event(self, subject: str, payload: dict) -> None:
            return None

    async def fake_connect(url: str):
        return FakeNats(), object()

    async def fake_ensure_streams(js) -> None:
        return None

    async def fake_pull_subscribe(
        js,
        subject: str,
        durable: str,
        max_deliver: int,
        ack_wait: int,
    ):
        return object()

    monkeypatch.setattr(agent_module, "connect", fake_connect)
    monkeypatch.setattr(agent_module, "ensure_streams", fake_ensure_streams)
    monkeypatch.setattr(agent_module, "pull_subscribe", fake_pull_subscribe)
    monkeypatch.setattr(agent_module, "make_redis", lambda url: FakeRedis())

    with pytest.raises(RuntimeError, match="heartbeat crashed"):
        await BrokenHeartbeatAgent().run()


def test_durable_name_is_safe_and_scoped_by_agent_and_subject() -> None:
    first = durable_consumer_name("risk.agent", "events.payment.reviewed")
    second = durable_consumer_name("audit.agent", "events.payment.reviewed")

    assert first != second
    assert first.startswith("agentmesh-risk-agent-events-payment-reviewed-")
    assert all(
        character.isascii() and (character.isalnum() or character in "-_") for character in first
    )


def test_compatible_system_config_change_keeps_durable_identity() -> None:
    original = TestAgent()
    changed_config = original.config.model_copy(update={"system": "renamed-system"})
    reconfigured = TestAgent(config=changed_config)

    assert original._consumer_id == reconfigured._consumer_id


def test_subject_change_gets_a_new_durable_identity() -> None:
    first = durable_consumer_name("risk-agent", "tasks.review")
    second = durable_consumer_name("risk-agent", "tasks.approve")

    assert first != second


async def test_existing_durable_with_different_subject_is_rejected() -> None:
    class FakeJetStream:
        def __init__(self) -> None:
            self.subscribe_called = False

        async def find_stream_name_by_subject(self, subject: str) -> str:
            return "TASKS"

        async def consumer_info(self, stream: str, durable: str):
            config = SimpleNamespace(
                filter_subject="tasks.old",
                filter_subjects=None,
            )
            return SimpleNamespace(config=config)

        async def pull_subscribe(self, *args, **kwargs):
            self.subscribe_called = True
            return object()

    js = FakeJetStream()

    with pytest.raises(DurableConsumerSubjectMismatchError, match="tasks.old"):
        await bus_module.pull_subscribe(js, "tasks.new", durable="safe-durable")

    assert js.subscribe_called is False


async def test_store_clients_are_closed_on_shutdown() -> None:
    class FakeRedis:
        def __init__(self) -> None:
            self.closed = False

        async def aclose(self) -> None:
            self.closed = True

    class FakeQdrant:
        def __init__(self) -> None:
            self.closed = False

        async def close(self) -> None:
            self.closed = True

    agent = TestAgent()
    redis = FakeRedis()
    qdrant = FakeQdrant()
    agent._redis = redis
    agent._qdrant = qdrant

    await agent._close_stores()

    assert redis.closed is True
    assert qdrant.closed is True


async def test_failed_nats_drain_falls_back_to_close(monkeypatch, tmp_path) -> None:
    class FakeNats:
        def __init__(self) -> None:
            self.closed = False

        async def drain(self) -> None:
            raise ConnectionError("reconnecting")

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(agent_module, "HEALTHY_MARKER", tmp_path / "healthy")
    agent = TestAgent()
    nc = FakeNats()
    agent._nc = nc
    agent._redis = None

    await agent._release_resources(publish_stopped=False)

    assert nc.closed is True
