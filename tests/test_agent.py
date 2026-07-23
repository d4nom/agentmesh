from __future__ import annotations

from types import SimpleNamespace

import pytest

import platform_core.agent as agent_module
from platform_core.agent import DEFAULT_MAX_DELIVER, BaseAgent
from platform_core.config import AgentConfig, LLMConfig
from platform_core.envelope import Envelope


def make_config() -> AgentConfig:
    return AgentConfig(
        name="test-agent",
        module="tests.test_agent:TestAgent",
        subscribes="tasks.test",
        publishes=["events.task.completed"],
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
    def __init__(self, data: bytes, *, num_delivered: int = 1) -> None:
        self.data = data
        self.metadata = SimpleNamespace(num_delivered=num_delivered)
        self.acked = False
        self.termed = False
        self.nak_delays: list[int] = []

    async def ack(self) -> None:
        self.acked = True

    async def term(self) -> None:
        self.termed = True

    async def nak(self, *, delay: int) -> None:
        self.nak_delays.append(delay)


class TestAgent(BaseAgent):
    __test__ = False

    def __init__(self) -> None:
        super().__init__(make_config())
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

    async def redis_is_down(client, message_id: str) -> bool:
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
        async def _consume_loop(self) -> None:
            raise RuntimeError("consumer crashed")

        async def _heartbeat_loop(self) -> None:
            await self._stop_event.wait()

        async def _publish_event(self, subject: str, payload: dict) -> None:
            return None

    nc = FakeNats()

    async def fake_connect(url: str):
        return nc, object()

    async def fake_ensure_streams(js) -> None:
        return None

    async def fake_pull_subscribe(js, subject: str, durable: str, max_deliver: int, ack_wait: int):
        return object()

    monkeypatch.setattr(agent_module, "connect", fake_connect)
    monkeypatch.setattr(agent_module, "ensure_streams", fake_ensure_streams)
    monkeypatch.setattr(agent_module, "pull_subscribe", fake_pull_subscribe)
    monkeypatch.setattr(agent_module, "make_redis", lambda url: object())

    agent = CrashingAgent()
    with pytest.raises(RuntimeError, match="consumer crashed"):
        await agent.run()

    assert nc.drained is True
