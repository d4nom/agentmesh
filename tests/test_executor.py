from __future__ import annotations

import pytest
from pydantic import ValidationError

from agents.executor import ExecutorAgent
from platform_core.config import AgentConfig, LLMConfig


def make_config(
    *,
    publishes: list[str] | None = None,
    params: dict | None = None,
) -> AgentConfig:
    return AgentConfig(
        name="executor",
        module="agents.executor:ExecutorAgent",
        subscribes="tasks.execute",
        publishes=publishes or ["events.incident.completed"],
        stores=["redis"],
        params=params or {"dry_run": True},
        system="test-system",
        llm=LLMConfig(),
        nats_url="nats://test",
        redis_url="redis://test",
        qdrant_url="http://qdrant",
        otel_endpoint="http://jaeger:4317",
    )


def test_live_execution_configuration_fails_closed_at_startup():
    with pytest.raises(ValidationError, match="dry_run"):
        ExecutorAgent(make_config(params={"dry_run": False}))


def test_event_output_uses_event_envelope_type():
    agent = ExecutorAgent(make_config(publishes=["events.incident.completed"]))

    assert agent._output_type == "event"


def test_summary_reroute_uses_task_envelope_type():
    agent = ExecutorAgent(make_config(publishes=["tasks.summarize"]))

    assert agent._output_type == "task"
