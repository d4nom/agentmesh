from __future__ import annotations

import pytest
from pydantic import ValidationError

from agents.configuration import NoAgentParams, require_store, single_publish_subject
from platform_core.config import AgentConfig, LLMConfig


def make_config(
    *,
    publishes: list[str] | None = None,
    stores: list[str] | None = None,
) -> AgentConfig:
    return AgentConfig(
        name="configured-agent",
        module="agents.example:ExampleAgent",
        subscribes="tasks.input",
        publishes=["tasks.output"] if publishes is None else publishes,
        stores=["redis"] if stores is None else stores,
        params={},
        system="test-system",
        llm=LLMConfig(),
        nats_url="nats://test",
        redis_url="redis://test",
        qdrant_url="http://qdrant",
        otel_endpoint="http://jaeger:4317",
    )


def test_no_params_schema_rejects_unknown_agent_parameter() -> None:
    with pytest.raises(ValidationError, match="unexpected"):
        NoAgentParams.model_validate({"unexpected": True})


def test_single_publish_subject_returns_validated_output() -> None:
    config = make_config()

    assert single_publish_subject(config, expected_type="task") == "tasks.output"


@pytest.mark.parametrize("publishes", [[], ["tasks.one", "tasks.two"]])
def test_single_publish_subject_rejects_wrong_cardinality(publishes) -> None:
    with pytest.raises(ValueError, match="exactly one"):
        single_publish_subject(
            make_config(publishes=publishes),
            expected_type="task",
        )


def test_single_publish_subject_rejects_wrong_subject_type() -> None:
    with pytest.raises(ValueError, match="task output"):
        single_publish_subject(
            make_config(publishes=["events.completed"]),
            expected_type="task",
        )


def test_required_store_is_checked_at_startup() -> None:
    with pytest.raises(ValueError, match="qdrant"):
        require_store(make_config(stores=["redis"]), "qdrant")
