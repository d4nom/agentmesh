import textwrap

import pytest
from pydantic import ValidationError

from platform_core.config import build_agent_config, load_system_config

SAMPLE_YAML = textwrap.dedent(
    """
    system: incident-triage
    llm:
      provider: ${LLM_PROVIDER:-mock}
      model: deepseek-chat
    agents:
      - name: parser
        module: agents.parser:ParserAgent
        subscribes: tasks.parse
        publishes: [tasks.retrieve]
        stores: [redis]
      - name: rag
        module: agents.rag:RagAgent
        subscribes: tasks.retrieve
        publishes: [tasks.execute]
        stores: [redis, qdrant]
        params:
          collection: runbooks
          top_k: 3
    """
)


def _write_config(tmp_path, text=SAMPLE_YAML):
    path = tmp_path / "system.yaml"
    path.write_text(text)
    return path


def test_env_substitution_uses_default_when_unset(tmp_path, monkeypatch):
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    config = load_system_config(_write_config(tmp_path))
    assert config.llm.provider == "mock"


def test_env_substitution_uses_actual_value(tmp_path, monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "deepseek")
    config = load_system_config(_write_config(tmp_path))
    assert config.llm.provider == "deepseek"


def test_missing_env_without_default_raises(tmp_path, monkeypatch):
    monkeypatch.delenv("REQUIRED_VAR", raising=False)
    text = "system: x\nagents: []\nnote: ${REQUIRED_VAR}\n"
    with pytest.raises(ValueError, match="REQUIRED_VAR"):
        load_system_config(_write_config(tmp_path, text))


def test_agent_spec_lookup(tmp_path):
    config = load_system_config(_write_config(tmp_path))
    spec = config.agent_spec("rag")
    assert spec.module == "agents.rag:RagAgent"
    assert spec.params == {"collection": "runbooks", "top_k": 3}


def test_agent_spec_missing_raises(tmp_path):
    config = load_system_config(_write_config(tmp_path))
    with pytest.raises(KeyError):
        config.agent_spec("does-not-exist")


def test_build_agent_config_resolves_infra_urls(tmp_path, monkeypatch):
    monkeypatch.setenv("NATS_URL", "nats://nats:4222")
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379/0")
    monkeypatch.setenv("QDRANT_URL", "http://qdrant:6333")
    system_config = load_system_config(_write_config(tmp_path))
    agent_config = build_agent_config(system_config, "parser")
    assert agent_config.name == "parser"
    assert agent_config.subscribes == "tasks.parse"
    assert agent_config.nats_url == "nats://nats:4222"
    assert agent_config.redis_url == "redis://redis:6379/0"
    assert agent_config.qdrant_url == "http://qdrant:6333"


def test_unknown_llm_provider_is_rejected(tmp_path):
    text = SAMPLE_YAML.replace(
        "provider: ${LLM_PROVIDER:-mock}",
        "provider: mook",
    )

    with pytest.raises(ValidationError, match="provider"):
        load_system_config(_write_config(tmp_path, text))


def test_unknown_config_field_is_rejected(tmp_path):
    text = SAMPLE_YAML.replace(
        "  - name: parser",
        "  - name: parser\n    typo_field: true",
    )

    with pytest.raises(ValidationError, match="typo_field"):
        load_system_config(_write_config(tmp_path, text))


def test_duplicate_agent_names_are_rejected(tmp_path):
    text = SAMPLE_YAML.replace("  - name: rag", "  - name: parser")

    with pytest.raises(ValidationError, match="agent names must be unique"):
        load_system_config(_write_config(tmp_path, text))


@pytest.mark.parametrize(
    "subject",
    ["tasks.>", "events.task.*", "custom.subject"],
)
def test_subscription_must_be_an_exact_platform_subject(tmp_path, subject):
    text = SAMPLE_YAML.replace("subscribes: tasks.parse", f"subscribes: {subject}")

    with pytest.raises(ValidationError, match="exact"):
        load_system_config(_write_config(tmp_path, text))


def test_publish_subject_must_be_exact(tmp_path):
    text = SAMPLE_YAML.replace("publishes: [tasks.retrieve]", "publishes: [tasks.*]")

    with pytest.raises(ValidationError, match="wildcards"):
        load_system_config(_write_config(tmp_path, text))


def test_unknown_shared_store_is_rejected(tmp_path):
    text = SAMPLE_YAML.replace("stores: [redis]", "stores: [redis, qdrnat]", 1)

    with pytest.raises(ValidationError, match="qdrnat"):
        load_system_config(_write_config(tmp_path, text))


def test_duplicate_shared_store_is_rejected(tmp_path):
    text = SAMPLE_YAML.replace("stores: [redis]", "stores: [redis, redis]", 1)

    with pytest.raises(ValidationError, match="duplicates"):
        load_system_config(_write_config(tmp_path, text))


def test_empty_agent_list_is_rejected(tmp_path):
    with pytest.raises(ValidationError, match="agents"):
        load_system_config(_write_config(tmp_path, "system: empty\nagents: []\n"))
