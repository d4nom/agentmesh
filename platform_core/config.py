from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:-(?P<default>[^}]*))?\}")


def _substitute_env(text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        var_name = match.group(1)
        default = match.group("default")
        value = os.environ.get(var_name)
        if value is not None and value != "":
            return value
        if default is not None:
            return default
        raise ValueError(f"environment variable '{var_name}' is not set and has no default")

    return _ENV_PATTERN.sub(replace, text)


class LLMConfig(BaseModel):
    provider: str = "mock"
    model: str = "deepseek-chat"


class AgentSpec(BaseModel):
    name: str
    module: str
    subscribes: str
    publishes: list[str] = []
    stores: list[str] = []
    params: dict[str, Any] = {}


class SystemConfig(BaseModel):
    system: str
    llm: LLMConfig = LLMConfig()
    agents: list[AgentSpec]

    def agent_spec(self, name: str) -> AgentSpec:
        for spec in self.agents:
            if spec.name == name:
                return spec
        raise KeyError(f"agent '{name}' not found in system config")


class AgentConfig(BaseModel):
    """Everything a BaseAgent instance needs to run: its own spec plus
    system-wide LLM settings and infra connection strings resolved from env."""

    name: str
    module: str
    subscribes: str
    publishes: list[str] = []
    stores: list[str] = []
    params: dict[str, Any] = {}
    system: str
    llm: LLMConfig
    nats_url: str
    redis_url: str
    qdrant_url: str
    otel_endpoint: str


def load_system_config(path: str | Path) -> SystemConfig:
    raw = Path(path).read_text()
    substituted = _substitute_env(raw)
    data = yaml.safe_load(substituted)
    return SystemConfig.model_validate(data)


def build_agent_config(system_config: SystemConfig, agent_name: str) -> AgentConfig:
    spec = system_config.agent_spec(agent_name)
    return AgentConfig(
        name=spec.name,
        module=spec.module,
        subscribes=spec.subscribes,
        publishes=spec.publishes,
        stores=spec.stores,
        params=spec.params,
        system=system_config.system,
        llm=system_config.llm,
        nats_url=os.environ.get("NATS_URL", "nats://localhost:4222"),
        redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
        qdrant_url=os.environ.get("QDRANT_URL", "http://localhost:6333"),
        otel_endpoint=os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"),
    )
