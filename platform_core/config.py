from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Annotated, Any, Literal, Self

import yaml
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(:-(?P<default>[^}]*))?\}")
_PLATFORM_SUBJECT_PATTERN = re.compile(
    r"^(?:tasks|events|dlq)\.[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*$"
)


def _validate_exact_platform_subject(value: str) -> str:
    if not _PLATFORM_SUBJECT_PATTERN.fullmatch(value):
        raise ValueError(
            "subject must be an exact tasks.*, events.* or dlq.* subject "
            "using alphanumeric, '_' or '-' tokens; wildcards are not allowed"
        )
    return value


PlatformSubject = Annotated[
    str,
    Field(min_length=1),
    AfterValidator(_validate_exact_platform_subject),
]
StoreName = Literal["redis", "qdrant"]


def _validate_unique_stores(values: list[StoreName]) -> list[StoreName]:
    if len(values) != len(set(values)):
        raise ValueError("stores must not contain duplicates")
    return values


StoreSelection = Annotated[
    list[StoreName],
    Field(max_length=2),
    AfterValidator(_validate_unique_stores),
]


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
    model_config = ConfigDict(extra="forbid")

    provider: Literal["mock", "deepseek"] = "mock"
    model: str = "deepseek-chat"


class AgentSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    module: str = Field(min_length=1)
    subscribes: PlatformSubject
    publishes: list[PlatformSubject] = Field(default_factory=list)
    stores: StoreSelection = Field(default_factory=list)
    params: dict[str, Any] = Field(default_factory=dict)


class SystemConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    system: str = Field(min_length=1)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    agents: list[AgentSpec] = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def validate_unique_agent_names(self) -> Self:
        names = [agent.name for agent in self.agents]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"agent names must be unique: {', '.join(duplicates)}")
        return self

    def agent_spec(self, name: str) -> AgentSpec:
        for spec in self.agents:
            if spec.name == name:
                return spec
        raise KeyError(f"agent '{name}' not found in system config")


class AgentConfig(BaseModel):
    """Everything a BaseAgent instance needs to run: its own spec plus
    system-wide LLM settings and infra connection strings resolved from env."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    module: str = Field(min_length=1)
    subscribes: PlatformSubject
    publishes: list[PlatformSubject] = Field(default_factory=list)
    stores: StoreSelection = Field(default_factory=list)
    params: dict[str, Any] = Field(default_factory=dict)
    system: str = Field(min_length=1)
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
