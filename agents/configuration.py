from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

from platform_core.bus import expected_envelope_type
from platform_core.config import AgentConfig, StoreName


class NoAgentParams(BaseModel):
    """Strict schema for agents that intentionally expose no parameters."""

    model_config = ConfigDict(extra="forbid")


def single_publish_subject(
    config: AgentConfig,
    *,
    expected_type: Literal["task", "event"],
) -> str:
    """Fail at startup when a single-output agent is wired incorrectly."""
    if len(config.publishes) != 1:
        raise ValueError(
            f"{config.name} requires exactly one publishes subject, got {len(config.publishes)}"
        )
    subject = config.publishes[0]
    actual_type = expected_envelope_type(subject)
    if actual_type != expected_type:
        raise ValueError(
            f"{config.name} requires a {expected_type} output subject, got '{subject}'"
        )
    return subject


def require_store(config: AgentConfig, store: StoreName) -> None:
    if store not in config.stores:
        raise ValueError(f"{config.name} requires stores to include '{store}'")
