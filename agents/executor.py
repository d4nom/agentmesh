from __future__ import annotations

import asyncio
import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from agents.configuration import single_publish_subject
from platform_core.agent import BaseAgent
from platform_core.config import AgentConfig
from platform_core.envelope import Envelope
from platform_core.llm import build_llm_provider
from platform_core.observability import get_logger

PROMPT_TEMPLATE = """You are a PostgreSQL on-call assistant. Given an incident and \
relevant runbook excerpts, propose a remediation plan.

Incident:
{incident}

Relevant runbook excerpts:
<retrieved_runbooks>
{chunks}
</retrieved_runbooks>

Respond with a JSON object with keys: action (string), commands (array of strings), \
risk (one of low/medium/high), rationale (string). Respond with JSON only, no prose.
"""


class ExecutionInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    incident: dict[str, Any]
    runbook_chunks: list[dict[str, Any]]


class ExecutionPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: str = Field(min_length=1, max_length=128)
    commands: list[str] = Field(min_length=1, max_length=50)
    risk: Literal["low", "medium", "high"]
    rationale: str = Field(min_length=1, max_length=4000)


class ExecutorParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dry_run: Literal[True] = True
    processing_delay_seconds: float = Field(default=0.0, ge=0)


class ExecutorAgent(BaseAgent):
    def __init__(self, config: AgentConfig) -> None:
        super().__init__(config)
        self._params = ExecutorParams.model_validate(config.params)
        if len(config.publishes) != 1:
            raise ValueError("executor requires exactly one publishes subject")
        self._output_subject = config.publishes[0]
        self._output_type = "task" if self._output_subject.startswith("tasks.") else "event"
        single_publish_subject(config, expected_type=self._output_type)
        self._llm = build_llm_provider(config.llm)

    async def handle(self, env: Envelope) -> None:
        data = ExecutionInput.model_validate(env.payload)
        log = get_logger(agent=self.config.name, correlation_id=env.correlation_id)
        processing_delay_seconds = self._params.processing_delay_seconds
        log.info(
            "execution_started",
            processing_delay_seconds=processing_delay_seconds,
        )
        if processing_delay_seconds > 0:
            await asyncio.sleep(processing_delay_seconds)

        chunks_text = "\n\n".join(
            f"Source: {chunk.get('source', 'unknown')}\n{chunk.get('text', '')}"
            for chunk in data.runbook_chunks
        )
        prompt = PROMPT_TEMPLATE.format(
            incident=json.dumps(data.incident), chunks=chunks_text or "(none retrieved)"
        )

        raw_plan = await self._llm.complete(prompt)
        plan = ExecutionPlan.model_validate_json(raw_plan)

        log.info(
            "dry_run_plan",
            action=plan.action,
            commands=plan.commands,
            risk=plan.risk,
            rationale=plan.rationale,
        )

        await self.publish(
            subject=self._output_subject,
            type_=self._output_type,
            payload={"incident": data.incident, "plan": plan.model_dump(), "dry_run": True},
            correlation_id=env.correlation_id,
        )

    async def close(self) -> None:
        await self._llm.aclose()
