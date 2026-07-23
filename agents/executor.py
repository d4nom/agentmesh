from __future__ import annotations

import asyncio
import json
from typing import Any

from pydantic import BaseModel

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
{chunks}

Respond with a JSON object with keys: action (string), commands (array of strings), \
risk (one of low/medium/high), rationale (string). Respond with JSON only, no prose.
"""


class ExecutionInput(BaseModel):
    incident: dict[str, Any]
    runbook_chunks: list[dict[str, Any]]


class ExecutionPlan(BaseModel):
    action: str
    commands: list[str]
    risk: str
    rationale: str


class ExecutorAgent(BaseAgent):
    def __init__(self, config: AgentConfig) -> None:
        super().__init__(config)
        self._llm = build_llm_provider(config.llm)

    async def handle(self, env: Envelope) -> None:
        data = ExecutionInput.model_validate(env.payload)
        log = get_logger(agent=self.config.name, correlation_id=env.correlation_id)
        processing_delay_seconds = float(self.config.params.get("processing_delay_seconds", 0))
        log.info(
            "execution_started",
            processing_delay_seconds=processing_delay_seconds,
        )
        if processing_delay_seconds > 0:
            await asyncio.sleep(processing_delay_seconds)

        chunks_text = "\n\n".join(chunk.get("text", "") for chunk in data.runbook_chunks)
        prompt = PROMPT_TEMPLATE.format(
            incident=json.dumps(data.incident), chunks=chunks_text or "(none retrieved)"
        )

        raw_plan = await self._llm.complete(prompt)
        plan = ExecutionPlan.model_validate_json(raw_plan)

        dry_run = bool(self.config.params.get("dry_run", True))
        if dry_run:
            log.info(
                "dry_run_plan",
                action=plan.action,
                commands=plan.commands,
                risk=plan.risk,
                rationale=plan.rationale,
            )
        else:
            log.warning(
                "live_execution_not_implemented",
                action=plan.action,
                reason="executor intentionally never runs commands against a real database",
            )

        await self.publish(
            subject=self.config.publishes[0],
            type_="event",
            payload={"incident": data.incident, "plan": plan.model_dump(), "dry_run": dry_run},
            correlation_id=env.correlation_id,
        )
