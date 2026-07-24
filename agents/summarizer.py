"""Second config's proof-of-flexibility agent: turns an executor plan into a
one-line human summary. Wired in purely via configs/triage_with_summary.yaml —
no platform_core or existing-agent code changes required."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from agents.configuration import NoAgentParams, single_publish_subject
from platform_core.agent import BaseAgent
from platform_core.config import AgentConfig
from platform_core.envelope import Envelope
from platform_core.observability import get_logger


class ExecutorOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    incident: dict[str, Any]
    plan: dict[str, Any]
    dry_run: bool


def build_summary(incident: dict[str, Any], plan: dict[str, Any]) -> str:
    severity = str(incident.get("severity", "unknown")).upper()
    return (
        f"[{severity}] {incident.get('error_class')} on {incident.get('host')}: "
        f"{plan.get('action')} (risk={plan.get('risk')}). {plan.get('rationale')}"
    )


class SummarizerAgent(BaseAgent):
    def __init__(self, config: AgentConfig) -> None:
        super().__init__(config)
        NoAgentParams.model_validate(config.params)
        self._output_subject = single_publish_subject(config, expected_type="event")

    async def handle(self, env: Envelope) -> None:
        data = ExecutorOutput.model_validate(env.payload)
        summary = build_summary(data.incident, data.plan)

        log = get_logger(agent=self.config.name, correlation_id=env.correlation_id)
        log.info("incident_summary", summary=summary)

        await self.publish(
            subject=self._output_subject,
            type_="event",
            payload={**data.model_dump(), "summary": summary},
            correlation_id=env.correlation_id,
        )
