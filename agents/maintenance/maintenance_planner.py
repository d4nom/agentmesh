from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ValidationError

from platform_core.agent import BaseAgent
from platform_core.config import AgentConfig
from platform_core.envelope import Envelope
from platform_core.llm import LLMProvider, build_llm_provider
from platform_core.observability import get_logger

from .mock_plans import MaintenanceMockProvider
from .schemas import MaintenancePlan, apply_sla_override

PROMPT_TEMPLATE = """You are a database maintenance planner. Given a validated \
maintenance request and relevant compliance excerpts, build an execution plan \
covering every subtask in order.

Request:
{request}

Relevant compliance excerpts:
{chunks}

Respond with a JSON object with keys: plan (array of {{order, action, steps, \
estimated_minutes}}, one entry per subtask, in order), total_estimated_minutes \
(int), sla_verdict (one of fits/at_risk/exceeds), downtime_note (string), risk \
(one of low/medium/high). Respond with JSON only, no prose.
"""


def _build_llm_provider(config: AgentConfig) -> LLMProvider:
    # platform_core.llm's MockProvider only knows Postgres incident plans; for
    # the maintenance domain's mock case we use our own, everything else
    # (deepseek) is the platform's existing, domain-agnostic provider.
    if config.llm.provider == "mock":
        return MaintenanceMockProvider(model=config.llm.model)
    return build_llm_provider(config.llm)


class PlanMaintenanceInput(BaseModel):
    request: dict[str, Any]
    compliance_chunks: list[dict[str, Any]]


class MaintenancePlannerAgent(BaseAgent):
    def __init__(self, config: AgentConfig) -> None:
        super().__init__(config)
        self._llm = _build_llm_provider(config)

    async def handle(self, env: Envelope) -> None:
        data = PlanMaintenanceInput.model_validate(env.payload)
        chunks_text = "\n\n".join(chunk.get("text", "") for chunk in data.compliance_chunks)
        prompt = PROMPT_TEMPLATE.format(
            request=json.dumps(data.request), chunks=chunks_text or "(none retrieved)"
        )

        log = get_logger(agent=self.config.name, correlation_id=env.correlation_id)

        raw_plan = await self._llm.complete(prompt)
        try:
            plan = MaintenancePlan.model_validate_json(raw_plan)
        except ValidationError as exc:
            log.error(
                "plan_validation_failed",
                errors=[
                    {"field": ".".join(str(p) for p in e["loc"]), "message": e["msg"]}
                    for e in exc.errors()
                ],
            )
            raise

        plan = apply_sla_override(plan, sla_minutes=data.request["sla_minutes"])

        log.info(
            "maintenance_plan",
            request_id=data.request.get("request_id"),
            plan=[entry.model_dump() for entry in plan.plan],
            total_estimated_minutes=plan.total_estimated_minutes,
            sla_verdict=plan.sla_verdict,
            risk=plan.risk,
        )

        await self.publish(
            subject=self.config.publishes[0],
            type_="event",
            payload={"request": data.request, "plan": plan.model_dump()},
            correlation_id=env.correlation_id,
        )
