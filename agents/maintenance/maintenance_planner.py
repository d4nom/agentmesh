from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ValidationError

from agents.configuration import NoAgentParams, single_publish_subject
from platform_core.agent import BaseAgent
from platform_core.config import AgentConfig
from platform_core.envelope import Envelope
from platform_core.llm import LLMProvider, build_llm_provider
from platform_core.observability import get_logger

from .mock_plans import MaintenanceMockProvider
from .schemas import MaintenancePlan, MaintenanceRequest, validate_and_finalize_plan

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
(one of low/medium/high). The platform independently validates the ordered \
subtask mapping and recomputes the total and SLA verdict. Respond with JSON \
only, no prose.
"""


def _build_llm_provider(config: AgentConfig) -> LLMProvider:
    # platform_core.llm's MockProvider only knows Postgres incident plans; for
    # the maintenance domain's mock case we use our own, everything else
    # (deepseek) is the platform's existing, domain-agnostic provider.
    if config.llm.provider == "mock":
        return MaintenanceMockProvider(model=config.llm.model)
    return build_llm_provider(config.llm)


class PlanMaintenanceInput(BaseModel):
    model_config = {"extra": "forbid"}
    request: MaintenanceRequest
    compliance_chunks: list[dict[str, Any]]


class MaintenancePlannerAgent(BaseAgent):
    def __init__(self, config: AgentConfig) -> None:
        super().__init__(config)
        NoAgentParams.model_validate(config.params)
        self._output_subject = single_publish_subject(config, expected_type="event")
        self._llm = _build_llm_provider(config)

    async def handle(self, env: Envelope) -> None:
        data = PlanMaintenanceInput.model_validate(env.payload)
        chunks_text = "\n\n".join(chunk.get("text", "") for chunk in data.compliance_chunks)
        prompt = PROMPT_TEMPLATE.format(
            request=json.dumps(data.request.model_dump()), chunks=chunks_text or "(none retrieved)"
        )

        log = get_logger(agent=self.config.name, correlation_id=env.correlation_id)

        raw_plan = await self._llm.complete(prompt)
        try:
            plan = MaintenancePlan.model_validate_json(raw_plan)
            plan = validate_and_finalize_plan(plan, data.request)
        except ValidationError as exc:
            log.error(
                "plan_validation_failed",
                errors=[
                    {"field": ".".join(str(p) for p in e["loc"]), "message": e["msg"]}
                    for e in exc.errors()
                ],
            )
            raise
        except ValueError as exc:
            log.error("plan_semantic_validation_failed", error=str(exc))
            raise

        log.info(
            "maintenance_plan",
            request_id=data.request.request_id,
            plan=[entry.model_dump() for entry in plan.plan],
            total_estimated_minutes=plan.total_estimated_minutes,
            sla_verdict=plan.sla_verdict,
            risk=plan.risk,
        )

        await self.publish(
            subject=self._output_subject,
            type_="event",
            payload={"request": data.request.model_dump(), "plan": plan.model_dump()},
            correlation_id=env.correlation_id,
        )

    async def close(self) -> None:
        await self._llm.aclose()
