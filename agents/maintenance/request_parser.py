"""Deterministic validator/normalizer. No LLM — same principle as agents/parser.py:
an agent doesn't have to be an LLM agent, and here the whole point is that we
don't trust the incoming request until it's passed a strict whitelist schema."""

from __future__ import annotations

from pydantic import ValidationError

from agents.configuration import NoAgentParams, single_publish_subject
from platform_core.agent import BaseAgent
from platform_core.config import AgentConfig
from platform_core.envelope import Envelope
from platform_core.observability import get_logger

from .schemas import MaintenanceRequest


def build_query(request: MaintenanceRequest) -> str:
    actions = ", ".join(s.action for s in sorted(request.subtasks, key=lambda s: s.order))
    return f"{request.purpose} {request.object_type} {actions}"


class RequestParserAgent(BaseAgent):
    def __init__(self, config: AgentConfig) -> None:
        super().__init__(config)
        NoAgentParams.model_validate(config.params)
        self._output_subject = single_publish_subject(config, expected_type="task")

    async def handle(self, env: Envelope) -> None:
        log = get_logger(agent=self.config.name, correlation_id=env.correlation_id)

        try:
            request = MaintenanceRequest.model_validate(env.payload)
        except ValidationError as exc:
            log.error(
                "request_validation_failed",
                errors=[
                    {"field": ".".join(str(p) for p in e["loc"]), "message": e["msg"]}
                    for e in exc.errors()
                ],
            )
            raise

        log.info(
            "request_validated",
            request_id=request.request_id,
            object_type=request.object_type,
            priority=request.priority,
            subtask_count=len(request.subtasks),
        )

        await self.publish(
            subject=self._output_subject,
            type_="task",
            payload={"request": request.model_dump(), "query": build_query(request)},
            correlation_id=env.correlation_id,
        )
