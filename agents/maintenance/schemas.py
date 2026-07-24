from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator

MaintenanceAction = Literal[
    "update_os",
    "update_dbms",
    "renew_certificates",
    "run_compliance_check",
    "update_role_model",
    "check_access",
]
PlanStep = Annotated[str, Field(min_length=1, max_length=1000)]


class Subtask(BaseModel):
    model_config = {"extra": "forbid"}
    order: int = Field(ge=1)
    action: MaintenanceAction
    constraints: str | None = Field(default=None, min_length=1, max_length=1000)


class MaintenanceRequest(BaseModel):
    model_config = {"extra": "forbid"}
    request_id: str = Field(pattern=r"^REQ-\d{4}-\d{4}$")
    priority: Literal["low", "medium", "high", "critical"]
    object: str = Field(min_length=1, max_length=64)
    object_type: Literal["patroni_cluster", "postgres_standalone", "replica_set"]
    purpose: Literal["risk_mitigation", "scheduled_maintenance", "incident_followup", "compliance"]
    subtasks: list[Subtask] = Field(min_length=1, max_length=50)
    sla_minutes: int = Field(gt=0, le=480)
    is_downtime: bool

    @model_validator(mode="after")
    def _validate_subtask_order(self) -> MaintenanceRequest:
        orders = [s.order for s in self.subtasks]
        expected = list(range(1, len(self.subtasks) + 1))
        if orders != expected:
            raise ValueError(
                f"subtasks must be stored in contiguous ascending order {expected}, got {orders}"
            )
        return self


class PlanEntry(BaseModel):
    model_config = {"extra": "forbid"}
    order: int = Field(ge=1)
    action: MaintenanceAction
    steps: list[PlanStep] = Field(min_length=1, max_length=100)
    estimated_minutes: int = Field(ge=0, le=480)


class MaintenancePlan(BaseModel):
    model_config = {"extra": "forbid"}
    plan: list[PlanEntry] = Field(min_length=1, max_length=50)
    total_estimated_minutes: int = Field(ge=0)
    sla_verdict: Literal["fits", "at_risk", "exceeds"]
    downtime_note: str = Field(max_length=4000)
    risk: Literal["low", "medium", "high"]


def calculate_sla_verdict(
    total_estimated_minutes: int, sla_minutes: int
) -> Literal["fits", "at_risk", "exceeds"]:
    """Classify an estimate using the documented 20% warning band.

    The comparison uses integer arithmetic so the boundary is exact:
    ``at_risk`` starts at 80% of the SLA and includes the SLA itself.
    """
    if sla_minutes <= 0:
        raise ValueError("sla_minutes must be positive")
    if total_estimated_minutes > sla_minutes:
        return "exceeds"
    if total_estimated_minutes * 5 >= sla_minutes * 4:
        return "at_risk"
    return "fits"


def validate_and_finalize_plan(
    plan: MaintenancePlan, request: MaintenanceRequest
) -> MaintenancePlan:
    """Validate model semantics and replace all model-derived SLA arithmetic.

    The model must preserve the request's one-to-one ordered subtask mapping.
    Its reported total and verdict are treated as untrusted placeholders:
    both are recomputed from the individual plan entries.
    """
    expected = [(subtask.order, subtask.action) for subtask in request.subtasks]
    actual = [(entry.order, entry.action) for entry in plan.plan]
    if actual != expected:
        raise ValueError(
            "plan must contain exactly one entry for every request subtask, "
            "in the same order and with the same action; "
            f"expected {expected}, got {actual}"
        )

    total = sum(entry.estimated_minutes for entry in plan.plan)
    verdict = calculate_sla_verdict(total, request.sla_minutes)

    # The retrieved downtime policy calls a critical SLA a hard ceiling and
    # requires the request to be re-scoped. Other priority/downtime decisions
    # need health, maintenance-window, or sign-off data absent from this input.
    if request.priority == "critical" and verdict == "exceeds":
        raise ValueError(
            f"critical request exceeds its hard SLA ceiling: {total} > "
            f"{request.sla_minutes} minutes; re-scope the request"
        )
    if request.priority == "high" and verdict == "exceeds" and not plan.downtime_note.strip():
        raise ValueError(
            "high-priority SLA overrun requires a non-empty downtime_note explaining the delay"
        )

    return plan.model_copy(
        update={
            "total_estimated_minutes": total,
            "sla_verdict": verdict,
        }
    )
