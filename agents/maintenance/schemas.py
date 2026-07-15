from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator


class Subtask(BaseModel):
    model_config = {"extra": "forbid"}
    order: int = Field(ge=1)
    action: Literal[
        "update_os",
        "update_dbms",
        "renew_certificates",
        "run_compliance_check",
        "update_role_model",
        "check_access",
    ]
    constraints: str | None = None


class MaintenanceRequest(BaseModel):
    model_config = {"extra": "forbid"}
    request_id: str = Field(pattern=r"^REQ-\d{4}-\d{4}$")
    priority: Literal["low", "medium", "high", "critical"]
    object: str = Field(min_length=1, max_length=64)
    object_type: Literal["patroni_cluster", "postgres_standalone", "replica_set"]
    purpose: Literal["risk_mitigation", "scheduled_maintenance", "incident_followup", "compliance"]
    subtasks: list[Subtask] = Field(min_length=1)
    sla_minutes: int = Field(gt=0, le=480)
    is_downtime: bool

    @model_validator(mode="after")
    def _unique_subtask_order(self) -> MaintenanceRequest:
        orders = [s.order for s in self.subtasks]
        if len(orders) != len(set(orders)):
            raise ValueError(f"subtasks must have unique order values, got {orders}")
        return self


class PlanEntry(BaseModel):
    model_config = {"extra": "forbid"}
    order: int = Field(ge=1)
    action: str
    steps: list[str] = Field(min_length=1)
    estimated_minutes: int = Field(ge=0)


class MaintenancePlan(BaseModel):
    model_config = {"extra": "forbid"}
    plan: list[PlanEntry] = Field(min_length=1)
    total_estimated_minutes: int = Field(ge=0)
    sla_verdict: Literal["fits", "at_risk", "exceeds"]
    downtime_note: str
    risk: Literal["low", "medium", "high"]


def apply_sla_override(plan: MaintenancePlan, sla_minutes: int) -> MaintenancePlan:
    """Never trust the model's own sla_verdict — recompute deterministically
    from the estimate it produced. A model claiming "fits" while its own plan
    adds up to more minutes than the SLA allows is still "exceeds"."""
    if plan.total_estimated_minutes > sla_minutes:
        return plan.model_copy(update={"sla_verdict": "exceeds"})
    return plan
