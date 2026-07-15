import json

import pytest
from pydantic import ValidationError

from agents.maintenance.mock_plans import _PLANS_BY_PURPOSE, MaintenanceMockProvider
from agents.maintenance.schemas import MaintenancePlan, MaintenanceRequest, apply_sla_override

VALID_REQUEST = {
    "request_id": "REQ-2024-0117",
    "priority": "critical",
    "object": "cluster-123",
    "object_type": "patroni_cluster",
    "purpose": "risk_mitigation",
    "subtasks": [
        {"order": 1, "action": "update_os", "constraints": "rolling, node-by-node"},
        {"order": 2, "action": "renew_certificates", "constraints": None},
        {"order": 3, "action": "run_compliance_check", "constraints": "CIS baseline"},
    ],
    "sla_minutes": 30,
    "is_downtime": False,
}


def test_valid_request_passes():
    request = MaintenanceRequest.model_validate(VALID_REQUEST)
    assert request.request_id == "REQ-2024-0117"
    assert len(request.subtasks) == 3


def test_invalid_object_type_rejected():
    bad = {**VALID_REQUEST, "object_type": "mongodb_cluster"}
    with pytest.raises(ValidationError):
        MaintenanceRequest.model_validate(bad)


def test_duplicate_subtask_order_rejected():
    bad = {
        **VALID_REQUEST,
        "subtasks": [
            {"order": 1, "action": "run_compliance_check", "constraints": None},
            {"order": 1, "action": "check_access", "constraints": None},
        ],
    }
    with pytest.raises(ValidationError, match="unique order"):
        MaintenanceRequest.model_validate(bad)


def test_sla_minutes_zero_rejected():
    bad = {**VALID_REQUEST, "sla_minutes": 0}
    with pytest.raises(ValidationError):
        MaintenanceRequest.model_validate(bad)


def test_extra_field_rejected():
    bad = {**VALID_REQUEST, "extra_field": "not allowed"}
    with pytest.raises(ValidationError):
        MaintenanceRequest.model_validate(bad)


def test_empty_subtasks_rejected():
    bad = {**VALID_REQUEST, "subtasks": []}
    with pytest.raises(ValidationError):
        MaintenanceRequest.model_validate(bad)


def test_invalid_action_rejected():
    bad = {
        **VALID_REQUEST,
        "subtasks": [{"order": 1, "action": "drop_database", "constraints": None}],
    }
    with pytest.raises(ValidationError):
        MaintenanceRequest.model_validate(bad)


@pytest.mark.parametrize("purpose", sorted(_PLANS_BY_PURPOSE))
def test_mock_plans_are_valid_against_schema(purpose):
    plan = MaintenancePlan.model_validate(_PLANS_BY_PURPOSE[purpose])
    assert plan.plan


async def test_mock_provider_matches_purpose():
    provider = MaintenanceMockProvider()
    prompt = json.dumps({"purpose": "compliance", "other": "field"})
    plan = json.loads(await provider.complete(prompt))
    assert plan == _PLANS_BY_PURPOSE["compliance"]


def test_sla_override_forces_exceeds_when_estimate_beats_sla():
    # The mock's own scheduled_maintenance plan claims "fits" even though its
    # 45-minute estimate blows a 30-minute SLA — this is deliberate, to prove
    # the override doesn't trust the model's self-assessment.
    plan = MaintenancePlan.model_validate(_PLANS_BY_PURPOSE["scheduled_maintenance"])
    assert plan.sla_verdict == "fits"
    assert plan.total_estimated_minutes == 45

    overridden = apply_sla_override(plan, sla_minutes=30)
    assert overridden.sla_verdict == "exceeds"


def test_sla_override_leaves_verdict_alone_when_estimate_fits():
    plan = MaintenancePlan.model_validate(_PLANS_BY_PURPOSE["risk_mitigation"])
    overridden = apply_sla_override(plan, sla_minutes=30)
    assert overridden.sla_verdict == "fits"
    assert overridden is plan
