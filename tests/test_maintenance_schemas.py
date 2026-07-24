import json
from copy import deepcopy

import pytest
from pydantic import ValidationError

from agents.maintenance.mock_plans import _PLANS_BY_PURPOSE, MaintenanceMockProvider
from agents.maintenance.schemas import (
    MaintenancePlan,
    MaintenanceRequest,
    calculate_sla_verdict,
    validate_and_finalize_plan,
)

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


def make_request(**updates) -> MaintenanceRequest:
    data = deepcopy(VALID_REQUEST)
    data.update(updates)
    return MaintenanceRequest.model_validate(data)


def make_plan(
    entries: list[dict],
    *,
    reported_total: int = 0,
    reported_verdict: str = "fits",
    downtime_note: str = "No downtime expected.",
):
    return MaintenancePlan.model_validate(
        {
            "plan": entries,
            "total_estimated_minutes": reported_total,
            "sla_verdict": reported_verdict,
            "downtime_note": downtime_note,
            "risk": "medium",
        }
    )


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
    with pytest.raises(ValidationError, match="contiguous ascending"):
        MaintenanceRequest.model_validate(bad)


@pytest.mark.parametrize("orders", [[2, 1], [1, 3]])
def test_non_contiguous_or_unsorted_subtask_order_rejected(orders):
    bad = {
        **VALID_REQUEST,
        "subtasks": [
            {
                "order": order,
                "action": "run_compliance_check",
                "constraints": None,
            }
            for order in orders
        ],
    }

    with pytest.raises(ValidationError, match="contiguous ascending"):
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


def test_plan_rejects_action_outside_request_whitelist():
    entries = deepcopy(_PLANS_BY_PURPOSE["risk_mitigation"]["plan"])
    entries[0]["action"] = "drop_database"
    with pytest.raises(ValidationError):
        make_plan(entries)


def test_finalize_recomputes_both_model_total_and_verdict():
    raw = deepcopy(_PLANS_BY_PURPOSE["risk_mitigation"])
    raw["total_estimated_minutes"] = 1
    raw["sla_verdict"] = "exceeds"
    proposal = MaintenancePlan.model_validate(raw)

    finalized = validate_and_finalize_plan(proposal, make_request())

    assert finalized.total_estimated_minutes == 25
    assert finalized.sla_verdict == "at_risk"
    # Finalization is side-effect free; the untrusted proposal stays intact.
    assert proposal.total_estimated_minutes == 1
    assert proposal.sla_verdict == "exceeds"


@pytest.mark.parametrize(
    ("total", "expected"),
    [
        (79, "fits"),
        (80, "at_risk"),
        (100, "at_risk"),
        (101, "exceeds"),
    ],
)
def test_sla_verdict_uses_inclusive_twenty_percent_warning_band(total, expected):
    assert calculate_sla_verdict(total, sla_minutes=100) == expected


def test_sla_verdict_rejects_non_positive_sla():
    with pytest.raises(ValueError, match="positive"):
        calculate_sla_verdict(10, sla_minutes=0)


@pytest.mark.parametrize(
    "entries",
    [
        # Missing request subtask.
        deepcopy(_PLANS_BY_PURPOSE["risk_mitigation"]["plan"][:-1]),
        # Extra plan entry.
        [
            *deepcopy(_PLANS_BY_PURPOSE["risk_mitigation"]["plan"]),
            {
                "order": 4,
                "action": "check_access",
                "steps": ["Check access"],
                "estimated_minutes": 1,
            },
        ],
        # Same entries, wrong sequence.
        [
            deepcopy(_PLANS_BY_PURPOSE["risk_mitigation"]["plan"][1]),
            deepcopy(_PLANS_BY_PURPOSE["risk_mitigation"]["plan"][0]),
            deepcopy(_PLANS_BY_PURPOSE["risk_mitigation"]["plan"][2]),
        ],
        # Allowed action, but not the action requested for this position.
        [
            {
                **deepcopy(_PLANS_BY_PURPOSE["risk_mitigation"]["plan"][0]),
                "action": "check_access",
            },
            *deepcopy(_PLANS_BY_PURPOSE["risk_mitigation"]["plan"][1:]),
        ],
        # Correct action in the correct position, but wrong order value.
        [
            {
                **deepcopy(_PLANS_BY_PURPOSE["risk_mitigation"]["plan"][0]),
                "order": 4,
            },
            *deepcopy(_PLANS_BY_PURPOSE["risk_mitigation"]["plan"][1:]),
        ],
    ],
    ids=["missing", "extra", "reordered", "wrong-action", "wrong-order"],
)
def test_finalize_rejects_plan_that_does_not_exactly_match_request(entries):
    proposal = make_plan(entries)
    with pytest.raises(ValueError, match="exactly one entry"):
        validate_and_finalize_plan(proposal, make_request())


def test_critical_request_over_hard_sla_is_rejected_for_rescoping():
    proposal = MaintenancePlan.model_validate(_PLANS_BY_PURPOSE["risk_mitigation"])
    request = make_request(sla_minutes=20)

    with pytest.raises(ValueError, match="critical request exceeds.*re-scope"):
        validate_and_finalize_plan(proposal, request)


def test_noncritical_request_over_advisory_sla_is_finalized_as_exceeds():
    proposal = MaintenancePlan.model_validate(_PLANS_BY_PURPOSE["risk_mitigation"])
    request = make_request(priority="medium", sla_minutes=20)

    finalized = validate_and_finalize_plan(proposal, request)
    assert finalized.total_estimated_minutes == 25
    assert finalized.sla_verdict == "exceeds"


def test_high_priority_overrun_requires_an_explicit_delay_note():
    proposal = make_plan(
        deepcopy(_PLANS_BY_PURPOSE["risk_mitigation"]["plan"]),
        downtime_note="   ",
    )
    request = make_request(priority="high", sla_minutes=20)

    with pytest.raises(ValueError, match="downtime_note.*explaining the delay"):
        validate_and_finalize_plan(proposal, request)
