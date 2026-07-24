"""Deterministic, network-free LLMProvider for the maintenance domain.

platform_core/llm.py's MockProvider is hardcoded to PostgreSQL incident
plans and offers no extension point, so this is a standalone provider that
implements the same structural interface (complete/aclose) rather than a
change to the core one.
"""

from __future__ import annotations

import json
import re

_PURPOSE_PATTERN = re.compile(r'"purpose":\s*"([a-z_]+)"')

_PLANS_BY_PURPOSE: dict[str, dict] = {
    "risk_mitigation": {
        "plan": [
            {
                "order": 1,
                "action": "update_os",
                "steps": [
                    "Drain traffic from the node",
                    "Apply OS security patches",
                    "Reboot the node",
                    "Verify replication has caught up before moving to the next node",
                ],
                "estimated_minutes": 12,
            },
            {
                "order": 2,
                "action": "renew_certificates",
                "steps": [
                    "Renew certificate on standby",
                    "Reload standby",
                    "Switchover to renew the former leader",
                ],
                "estimated_minutes": 8,
            },
            {
                "order": 3,
                "action": "run_compliance_check",
                "steps": ["Run CIS baseline scan", "Collect and archive the report"],
                "estimated_minutes": 5,
            },
        ],
        "total_estimated_minutes": 25,
        "sla_verdict": "fits",
        "downtime_note": "No downtime expected; rolling node-by-node with a pre-update switchover.",
        "risk": "medium",
    },
    "scheduled_maintenance": {
        "plan": [
            {
                "order": 1,
                "action": "renew_certificates",
                "steps": [
                    "Renew certificate on standby",
                    "Reload standby",
                    "Perform switchover",
                    "Renew certificate on the former leader",
                    "Verify the certificate chain across all nodes",
                ],
                "estimated_minutes": 45,
            }
        ],
        "total_estimated_minutes": 45,
        # Deliberately wrong on purpose: this is the mock's own (over-optimistic)
        # self-assessment. The planner must replace it via
        # validate_and_finalize_plan() rather than trust it — that's the whole
        # point of the tight-SLA demo.
        "sla_verdict": "fits",
        "downtime_note": "No downtime, but cross-node certificate propagation "
        "adds coordination overhead.",
        "risk": "medium",
    },
    "incident_followup": {
        "plan": [
            {
                "order": 1,
                "action": "run_compliance_check",
                "steps": [
                    "Re-run the CIS baseline scan against affected nodes",
                    "Diff against the last known-good report",
                ],
                "estimated_minutes": 10,
            },
            {
                "order": 2,
                "action": "check_access",
                "steps": [
                    "Audit recent role grants",
                    "Confirm no unexpected superuser accounts were created",
                ],
                "estimated_minutes": 8,
            },
        ],
        "total_estimated_minutes": 18,
        "sla_verdict": "fits",
        "downtime_note": "Read-only checks, no downtime.",
        "risk": "low",
    },
    "compliance": {
        "plan": [
            {
                "order": 1,
                "action": "run_compliance_check",
                "steps": ["Run the full CIS baseline scan", "Generate the compliance report"],
                "estimated_minutes": 15,
            },
            {
                "order": 2,
                "action": "update_role_model",
                "steps": [
                    "Review role grants against the least-privilege baseline",
                    "Revoke excess grants",
                    "Document the changes",
                ],
                "estimated_minutes": 20,
            },
            {
                "order": 3,
                "action": "check_access",
                "steps": [
                    "Verify pg_hba.conf against network policy",
                    "Confirm no wildcard host entries remain",
                ],
                "estimated_minutes": 10,
            },
        ],
        "total_estimated_minutes": 45,
        "sla_verdict": "at_risk",
        "downtime_note": "No downtime; role changes take effect on next connection.",
        "risk": "medium",
    },
}

_DEFAULT_PLAN = {
    "plan": [
        {
            "order": 1,
            "action": "run_compliance_check",
            "steps": ["No matching heuristic for this purpose; escalate for manual review."],
            "estimated_minutes": 0,
        }
    ],
    "total_estimated_minutes": 0,
    "sla_verdict": "at_risk",
    "downtime_note": "Unknown purpose; manual review required.",
    "risk": "medium",
}


class MaintenanceMockProvider:
    """Reads the request's own `purpose` field out of the prompt and returns
    the matching canned plan — same error_class-matching approach as
    platform_core/llm.py's MockProvider, applied to this domain's field."""

    def __init__(self, model: str = "mock") -> None:
        self.model = model

    async def complete(self, prompt: str, *, system: str | None = None) -> str:
        match = _PURPOSE_PATTERN.search(prompt)
        purpose = match.group(1) if match else None
        plan = _PLANS_BY_PURPOSE.get(purpose, _DEFAULT_PLAN)
        return json.dumps(plan)

    async def aclose(self) -> None:
        return None
