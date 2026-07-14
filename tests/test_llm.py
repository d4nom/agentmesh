import json

import pytest

from platform_core.llm import _PLANS_BY_ERROR_CLASS, MockProvider


def _prompt_for(error_class: str) -> str:
    return (
        "Incident:\n"
        f'{{"error_class": "{error_class}", "severity": "critical", '
        f'"host": "pg-01", "raw_excerpt": "..."}}\n\n'
        "Relevant runbook excerpts:\n(none retrieved)\n"
    )


@pytest.mark.parametrize("error_class", sorted(_PLANS_BY_ERROR_CLASS))
async def test_mock_provider_returns_the_matching_scenario_plan(error_class):
    provider = MockProvider()
    plan = json.loads(await provider.complete(_prompt_for(error_class)))
    assert plan == _PLANS_BY_ERROR_CLASS[error_class]


async def test_mock_provider_does_not_cross_match_other_scenarios():
    provider = MockProvider()
    for error_class in _PLANS_BY_ERROR_CLASS:
        plan = json.loads(await provider.complete(_prompt_for(error_class)))
        for other_class, other_plan in _PLANS_BY_ERROR_CLASS.items():
            if other_class != error_class:
                assert plan != other_plan


async def test_mock_provider_falls_back_to_default_for_unknown_error_class():
    provider = MockProvider()
    plan = json.loads(await provider.complete(_prompt_for("something_else")))
    assert plan["action"] == "manual_triage"


async def test_mock_provider_ignores_keywords_in_unrelated_runbook_chunks():
    """Retrieved runbook chunks can mention other incidents' keywords (e.g. the
    long-running-transactions runbook mentions 'vacuum horizon') — matching must
    key off error_class, not loose substrings, or it'll pick the wrong plan."""
    provider = MockProvider()
    prompt = (
        'Incident:\n{"error_class": "long_running_transaction", "severity": "warning", '
        '"host": "pg-01", "raw_excerpt": "idle in transaction for 45 minutes"}\n\n'
        "Relevant runbook excerpts:\n"
        "Autovacuum unable to reclaim dead tuples (vacuum horizon stuck). "
        "Terminate long-idle sessions holding replication slots."
    )
    plan = json.loads(await provider.complete(prompt))
    assert plan == _PLANS_BY_ERROR_CLASS["long_running_transaction"]
