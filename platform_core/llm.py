from __future__ import annotations

import json
import os
import re
from typing import Protocol

import httpx

from platform_core.config import LLMConfig

_ERROR_CLASS_PATTERN = re.compile(r'"error_class":\s*"([a-z_]+)"')

_PLANS_BY_ERROR_CLASS: dict[str, dict] = {
    "connection_exhaustion": {
        "action": "reduce_connection_pressure",
        "commands": [
            "SELECT count(*) FROM pg_stat_activity;",
            "ALTER SYSTEM SET max_connections = 300;",
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE state = 'idle' AND state_change < now() - interval '10 minutes';",
        ],
        "risk": "medium",
        "rationale": "Connection slots exhausted; terminate idle sessions and raise the "
        "pool ceiling before it recurs.",
    },
    "wal_disk_full": {
        "action": "reclaim_wal_disk_space",
        "commands": [
            "SELECT slot_name, active, wal_status FROM pg_replication_slots;",
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            "WHERE active = false;",
            "df -h $PGDATA/pg_wal",
        ],
        "risk": "high",
        "rationale": "WAL directory growth usually traces to a stalled or orphaned "
        "replication slot retaining segments.",
    },
    "replication_lag": {
        "action": "investigate_replication_lag",
        "commands": [
            "SELECT client_addr, state, sent_lsn, replay_lsn, replay_lag FROM pg_stat_replication;",
            "SELECT pg_current_wal_lsn();",
        ],
        "risk": "low",
        "rationale": "Check replay lag and network path to the replica before taking "
        "any corrective action.",
    },
    "bloat_vacuum": {
        "action": "run_targeted_vacuum",
        "commands": [
            "SELECT relname, n_dead_tup FROM pg_stat_user_tables ORDER BY n_dead_tup DESC "
            "LIMIT 10;",
            "VACUUM (ANALYZE, VERBOSE) <table>;",
        ],
        "risk": "medium",
        "rationale": "High dead-tuple counts indicate autovacuum is falling behind on this table.",
    },
    "long_running_transaction": {
        "action": "terminate_stuck_transaction",
        "commands": [
            "SELECT pid, usename, application_name, state, xact_start, query "
            "FROM pg_stat_activity WHERE xact_start < now() - interval '5 minutes';",
            "SELECT pg_terminate_backend(<pid>);",
        ],
        "risk": "medium",
        "rationale": "A long-lived transaction is holding back the vacuum horizon; "
        "confirm it isn't a legitimate batch job before terminating it.",
    },
    "failed_backup": {
        "action": "investigate_archive_failure",
        "commands": [
            "SELECT archived_count, failed_count, last_archived_wal, last_failed_wal "
            "FROM pg_stat_archiver;",
            "pg_basebackup -D /tmp/backup_test -Fp -Xs -P -v",
        ],
        "risk": "low",
        "rationale": "Archive command is failing; check target disk space/connectivity "
        "before it turns into a WAL growth incident too.",
    },
}

_DEFAULT_PLAN = {
    "action": "manual_triage",
    "commands": ["SELECT * FROM pg_stat_activity;"],
    "risk": "unknown",
    "rationale": "No matching heuristic for this incident; escalate for manual review.",
}


class LLMProvider(Protocol):
    async def complete(self, prompt: str, *, system: str | None = None) -> str: ...

    async def aclose(self) -> None: ...


class MockProvider:
    """Deterministic, network-free provider: reads the incident's own `error_class`
    out of the prompt and returns the matching canned plan, so the demo is
    reproducible without an API key and never returns another scenario's plan."""

    def __init__(self, model: str = "mock") -> None:
        self.model = model

    async def complete(self, prompt: str, *, system: str | None = None) -> str:
        match = _ERROR_CLASS_PATTERN.search(prompt)
        error_class = match.group(1) if match else None
        plan = _PLANS_BY_ERROR_CLASS.get(error_class, _DEFAULT_PLAN)
        return json.dumps(plan)

    async def aclose(self) -> None:
        return None


class DeepSeekProvider:
    """OpenAI-compatible chat completions client for the DeepSeek API."""

    def __init__(
        self,
        model: str,
        api_key: str,
        base_url: str = "https://api.deepseek.com",
    ) -> None:
        self.model = model
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=30.0,
        )

    async def complete(self, prompt: str, *, system: str | None = None) -> str:
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        response = await self._client.post(
            "/chat/completions",
            json={"model": self.model, "messages": messages},
        )
        response.raise_for_status()
        data = response.json()
        return data["choices"][0]["message"]["content"]

    async def aclose(self) -> None:
        await self._client.aclose()


def build_llm_provider(config: LLMConfig) -> LLMProvider:
    if config.provider == "deepseek":
        api_key = os.environ.get("DEEPSEEK_API_KEY")
        if not api_key:
            raise ValueError("DEEPSEEK_API_KEY must be set when llm.provider is 'deepseek'")
        return DeepSeekProvider(model=config.model, api_key=api_key)
    return MockProvider(model=config.model)
