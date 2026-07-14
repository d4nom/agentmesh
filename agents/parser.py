"""Heuristic log parser. No LLM involved — proves an agent doesn't have to be one."""

from __future__ import annotations

import re

from pydantic import BaseModel

from platform_core.agent import BaseAgent
from platform_core.envelope import Envelope
from platform_core.observability import get_logger

EXCERPT_MAX_CHARS = 300

_CLASSIFICATION_RULES: list[tuple[re.Pattern[str], str, str]] = [
    (
        re.compile(r"remaining connection slots|too many connections", re.I),
        "connection_exhaustion",
        "critical",
    ),
    (
        re.compile(r"no space left on device|pg_wal.*(disk|full)|wal.*no space", re.I),
        "wal_disk_full",
        "critical",
    ),
    (re.compile(r"replication.*(lag|delay)|replica.*behind", re.I), "replication_lag", "warning"),
    (
        re.compile(r"dead tuples|autovacuum.*(behind|bloat)|table bloat", re.I),
        "bloat_vacuum",
        "warning",
    ),
    (
        re.compile(r"idle in transaction|long.running transaction", re.I),
        "long_running_transaction",
        "warning",
    ),
    (
        re.compile(r"backup failed|archive_command failed|pg_basebackup.*error", re.I),
        "failed_backup",
        "critical",
    ),
]

_HOST_PATTERN = re.compile(r"host[=:]\s*([\w.-]+)", re.I)


def classify(raw_log: str) -> tuple[str, str]:
    for pattern, error_class, severity in _CLASSIFICATION_RULES:
        if pattern.search(raw_log):
            return error_class, severity
    return "unknown", "warning"


def extract_host(raw_log: str, fallback: str | None) -> str:
    match = _HOST_PATTERN.search(raw_log)
    if match:
        return match.group(1)
    return fallback or "unknown-host"


def extract_excerpt(raw_log: str) -> str:
    return raw_log.strip()[:EXCERPT_MAX_CHARS]


class RawIncidentPayload(BaseModel):
    raw_log: str
    host: str | None = None


class ParsedIncident(BaseModel):
    error_class: str
    severity: str
    host: str
    raw_excerpt: str


class ParserAgent(BaseAgent):
    async def handle(self, env: Envelope) -> None:
        raw = RawIncidentPayload.model_validate(env.payload)
        error_class, severity = classify(raw.raw_log)
        parsed = ParsedIncident(
            error_class=error_class,
            severity=severity,
            host=extract_host(raw.raw_log, raw.host),
            raw_excerpt=extract_excerpt(raw.raw_log),
        )

        log = get_logger(agent=self.config.name, correlation_id=env.correlation_id)
        log.info("incident_classified", error_class=error_class, severity=severity)

        await self.publish(
            subject=self.config.publishes[0],
            type_="task",
            payload=parsed.model_dump(),
            correlation_id=env.correlation_id,
        )
