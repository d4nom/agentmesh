from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


def new_id() -> str:
    return str(uuid.uuid4())


class Envelope(BaseModel):
    spec_version: str = "1.0"
    message_id: str = Field(default_factory=new_id)
    correlation_id: str
    traceparent: str | None = None
    sender: str
    subject: str
    type: Literal["task", "result", "event", "error"]
    reply_to: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    ttl_ms: int = 60000
    payload: dict[str, Any]
