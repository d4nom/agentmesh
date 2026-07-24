from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

MAX_TTL_MS = 15 * 60 * 1000


def new_id() -> str:
    return str(uuid.uuid4())


class Envelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    spec_version: Literal["1.0"] = "1.0"
    message_id: str = Field(default_factory=new_id)
    correlation_id: str = Field(min_length=1)
    traceparent: str | None = None
    sender: str = Field(min_length=1)
    subject: str = Field(min_length=1)
    type: Literal["task", "event", "error"]
    reply_to: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    ttl_ms: int = Field(default=60000, gt=0, le=MAX_TTL_MS)
    payload: dict[str, Any]

    @field_validator("message_id")
    @classmethod
    def validate_message_id(cls, value: str) -> str:
        try:
            parsed = uuid.UUID(value)
        except ValueError as exc:
            raise ValueError("message_id must be a UUID") from exc
        if parsed.version != 4:
            raise ValueError("message_id must be a UUID4")
        return value
