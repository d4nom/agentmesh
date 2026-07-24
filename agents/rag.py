from __future__ import annotations

import asyncio
from typing import Literal

from fastembed import TextEmbedding
from pydantic import BaseModel, ConfigDict, Field

from agents.configuration import require_store, single_publish_subject
from platform_core.agent import BaseAgent
from platform_core.config import AgentConfig
from platform_core.embeddings import EMBEDDING_MODEL, fastembed_cache_dir
from platform_core.envelope import Envelope
from platform_core.observability import get_logger

DEFAULT_COLLECTION = "runbooks"
DEFAULT_TOP_K = 3


class RagParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    collection: str = Field(default=DEFAULT_COLLECTION, min_length=1, max_length=255)
    top_k: int = Field(default=DEFAULT_TOP_K, ge=1, le=100)


class ParsedIncident(BaseModel):
    model_config = ConfigDict(extra="forbid")

    error_class: str
    severity: Literal["critical", "warning"]
    host: str = Field(min_length=1, max_length=255)
    raw_excerpt: str = Field(max_length=300)


class RagAgent(BaseAgent):
    def __init__(self, config: AgentConfig) -> None:
        super().__init__(config)
        self._params = RagParams.model_validate(config.params)
        self._output_subject = single_publish_subject(config, expected_type="task")
        require_store(config, "qdrant")
        self._embedder = TextEmbedding(model_name=EMBEDDING_MODEL, cache_dir=fastembed_cache_dir())

    async def _embed(self, text: str) -> list[float]:
        def _run() -> list[float]:
            return next(iter(self._embedder.embed([text]))).tolist()

        return await asyncio.to_thread(_run)

    async def handle(self, env: Envelope) -> None:
        incident = ParsedIncident.model_validate(env.payload)
        query = f"{incident.error_class} {incident.raw_excerpt}"
        vector = await self._embed(query)

        collection = self._params.collection
        top_k = self._params.top_k

        response = await self._qdrant.query_points(
            collection_name=collection, query=vector, limit=top_k
        )
        chunks = [
            {"source": point.payload.get("source"), "text": point.payload.get("text")}
            for point in response.points
        ]

        log = get_logger(agent=self.config.name, correlation_id=env.correlation_id)
        log.info("runbooks_retrieved", collection=collection, count=len(chunks))

        await self.publish(
            subject=self._output_subject,
            type_="task",
            payload={"incident": incident.model_dump(), "runbook_chunks": chunks},
            correlation_id=env.correlation_id,
        )
