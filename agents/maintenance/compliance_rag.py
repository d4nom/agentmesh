"""Same retrieval logic as agents/rag.py, deliberately duplicated rather than
imported: independent modules are worth more than DRY here, so the two
domains never couple through each other's internals."""

from __future__ import annotations

import asyncio
from typing import Any

from fastembed import TextEmbedding
from pydantic import BaseModel, ConfigDict, Field

from agents.configuration import require_store, single_publish_subject
from platform_core.agent import BaseAgent
from platform_core.config import AgentConfig
from platform_core.embeddings import EMBEDDING_MODEL, fastembed_cache_dir
from platform_core.envelope import Envelope
from platform_core.observability import get_logger

DEFAULT_COLLECTION = "compliance_scenarios"
DEFAULT_TOP_K = 2


class ComplianceRagParams(BaseModel):
    model_config = ConfigDict(extra="forbid")

    collection: str = Field(default=DEFAULT_COLLECTION, min_length=1, max_length=255)
    top_k: int = Field(default=DEFAULT_TOP_K, ge=1, le=100)


class RetrieveComplianceInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request: dict[str, Any]
    query: str = Field(min_length=1, max_length=1000)


class ComplianceRagAgent(BaseAgent):
    def __init__(self, config: AgentConfig) -> None:
        super().__init__(config)
        self._params = ComplianceRagParams.model_validate(config.params)
        self._output_subject = single_publish_subject(config, expected_type="task")
        require_store(config, "qdrant")
        self._embedder = TextEmbedding(model_name=EMBEDDING_MODEL, cache_dir=fastembed_cache_dir())

    async def _embed(self, text: str) -> list[float]:
        def _run() -> list[float]:
            return next(iter(self._embedder.embed([text]))).tolist()

        return await asyncio.to_thread(_run)

    async def handle(self, env: Envelope) -> None:
        data = RetrieveComplianceInput.model_validate(env.payload)
        vector = await self._embed(data.query)

        collection = self._params.collection
        top_k = self._params.top_k

        response = await self._qdrant.query_points(
            collection_name=collection, query=vector, limit=top_k
        )
        chunks = [
            {
                "source": point.payload.get("source"),
                "title": point.payload.get("title"),
                "text": point.payload.get("text"),
            }
            for point in response.points
        ]

        log = get_logger(agent=self.config.name, correlation_id=env.correlation_id)
        log.info(
            "compliance_retrieved",
            collection=collection,
            count=len(chunks),
            scenario_titles=[c["title"] for c in chunks],
        )

        await self.publish(
            subject=self._output_subject,
            type_="task",
            payload={"request": data.request, "compliance_chunks": chunks},
            correlation_id=env.correlation_id,
        )
