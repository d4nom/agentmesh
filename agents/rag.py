from __future__ import annotations

import asyncio

from fastembed import TextEmbedding
from pydantic import BaseModel

from platform_core.agent import BaseAgent
from platform_core.config import AgentConfig
from platform_core.envelope import Envelope
from platform_core.observability import get_logger

EMBEDDING_MODEL = "BAAI/bge-small-en-v1.5"
DEFAULT_COLLECTION = "runbooks"
DEFAULT_TOP_K = 3


class ParsedIncident(BaseModel):
    error_class: str
    severity: str
    host: str
    raw_excerpt: str


class RagAgent(BaseAgent):
    def __init__(self, config: AgentConfig) -> None:
        super().__init__(config)
        self._embedder = TextEmbedding(model_name=EMBEDDING_MODEL)

    async def _embed(self, text: str) -> list[float]:
        def _run() -> list[float]:
            return next(iter(self._embedder.embed([text]))).tolist()

        return await asyncio.to_thread(_run)

    async def handle(self, env: Envelope) -> None:
        incident = ParsedIncident.model_validate(env.payload)
        query = f"{incident.error_class} {incident.raw_excerpt}"
        vector = await self._embed(query)

        collection = self.config.params.get("collection", DEFAULT_COLLECTION)
        top_k = self.config.params.get("top_k", DEFAULT_TOP_K)

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
            subject=self.config.publishes[0],
            type_="task",
            payload={"incident": incident.model_dump(), "runbook_chunks": chunks},
            correlation_id=env.correlation_id,
        )
