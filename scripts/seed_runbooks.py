"""One-shot job: chunk data/runbooks/*.md, embed with fastembed, upsert into Qdrant."""

from __future__ import annotations

import os
import re
from pathlib import Path

from fastembed import TextEmbedding
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

RUNBOOKS_DIR = Path(__file__).resolve().parent.parent / "data" / "runbooks"
COLLECTION = os.environ.get("RUNBOOKS_COLLECTION", "runbooks")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
EMBEDDING_MODEL = "BAAI/bge-small-en-v1.5"

_SECTION_SPLIT = re.compile(r"\n(?=## )")


def chunk_markdown(text: str) -> list[str]:
    return [section.strip() for section in _SECTION_SPLIT.split(text) if section.strip()]


def main() -> None:
    embedder = TextEmbedding(model_name=EMBEDDING_MODEL)
    client = QdrantClient(url=QDRANT_URL)

    texts: list[str] = []
    payloads: list[dict] = []
    for path in sorted(RUNBOOKS_DIR.glob("*.md")):
        for chunk in chunk_markdown(path.read_text()):
            texts.append(chunk)
            payloads.append({"source": path.name, "text": chunk})

    if not texts:
        raise SystemExit(f"no runbook chunks found under {RUNBOOKS_DIR}")

    vectors = list(embedder.embed(texts))
    dim = len(vectors[0])

    if not client.collection_exists(COLLECTION):
        client.create_collection(
            COLLECTION,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
        )

    points = [
        PointStruct(id=i, vector=vector.tolist(), payload=payload)
        for i, (vector, payload) in enumerate(zip(vectors, payloads, strict=True))
    ]
    client.upsert(COLLECTION, points=points)
    print(f"seeded {len(points)} chunks from {len(list(RUNBOOKS_DIR.glob('*.md')))} "
          f"runbooks into collection '{COLLECTION}'")


if __name__ == "__main__":
    main()
