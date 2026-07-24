"""One-shot job: chunk data/compliance/*.md, embed with fastembed, upsert into Qdrant.

Independent of scripts/seed_runbooks.py by design (see agents/maintenance —
the second domain deliberately duplicates rather than shares code with the
first one) and targets its own `compliance_scenarios` collection, separate
from `runbooks`.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from fastembed import TextEmbedding
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, PointStruct, VectorParams

from platform_core.embeddings import EMBEDDING_MODEL, fastembed_cache_dir

COMPLIANCE_DIR = Path(__file__).resolve().parent.parent / "data" / "compliance"
COLLECTION = os.environ.get("COMPLIANCE_COLLECTION", "compliance_scenarios")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
QDRANT_TIMEOUT_SECONDS = 30

_SECTION_SPLIT = re.compile(r"\n(?=## )")
_H1_PATTERN = re.compile(r"^#\s+(.+)$", re.MULTILINE)
_SECTION_TITLE_PATTERN = re.compile(r"^##\s+(.+)$", re.MULTILINE)


def chunk_markdown(text: str) -> list[tuple[str, str]]:
    """Returns a list of (title, chunk_text) pairs, one per ## section."""
    doc_title_match = _H1_PATTERN.search(text)
    doc_title = doc_title_match.group(1).strip() if doc_title_match else "Untitled"

    chunks: list[tuple[str, str]] = []
    for section in _SECTION_SPLIT.split(text):
        section = section.strip()
        if not section:
            continue
        section_match = _SECTION_TITLE_PATTERN.search(section)
        title = f"{doc_title} — {section_match.group(1).strip()}" if section_match else doc_title
        chunks.append((title, section))
    return chunks


def main() -> None:
    embedder = TextEmbedding(model_name=EMBEDDING_MODEL, cache_dir=fastembed_cache_dir())
    client = QdrantClient(url=QDRANT_URL, timeout=QDRANT_TIMEOUT_SECONDS)

    texts: list[str] = []
    payloads: list[dict] = []
    for path in sorted(COMPLIANCE_DIR.glob("*.md")):
        for title, chunk in chunk_markdown(path.read_text()):
            texts.append(chunk)
            payloads.append({"source": path.name, "title": title, "text": chunk})

    if not texts:
        raise SystemExit(f"no compliance chunks found under {COMPLIANCE_DIR}")

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
    print(
        f"seeded {len(points)} chunks from {len(list(COMPLIANCE_DIR.glob('*.md')))} "
        f"compliance docs into collection '{COLLECTION}'"
    )


if __name__ == "__main__":
    main()
