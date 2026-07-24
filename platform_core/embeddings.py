from __future__ import annotations

import os

DEFAULT_EMBEDDING_MODEL = "BAAI/bge-small-en-v1.5"
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL)


def fastembed_cache_dir() -> str | None:
    """Shared with the Dockerfile's build-time bake step so the model downloaded
    at build time is found by TextEmbedding() at runtime with no network needed."""
    return os.environ.get("FASTEMBED_CACHE_DIR")
