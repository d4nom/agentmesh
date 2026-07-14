FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /bin/

# procps provides pkill, used by scripts/chaos.sh to crash the runner process
# from inside the container (docker compose exec ... pkill) without going
# through `docker compose kill`, which Docker treats as an operator-requested
# stop and won't apply the restart policy to.
RUN apt-get update && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    FASTEMBED_CACHE_DIR=/app/.fastembed_cache

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

COPY platform_core/ ./platform_core/
COPY agents/ ./agents/
COPY configs/ ./configs/
COPY data/ ./data/
COPY scripts/ ./scripts/
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:${PATH}"

# Bake the embedding model into the image at build time: rag/seed otherwise
# pull it from Hugging Face on first use, adding ~10s+ startup latency and
# failing outright on networks that block HF.
RUN python -c "from platform_core.embeddings import EMBEDDING_MODEL, fastembed_cache_dir; \
from fastembed import TextEmbedding; TextEmbedding(model_name=EMBEDDING_MODEL, cache_dir=fastembed_cache_dir())"

ENTRYPOINT ["python", "-m", "platform_core.runner"]
