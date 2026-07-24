FROM python:3.12.13-slim AS base

COPY --from=ghcr.io/astral-sh/uv:0.5.31 /uv /uvx /bin/

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
    FASTEMBED_CACHE_DIR=/app/.fastembed_cache \
    EMBEDDING_MODEL=BAAI/bge-small-en-v1.5

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Download the runtime model as the same unprivileged user that will load it.
# Keeping user creation and cache ownership before application source copies
# avoids re-chowning the large model layer after ordinary code changes.
RUN useradd --create-home --uid 10001 agentmesh \
    && mkdir -p "${FASTEMBED_CACHE_DIR}" \
    && chown agentmesh:agentmesh "${FASTEMBED_CACHE_DIR}"
USER agentmesh

# Bake the embedding model before application sources are copied. This large
# layer stays cached across ordinary code changes and runtime never needs
# Hugging Face access.
RUN .venv/bin/python -c "import os; from fastembed import TextEmbedding; \
TextEmbedding(model_name=os.environ['EMBEDDING_MODEL'], cache_dir=os.environ['FASTEMBED_CACHE_DIR'])"

USER root

COPY platform_core/ ./platform_core/
COPY agents/ ./agents/
COPY configs/ ./configs/
COPY data/ ./data/
COPY scripts/ ./scripts/
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:${PATH}"

# Runtime agents do not need root privileges. The shared image remains compatible
# with the chaos demo because its helper process and the runner use the same UID.
USER agentmesh

ENTRYPOINT ["python", "-m", "platform_core.runner"]
