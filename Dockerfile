FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

COPY pyproject.toml ./
RUN uv sync --no-install-project --no-dev

COPY platform_core/ ./platform_core/
COPY agents/ ./agents/
COPY configs/ ./configs/
COPY data/ ./data/
RUN uv sync --no-dev

ENV PATH="/app/.venv/bin:${PATH}"

ENTRYPOINT ["python", "-m", "platform_core.runner"]
