# One image for every Python service. The command decides which service runs (see docker-compose.yml, profile "apps").
# PROD: multi-stage build, non-root user (done below), pinned digests, image scanning, one image per service if they diverge.
FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PATH="/app/.venv/bin:$PATH"
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
COPY schemas ./schemas
COPY app.py ./
RUN uv sync --frozen --no-dev && useradd -m app && chown -R app /app
USER app
