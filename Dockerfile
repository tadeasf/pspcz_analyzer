# ── Builder stage ─────────────────────────────────────────────
FROM python:3.13-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Install dependencies (cached layer)
COPY pyproject.toml uv.lock ./
RUN uv sync --no-dev --frozen --no-install-project

# Copy project and install it
COPY pspcz_analyzer/ pspcz_analyzer/
RUN uv sync --no-dev --frozen

# ── Runtime stage ─────────────────────────────────────────────
FROM python:3.13-slim

WORKDIR /app

# Copy the virtual environment from builder
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/pspcz_analyzer /app/pspcz_analyzer

# Put venv on PATH
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PSPCZ_DEV=0

EXPOSE 8000

CMD ["python", "-m", "pspcz_analyzer.main"]
