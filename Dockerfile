# ── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

RUN pip install --no-cache-dir --default-timeout=100 "poetry>=2.1.3,<3.0.0"

COPY pyproject.toml poetry.lock* README.md ./

ENV POETRY_INSTALLER_MAX_WORKERS=1 \
    POETRY_REQUESTS_TIMEOUT=120 \
    POETRY_RETRIES=5


RUN poetry config virtualenvs.in-project true \
    && poetry install --only main --no-interaction --no-ansi --no-root

COPY . .

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app


RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser


COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/app ./app
COPY --from=builder /app/migrations ./migrations
COPY --from=builder /app/alembic.ini ./alembic.ini
COPY --from=builder /app/pyproject.toml ./pyproject.toml


ENV PATH="/app/.venv/bin:$PATH"


USER appuser


CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
