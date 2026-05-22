# ── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

RUN pip install --no-cache-dir --default-timeout=100 "poetry>=2.1.3,<3.0.0"

COPY pyproject.toml poetry.lock* README.md ./

ENV POETRY_INSTALLER_MAX_WORKERS=1 \
    POETRY_REQUESTS_TIMEOUT=120 \
    POETRY_RETRIES=5

# 1. Create the safe zone
RUN python -m venv /opt/venv

# 2. CRITICAL: Tell Poetry exactly where the active virtual environment is
ENV VIRTUAL_ENV="/opt/venv"
ENV PATH="/opt/venv/bin:$PATH"

RUN poetry config virtualenvs.create false \
    && poetry install --only main --no-interaction --no-ansi --no-root

COPY . .

# ── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

# 3. Copy the natively built safe zone
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app/app ./app
COPY --from=builder /app/migrations ./migrations
COPY --from=builder /app/alembic.ini ./alembic.ini
COPY --from=builder /app/pyproject.toml ./pyproject.toml

# 4. Activate the environment for the runtime container
ENV VIRTUAL_ENV="/opt/venv"
ENV PATH="/opt/venv/bin:$PATH"

USER appuser

CMD ["sh", "-c", "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload"]
