# Contributing

## Local Development Setup

### Prerequisites
- Docker Desktop (running)
- Python 3.12+
- Poetry
- pyenv (recommended)

### First-time setup
```bash
git clone <repo-url>
cd async-job-queue

# Install dependencies
poetry install

# Install pre-commit hooks (runs ruff + mypy before every commit)
poetry run pre-commit install

# Start the full stack
make dev

# Wait ~20 seconds for migrations to run automatically

# Verify everything is healthy
curl http://localhost:8000/health

```

### Running tests

```bash
make test           # full suite (requires Docker running)
make ci             # ruff + mypy + full test suite

# Faster iteration — unit tests only (no Docker needed)
poetry run pytest tests/unit/ -v

```

### Common commands

```bash
make dev            # start full stack (app + redis + postgres + prometheus + grafana)
make down           # stop all containers
make logs           # tail all container logs
make ci             # run linting, type checking, and tests
make load-test      # start Locust UI at http://localhost:8089

```

### Adding a new job type

1. Add a handler branch in `app/services/worker_service.py` inside `_dispatch()`
2. Add the job type to the Locust scenario in `tests/load/locustfile.py`
3. Write a unit test in `tests/unit/test_worker.py`

### Database migrations

```bash
# After changing a model in app/models/
poetry run alembic revision --autogenerate -m "describe your change"
poetry run alembic upgrade head

# Inside Docker (if running make dev)
docker compose exec app alembic upgrade head

```

### Changing scheduler weights at runtime

```bash
# Shift all capacity to critical during an incident
curl -X PATCH http://localhost:8000/queues/weights \
  -H "Content-Type: application/json" \
  -d '{"critical": 100, "high": 0, "normal": 0}'

# Reset to config defaults
curl -X DELETE http://localhost:8000/queues/weights

```

### Replaying a failed job from the DLQ

```bash
# Check DLQ depth
curl http://localhost:8000/queues/metrics | python3 -m json.tool

# Replay a specific message (get message_id from Redis or logs)
curl -X POST http://localhost:8000/queues/dlq/<message-id>/replay

```

## Project Structure

```text
app/
├── core/           # Redis client, DB engine, logging config
├── models/         # SQLAlchemy ORM models
├── routers/        # FastAPI route handlers (thin — delegate to services)
├── schemas/        # Pydantic request/response models
├── services/       # Business logic (no FastAPI imports)
└── workers/        # Background worker processes

tests/
├── unit/           # Pure Python tests (no I/O, no Docker)
├── integration/    # Tests against real Redis + PostgreSQL
└── load/           # Locust load test scenarios

```

## Code Standards

This project uses:

* **ruff** for linting and formatting (runs automatically via pre-commit)
* **mypy** for type checking (runs automatically via pre-commit)
* **Conventional Commits** for commit messages

Commit format: `type: description`
Examples: `feat:`, `fix:`, `docs:`, `chore:`, `ci:`, `refactor:`
