# Changelog

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [0.1.0] — 2026-05-21

### Added
- Async job queue with three priority streams (critical/high/normal)
- Weighted fair scheduler (60/30/10) with runtime weight updates via API
- Two-watermark backpressure band (HIGH_WATERMARK=10k, LOW_WATERMARK=2k)
- Redis Streams consumer groups with PENDING semantics for at-least-once delivery
- Visibility timeout reaper with per-priority timeouts (30s/60s/120s)
- Heartbeat mechanism for zombie job detection (stale after 60s)
- Dead Letter Queue (queue:dlq) with manual replay endpoint
- Exponential backoff with full jitter on job retry (max 60s)
- Per-job execution timeout via asyncio.wait_for (default 30s)
- PostgreSQL job state machine (PENDING → RUNNING → COMPLETED/FAILED)
- Alembic migrations with auto-run on container startup
- FastAPI endpoints: POST /jobs, GET /jobs/{id}, GET /queues/metrics, PATCH /queues/weights, POST /queues/dlq/{id}/replay
- Structlog structured JSON logging across all services
- Prometheus metrics at /metrics
- Grafana dashboard auto-provisioned from prometheus datasource
- Multi-stage Dockerfile with non-root user (appuser)
- GitHub Actions CI pipeline (ruff + mypy + pytest on every push)
- Pre-commit hooks (ruff, mypy, trailing whitespace, YAML check)
- Locust load tests with legitimate, burst, and mixed workload scenarios
- 39 tests: unit (scheduler, config, worker, reaper) + integration (API, backpressure, DLQ, weights, Redis)
