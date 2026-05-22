# Async Job Queue with Backpressure & Priority Scheduling — Design Document

## Problem Statement

Any system that does work outside the HTTP request-response cycle needs a
job queue. The interesting engineering is not the queue itself — it is
everything around it: how do you prevent a flood of jobs from overwhelming
workers? How do you ensure critical jobs are processed before low-priority
ones? What happens when a job fails permanently, and how do you tell the
difference between a transient error and a poison pill?

This system answers those questions with a production-grade implementation
using Redis Streams, PostgreSQL, and FastAPI. The centerpiece is
backpressure — the mechanism by which a downstream system signals upstream
producers to slow down before it reaches capacity, not after.

---

## Constraints

| Constraint | Value | Rationale |
|---|---|---|
| Backpressure threshold | 10,000 jobs (high), 2,000 (low) | Band prevents oscillation at boundary |
| Job execution timeout | 30 seconds | Prevents hung handlers from blocking workers indefinitely |
| Visibility timeout | 30s (critical), 60s (high), 120s (normal) | Faster recovery for time-sensitive work |
| Heartbeat interval | 5 seconds | Zombie detection within 60s of crash |
| Max retries | 5 with exponential backoff | 1s → 2s → 4s → 8s → 16s with full jitter |
| Worker count | Configurable, default 2 | Scale via WORKER_COUNT env var |
| Queue weights | 60/30/10 (critical/high/normal) | Runtime-changeable via PATCH /queues/weights |

---

## Architecture
Client → POST /jobs → Backpressure check → PostgreSQL (PENDING)
→ Redis Streams (queue:critical/high/normal)
Worker loop → Weighted scheduler → XREADGROUP → Execute with timeout
→ Success: XACK + mark COMPLETED
→ Failure (retryable): exponential backoff + re-enqueue
→ Failure (exhausted): mark FAILED + enqueue_dlq → queue:dlq
Reaper (every 10s) → XPENDING check per priority (per-timeout)
→ Re-enqueue stale messages
→ Mark zombie jobs FAILED (stale heartbeat > 60s)

---

## Key Decisions

### 1. Redis Streams over a simple Redis list

**Chosen:** Redis Streams with XREADGROUP consumer groups

**Alternatives:** Redis LIST (LPUSH/RPOP), Kafka, RabbitMQ, Celery

**Why:** A Redis LIST gives push/pop semantics but no concept of in-flight
messages. If a worker dies after popping but before completing, the job is
lost permanently. Redis Streams XREADGROUP delivers messages into a PENDING
state — the message stays visible in the pending list until explicitly
ACKed. XPENDING lets the reaper query for messages that have been claimed
but not acknowledged, which is the foundation of the visibility timeout
crash recovery mechanism. This is exactly how SQS works internally.

---

### 2. Two-watermark backpressure band

**Chosen:** High watermark (reject) + Low watermark (resume)

**Alternatives:** Single threshold, token bucket, external admission controller

**Why:** A single threshold causes oscillation — the system rapidly
switches between accepting and rejecting as depth fluctuates around the
threshold. The band prevents this: once the system enters backpressure at
10,000 jobs, it stays in that state until depth drains below 2,000. The
state is stored in Redis so all app instances share the same view. This
is analogous to TCP's receive window — the receiver advertises capacity,
not just a binary open/closed signal.

---

### 3. Weighted fair priority scheduling

**Chosen:** Weighted random selection across three priority streams (60/30/10)

**Alternatives:** Strict priority (always drain critical first), single
stream with priority field

**Why:** Strict priority causes starvation — low-priority jobs never execute
if the critical queue is always non-empty. Weighted fair scheduling
guarantees every priority level gets some share of worker capacity. The
weights are runtime-configurable in Redis (PATCH /queues/weights), so
during an incident you can temporarily shift to 100/0/0 without
redeploying. Separate streams also enable different visibility timeouts
per priority — critical gets 30s, normal gets 120s — which is not possible
with a single stream plus a priority field.

---

### 4. PostgreSQL for job state, Redis Streams for transport

**Chosen:** PostgreSQL (ACID job state) + Redis Streams (message transport)

**Alternatives:** Redis only, MongoDB, single-system approach

**Why:** Redis is fast but volatile — a restart loses all data. PostgreSQL
gives ACID guarantees for job state transitions, queryable history, and
heartbeat tracking. Redis Streams handle the transport mechanics (PENDING,
XACK, consumer groups). The two systems have complementary strengths.
The jobs table is the source of truth; the streams are the delivery
mechanism.

---

### 5. Dead Letter Queue with manual replay

**Chosen:** `queue:dlq` Redis Stream, exposed via POST /queues/dlq/{id}/replay

**Alternatives:** Drop permanently failed jobs, write to separate DB table only

**Why:** Silently dropping failed jobs is unacceptable. The DLQ makes
permanent failures observable — operators can alert on DLQ depth and
investigate payloads before replaying. The replay endpoint re-enqueues a
DLQ message into the original priority stream without modifying it, so
replaying is idempotent. DLQ depth is exposed in GET /queues/metrics.

---

### 6. Exponential backoff with full jitter on retry

**Chosen:** `min(base * 2^retry_count, 60s) * random(0, 1)`

**Alternatives:** Fixed delay, linear backoff, no delay

**Why:** Without jitter, all jobs that fail simultaneously retry at exactly
the same moment, creating a thundering herd that amplifies the original
failure. Full jitter spreads retries randomly across the backoff window,
reducing correlated load. The ceiling (60s) prevents unbounded delays for
jobs that have retried many times.

---

## Known Limitations

**Single Redis instance.** Redis is the single point of failure for both
queue transport and backpressure state. A Redis outage stops all job
submission and processing. Mitigation: Redis Sentinel or Cluster for HA.

**Dev server under burst load.** Uvicorn in `--reload` mode is
single-process. Under extreme concurrent load (50+ users), connection resets
occur before the app layer is even reached. Production deployments use
multiple Uvicorn workers or Gunicorn.

**Backoff blocks the worker.** The current implementation calls
`asyncio.sleep(delay)` inside the worker loop before re-enqueuing. Under
high failure rates, this reduces worker throughput. A better approach is
a separate delayed-enqueue mechanism (Redis sorted set scored by
enqueue-at timestamp).

**No per-job-type timeout configuration.** All jobs share the same 30s
timeout. A `generate_report` job legitimately takes longer than a
`send_email` job. A future version should accept timeout as a job field.

**Scheduler weights are not validated at runtime.** PATCH /queues/weights
validates that weights sum to 100, but a process restart reads weights from
Redis without validation. If weights were corrupted in Redis, the scheduler
would behave incorrectly.

---

## What I Would Change at 10x Scale

**Move to Redis Cluster.** Single-node Redis becomes a bottleneck and a
single point of failure above ~100k jobs/day. Redis Cluster shards the
keyspace across multiple nodes.

**Separate the scheduler into its own service.** Currently each worker
process runs the weighted scheduler independently. At scale, a dedicated
scheduling service with global visibility across all queues would make
better decisions and enable more sophisticated algorithms.

**Add delayed-enqueue via sorted set.** Store retry jobs in a Redis sorted
set scored by their enqueue-at timestamp. A separate process polls the set
and moves jobs to the main stream when their time comes. This decouples
retry delay from worker availability.

**Prometheus alerting rules.** Add alert rules for: DLQ depth > 0 for
more than 5 minutes, queue depth > 80% of high watermark for sustained
periods, worker heartbeat age > 2× job timeout.

**Replace uvicorn --reload with Gunicorn + uvicorn workers.** Production
should run `gunicorn -w 4 -k uvicorn.workers.UvicornWorker app.main:app`
to utilize multiple CPU cores for concurrent job submissions.
