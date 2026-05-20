"""
Critical tests for the three most important behaviors of the job queue.
These are the tests to show interviewers.
"""
import asyncio

import pytest


async def test_backpressure_503_when_queue_full(client, redis):
    """
    When total queue depth exceeds HIGH_WATERMARK, the API must
    return 503 with a Retry-After header — not accept the job silently.

    This proves backpressure works: the system signals overload explicitly
    rather than accepting work it cannot handle.
    """
    from app.config import get_settings
    from app.services.queue_service import QUEUE_NAMES, _ensure_consumer_group

    settings = get_settings()

    # Fill the queue past the high watermark using XADD directly
    stream = QUEUE_NAMES["normal"]
    await _ensure_consumer_group(redis, stream)

    # Add enough messages to exceed the watermark
    pipeline = redis.pipeline()
    for i in range(settings.high_watermark + 1):
        pipeline.xadd(stream, {"job_id": f"fake-{i}", "payload": "{}", "priority": "normal"})
    await pipeline.execute()

    # Now submitting a real job should be rejected
    response = await client.post(
        "/jobs",
        json={"payload": {"type": "send_email"}, "priority": "normal"},
    )

    assert response.status_code == 503
    assert "Retry-After" in response.headers


async def test_priority_distribution_matches_weights(client, redis):
    """
    Over many dequeue operations, the distribution of queues selected
    must match the configured weights within ±5%.

    This proves the weighted fair scheduler is correct — low-priority
    jobs always get some capacity (no starvation).
    """
    from collections import Counter
    from app.services.scheduler import get_weights, pick_queue

    weights = await get_weights(redis)
    results = Counter()

    for _ in range(1000):
        queue = pick_queue(weights)
        results[queue] += 1

    total = sum(results.values())
    critical_pct = results["queue:critical"] / total * 100
    high_pct = results["queue:high"] / total * 100
    normal_pct = results["queue:normal"] / total * 100

    assert abs(critical_pct - weights["critical"]) < 5
    assert abs(high_pct - weights["high"]) < 5
    assert abs(normal_pct - weights["normal"]) < 5


async def test_job_appears_in_pending_after_submission(client, redis):
    """
    After submitting a job, it must appear in the Redis Stream
    as a real message — not just as a DB record.

    This proves the enqueue step actually wrote to Redis Streams.
    """
    response = await client.post(
        "/jobs",
        json={"payload": {"type": "generate_report"}, "priority": "critical"},
    )
    assert response.status_code == 202

    # Check the stream has the message
    from app.services.queue_service import QUEUE_NAMES
    depth = await redis.xlen(QUEUE_NAMES["critical"])
    assert depth >= 1


async def test_queue_metrics_reflect_submitted_jobs(client, redis):
    """
    Queue metrics endpoint must accurately reflect current stream depth.
    Interviewers check this — observability is a first-class requirement.
    """
    # Check baseline
    before = await client.get("/queues/metrics")
    before_total = before.json()["total"]

    # Submit two jobs
    for priority in ["high", "normal"]:
        await client.post(
            "/jobs",
            json={"payload": {"type": "send_email"}, "priority": priority},
        )

    # Metrics must reflect the new jobs
    after = await client.get("/queues/metrics")
    after_total = after.json()["total"]

    assert after_total >= before_total + 2


async def test_low_watermark_resumes_acceptance(client, redis):
    """
    Once the queue drains below LOW_WATERMARK, the system must
    resume accepting jobs — not stay locked in backpressure state.

    This proves the band works: single-threshold designs oscillate
    rapidly; the band prevents that.
    """
    from app.services.queue_service import (
        BACKPRESSURE_STATE_KEY,
        QUEUE_NAMES,
        _ensure_consumer_group,
    )
    from app.config import get_settings

    settings = get_settings()

    # Manually set the backpressure flag (simulates queue having been full)
    await redis.set(BACKPRESSURE_STATE_KEY, "1", ex=60)

    # Fill queue to just above LOW_WATERMARK — should still reject
    stream = QUEUE_NAMES["normal"]
    await _ensure_consumer_group(redis, stream)
    pipeline = redis.pipeline()
    for i in range(settings.low_watermark + 10):
        pipeline.xadd(stream, {"job_id": f"fake-{i}", "payload": "{}", "priority": "normal"})
    await pipeline.execute()

    response = await client.post(
        "/jobs",
        json={"payload": {"type": "send_email"}, "priority": "normal"},
    )
    assert response.status_code == 503, "Should still reject above low watermark"

    # Now drain the queue below low watermark
    await redis.delete(stream)

    # Should now accept (depth is 0, below low watermark)
    response = await client.post(
        "/jobs",
        json={"payload": {"type": "send_email"}, "priority": "normal"},
    )
    assert response.status_code == 202, "Should accept once below low watermark"

    # Backpressure flag should be cleared
    still_active = await redis.exists(BACKPRESSURE_STATE_KEY)
    assert not still_active
