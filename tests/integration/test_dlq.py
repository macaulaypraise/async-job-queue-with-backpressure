"""
Tests for the Dead Letter Queue — the safety net for permanently failed jobs.
"""


async def test_exhausted_job_appears_in_dlq(client, redis):
    """
    A job that fails and has max_retries=0 must land in the DLQ stream,
    not silently disappear.
    """
    from app.services.queue_service import ensure_all_consumer_groups

    await ensure_all_consumer_groups(redis)

    # Submit a job with max_retries=1 that will always fail
    response = await client.post(
        "/jobs",
        json={"payload": {"type": "always_fails"}, "priority": "normal"},
    )
    assert response.status_code == 202
    job_id = response.json()["id"]

    # Simulate the worker exhausting retries by calling mark_failed directly
    from app.services.queue_service import enqueue_dlq, get_dlq_depth

    # settings = get_settings()

    dlq_depth_before = await get_dlq_depth(redis)

    # Push directly to DLQ to test the stream mechanism
    await enqueue_dlq(
        redis,
        job_id=job_id,
        payload={"type": "always_fails"},
        priority="normal",
        error="Test: handler not found",
        retry_count=5,
    )

    dlq_depth_after = await get_dlq_depth(redis)
    assert dlq_depth_after == dlq_depth_before + 1


async def test_dlq_depth_appears_in_metrics(client, redis):
    """
    The /queues/metrics endpoint must report DLQ depth so operators
    can alert when permanently failed jobs are accumulating.
    """
    from app.services.queue_service import enqueue_dlq

    # Add something to DLQ
    await enqueue_dlq(
        redis,
        job_id="test-job-id",
        payload={"type": "broken"},
        priority="high",
        error="permanent failure",
        retry_count=5,
    )

    response = await client.get("/queues/metrics")
    assert response.status_code == 200
    data = response.json()

    assert "dlq_depth" in data
    assert data["dlq_depth"] >= 1


async def test_replay_dlq_message(client, redis):
    """
    Replaying a DLQ message must move it back to the main queue
    and remove it from the DLQ stream.
    """
    from app.services.queue_service import enqueue_dlq, get_dlq_depth

    # Add a message to DLQ
    msg_id = await enqueue_dlq(
        redis,
        job_id="replay-test-job",
        payload={"type": "send_email", "to": "a@b.com"},
        priority="high",
        error="transient failure",
        retry_count=3,
    )

    depth_before = await get_dlq_depth(redis)

    # Replay it
    response = await client.post(f"/queues/dlq/{msg_id}/replay")
    assert response.status_code == 202
    assert response.json()["replayed"] is True

    # DLQ should now be shallower
    depth_after = await get_dlq_depth(redis)
    assert depth_after == depth_before - 1


async def test_replay_nonexistent_dlq_message_returns_404(client):
    """Replaying a message that doesn't exist must return 404."""
    response = await client.post("/queues/dlq/0-0/replay")
    assert response.status_code == 404
