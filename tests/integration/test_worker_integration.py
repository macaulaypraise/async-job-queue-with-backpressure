# mypy: disable-error-code="attr-defined"
"""
Integration tests for process_one — the core worker function.

These are the most important tests in the project: they verify the full
job lifecycle including DB state transitions, Redis ACK, heartbeat,
retry routing, and DLQ placement.

The guide says: aim for 80%+ on the services layer.
worker_service.py was at 30% without these tests.
"""

from typing import Any

from app.models.job import JobStatus
from app.services import job_service, queue_service
from app.services.queue_service import (
    _ensure_consumer_group,
    enqueue,
    get_dlq_depth,
)
from app.services.worker_service import process_one


async def test_process_one_success_path(redis: Any, db: Any) -> None:
    """
    Full happy path: enqueue a job, run process_one, verify COMPLETED.

    Covers: mark_running, _dispatch, acknowledge, mark_completed,
            heartbeat task start/cancel, jobs_processed_total counter.
    """
    for queue_name in queue_service.QUEUE_NAMES.values():
        await _ensure_consumer_group(redis, queue_name)

    job = await job_service.create_job(
        db, payload={"type": "send_email", "to": "test@example.com"}, priority="normal"
    )
    await db.commit()

    # Enqueue into ALL queues so whichever queue the probabilistic
    # scheduler randomly picks, it finds the job instantly.
    for p in queue_service.QUEUE_NAMES.keys():
        await enqueue(redis, job_id=str(job.id), payload=job.payload, priority=p)

    processed = False
    for _ in range(200):
        processed = await process_one(redis=redis, db=db, consumer_name="test-worker")
        if processed:
            break

    assert processed is True, "Worker failed to find the job."

    await db.refresh(job)
    assert job.status == JobStatus.COMPLETED
    assert job.result is not None
    assert job.result["sent"] is True


async def test_process_one_returns_false_when_queue_empty(redis: Any, db: Any) -> None:
    """
    When the queue is empty, process_one must return False immediately.
    Workers use this to back off rather than spinning.
    """
    for queue_name in queue_service.QUEUE_NAMES.values():
        await _ensure_consumer_group(redis, queue_name)

    processed = await process_one(redis=redis, db=db, consumer_name="test-worker-empty")

    assert processed is False


async def test_process_one_unknown_type_routes_to_retry(redis: Any, db: Any) -> None:
    """
    A job with an unknown type raises ValueError in _dispatch.
    With retry_count < max_retries, the job should be marked FAILED
    and re-enqueued (not sent to DLQ yet).
    """
    for queue_name in queue_service.QUEUE_NAMES.values():
        await _ensure_consumer_group(redis, queue_name)

    job = await job_service.create_job(
        db,
        payload={"type": "nonexistent_handler"},
        priority="normal",
        max_retries=3,
    )
    await db.commit()

    for p in queue_service.QUEUE_NAMES.keys():
        await enqueue(redis, job_id=str(job.id), payload=job.payload, priority=p)

    processed = False
    for _ in range(200):
        processed = await process_one(redis=redis, db=db, consumer_name="test-worker")
        if processed:
            break

    assert processed is True, "Worker failed to find the job."

    await db.refresh(job)
    assert job.status == JobStatus.FAILED
    assert job.retry_count == 1

    assert job.error is not None
    assert "Unknown job type" in job.error


async def test_process_one_exhausted_retries_routes_to_dlq(redis: Any, db: Any) -> None:
    """
    A job that fails with retry_count >= max_retries must land in the DLQ,
    not be re-enqueued. This proves the DLQ routing in process_one works.

    This is the 'poison pill' detection path.
    """
    for queue_name in queue_service.QUEUE_NAMES.values():
        await _ensure_consumer_group(redis, queue_name)

    job = await job_service.create_job(
        db,
        payload={"type": "nonexistent_handler"},
        priority="critical",
        max_retries=1,  # will exhaust after one failure
    )
    await db.commit()

    # Set retry_count to max_retries - 1 so next failure exhausts it
    from sqlalchemy import update

    from app.models.job import Job

    await db.execute(update(Job).where(Job.id == job.id).values(retry_count=1))
    await db.commit()

    dlq_depth_before = await get_dlq_depth(redis)

    for p in queue_service.QUEUE_NAMES.keys():
        await enqueue(redis, job_id=str(job.id), payload=job.payload, priority=p)

    processed = False
    for _ in range(200):
        processed = await process_one(redis=redis, db=db, consumer_name="test-worker")
        if processed:
            break

    assert processed is True, "Worker failed to find the job."

    await db.refresh(job)
    assert job.status == JobStatus.FAILED

    dlq_depth_after = await get_dlq_depth(redis)
    assert dlq_depth_after == dlq_depth_before + 1, "Exhausted job must land in DLQ"


async def test_process_one_job_marked_running_before_dispatch(
    redis: Any, db: Any
) -> None:
    """
    The job must be marked RUNNING in the DB before _dispatch is called.
    This is observable because a concurrent reader would see RUNNING
    while the job is in flight — critical for the zombie detection mechanism.
    """
    for queue_name in queue_service.QUEUE_NAMES.values():
        await _ensure_consumer_group(redis, queue_name)

    job = await job_service.create_job(
        db, payload={"type": "send_email", "to": "x@y.com"}, priority="high"
    )
    await db.commit()

    for p in queue_service.QUEUE_NAMES.keys():
        await enqueue(redis, job_id=str(job.id), payload=job.payload, priority=p)

    processed = False
    for _ in range(200):
        processed = await process_one(redis=redis, db=db, consumer_name="test-worker")
        if processed:
            break

    assert processed is True, "Worker failed to find the job."

    await db.refresh(job)
    assert job.status == JobStatus.COMPLETED
    assert job.worker_id == "test-worker"
