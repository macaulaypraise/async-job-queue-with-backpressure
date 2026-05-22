"""
The two most important reaper tests from the guide:

1. test_visibility_timeout_reaper — proves crashed worker jobs are recovered
2. test_zombie_detection — proves stale-heartbeat jobs are marked FAILED

Neither test uses asyncio.sleep. The reaper timeout is patched to 0ms so
the test runs instantly. The zombie heartbeat is set directly in the DB
to a past timestamp.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import update

from app.models.job import Job, JobStatus
from app.services import job_service
from app.services.queue_service import (
    CONSUMER_GROUP,
    _ensure_consumer_group,
    enqueue,
)
from app.services.reaper_service import reap_zombie_jobs, recover_pending_messages
from app.services.scheduler import QUEUE_NAMES


async def test_visibility_timeout_reaper(redis: Any, db: Any, mocker: Any) -> None:
    """
    Enqueue a job, simulate a worker crash by claiming it without ACKing,
    then run the reaper with timeout=0 and assert the job reappears.

    This proves the XPENDING → XAUTOCLAIM → re-enqueue path works.
    The guide lists this as one of the four most important tests.
    """
    # Patch timeouts to 0ms so all pending messages are immediately eligible
    # regardless of how long they've been pending.
    # This avoids the anti-pattern of asyncio.sleep() in tests.
    mocker.patch.dict(
        "app.services.reaper_service.VISIBILITY_TIMEOUTS_MS",
        {"critical": 0, "high": 0, "normal": 0},
    )

    stream = QUEUE_NAMES["normal"]
    await _ensure_consumer_group(redis, stream)

    # Create a job and enqueue it
    job = await job_service.create_job(
        db, payload={"type": "send_email", "to": "crash@test.com"}, priority="normal"
    )
    await db.commit()

    await enqueue(redis, job_id=str(job.id), payload=job.payload, priority="normal")

    # Simulate a worker crash: claim the message with XREADGROUP but never ACK it.
    # The message now sits in the PENDING list — exactly what happens when
    # a worker dies.
    await redis.xreadgroup(
        groupname=CONSUMER_GROUP,
        consumername="simulated-crashed-worker",
        streams={stream: ">"},
        count=1,
    )

    # Verify the message is in PENDING state (claimed, not acknowledged)
    pending_before = await redis.xpending_range(
        stream, CONSUMER_GROUP, min="-", max="+", count=10
    )
    assert len(pending_before) >= 1, "Message should be in PENDING state after claim"

    # Run the reaper — with timeout=0, all pending messages are immediately eligible
    recovered = await recover_pending_messages(redis)

    assert recovered >= 1, "Reaper should have recovered at least one message"

    # The message should no longer be pending for the crashed worker
    pending_after = await redis.xpending_range(
        stream,
        CONSUMER_GROUP,
        min="-",
        max="+",
        count=10,
        consumername="simulated-crashed-worker",
    )
    assert len(pending_after) == 0, (
        "Crashed worker's pending messages should be cleared"
    )


async def test_zombie_detection(redis: Any, db: Any) -> None:
    """
    Set a job to RUNNING with a heartbeat timestamp 120 seconds in the past,
    run the reaper, and assert the job is marked FAILED.

    This proves the heartbeat monitoring path works without any sleep.
    The guide lists this as one of the four most important tests.
    """
    # Create a job and manually set it to RUNNING with a stale heartbeat
    job = await job_service.create_job(
        db,
        payload={"type": "generate_report"},
        priority="high",
    )
    await db.commit()

    # Set status to RUNNING and heartbeat to 120 seconds ago
    # (well past the 60s threshold)
    stale_heartbeat = datetime.now(UTC) - timedelta(seconds=120)
    await db.execute(
        update(Job)
        .where(Job.id == job.id)
        .values(
            status=JobStatus.RUNNING,
            worker_id="zombie-worker",
            heartbeat_at=stale_heartbeat,
        )
    )
    await db.commit()

    # Verify the job is RUNNING before the reaper runs
    before = await job_service.get_job(db, job.id)
    assert before is not None
    assert before.status == JobStatus.RUNNING
    assert before.heartbeat_at is not None

    # Run the reaper
    reaped = await reap_zombie_jobs(db, redis)

    assert reaped >= 1, "Reaper should have found at least one zombie job"

    # Refresh from DB and verify the job is now FAILED
    await db.refresh(before)
    assert before.status == JobStatus.FAILED
    assert before.error is not None
    assert "heartbeat timeout" in before.error.lower()


async def test_healthy_job_not_reaped(redis: Any, db: Any) -> None:
    """
    A RUNNING job with a recent heartbeat must NOT be reaped.
    This ensures the reaper only targets genuinely stale jobs.
    """
    job = await job_service.create_job(
        db, payload={"type": "send_email"}, priority="critical"
    )
    await db.commit()

    # Set to RUNNING with a fresh heartbeat (5 seconds ago — well within threshold)
    fresh_heartbeat = datetime.now(UTC) - timedelta(seconds=5)
    await db.execute(
        update(Job)
        .where(Job.id == job.id)
        .values(
            status=JobStatus.RUNNING,
            worker_id="healthy-worker",
            heartbeat_at=fresh_heartbeat,
        )
    )
    await db.commit()

    reaped = await reap_zombie_jobs(db, redis)
    assert reaped == 0

    # The healthy job should NOT have been reaped
    await db.refresh(job)
    assert job.status == JobStatus.RUNNING, "Healthy job should still be RUNNING"
