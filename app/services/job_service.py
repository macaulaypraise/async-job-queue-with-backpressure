import uuid
from datetime import UTC, datetime
from typing import Any

import structlog
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import Job, JobPriority, JobStatus

logger = structlog.get_logger()


async def create_job(
    db: AsyncSession,
    payload: dict[str, Any],
    priority: str = JobPriority.NORMAL,
    max_retries: int = 5,
) -> Job:
    """Insert a new job row with PENDING status."""
    job = Job(
        id=uuid.uuid4(),
        payload=payload,
        priority=priority,
        status=JobStatus.PENDING,
        max_retries=max_retries,
    )
    db.add(job)
    await db.flush()  # writes to DB within the current transaction
    logger.info("job_created", job_id=str(job.id), priority=priority)
    return job


async def get_job(db: AsyncSession, job_id: uuid.UUID) -> Job | None:
    """Fetch a single job by ID."""
    result = await db.execute(select(Job).where(Job.id == job_id))
    return result.scalar_one_or_none()


async def mark_running(db: AsyncSession, job_id: uuid.UUID, worker_id: str) -> None:
    """Transition job to RUNNING and record which worker claimed it."""
    await db.execute(
        update(Job)
        .where(Job.id == job_id, Job.status == JobStatus.PENDING)
        .values(
            status=JobStatus.RUNNING,
            worker_id=worker_id,
            heartbeat_at=datetime.now(UTC),
        )
    )


async def mark_completed(
    db: AsyncSession, job_id: uuid.UUID, result: dict[str, Any]
) -> None:
    """Transition job to COMPLETED and store the result."""
    await db.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(status=JobStatus.COMPLETED, result=result)
    )
    logger.info("job_completed", job_id=str(job_id))


async def mark_failed(
    db: AsyncSession, job_id: uuid.UUID, error: str, retry_count: int
) -> None:
    """Transition job to FAILED and record the error and retry count."""
    await db.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(
            status=JobStatus.FAILED,
            error=error,
            retry_count=retry_count,
        )
    )


async def update_heartbeat(db: AsyncSession, job_id: uuid.UUID) -> None:
    """
    Update the heartbeat timestamp for a running job.
    Called every 5 seconds by the worker.
    A stale heartbeat means the worker crashed — the reaper handles this.
    """
    await db.execute(
        update(Job).where(Job.id == job_id).values(heartbeat_at=datetime.now(UTC))
    )


async def find_zombie_jobs(
    db: AsyncSession, max_heartbeat_age_seconds: int = 60
) -> list[Job]:
    """
    Find RUNNING jobs whose heartbeat has gone stale.
    These represent workers that crashed mid-job.
    """
    from sqlalchemy import func, text

    result = await db.execute(
        select(Job).where(
            Job.status == JobStatus.RUNNING,
            Job.heartbeat_at
            < func.now() - text(f"interval '{max_heartbeat_age_seconds} seconds'"),
        )
    )
    return list(result.scalars().all())
