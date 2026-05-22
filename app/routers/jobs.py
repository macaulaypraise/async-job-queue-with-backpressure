import uuid

from fastapi import APIRouter, HTTPException

from app.core.exceptions import BackpressureError
from app.core.metrics import backpressure_rejections_total
from app.dependencies import DbDep, RedisDep
from app.models.job import Job
from app.schemas.job import JobCreate, JobResponse
from app.services import job_service, queue_service

router = APIRouter(prefix="/jobs", tags=["jobs"])


@router.post("", status_code=202, response_model=JobResponse)
async def create_job(body: JobCreate, db: DbDep, redis: RedisDep) -> Job:
    """
    Submit a new job to the queue.

    Returns 202 Accepted — the job is queued, not yet processed.
    Returns 503 if the queue is above the high watermark (backpressure).
    """
    try:
        # 1. Write job record to PostgreSQL first (source of truth)
        job = await job_service.create_job(
            db,
            payload=body.payload,
            priority=body.priority,
        )
        await db.commit()
        await db.refresh(job)

        # 2. Enqueue to Redis Streams (may raise BackpressureError)
        await queue_service.enqueue(
            redis,
            job_id=str(job.id),
            payload=body.payload,
            priority=body.priority,
        )

        return job

    except BackpressureError as e:
        backpressure_rejections_total.inc()
        raise HTTPException(
            status_code=503,
            detail=str(e),
            headers={"Retry-After": "10"},
        )


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: uuid.UUID, db: DbDep) -> Job:
    """
    Fetch the current status and result of a job.
    Poll this endpoint to check if your job has completed.
    """
    job = await job_service.get_job(db, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return job
