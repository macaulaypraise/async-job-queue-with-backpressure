import asyncio
import uuid

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.services import job_service, queue_service
from app.services.queue_service import QueueMessage


async def _heartbeat_loop(
    db: AsyncSession, job_id: uuid.UUID, interval: int = 5
) -> None:
    """
    Writes a heartbeat to the DB every `interval` seconds while a job runs.
    Cancelled when the job finishes or fails.
    """
    while True:
        await asyncio.sleep(interval)
        await job_service.update_heartbeat(db, job_id)
        await db.commit()


async def process_one(
    redis: Redis,
    db: AsyncSession,
    consumer_name: str,
) -> bool:
    """
    Pull one job from the queue and execute it.

    Returns True if a job was processed, False if the queue was empty.
    """
    message: QueueMessage | None = await queue_service.dequeue(redis, consumer_name)

    if message is None:
        return False

    job_id = uuid.UUID(message.job_id)

    # Mark job as running in the database
    await job_service.mark_running(db, job_id, worker_id=consumer_name)
    await db.commit()

    # Start heartbeat task — runs concurrently while the job executes
    heartbeat_task = asyncio.create_task(
        _heartbeat_loop(db, job_id)
    )

    try:
        # --- Execute the job handler ---
        result = await _dispatch(message.payload)

        # Success path
        await queue_service.acknowledge(redis, message.stream, message.message_id)
        await job_service.mark_completed(db, job_id, result=result)
        await db.commit()

    except Exception as e:
        # Failure path
        job = await job_service.get_job(db, job_id)
        retry_count = (job.retry_count if job else 0) + 1

        await job_service.mark_failed(
            db,
            job_id=job_id,
            error=str(e),
            retry_count=retry_count,
        )
        await db.commit()
        await queue_service.acknowledge(redis, message.stream, message.message_id)

    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass

    return True


async def _dispatch(payload: dict) -> dict:
    """
    Route a job to the correct handler based on its 'type' field.

    In a real system this would import from app/handlers/.
    For now we simulate work with a sleep.
    """
    job_type = payload.get("type", "unknown")

    if job_type == "send_email":
        await asyncio.sleep(0.1)  # simulate I/O
        return {"sent": True, "to": payload.get("to")}

    elif job_type == "generate_report":
        await asyncio.sleep(0.5)  # simulate heavier work
        return {"report_id": str(uuid.uuid4())}

    else:
        raise ValueError(f"Unknown job type: {job_type!r}")
