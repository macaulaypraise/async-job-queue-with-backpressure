import asyncio

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.services import job_service
from app.services.queue_service import (
    CONSUMER_GROUP,
    QUEUE_NAMES,
    acknowledge,
    enqueue,
)

# How long a message can sit in PENDING before we consider the worker dead
VISIBILITY_TIMEOUT_MS = 30_000  # 30 seconds


async def recover_pending_messages(redis: Redis) -> int:
    """
    Find messages claimed by workers but not acknowledged within the
    visibility timeout. Re-enqueue them so another worker can pick them up.

    This is how crashed workers are handled — their in-flight jobs
    automatically reappear for processing.

    Returns the count of messages recovered.
    """
    recovered = 0

    for priority, stream in QUEUE_NAMES.items():
        try:
            pending = await redis.xpending_range(
                stream,
                CONSUMER_GROUP,
                min="-",
                max="+",
                count=100,
                idle=VISIBILITY_TIMEOUT_MS,
            )

            for entry in pending:
                message_id = entry["message_id"]

                # Claim the stale message so we can inspect it
                claimed = await redis.xautoclaim(
                    stream,
                    CONSUMER_GROUP,
                    "reaper",
                    min_idle_time=VISIBILITY_TIMEOUT_MS,
                    start_id=message_id,
                    count=1,
                )

                if claimed and claimed[1]:
                    _, messages = claimed[0], claimed[1]
                    for msg_id, data in messages:
                        # Acknowledge the stale message and re-enqueue
                        await acknowledge(redis, stream, msg_id)
                        await enqueue(
                            redis,
                            job_id=data["job_id"],
                            payload=__import__("json").loads(data["payload"]),
                            priority=data["priority"],
                        )
                        recovered += 1

        except Exception:
            continue

    return recovered


async def reap_zombie_jobs(db: AsyncSession, redis: Redis) -> int:
    """
    Find RUNNING jobs with stale heartbeats and mark them FAILED.
    These jobs will be recovered by recover_pending_messages on the next cycle.

    Returns the count of zombies reaped.
    """
    zombies = await job_service.find_zombie_jobs(db, max_heartbeat_age_seconds=60)

    for job in zombies:
        await job_service.mark_failed(
            db,
            job_id=job.id,
            error="Worker heartbeat timeout — job marked as zombie",
            retry_count=job.retry_count,
        )

    await db.commit()
    return len(zombies)
