import asyncio
import json
import structlog

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.services import job_service
from app.services.queue_service import (
    CONSUMER_GROUP,
    QUEUE_NAMES,
    acknowledge,
    enqueue,
)

logger = structlog.get_logger()

# Visibility timeouts per priority — matches the architecture diagram.
# Critical jobs are re-queued fastest; normal jobs get more time before
# the reaper intervenes, reducing unnecessary requeues for slow-but-alive workers.
VISIBILITY_TIMEOUTS_MS: dict[str, int] = {
    "critical": 30_000,   # 30 seconds
    "high":     60_000,   # 60 seconds
    "normal":  120_000,   # 120 seconds
}


async def recover_pending_messages(redis: Redis) -> int:
    """
    Find messages claimed by workers but not acknowledged within their
    priority-specific visibility timeout. Re-enqueue them so another
    worker can process them.

    Critical jobs are recovered after 30s, high after 60s, normal after 120s.
    Returns the count of messages recovered.
    """
    recovered = 0

    for priority, stream in QUEUE_NAMES.items():
        timeout_ms = VISIBILITY_TIMEOUTS_MS[priority]
        recovered_this_stream = 0   # ← track per stream

        try:
            pending = await redis.xpending_range(
                stream, CONSUMER_GROUP,
                min="-", max="+",
                count=100, idle=timeout_ms,
            )

            for entry in pending:
                message_id = entry["message_id"]
                claimed = await redis.xautoclaim(
                    stream, CONSUMER_GROUP, "reaper",
                    min_idle_time=timeout_ms,
                    start_id=message_id, count=1,
                )
                if claimed and claimed[1]:
                    _, messages = claimed[0], claimed[1]
                    for msg_id, data in messages:
                        await acknowledge(redis, stream, msg_id)
                        await enqueue(
                            redis,
                            job_id=data["job_id"],
                            payload=json.loads(data["payload"]),
                            priority=data["priority"],
                        )
                        recovered_this_stream += 1
                        recovered += 1

            if recovered_this_stream:     # ← now inside the for loop, after the try block
                logger.info(
                    "reaper_recovered_messages",
                    stream=stream,
                    count=recovered_this_stream,
                    timeout_ms=timeout_ms,
                )

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
