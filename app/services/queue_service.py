import json
from dataclasses import dataclass
from typing import Any, cast

import structlog
from redis.asyncio import Redis

from app.config import get_settings
from app.core.exceptions import BackpressureError
from app.services.scheduler import QUEUE_NAMES, get_weights, pick_queue

logger = structlog.get_logger()


CONSUMER_GROUP = "job-workers"
QUEUE_DLQ = "queue:dlq"
DLQ_CONSUMER_GROUP = "dlq-workers"
BACKPRESSURE_STATE_KEY = "backpressure:active"


@dataclass
class QueueMessage:
    """A single message read from a Redis Stream."""

    message_id: str
    stream: str
    job_id: str
    payload: dict[str, Any]
    priority: str


async def _ensure_consumer_group(redis: Redis, stream: str) -> None:
    """
    Create the consumer group if it does not exist.
    MKSTREAM creates the stream itself if it does not exist yet.
    """
    try:
        await redis.xgroup_create(stream, CONSUMER_GROUP, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise


async def enqueue(
    redis: Redis, job_id: str, payload: dict[str, Any], priority: str
) -> str:
    """
    Push a job onto the appropriate priority stream.

    Uses a two-watermark band to prevent oscillation:
    - If total depth >= HIGH_WATERMARK → enter backpressure, reject
    - If in backpressure AND depth >= LOW_WATERMARK → stay rejected
    - If in backpressure AND depth < LOW_WATERMARK → exit backpressure, accept

    Raises BackpressureError if currently rejecting.
    Returns the Redis stream message ID.
    """
    settings = get_settings()
    stream = QUEUE_NAMES.get(priority, QUEUE_NAMES["normal"])

    # Count total depth across all streams
    total_depth = 0
    for s in QUEUE_NAMES.values():
        try:
            total_depth += await redis.xlen(s)
        except Exception:
            pass

    # Check whether we're currently in backpressure state
    in_backpressure = await redis.exists(BACKPRESSURE_STATE_KEY)

    if in_backpressure:
        if total_depth >= settings.low_watermark:
            # Still above low watermark — stay in backpressure
            raise BackpressureError(
                f"Queue depth {total_depth} still above low watermark "
                f"{settings.low_watermark}. Retry later."
            )
        else:
            # Drained below low watermark — exit backpressure
            await redis.delete(BACKPRESSURE_STATE_KEY)

    elif total_depth >= settings.high_watermark:
        # Crossed high watermark — enter backpressure
        await redis.set(BACKPRESSURE_STATE_KEY, "1", ex=300)  # 5min TTL as safety
        raise BackpressureError(
            f"Queue depth {total_depth} exceeds high watermark "
            f"{settings.high_watermark}. Retry later."
        )

    await _ensure_consumer_group(redis, stream)

    message_id = await redis.xadd(
        stream,
        {
            "job_id": job_id,
            "payload": json.dumps(payload),
            "priority": priority,
        },
    )
    logger.info("job_enqueued", job_id=job_id, priority=priority, stream=stream)
    return cast(str, message_id)


async def dequeue(redis: Redis, consumer_name: str) -> QueueMessage | None:
    """
    Pull one job from a queue selected by the weighted scheduler.

    Uses XREADGROUP so the message stays in PENDING state until
    explicitly acknowledged — this is what enables crash recovery.
    """
    weights = await get_weights(redis)
    stream = pick_queue(weights)

    results = await redis.xreadgroup(
        groupname=CONSUMER_GROUP,
        consumername=consumer_name,
        streams={stream: ">"},  # ">" means: only undelivered messages
        count=1,
        block=2000,  # wait up to 2 seconds if the stream is empty
    )

    if not results:
        return None

    stream_name, messages = results[0]
    message_id, data = messages[0]

    return QueueMessage(
        message_id=message_id,
        stream=stream_name,
        job_id=data["job_id"],
        payload=json.loads(data["payload"]),
        priority=data["priority"],
    )


async def acknowledge(redis: Redis, stream: str, message_id: str) -> None:
    """
    Mark a message as successfully processed.
    Removes it from the PENDING entries list.
    """
    await redis.xack(stream, CONSUMER_GROUP, message_id)


async def get_queue_depths(redis: Redis) -> dict[str, int]:
    """Returns the current depth of each priority stream."""
    depths = {}
    for priority, stream in QUEUE_NAMES.items():
        try:
            depths[priority] = await redis.xlen(stream)
        except Exception:
            depths[priority] = 0
    return depths


async def get_pending_messages(
    redis: Redis, stream: str, min_idle_ms: int
) -> list[Any]:
    """
    Returns messages that have been delivered but not acknowledged
    and have been idle for at least min_idle_ms milliseconds.
    Used by the reaper to find crashed-worker jobs.
    """
    return cast(
        list[Any],
        await redis.xpending_range(
            stream,
            CONSUMER_GROUP,
            min="-",
            max="+",
            count=100,
            idle=min_idle_ms,
        ),
    )


async def ensure_all_consumer_groups(redis: Redis) -> None:
    """
    Create consumer groups for all priority streams at startup.
    Called by the worker before entering its processing loop.
    Without this, XREADGROUP fails if no job has been enqueued yet.
    """
    for stream in QUEUE_NAMES.values():
        await _ensure_consumer_group(redis, stream)


async def enqueue_dlq(
    redis: Redis,
    job_id: str,
    payload: dict[str, Any],
    priority: str,
    error: str,
    retry_count: int,
) -> str:
    """
    Move a permanently failed job to the Dead Letter Queue.

    Called when retry_count >= max_retries. The DLQ is a separate
    Redis Stream that holds jobs for human investigation and manual replay.
    It does NOT have a consumer group — messages sit there until acted on.

    Returns the DLQ stream message ID.
    """
    try:
        await redis.xgroup_create(QUEUE_DLQ, DLQ_CONSUMER_GROUP, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise

    message_id = await redis.xadd(
        QUEUE_DLQ,
        {
            "job_id": job_id,
            "payload": json.dumps(payload),
            "priority": priority,
            "error": error,
            "retry_count": str(retry_count),
        },
    )
    logger.warning(
        "job_sent_to_dlq", job_id=job_id, retry_count=retry_count, error=error
    )
    return cast(str, message_id)


async def get_dlq_depth(redis: Redis) -> int:
    """Returns the current number of messages in the Dead Letter Queue."""
    try:
        return cast(int, await redis.xlen(QUEUE_DLQ))
    except Exception:
        return 0


async def replay_dlq_message(redis: Redis, message_id: str) -> str | None:
    """
    Read one message from the DLQ by ID and re-enqueue it to its
    original priority stream so workers can pick it up again.

    Used by the POST /queues/dlq/{message_id}/replay endpoint.
    Returns the new stream message ID, or None if the message wasn't found.
    """
    # Read the specific message from the DLQ stream
    results = await redis.xrange(QUEUE_DLQ, min=message_id, max=message_id)
    if not results:
        return None

    msg_id, data = results[0]

    # Re-enqueue to the original priority stream
    new_message_id = await enqueue(
        redis,
        job_id=data["job_id"],
        payload=json.loads(data["payload"]),
        priority=data["priority"],
    )

    # Remove from DLQ — it's been replayed
    await redis.xdel(QUEUE_DLQ, msg_id)

    return new_message_id


async def is_in_backpressure(redis: Redis) -> bool:
    """Returns True if the system is currently rejecting new jobs."""
    return bool(await redis.exists(BACKPRESSURE_STATE_KEY))
