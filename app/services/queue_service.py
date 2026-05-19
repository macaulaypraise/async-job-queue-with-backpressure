import json
import uuid
from dataclasses import dataclass

from redis.asyncio import Redis

from app.config import get_settings
from app.services.scheduler import QUEUE_NAMES, get_weights, pick_queue

CONSUMER_GROUP = "job-workers"


class BackpressureError(Exception):
    """Raised when the queue depth exceeds the high watermark."""
    pass


@dataclass
class QueueMessage:
    """A single message read from a Redis Stream."""
    message_id: str
    stream: str
    job_id: str
    payload: dict
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


async def enqueue(redis: Redis, job_id: str, payload: dict, priority: str) -> str:
    """
    Push a job onto the appropriate priority stream.

    Checks queue depth against the high watermark before accepting.
    Raises BackpressureError if the queue is too full.

    Returns the Redis stream message ID.
    """
    settings = get_settings()
    stream = QUEUE_NAMES.get(priority, QUEUE_NAMES["normal"])

    # Backpressure check — count pending messages across all streams
    total_depth = 0
    for s in QUEUE_NAMES.values():
        try:
            info = await redis.xlen(s)
            total_depth += info
        except Exception:
            pass

    if total_depth >= settings.high_watermark:
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
    return message_id


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


async def get_pending_messages(redis: Redis, stream: str, min_idle_ms: int):
    """
    Returns messages that have been delivered but not acknowledged
    and have been idle for at least min_idle_ms milliseconds.
    Used by the reaper to find crashed-worker jobs.
    """
    return await redis.xpending_range(
        stream,
        CONSUMER_GROUP,
        min="-",
        max="+",
        count=100,
        idle=min_idle_ms,
    )
