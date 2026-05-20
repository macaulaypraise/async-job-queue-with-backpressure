from fastapi import APIRouter, HTTPException

from app.config import get_settings
from app.dependencies import RedisDep
from app.schemas.job import QueueDepthResponse, WeightUpdateRequest, WeightResponse
from app.services.queue_service import (
    get_dlq_depth,
    get_queue_depths,
    is_in_backpressure,
    replay_dlq_message,
)

router = APIRouter(prefix="/queues", tags=["queues"])


@router.get("/metrics", response_model=QueueDepthResponse)
async def queue_metrics(redis: RedisDep):
    settings = get_settings()
    depths = await get_queue_depths(redis)
    total = sum(depths.values())
    dlq_depth = await get_dlq_depth(redis)
    backpressure_active = await is_in_backpressure(redis)

    return QueueDepthResponse(
        depths=depths,
        total=total,
        high_watermark=settings.high_watermark,
        low_watermark=settings.low_watermark,
        accepting_jobs=not backpressure_active,
        dlq_depth=dlq_depth,
        backpressure_active=backpressure_active,
    )


@router.post("/dlq/{message_id}/replay", status_code=202)
async def replay_dlq(message_id: str, redis: RedisDep):
    """
    Replay a Dead Letter Queue message back into the main processing queue.

    Use this after investigating and fixing the root cause of a failure.
    The message is removed from the DLQ and re-enqueued for a worker to pick up.
    """
    new_id = await replay_dlq_message(redis, message_id)
    if new_id is None:
        raise HTTPException(
            status_code=404,
            detail=f"DLQ message {message_id!r} not found",
        )
    return {"replayed": True, "new_stream_message_id": new_id}


@router.get("/weights", response_model=WeightResponse)
async def get_weights_endpoint(redis: RedisDep):
    """
    Return current scheduler weights.
    Shows Redis-stored values if set, otherwise config defaults.
    """
    from app.services.scheduler import get_weights
    weights = await get_weights(redis)

    # Determine if values came from Redis or config fallback
    stored = await redis.get("scheduler:weight:critical")
    source = "redis" if stored is not None else "config"

    return WeightResponse(
        critical=weights["critical"],
        high=weights["high"],
        normal=weights["normal"],
        source=source,
    )


@router.patch("/weights", response_model=WeightResponse)
async def update_weights(body: WeightUpdateRequest, redis: RedisDep):
    """
    Update scheduler weights at runtime without redeploying.

    Weights take effect on the next worker scheduling cycle.
    Use during incidents to prioritize critical work:
      PATCH /queues/weights {"critical": 100, "high": 0, "normal": 0}

    Weights must sum to 100.
    """
    await redis.set("scheduler:weight:critical", str(body.critical))
    await redis.set("scheduler:weight:high", str(body.high))
    await redis.set("scheduler:weight:normal", str(body.normal))

    return WeightResponse(
        critical=body.critical,
        high=body.high,
        normal=body.normal,
        source="redis",
    )


@router.delete("/weights")
async def reset_weights(redis: RedisDep):
    """
    Reset weights to config defaults by removing Redis overrides.
    The scheduler falls back to config values automatically.
    """
    await redis.delete(
        "scheduler:weight:critical",
        "scheduler:weight:high",
        "scheduler:weight:normal",
    )
    return {"reset": True, "source": "config"}
