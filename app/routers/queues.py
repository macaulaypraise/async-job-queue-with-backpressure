from fastapi import APIRouter

from app.config import get_settings
from app.dependencies import RedisDep
from app.schemas.job import QueueDepthResponse
from app.services.queue_service import get_queue_depths

router = APIRouter(prefix="/queues", tags=["queues"])


@router.get("/metrics", response_model=QueueDepthResponse)
async def queue_metrics(redis: RedisDep):
    """
    Returns current queue depths and whether the system is accepting jobs.
    Use this to monitor backpressure state and worker lag.
    """
    settings = get_settings()
    depths = await get_queue_depths(redis)
    total = sum(depths.values())

    return QueueDepthResponse(
        depths=depths,
        total=total,
        high_watermark=settings.high_watermark,
        low_watermark=settings.low_watermark,
        accepting_jobs=total < settings.high_watermark,
    )
