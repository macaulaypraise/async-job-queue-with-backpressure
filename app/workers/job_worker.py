import asyncio
import signal
import uuid
import structlog
import logging
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings
from app.core.redis_client import close_redis_client, create_redis_client
from app.services import job_service, queue_service, worker_service

logger = structlog.get_logger()
settings = get_settings()

# Each worker process gets a unique ID so we can track which worker ran which job
WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"


async def run_worker(worker_id: str, stop_event: asyncio.Event) -> None:
    redis = await create_redis_client()
    engine = create_async_engine(settings.database_url)
    SessionFactory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    # Ensure all consumer groups exist before polling
    from app.services.queue_service import ensure_all_consumer_groups
    await ensure_all_consumer_groups(redis)

    logger.info("worker_started", worker_id=worker_id)

    try:
        while not stop_event.is_set():
            async with SessionFactory() as db:
                try:
                    processed = await worker_service.process_one(
                        redis=redis,
                        db=db,
                        consumer_name=worker_id,
                    )

                    if not processed:
                        # Queue was empty — wait briefly before polling again
                        await asyncio.sleep(0.1)

                except Exception as e:
                    logger.error("worker_error", worker_id=worker_id, error=str(e))
                    await asyncio.sleep(1)  # back off on unexpected errors

    finally:
        await close_redis_client(redis)
        await engine.dispose()
        logger.info("worker_stopped", worker_id=worker_id)


async def main() -> None:
    """
    Entry point for the worker process.
    Starts WORKER_COUNT workers concurrently and handles graceful shutdown.
    """
    stop_event = asyncio.Event()

    def handle_signal():
        logger.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, handle_signal)
    loop.add_signal_handler(signal.SIGINT, handle_signal)

    workers = [
        asyncio.create_task(run_worker(f"{WORKER_ID}-{i}", stop_event))
        for i in range(settings.worker_count)
    ]

    logger.info("all_workers_started", count=settings.worker_count)
    await asyncio.gather(*workers, return_exceptions=True)
    logger.info("all_workers_stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(main())
