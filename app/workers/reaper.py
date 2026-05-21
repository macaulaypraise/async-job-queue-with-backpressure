import asyncio
import logging
import signal

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings
from app.core.redis_client import close_redis_client, create_redis_client
from app.services.reaper_service import reap_zombie_jobs, recover_pending_messages

logger = structlog.get_logger()
settings = get_settings()

REAPER_INTERVAL_SECONDS = 10


async def run_reaper(stop_event: asyncio.Event) -> None:
    """
    Reaper loop — runs every REAPER_INTERVAL_SECONDS.

    Two responsibilities per cycle:
    1. recover_pending_messages: re-enqueue jobs from crashed workers
       (visibility timeout exceeded in Redis Streams PENDING list)
    2. reap_zombie_jobs: mark RUNNING jobs with stale heartbeats as FAILED
       (worker was alive but job handler hung or timed out)
    """
    redis = await create_redis_client()
    engine = create_async_engine(settings.database_url)
    SessionFactory = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )

    logger.info("reaper_started")

    try:
        while not stop_event.is_set():
            try:
                # Recover messages from crashed workers
                recovered = await recover_pending_messages(redis)
                if recovered:
                    logger.info("reaper_recovered", count=recovered)

                # Reap zombie jobs (stale heartbeats)
                async with SessionFactory() as db:
                    reaped = await reap_zombie_jobs(db, redis)
                    if reaped:
                        logger.info("reaper_zombie_reaped", count=reaped)

            except Exception as e:
                logger.error("reaper_error", error=str(e))

            # Wait before next cycle — interruptible by stop_event
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=REAPER_INTERVAL_SECONDS,
                )
            except TimeoutError:
                pass  # Normal — means stop_event wasn't set, continue looping

    finally:
        await close_redis_client(redis)
        await engine.dispose()
        logger.info("reaper_stopped")


async def main() -> None:
    stop_event = asyncio.Event()

    def handle_signal() -> None:
        logger.info("Reaper shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, handle_signal)
    loop.add_signal_handler(signal.SIGINT, handle_signal)

    await run_reaper(stop_event)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(main())
