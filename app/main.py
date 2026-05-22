from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from prometheus_client import REGISTRY, make_asgi_app
from sqlalchemy import text

from app.core.database import get_db
from app.core.logging import configure_logging
from app.core.metrics import queue_depth
from app.core.redis_client import close_redis_client, create_redis_client
from app.routers import jobs, queues
from app.services.queue_service import get_queue_depths

# Configure structured logging before anything else runs
configure_logging()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.redis = await create_redis_client()

    # Background task: update queue depth gauge every 15 seconds
    async def update_queue_depth_gauge() -> None:
        print("Metrics background task started...")  # Added for debugging
        while True:
            try:
                depths = await get_queue_depths(app.state.redis)
                for priority, depth in depths.items():
                    queue_depth.labels(priority=priority).set(depth)
            except Exception as e:
                print(f"Metrics task error: {e}")
            await asyncio.sleep(15)

    import asyncio

    gauge_task: asyncio.Task[None] = asyncio.create_task(update_queue_depth_gauge())

    yield

    gauge_task.cancel()
    await close_redis_client(app.state.redis)


app = FastAPI(
    title="Async Job Queue",
    description="Job queue with backpressure and priority scheduling",
    version="0.1.0",
    lifespan=lifespan,
)

# Routers
app.include_router(jobs.router)
app.include_router(queues.router)

# Prometheus metrics endpoint
metrics_app = make_asgi_app(registry=REGISTRY)
app.mount("/metrics", metrics_app)


@app.get("/health", tags=["ops"])
async def health(request: Request) -> JSONResponse:
    status = {"status": "ok", "redis": "ok", "postgres": "ok"}
    http_status = 200

    # Check Redis
    try:
        await request.app.state.redis.ping()
    except Exception as e:
        status["redis"] = f"error: {e}"
        status["status"] = "degraded"
        http_status = 503

    # Check Postgres
    try:
        # Get a temporary connection from the pool
        async for session in get_db():
            await session.execute(text("SELECT 1"))
            break  # Success
    except Exception as e:
        status["postgres"] = f"error: {e}"
        status["status"] = "degraded"
        http_status = 503

    return JSONResponse(content=status, status_code=http_status)
