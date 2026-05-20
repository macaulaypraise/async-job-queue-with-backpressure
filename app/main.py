from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app

from app.core.redis_client import close_redis_client, create_redis_client
from app.routers import jobs, queues


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    app.state.redis = await create_redis_client()
    yield
    # Shutdown
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
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/health", tags=["ops"])
async def health(request: Request):
    status = {"status": "ok", "redis": "ok"}
    http_status = 200

    try:
        await request.app.state.redis.ping()
    except Exception as e:
        status["redis"] = f"error: {e}"
        status["status"] = "degraded"
        http_status = 503

    return JSONResponse(content=status, status_code=http_status)
