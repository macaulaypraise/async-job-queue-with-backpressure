from contextlib import asynccontextmanager

from fastapi import FastAPI, Request          # ← add Request here
from fastapi.responses import JSONResponse   # ← move this import to the top

from app.core.redis_client import close_redis_client, create_redis_client


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = await create_redis_client()
    yield
    await close_redis_client(app.state.redis)


app = FastAPI(
    title="Async Job Queue",
    description="Job queue with backpressure and priority scheduling",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health(request: Request):          # ← Request type hint is the fix
    status = {"status": "ok", "redis": "ok", "postgres": "ok"}
    http_status = 200

    try:
        await request.app.state.redis.ping()
    except Exception as e:
        status["redis"] = f"error: {e}"
        status["status"] = "degraded"
        http_status = 503

    return JSONResponse(content=status, status_code=http_status)
