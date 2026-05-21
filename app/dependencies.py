from typing import Annotated, cast

from fastapi import Depends, Request
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.core.database import get_db

# Settings dependency
SettingsDep = Annotated[Settings, Depends(get_settings)]

# Database session dependency
DbDep = Annotated[AsyncSession, Depends(get_db)]


async def get_redis(request: Request) -> Redis:
    """
    Pulls the Redis client from FastAPI app state.
    The client is attached to app.state.redis in the lifespan function.
    """
    return cast(Redis, request.app.state.redis)


# Redis dependency
RedisDep = Annotated[Redis, Depends(get_redis)]
