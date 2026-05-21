from collections.abc import Awaitable
from typing import cast

from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from app.config import get_settings

settings = get_settings()

# Module-level client instance — created once, shared across the app.
# NOT instantiated here directly; created in the lifespan function in main.py
# and stored on app.state.redis so it can be properly closed on shutdown.
redis_client: Redis | None = None


async def create_redis_client() -> Redis:
    """
    Create and verify the Redis connection.
    Called once during app startup inside the lifespan function.
    """
    client = Redis.from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=True,  # returns strings instead of bytes
    )
    try:
        await cast(Awaitable[bool], client.ping())
    except RedisConnectionError as e:
        raise RuntimeError(f"Could not connect to Redis at {settings.redis_url}") from e

    return client


async def close_redis_client(client: Redis) -> None:
    """Called once during app shutdown to release the connection pool."""
    await client.aclose()
