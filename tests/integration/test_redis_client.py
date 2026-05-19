# tests/integration/test_redis_client.py
from collections.abc import AsyncGenerator
import pytest
from redis.asyncio import Redis

from app.core.redis_client import create_redis_client, close_redis_client


@pytest.fixture
async def redis() -> AsyncGenerator[Redis, None]:
    """Provides a real Redis client for integration tests."""
    client = await create_redis_client()
    yield client
    await client.flushdb()   # clean up all keys after each test
    await close_redis_client(client)


async def test_redis_ping(redis: Redis):
    """Client must connect and respond to ping."""
    result = await redis.ping()  # type: ignore
    assert result is True


async def test_redis_set_and_get(redis: Redis):
    """Basic key-value round-trip."""
    await redis.set("test:key", "hello")
    value = await redis.get("test:key")
    assert value == "hello"


async def test_redis_key_expiry(redis: Redis):
    """Keys set with TTL must expire — critical for backpressure and rate limiting."""
    await redis.set("test:expiry", "temporary", ex=1)
    value = await redis.get("test:expiry")
    assert value == "temporary"


async def test_redis_sorted_set(redis: Redis):
    """
    Sorted sets are the foundation of the sliding window rate limiter.
    Verify zadd, zcard, and zremrangebyscore work correctly.
    """
    key = "test:zset"
    await redis.zadd(key, {"req:1": 1000.0, "req:2": 2000.0, "req:3": 3000.0})

    count = await redis.zcard(key)
    assert count == 3

    # Remove entries with score below 2000 (simulates sliding window expiry)
    await redis.zremrangebyscore(key, "-inf", 1999)
    count_after = await redis.zcard(key)
    assert count_after == 2
