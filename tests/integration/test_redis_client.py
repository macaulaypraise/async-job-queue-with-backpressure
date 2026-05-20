# tests/integration/test_redis_client.py
from redis.asyncio import Redis


async def test_redis_ping(redis: Redis):
    result = await redis.ping() # type: ignore
    assert result is True


async def test_redis_set_and_get(redis: Redis):
    await redis.set("test:key", "hello")
    value = await redis.get("test:key")
    assert value == "hello"


async def test_redis_key_expiry(redis: Redis):
    await redis.set("test:expiry", "temporary", ex=1)
    value = await redis.get("test:expiry")
    assert value == "temporary"


async def test_redis_sorted_set(redis: Redis):
    key = "test:zset"
    await redis.zadd(key, {"req:1": 1000.0, "req:2": 2000.0, "req:3": 3000.0})
    count = await redis.zcard(key)
    assert count == 3
    await redis.zremrangebyscore(key, "-inf", 1999)
    count_after = await redis.zcard(key)
    assert count_after == 2
