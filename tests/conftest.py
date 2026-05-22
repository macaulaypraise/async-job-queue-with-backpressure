import os
from collections.abc import AsyncGenerator

import pytest
from dotenv import load_dotenv
from httpx import ASGITransport, AsyncClient
from redis.asyncio import Redis  # Fixes 'Redis' unresolved reference
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,  # Fixes 'AsyncEngine' unresolved reference
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import get_settings
from app.core.database import Base, get_db
from app.core.redis_client import close_redis_client, create_redis_client
from app.main import app

# ── 1. Load test environment variables ───────────────────────────────────────
load_dotenv(".env.test", override=True)

TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")
TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL")

if not TEST_DATABASE_URL or not TEST_REDIS_URL:
    raise RuntimeError(
        "Missing required test environment variables. "
        "Ensure .env.test is loaded or variables are exported in CI."
    )

os.environ["DATABASE_URL"] = TEST_DATABASE_URL
os.environ["REDIS_URL"] = TEST_REDIS_URL
os.environ["APP_ENV"] = "test"

# ── 2. Reload settings ───────────────────────────────────────────────────────
get_settings.cache_clear()
settings = get_settings()

# ── 3. Session-scoped fixtures ───────────────────────────────────────────────


@pytest.fixture(scope="session")
async def engine() -> AsyncGenerator[AsyncEngine, None]:
    """One engine for the entire test session."""
    eng = create_async_engine(settings.database_url, echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest.fixture(scope="session")
async def redis_client() -> AsyncGenerator[Redis, None]:
    """One Redis client for the entire test session."""
    client = await create_redis_client()
    yield client
    await close_redis_client(client)


# ── 4. Autouse fixtures ──────────────────────────────────────────────────────


@pytest.fixture(autouse=True, scope="function")
async def reset_database(engine: AsyncEngine) -> AsyncGenerator[None, None]:
    """Truncate tables before each test."""
    async with engine.begin() as conn:
        tables = ", ".join(Base.metadata.tables.keys())
        if tables:
            query = f"TRUNCATE TABLE {tables} RESTART IDENTITY CASCADE"
            await conn.execute(text(query))
    yield


@pytest.fixture(autouse=True, scope="function")
async def flush_redis(redis_client: Redis) -> AsyncGenerator[None, None]:
    """Flush Redis before each test."""
    await redis_client.flushdb()
    yield


# ── 5. Function-scoped fixtures ──────────────────────────────────────────────


@pytest.fixture
async def db(engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    """Fresh DB session per test."""
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        yield session


@pytest.fixture
async def redis(redis_client: Redis) -> Redis:
    """Alias so tests receive 'redis' as a fixture name."""
    return redis_client


@pytest.fixture
async def client(db: AsyncSession, redis: Redis) -> AsyncGenerator[AsyncClient, None]:
    """
    Test HTTP client with DB and Redis injected.
    Overrides FastAPI's dependency system for the duration of each test.
    """

    async def override_get_db() -> AsyncGenerator[AsyncSession, None]:
        yield db

    app.dependency_overrides[get_db] = override_get_db
    app.state.redis = redis

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c

    app.dependency_overrides.clear()
