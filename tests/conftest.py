import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import get_settings
from app.core.database import Base, get_db
from app.core.redis_client import close_redis_client, create_redis_client
from app.main import app

settings = get_settings()


# ── Session-scoped: created ONCE, shared across all tests ──────────────────

@pytest.fixture(scope="session")
async def engine():
    """One engine for the entire test session."""
    eng = create_async_engine(settings.database_url, echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await eng.dispose()


@pytest.fixture(scope="session")
async def redis_client():
    """One Redis client for the entire test session."""
    client = await create_redis_client()
    yield client
    await close_redis_client(client)


# ── Autouse: runs before every test ────────────────────────────────────────

@pytest.fixture(autouse=True)
async def flush_redis(redis_client):
    """Flush Redis before each test — ensures isolation without reconnecting."""
    await redis_client.flushdb()
    yield


# ── Function-scoped: fresh per test, derived from session objects ───────────

@pytest.fixture
async def db(engine):
    """Fresh DB session per test, rolled back after."""
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        yield session
        await session.rollback()


@pytest.fixture
async def redis(redis_client):
    """Alias so tests receive 'redis' as a fixture name."""
    return redis_client


@pytest.fixture
async def client(db, redis):
    """
    Test HTTP client with DB and Redis injected.
    Overrides FastAPI's dependency system for the duration of each test.
    """
    async def override_get_db():
        yield db

    app.dependency_overrides[get_db] = override_get_db
    app.state.redis = redis

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c

    app.dependency_overrides.clear()
