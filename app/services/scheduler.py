import random

from redis.asyncio import Redis

from app.config import get_settings

# Stream names — one per priority level
QUEUE_CRITICAL = "queue:critical"
QUEUE_HIGH = "queue:high"
QUEUE_NORMAL = "queue:normal"

QUEUE_NAMES = {
    "critical": QUEUE_CRITICAL,
    "high": QUEUE_HIGH,
    "normal": QUEUE_NORMAL,
}


async def get_weights(redis: Redis) -> dict[str, int]:
    """
    Read priority weights from Redis so they can be changed at runtime
    without redeploying. Falls back to config values if not set in Redis.

    Keys: scheduler:weight:critical / high / normal
    """
    settings = get_settings()
    defaults = {
        "critical": settings.weight_critical,
        "high": settings.weight_high,
        "normal": settings.weight_normal,
    }

    weights = {}
    for priority, default in defaults.items():
        stored = await redis.get(f"scheduler:weight:{priority}")
        weights[priority] = int(stored) if stored is not None else default

    return weights


def pick_queue(weights: dict[str, int]) -> str:
    """
    Select a queue name using weighted random selection.

    weights = {"critical": 60, "high": 30, "normal": 10}

    Roll a number 1-100. Map it to a bucket:
      1-60   → critical
      61-90  → high
      91-100 → normal

    Returns the Redis stream name for the selected queue.
    """
    roll = random.randint(1, 100)
    cumulative = 0

    for priority, weight in weights.items():
        cumulative += weight
        if roll <= cumulative:
            return QUEUE_NAMES[priority]

    # Fallback — should never be reached if weights sum to 100
    return QUEUE_CRITICAL
