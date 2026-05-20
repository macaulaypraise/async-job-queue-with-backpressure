async def test_get_weights_returns_defaults(client):
    """GET /queues/weights must return current weights with a source label."""
    response = await client.get("/queues/weights")
    assert response.status_code == 200
    data = response.json()

    assert "critical" in data
    assert "high" in data
    assert "normal" in data
    assert data["critical"] + data["high"] + data["normal"] == 100
    assert data["source"] in ("redis", "config")


async def test_update_weights_takes_effect(client):
    """PATCH /queues/weights must store new weights readable by GET."""
    response = await client.patch(
        "/queues/weights",
        json={"critical": 80, "high": 15, "normal": 5},
    )
    assert response.status_code == 200
    assert response.json()["critical"] == 80
    assert response.json()["source"] == "redis"

    # GET should now reflect the new values
    get_response = await client.get("/queues/weights")
    data = get_response.json()
    assert data["critical"] == 80
    assert data["high"] == 15
    assert data["normal"] == 5


async def test_weights_must_sum_to_100(client):
    """Weights that don't sum to 100 must be rejected with 422."""
    response = await client.patch(
        "/queues/weights",
        json={"critical": 50, "high": 50, "normal": 50},
    )
    assert response.status_code == 422


async def test_negative_weights_rejected(client):
    """Negative weights must be rejected."""
    response = await client.patch(
        "/queues/weights",
        json={"critical": 110, "high": -5, "normal": -5},
    )
    assert response.status_code == 422


async def test_reset_weights_clears_redis_overrides(client, redis):
    """DELETE /queues/weights must remove Redis keys and revert to config."""
    # Set custom weights first
    await client.patch(
        "/queues/weights",
        json={"critical": 100, "high": 0, "normal": 0},
    )

    # Reset
    response = await client.delete("/queues/weights")
    assert response.status_code == 200
    assert response.json()["reset"] is True

    # Source should now be config
    get_response = await client.get("/queues/weights")
    assert get_response.json()["source"] == "config"
