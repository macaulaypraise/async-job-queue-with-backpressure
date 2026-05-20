async def test_health_returns_ok(client):
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


async def test_create_job_returns_202(client):
    response = await client.post(
        "/jobs",
        json={"payload": {"type": "send_email", "to": "a@b.com"}, "priority": "normal"},
    )
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "pending"
    assert data["priority"] == "normal"


async def test_get_job_returns_correct_status(client):
    create = await client.post(
        "/jobs",
        json={"payload": {"type": "generate_report"}, "priority": "high"},
    )
    assert create.status_code == 202
    job_id = create.json()["id"]

    get = await client.get(f"/jobs/{job_id}")
    assert get.status_code == 200
    assert get.json()["id"] == job_id


async def test_get_nonexistent_job_returns_404(client):
    response = await client.get("/jobs/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404


async def test_invalid_priority_rejected(client):
    response = await client.post(
        "/jobs",
        json={"payload": {"type": "send_email"}, "priority": "urgent"},
    )
    assert response.status_code == 422


async def test_queue_metrics_returns_depths(client):
    response = await client.get("/queues/metrics")
    assert response.status_code == 200
    data = response.json()
    assert "depths" in data
    assert "accepting_jobs" in data
    assert data["accepting_jobs"] is True
