"""
Locust load test for the Async Job Queue.

Run with:
    make load-test
    # Navigate to http://localhost:8089

Or headless:
    poetry run locust -f tests/load/locustfile.py \
        --headless --users 50 --spawn-rate 5 \
        --run-time 30s --host http://localhost:8000

Scenarios:
    - LegitimateUser: normal job submission and polling
    - BurstUser: submits many jobs rapidly to trigger backpressure
    - MixedWorkload: combination of priorities to verify scheduler
"""

import random
import uuid

from locust import HttpUser, between, task


class LegitimateUser(HttpUser):  # type: ignore[misc]
    """
    Simulates a normal client: submits a job then polls for its result.
    This is the baseline load pattern.
    """

    wait_time = between(0.5, 2.0)
    weight = 3

    def on_start(self) -> None:
        self.submitted_jobs: list[str] = []

    @task(5)
    def submit_job(self) -> None:
        """Submit a job and track its ID for polling."""
        job_type = random.choice(["send_email", "generate_report"])
        priority = random.choice(["normal", "high"])

        payload = {"type": job_type}
        if job_type == "send_email":
            payload["to"] = f"user-{uuid.uuid4().hex[:8]}@example.com"

        with self.client.post(
            "/jobs",
            json={"payload": payload, "priority": priority},
            catch_response=True,
        ) as response:
            if response.status_code == 202:
                self.submitted_jobs.append(response.json()["id"])
                response.success()
            elif response.status_code == 503:
                # Backpressure is expected under load — not a failure
                response.success()
            else:
                response.failure(f"Unexpected status: {response.status_code}")

    @task(2)
    def poll_job_status(self) -> None:
        """Poll a previously submitted job for completion."""
        if not self.submitted_jobs:
            return
        job_id = random.choice(self.submitted_jobs)
        self.client.get(f"/jobs/{job_id}", name="/jobs/[id]")

    @task(1)
    def check_queue_metrics(self) -> None:
        """Check queue depth — simulates a monitoring agent."""
        self.client.get("/queues/metrics")


class BurstUser(HttpUser):  # type: ignore[misc]
    """
    Submits jobs as fast as possible to trigger backpressure.
    Verifies the system degrades gracefully under overload.
    """

    wait_time = between(0.01, 0.05)
    weight = 1

    @task
    def flood_queue(self) -> None:
        """Rapid submission — expects mix of 202 and 503 responses."""
        priority = random.choice(["critical", "high", "normal"])
        with self.client.post(
            "/jobs",
            json={
                "payload": {"type": "send_email", "to": "flood@test.com"},
                "priority": priority,
            },
            catch_response=True,
        ) as response:
            if response.status_code in (202, 503):
                response.success()
            else:
                response.failure(f"Unexpected: {response.status_code}")


class MixedWorkload(HttpUser):  # type: ignore[misc]
    """
    Submits a realistic mix of priorities to verify the scheduler
    is distributing work across all three queues.
    """

    wait_time = between(0.2, 1.0)
    weight = 2

    @task(6)
    def submit_critical(self) -> None:
        self.client.post(
            "/jobs",
            json={
                "payload": {"type": "send_email", "to": "vip@example.com"},
                "priority": "critical",
            },
            name="/jobs [critical]",
        )

    @task(3)
    def submit_high(self) -> None:
        self.client.post(
            "/jobs",
            json={"payload": {"type": "generate_report"}, "priority": "high"},
            name="/jobs [high]",
        )

    @task(1)
    def submit_normal(self) -> None:
        self.client.post(
            "/jobs",
            json={
                "payload": {"type": "send_email", "to": "batch@example.com"},
                "priority": "normal",
            },
            name="/jobs [normal]",
        )
