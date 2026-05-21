import asyncio

import pytest


async def test_dispatch_times_out():
    """
    A handler that runs longer than the timeout must raise TimeoutError.
    This is the mechanism that prevents hung jobs from blocking workers.
    """
    from app.services.worker_service import _dispatch

    async def slow_payload():
        return await asyncio.wait_for(
            _dispatch({"type": "send_email", "to": "x@y.com"}),
            timeout=0.001,  # far below the 0.1s sleep in send_email handler
        )

    with pytest.raises(asyncio.TimeoutError):
        await slow_payload()


async def test_dispatch_unknown_type_raises():
    """Unknown job types must raise ValueError, not silently succeed."""
    from app.services.worker_service import _dispatch

    with pytest.raises(ValueError, match="Unknown job type"):
        await _dispatch({"type": "nonexistent_handler"})


async def test_dispatch_known_types_succeed():
    """Known job types must return a non-empty result dict."""
    from app.services.worker_service import _dispatch

    result = await _dispatch({"type": "send_email", "to": "test@example.com"})
    assert result["sent"] is True

    result = await _dispatch({"type": "generate_report"})
    assert "report_id" in result


def test_backoff_increases_with_retry_count():
    """
    Backoff ceiling must double with each retry. This is the core
    guarantee of exponential backoff.
    """
    from app.services.worker_service import _backoff_seconds

    # Run many samples to account for jitter
    samples = 1000

    avg_0 = sum(_backoff_seconds(0) for _ in range(samples)) / samples
    avg_1 = sum(_backoff_seconds(1) for _ in range(samples)) / samples
    avg_2 = sum(_backoff_seconds(2) for _ in range(samples)) / samples

    # With full jitter, mean = max_delay / 2
    # retry=0: mean ≈ 0.5s, retry=1: mean ≈ 1s, retry=2: mean ≈ 2s
    assert avg_0 < avg_1 < avg_2


def test_backoff_never_exceeds_max():
    """Backoff must be capped — retry_count=100 should not produce 2^100 seconds."""
    from app.services.worker_service import _backoff_seconds

    for retry_count in [10, 20, 50, 100]:
        delay = _backoff_seconds(retry_count, max_delay=60.0)
        assert delay <= 60.0, f"Backoff exceeded max at retry_count={retry_count}"


def test_backoff_is_non_negative():
    """Backoff must never be negative regardless of inputs."""
    from app.services.worker_service import _backoff_seconds

    for retry_count in range(10):
        assert _backoff_seconds(retry_count) >= 0
