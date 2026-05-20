from app.services.reaper_service import VISIBILITY_TIMEOUTS_MS
from app.services.queue_service import QUEUE_NAMES


def test_visibility_timeouts_defined_for_all_queues():
    """Every priority queue must have a visibility timeout defined."""
    for priority in QUEUE_NAMES:
        assert priority in VISIBILITY_TIMEOUTS_MS, (
            f"Missing visibility timeout for priority: {priority!r}"
        )


def test_visibility_timeouts_ordered_correctly():
    """
    Critical must be shortest, normal must be longest.
    This is load-bearing: the architecture diagram specifies this ordering.
    """
    assert VISIBILITY_TIMEOUTS_MS["critical"] < VISIBILITY_TIMEOUTS_MS["high"]
    assert VISIBILITY_TIMEOUTS_MS["high"] < VISIBILITY_TIMEOUTS_MS["normal"]


def test_visibility_timeout_values_match_spec():
    """Exact values from the architecture diagram."""
    assert VISIBILITY_TIMEOUTS_MS["critical"] == 30_000
    assert VISIBILITY_TIMEOUTS_MS["high"] == 60_000
    assert VISIBILITY_TIMEOUTS_MS["normal"] == 120_000
