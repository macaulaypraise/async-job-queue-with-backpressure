from app.config import get_settings


def test_settings_load_defaults():
    """Settings must load without errors and have correct types."""
    get_settings.cache_clear()  # ensure fresh read
    settings = get_settings()

    assert isinstance(settings.high_watermark, int)
    assert isinstance(settings.low_watermark, int)
    assert settings.high_watermark > settings.low_watermark


def test_priority_weights_sum_to_100():
    """Weights must sum to exactly 100 for the scheduler to be correct."""
    get_settings.cache_clear()
    settings = get_settings()

    total = settings.weight_critical + settings.weight_high + settings.weight_normal
    assert total == 100, f"Weights sum to {total}, expected 100"


def test_watermark_relationship():
    """High watermark must always be above low watermark."""
    get_settings.cache_clear()
    settings = get_settings()

    assert settings.high_watermark > settings.low_watermark
