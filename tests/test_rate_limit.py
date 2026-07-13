import pytest

from src import rate_limit


@pytest.fixture(autouse=True)
def _tight_limit(monkeypatch):
    # keep tests fast and independent of the real defaults
    monkeypatch.setattr(rate_limit, "RATE_LIMIT_SEND_MAX_MESSAGES", 3)
    monkeypatch.setattr(rate_limit, "RATE_LIMIT_SEND_WINDOW_SECONDS", 60)


async def test_allows_up_to_the_limit(fake_redis):
    for _ in range(3):
        result = await rate_limit.check_send_rate_limit("alice")
        assert result.allowed

    result = await rate_limit.check_send_rate_limit("alice")
    assert not result.allowed
    assert result.retry_after_seconds > 0


async def test_limit_is_scoped_per_user(fake_redis):
    for _ in range(3):
        assert (await rate_limit.check_send_rate_limit("alice")).allowed

    assert not (await rate_limit.check_send_rate_limit("alice")).allowed
    # bob has his own independent window
    assert (await rate_limit.check_send_rate_limit("bob")).allowed


async def test_window_key_expires(fake_redis):
    for _ in range(3):
        await rate_limit.check_send_rate_limit("alice")

    ttl = await fake_redis.ttl(rate_limit.rate_limit_send_key("alice"))
    assert 0 < ttl <= 60
