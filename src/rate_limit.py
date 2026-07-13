from dataclasses import dataclass

from src.config import (
    RATE_LIMIT_SEND_MAX_MESSAGES,
    RATE_LIMIT_SEND_WINDOW_SECONDS,
    rate_limit_send_key,
)
from src.redis_client import get_redis


@dataclass
class RateLimitResult:
    allowed: bool
    retry_after_seconds: int = 0


async def check_send_rate_limit(user_id: str) -> RateLimitResult:
    """Fixed-window counter in Redis: INCR a per-user key, set it to expire
    after the window on the first increment in that window. Simple (no
    sliding log, so a client can burst up to ~2x the limit right at a
    window boundary) but that's an acceptable tradeoff for what this is —
    stopping someone from hammering a gateway, not precise billing."""
    redis = get_redis()
    key = rate_limit_send_key(user_id)

    count = await redis.incr(key)
    if count == 1:
        await redis.expire(key, RATE_LIMIT_SEND_WINDOW_SECONDS)

    if count <= RATE_LIMIT_SEND_MAX_MESSAGES:
        return RateLimitResult(allowed=True)

    ttl = await redis.ttl(key)
    return RateLimitResult(allowed=False, retry_after_seconds=max(ttl, 0))
