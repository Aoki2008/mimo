"""
Token-bucket rate limiter, keyed by API key id.

Per-key buckets are created on demand. Each request costs 1 token. When
the bucket is empty, ``RateLimitError`` is raised — adapters serialize
this into the protocol-specific 429 envelope.

This is intentionally simple — single-process, in-memory, no Redis. The
limit is configurable per-principal: if the validated principal exposes
a ``rate_limit_per_min`` attribute, that value wins; otherwise we fall
back to ``default_per_min``.
"""
from __future__ import annotations

import threading
import time
from typing import Any

from gateway.core import Middleware, RateLimitError, RequestContext


class _Bucket:
    __slots__ = ("capacity", "rate_per_sec", "tokens", "last_refill")

    def __init__(self, capacity: float, rate_per_sec: float):
        self.capacity = capacity
        self.rate_per_sec = rate_per_sec
        self.tokens = capacity
        self.last_refill = time.monotonic()

    def take(self, cost: float = 1.0) -> bool:
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate_per_sec)
        self.last_refill = now
        if self.tokens >= cost:
            self.tokens -= cost
            return True
        return False


class TokenBucketRateLimitMiddleware(Middleware):
    """Per-key token bucket. Keyed by ``ctx.api_key_id`` (anonymous → ``"_anon"``)."""

    def __init__(
        self,
        *,
        default_per_min: int = 60,
        burst_factor: float = 1.5,
    ):
        self._default_rps = default_per_min / 60.0
        self._burst_factor = burst_factor
        self._buckets: dict[str, _Bucket] = {}
        self._lock = threading.Lock()

    async def process(self, ctx: RequestContext, next_handler):  # type: ignore[override]
        key = ctx.api_key_id or "_anon"
        rps = self._rate_for(ctx.principal)
        bucket = self._get_bucket(key, rps)
        if not bucket.take():
            ctx.decide(f"rate_limit:reject:{key}")
            raise RateLimitError(f"Rate limit exceeded for key {key}")
        return await next_handler(ctx)

    def _rate_for(self, principal: Any) -> float:
        if principal is not None:
            v = getattr(principal, "rate_limit_per_min", None)
            if v is None and isinstance(principal, dict):
                v = principal.get("rate_limit_per_min")
            if v:
                try:
                    return float(v) / 60.0
                except (TypeError, ValueError):
                    pass
        return self._default_rps

    def _get_bucket(self, key: str, rate_per_sec: float) -> _Bucket:
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None or bucket.rate_per_sec != rate_per_sec:
                # Allow short bursts up to burst_factor × per-second rate,
                # but never less than 1 token of capacity.
                capacity = max(1.0, rate_per_sec * self._burst_factor)
                bucket = _Bucket(capacity=capacity, rate_per_sec=rate_per_sec)
                self._buckets[key] = bucket
            return bucket
