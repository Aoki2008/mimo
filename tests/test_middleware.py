"""Unit tests for gateway.middleware + Pipeline integration."""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import pytest

from gateway.core import (
    AuthError,
    BadRequestError,
    Pipeline,
    RateLimitError,
    RequestContext,
)
from gateway.middleware import (
    APIKeyAuthMiddleware,
    LoggingMiddleware,
    TimingMiddleware,
    TokenBucketRateLimitMiddleware,
)


def _run(coro):
    return asyncio.run(coro)


# ───────── helpers ─────────


@dataclass
class _Principal:
    key_id: str
    rate_limit_per_min: int = 60


async def _ok_handler(ctx: RequestContext) -> str:
    ctx.decide("handler:ran")
    return "ok"


def _make_validator(valid_token: str, principal: _Principal):
    async def validator(token: str):
        if token != valid_token:
            raise AuthError("bad token")
        return principal
    return validator


# ───────── auth ─────────


def test_auth_extracts_bearer():
    validator = _make_validator("sk-abc", _Principal(key_id="acct1"))
    mw = APIKeyAuthMiddleware(validator)
    pipeline = Pipeline([mw], _ok_handler)
    ctx = RequestContext(headers={"authorization": "Bearer sk-abc"})
    result = _run(pipeline(ctx))
    assert result == "ok"
    assert ctx.api_key_id == "acct1"
    assert ctx.principal.key_id == "acct1"
    assert any(d.startswith("auth:ok:") for d in ctx.decisions)


def test_auth_extracts_x_api_key_header():
    validator = _make_validator("sk-anth", _Principal(key_id="anth_1"))
    mw = APIKeyAuthMiddleware(validator)
    pipeline = Pipeline([mw], _ok_handler)
    ctx = RequestContext(headers={"x-api-key": "sk-anth"})
    _run(pipeline(ctx))
    assert ctx.principal.key_id == "anth_1"


def test_auth_missing_header_raises():
    validator = _make_validator("sk-abc", _Principal(key_id="x"))
    mw = APIKeyAuthMiddleware(validator)
    pipeline = Pipeline([mw], _ok_handler)
    with pytest.raises(AuthError):
        _run(pipeline(RequestContext(headers={})))


def test_auth_wrong_scheme_treated_as_missing():
    validator = _make_validator("sk-abc", _Principal(key_id="x"))
    mw = APIKeyAuthMiddleware(validator)
    pipeline = Pipeline([mw], _ok_handler)
    with pytest.raises(AuthError):
        _run(pipeline(RequestContext(headers={"authorization": "Basic abc"})))


def test_auth_validator_rejection_propagates():
    async def validator(token):
        raise AuthError("bad key")
    mw = APIKeyAuthMiddleware(validator)
    pipeline = Pipeline([mw], _ok_handler)
    with pytest.raises(AuthError, match="bad key"):
        _run(pipeline(RequestContext(headers={"authorization": "Bearer wrong"})))


def test_auth_no_principal_id_falls_back_to_token_prefix():
    async def validator(token):
        return object()  # opaque, no key_id/id/name
    mw = APIKeyAuthMiddleware(validator)
    pipeline = Pipeline([mw], _ok_handler)
    ctx = RequestContext(headers={"authorization": "Bearer abcdefghij_more"})
    _run(pipeline(ctx))
    assert ctx.api_key_id == "abcdefgh"  # first 8 chars


# ───────── timing ─────────


def test_timing_records_handler_latency():
    async def slow(ctx):
        await asyncio.sleep(0.02)
        return None
    pipeline = Pipeline([TimingMiddleware()], slow)
    ctx = RequestContext()
    _run(pipeline(ctx))
    assert ctx.upstream_latency_ms is not None
    assert ctx.upstream_latency_ms >= 15  # ~20ms


def test_timing_records_latency_even_on_error():
    async def boom(ctx):
        raise BadRequestError("nope")
    pipeline = Pipeline([TimingMiddleware()], boom)
    ctx = RequestContext()
    with pytest.raises(BadRequestError):
        _run(pipeline(ctx))
    assert ctx.upstream_latency_ms is not None


# ───────── logging ─────────


def test_logging_emits_start_and_end_records():
    records: list[dict] = []
    pipeline = Pipeline([LoggingMiddleware(sink=records.append)], _ok_handler)
    ctx = RequestContext(src_protocol="anthropic", src_path="/v1/messages",
                         src_method="POST", model="claude-3-5-sonnet")
    _run(pipeline(ctx))
    events = [r["event"] for r in records]
    assert events == ["request_start", "request_end"]
    assert records[0]["model"] == "claude-3-5-sonnet"
    assert records[1]["err"] is None


def test_logging_captures_error_into_ctx():
    records: list[dict] = []

    async def boom(ctx):
        raise BadRequestError("missing model")

    pipeline = Pipeline([LoggingMiddleware(sink=records.append)], boom)
    ctx = RequestContext()
    with pytest.raises(BadRequestError):
        _run(pipeline(ctx))
    assert ctx.error and "invalid_request" in ctx.error
    end = records[-1]
    assert end["event"] == "request_end"
    assert end["err"] and "missing model" in end["err"]


def test_logging_captures_unexpected_exception():
    records: list[dict] = []

    async def boom(ctx):
        raise RuntimeError("oops")

    pipeline = Pipeline([LoggingMiddleware(sink=records.append)], boom)
    ctx = RequestContext()
    with pytest.raises(RuntimeError):
        _run(pipeline(ctx))
    assert ctx.error and "RuntimeError" in ctx.error


# ───────── rate limit ─────────


def test_rate_limit_blocks_after_burst():
    """default_per_min=60 → 1 rps with burst_factor=1.5 → capacity ≈ 1.5 tokens."""
    mw = TokenBucketRateLimitMiddleware(default_per_min=60, burst_factor=1.5)
    pipeline = Pipeline([mw], _ok_handler)

    async def hammer():
        results = []
        for _ in range(5):
            ctx = RequestContext()
            ctx.api_key_id = "k1"
            try:
                results.append(await pipeline(ctx))
            except RateLimitError:
                results.append("limited")
        return results

    out = _run(hammer())
    # First request goes through (capacity ≈ 1.5), rest get rate-limited
    # because no time has passed for refill.
    assert out[0] == "ok"
    assert "limited" in out


def test_rate_limit_per_principal_override():
    """A principal carrying rate_limit_per_min=600 gets a roomier bucket."""
    mw = TokenBucketRateLimitMiddleware(default_per_min=60, burst_factor=1.5)

    async def fast_handler(ctx):
        return "ok"

    pipeline = Pipeline([mw], fast_handler)

    async def hammer():
        ok = 0
        for _ in range(8):
            ctx = RequestContext()
            ctx.api_key_id = "premium"
            ctx.principal = _Principal(key_id="premium", rate_limit_per_min=600)
            try:
                await pipeline(ctx)
                ok += 1
            except RateLimitError:
                pass
        return ok

    ok_count = _run(hammer())
    # 600/min → 10rps × 1.5 burst = 15 capacity → all 8 succeed back-to-back.
    assert ok_count == 8


def test_rate_limit_separate_buckets_per_key():
    mw = TokenBucketRateLimitMiddleware(default_per_min=60, burst_factor=1.0)
    pipeline = Pipeline([mw], _ok_handler)

    async def call(key: str) -> str:
        ctx = RequestContext()
        ctx.api_key_id = key
        try:
            return await pipeline(ctx)
        except RateLimitError:
            return "limited"

    async def parallel():
        return await asyncio.gather(call("a"), call("b"))

    assert _run(parallel()) == ["ok", "ok"]


def test_rate_limit_refill_over_time():
    mw = TokenBucketRateLimitMiddleware(default_per_min=600, burst_factor=1.0)
    # 600/min = 10/s, capacity 10. We pre-deplete via the take() path.

    async def burn_then_wait():
        ctx_a = RequestContext()
        ctx_a.api_key_id = "k"
        for _ in range(20):
            try:
                await Pipeline([mw], _ok_handler)(ctx_a)
            except RateLimitError:
                break
        # Now wait long enough to refill some tokens.
        await asyncio.sleep(0.2)  # ~2 tokens refilled
        ctx_b = RequestContext()
        ctx_b.api_key_id = "k"
        return await Pipeline([mw], _ok_handler)(ctx_b)

    assert _run(burn_then_wait()) == "ok"


# ───────── pipeline composition ─────────


def test_pipeline_chains_middlewares_in_lifo_order():
    """Verify outer-to-inner ordering is correct when many middlewares are stacked."""
    from gateway.core import Middleware

    trace: list[str] = []

    class Recorder(Middleware):
        def __init__(self, label: str):
            self._label = label

        async def process(self, ctx, next_handler):
            trace.append(f"before:{self._label}")
            result = await next_handler(ctx)
            trace.append(f"after:{self._label}")
            return result

    async def handler(ctx):
        trace.append("handler")
        return None

    pipeline = Pipeline([Recorder("A"), Recorder("B"), Recorder("C")], handler)
    _run(pipeline(RequestContext()))
    assert trace == [
        "before:A", "before:B", "before:C",
        "handler",
        "after:C", "after:B", "after:A",
    ]


def test_pipeline_full_stack_auth_timing_logging_ratelimit():
    """Compose the four middlewares and verify they cooperate."""
    records: list[dict] = []
    validator = _make_validator(
        "sk-good", _Principal(key_id="acct1", rate_limit_per_min=600))

    pipeline = Pipeline(
        [
            LoggingMiddleware(sink=records.append),
            TimingMiddleware(),
            APIKeyAuthMiddleware(validator),
            TokenBucketRateLimitMiddleware(default_per_min=60),
        ],
        _ok_handler,
    )

    ctx = RequestContext(
        src_protocol="anthropic", src_path="/v1/messages",
        headers={"authorization": "Bearer sk-good"},
        model="claude-3-5-sonnet",
    )
    result = _run(pipeline(ctx))
    assert result == "ok"
    assert ctx.upstream_latency_ms is not None
    assert ctx.api_key_id == "acct1"
    # logging recorded both edges; timing populated upstream_latency_ms
    assert any(r["event"] == "request_start" for r in records)
    end = next(r for r in records if r["event"] == "request_end")
    assert end["key"] == "acct1"
    assert end["err"] is None


def test_pipeline_auth_failure_short_circuits_inner_middlewares():
    """Auth raising must not run the rate limiter or the handler."""
    handler_ran = []

    async def handler(ctx):
        handler_ran.append(True)
        return "should-not-run"

    async def rejecting_validator(token):
        raise AuthError("nope")

    pipeline = Pipeline(
        [
            APIKeyAuthMiddleware(rejecting_validator),
            TokenBucketRateLimitMiddleware(default_per_min=60),
        ],
        handler,
    )
    with pytest.raises(AuthError):
        _run(pipeline(RequestContext(headers={"authorization": "Bearer x"})))
    assert handler_ran == []
