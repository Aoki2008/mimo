"""
Request handler — the terminal step of the Pipeline.

Given an authenticated/rate-limited RequestContext plus the parsed body,
the handler:

  1. Asks the Router for a backend and records the routing decision.
  2. Asks the OpenAI Chat upstream codec to serialize the IES request.
  3. Calls the upstream via UpstreamTransport.
  4. Hands the upstream response back to the client adapter for
     serialization (streaming or non-streaming, as the request asked).

The handler also drives load-balancer state on the Backend object: it
inc/decs ``in_flight`` around the upstream call and records latency
into the EWMA so subsequent routing decisions see real numbers.

The handler owns the upstream lifecycle: the streaming AsyncIterator it
returns transitively closes the httpx response when fully drained.
"""
from __future__ import annotations

import time
from collections.abc import AsyncIterator
from typing import Any, Protocol

from gateway.adapters import OpenAIChatAdapter, ProtocolAdapter, UpstreamCodec
from gateway.core import (
    AdapterError,
    GatewayError,
    InternalEvent,
    RequestContext,
    UpstreamError,
)
from gateway.routing import Router
from gateway.transport import UpstreamTransport


class DecisionLogWriter(Protocol):
    def write(self, decision: Any) -> None: ...


class MetricsRecorder(Protocol):
    """Optional sink for per-request metrics."""

    def record(
        self,
        *,
        ctx: RequestContext,
        backend_id: str,
        status_code: int,
        latency_ms: float,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        error: str = "",
    ) -> None: ...


class GatewayHandler:
    """Glue between the client adapter, the router, and the upstream codec."""

    def __init__(
        self,
        *,
        router: Router,
        transport: UpstreamTransport,
        upstream_codec: UpstreamCodec | None = None,
        decision_log: DecisionLogWriter | None = None,
        metrics: MetricsRecorder | None = None,
        upstream_path: str = "/v1/chat/completions",
        upstream_timeout_s: float = 600.0,
    ):
        self._router = router
        self._transport = transport
        # Default to OpenAIChatAdapter as the upstream codec — that's what
        # MiMo speaks. Someone could swap it for a different upstream proto
        # later without changing the handler.
        self._codec = upstream_codec or OpenAIChatAdapter()
        self._decision_log = decision_log
        self._metrics = metrics
        self._upstream_path = upstream_path
        self._upstream_timeout_s = upstream_timeout_s

    async def handle(
        self,
        ctx: RequestContext,
        adapter: ProtocolAdapter,
        body: dict[str, Any],
    ) -> tuple[bytes, AsyncIterator[bytes] | None, str]:
        """Execute the full request lifecycle.

        Returns ``(headers_content_type, stream_body_or_none, response_bytes_or_empty)``::

          * For non-stream: ``(content_type, None, body_bytes)``
          * For stream:     ``(content_type, async_iter, b"")``

        Raises GatewayError; callers (the FastAPI route) translate that
        into the proper protocol error envelope via ``adapter.error_envelope``.
        """
        req = adapter.parse_request(body)
        ctx.model = req.model
        ctx.is_stream = req.stream

        backend, decision = self._router.choose(
            request_id=ctx.request_id, model=req.model,
        )
        ctx.target_backend_id = backend.backend_id
        ctx.upstream_url = backend.base_url.rstrip("/") + self._upstream_path
        ctx.decide(f"route:{backend.backend_id}:{decision.reason}")
        if self._decision_log is not None:
            try:
                self._decision_log.write(decision)
            except Exception:
                pass  # best-effort

        upstream_body = self._codec.serialize_to_upstream(req)
        headers = {"Content-Type": "application/json"}
        if backend.api_key:
            headers["Authorization"] = f"Bearer {backend.api_key}"

        if req.stream:
            return await self._handle_stream(
                ctx, adapter, backend, upstream_body, headers,
            )
        return await self._handle_non_stream(
            ctx, adapter, backend, upstream_body, headers,
        )

    async def _handle_non_stream(
        self, ctx: RequestContext, adapter: ProtocolAdapter,
        backend, upstream_body, headers,
    ) -> tuple[str, AsyncIterator[bytes] | None, bytes]:
        backend.inc_in_flight()
        started = time.monotonic()
        try:
            try:
                status, raw = await self._transport.post_json(
                    ctx.upstream_url, upstream_body,
                    headers=headers, timeout_s=self._upstream_timeout_s,
                )
            except GatewayError as e:
                backend.record_failure(f"{e.error_code}: {e.message}")
                self._record_metric(ctx, backend.backend_id, 0,
                                    (time.monotonic() - started) * 1000,
                                    error=e.message)
                raise
            except Exception as e:
                backend.record_failure(f"transport: {type(e).__name__}: {e}")
                self._record_metric(ctx, backend.backend_id, 0,
                                    (time.monotonic() - started) * 1000,
                                    error=str(e))
                raise UpstreamError(f"Upstream call failed: {e}") from e

            ctx.upstream_status = status

            if status >= 400:
                backend.record_failure(f"upstream http {status}")
                self._record_metric(ctx, backend.backend_id, status,
                                    (time.monotonic() - started) * 1000,
                                    error=f"http {status}")
                raise UpstreamError(
                    f"Upstream returned {status}: {raw[:200]!r}",
                    details={"status": status},
                )

            latency_ms = (time.monotonic() - started) * 1000
            backend.record_success()
            backend.record_latency(latency_ms)

            try:
                events = self._codec.parse_upstream_response(raw)
            except GatewayError:
                raise
            except Exception as e:
                raise AdapterError(f"Failed to parse upstream JSON: {e}") from e

            prompt_t, completion_t = _extract_token_counts(events)
            self._record_metric(
                ctx, backend.backend_id, status, latency_ms,
                prompt_tokens=prompt_t, completion_tokens=completion_t,
            )

            body_bytes = adapter.serialize_response(events)
            return _content_type_for(adapter, stream=False), None, body_bytes
        finally:
            backend.dec_in_flight()

    async def _handle_stream(
        self, ctx: RequestContext, adapter: ProtocolAdapter,
        backend, upstream_body, headers,
    ) -> tuple[str, AsyncIterator[bytes] | None, bytes]:
        backend.inc_in_flight()
        started = time.monotonic()
        try:
            status, raw_iter = await self._transport.post_stream(
                ctx.upstream_url, upstream_body,
                headers=headers, timeout_s=self._upstream_timeout_s,
            )
        except GatewayError as e:
            backend.record_failure(f"{e.error_code}: {e.message}")
            backend.dec_in_flight()
            self._record_metric(ctx, backend.backend_id, 0,
                                (time.monotonic() - started) * 1000,
                                error=e.message)
            raise
        except Exception as e:
            backend.record_failure(f"transport: {type(e).__name__}: {e}")
            backend.dec_in_flight()
            self._record_metric(ctx, backend.backend_id, 0,
                                (time.monotonic() - started) * 1000,
                                error=str(e))
            raise UpstreamError(f"Upstream call failed: {e}") from e

        ctx.upstream_status = status
        if status >= 400:
            try:
                async for _ in raw_iter:
                    pass
            except Exception:
                pass
            backend.record_failure(f"upstream http {status}")
            backend.dec_in_flight()
            self._record_metric(ctx, backend.backend_id, status,
                                (time.monotonic() - started) * 1000,
                                error=f"http {status}")
            raise UpstreamError(
                f"Upstream returned {status}",
                details={"status": status},
            )

        # Mark success at stream start; mid-stream failures still surface
        # via the IES StreamError event.
        backend.record_success()

        ies_events = self._codec.parse_upstream_stream(raw_iter)
        client_bytes = adapter.serialize_response_stream(ies_events)
        recorder = self._metrics
        bid = backend.backend_id

        async def counted_chunks() -> AsyncIterator[bytes]:
            try:
                async for chunk in client_bytes:
                    ctx.response_chunks += 1
                    yield chunk
            finally:
                latency_ms = (time.monotonic() - started) * 1000
                backend.record_latency(latency_ms)
                backend.dec_in_flight()
                if recorder is not None:
                    try:
                        recorder.record(
                            ctx=ctx, backend_id=bid, status_code=status,
                            latency_ms=latency_ms,
                        )
                    except Exception:
                        pass

        return _content_type_for(adapter, stream=True), counted_chunks(), b""

    def _record_metric(
        self, ctx: RequestContext, backend_id: str, status_code: int,
        latency_ms: float, *, prompt_tokens: int = 0,
        completion_tokens: int = 0, error: str = "",
    ) -> None:
        if self._metrics is None:
            return
        try:
            self._metrics.record(
                ctx=ctx, backend_id=backend_id, status_code=status_code,
                latency_ms=latency_ms, prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens, error=error,
            )
        except Exception:
            pass


def _content_type_for(adapter: ProtocolAdapter, *, stream: bool) -> str:
    if stream:
        return "text/event-stream"
    if adapter.name == "anthropic":
        return "application/json"
    return "application/json"


def _extract_token_counts(events) -> tuple[int, int]:
    """Pull (prompt_tokens, completion_tokens) from a non-stream IES event list.

    Returns (0, 0) if the upstream didn't include usage. We probe lightly
    so we don't depend on a particular event type — any object with a
    ``usage`` attribute or dict matching the OpenAI shape works.
    """
    for ev in events:
        usage = getattr(ev, "usage", None)
        if usage is None and isinstance(ev, dict):
            usage = ev.get("usage")
        if usage:
            try:
                if isinstance(usage, dict):
                    return (
                        int(usage.get("prompt_tokens") or 0),
                        int(usage.get("completion_tokens") or 0),
                    )
                return (
                    int(getattr(usage, "prompt_tokens", 0) or 0),
                    int(getattr(usage, "completion_tokens", 0) or 0),
                )
            except (TypeError, ValueError):
                pass
    return 0, 0


# helper for typing-only import
_ = InternalEvent
