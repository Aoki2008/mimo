"""
Chat-based health probe.

Hits the upstream's ``/v1/chat/completions`` with a tiny non-streaming
request and treats any 2xx response with at least one choice as healthy.
This is stronger than a TCP/HTTP HEAD probe because it exercises the
auth path, the model loader, and the JSON serializer end-to-end — i.e.
the same code paths a real request would.

Probes are deliberately conservative: ``max_tokens=1``, no streaming,
short timeout. The upstream cost is one tiny inference per probe cycle.
"""
from __future__ import annotations

import json
import time
from typing import Any, Protocol

from .backend import Backend


class _AsyncClient(Protocol):
    """Minimal subset of httpx.AsyncClient we depend on. Used for testing."""

    async def post(
        self,
        url: str,
        *,
        json: dict[str, Any] | None = ...,
        headers: dict[str, str] | None = ...,
        timeout: float | None = ...,
    ) -> "_AsyncResponse": ...


class _AsyncResponse(Protocol):
    status_code: int

    @property
    def text(self) -> str: ...

    def json(self) -> Any: ...


PROBE_REQUEST = {
    "messages": [{"role": "user", "content": "ping"}],
    "max_tokens": 1,
    "stream": False,
    "temperature": 0,
}


class ChatProbeResult:
    __slots__ = ("ok", "latency_ms", "status_code", "error")

    def __init__(
        self,
        ok: bool,
        latency_ms: float,
        status_code: int = 0,
        error: str = "",
    ):
        self.ok = ok
        self.latency_ms = latency_ms
        self.status_code = status_code
        self.error = error

    def __repr__(self) -> str:  # pragma: no cover - debug only
        return (
            f"ChatProbeResult(ok={self.ok}, latency_ms={self.latency_ms:.1f}, "
            f"status={self.status_code}, error={self.error!r})"
        )


async def chat_probe(
    backend: Backend,
    client: _AsyncClient,
    *,
    timeout_s: float = 5.0,
    cooldown_s: float = 30.0,
    failure_threshold: int = 3,
) -> ChatProbeResult:
    """Probe ``backend`` and update its health state in place.

    Returns a result regardless of outcome — call sites can log it. The
    backend's own ``record_success`` / ``record_failure`` methods are the
    canonical place where breaker state changes.
    """
    body = dict(PROBE_REQUEST)
    body["model"] = backend.model
    headers = {"Content-Type": "application/json"}
    if backend.api_key:
        headers["Authorization"] = f"Bearer {backend.api_key}"

    url = backend.base_url.rstrip("/") + "/v1/chat/completions"
    started = time.monotonic()

    try:
        resp = await client.post(url, json=body, headers=headers, timeout=timeout_s)
    except Exception as e:
        latency = (time.monotonic() - started) * 1000
        backend.record_failure(
            f"transport: {type(e).__name__}: {e}",
            cooldown_s=cooldown_s, threshold=failure_threshold,
        )
        return ChatProbeResult(ok=False, latency_ms=latency, error=str(e))

    latency = (time.monotonic() - started) * 1000
    status = getattr(resp, "status_code", 0)

    if status < 200 or status >= 300:
        snippet = (getattr(resp, "text", "") or "")[:200]
        backend.record_failure(
            f"http {status}: {snippet}",
            cooldown_s=cooldown_s, threshold=failure_threshold,
        )
        return ChatProbeResult(
            ok=False, latency_ms=latency, status_code=status,
            error=f"http {status}",
        )

    try:
        data = resp.json()
    except (ValueError, json.JSONDecodeError) as e:
        backend.record_failure(
            f"non-json body: {e}",
            cooldown_s=cooldown_s, threshold=failure_threshold,
        )
        return ChatProbeResult(
            ok=False, latency_ms=latency, status_code=status,
            error="invalid json",
        )

    if not isinstance(data, dict) or not data.get("choices"):
        backend.record_failure(
            "no choices in response",
            cooldown_s=cooldown_s, threshold=failure_threshold,
        )
        return ChatProbeResult(
            ok=False, latency_ms=latency, status_code=status,
            error="empty choices",
        )

    backend.record_success()
    return ChatProbeResult(ok=True, latency_ms=latency, status_code=status)
