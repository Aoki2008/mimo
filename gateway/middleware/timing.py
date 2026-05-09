"""
Timing middleware.

Records the wall time spent inside the wrapped handler — does NOT cover
upstream streaming time once the handler returns its iterator (handlers
own their own iterator lifecycles). Use ``ctx.total_latency_ms`` if you
want end-to-end including streaming.
"""
from __future__ import annotations

import time

from gateway.core import Middleware, RequestContext


class TimingMiddleware(Middleware):
    """Stamp inner-handler latency on the context."""

    async def process(self, ctx: RequestContext, next_handler):  # type: ignore[override]
        start = time.monotonic()
        try:
            return await next_handler(ctx)
        finally:
            ctx.upstream_latency_ms = round((time.monotonic() - start) * 1000, 2)
