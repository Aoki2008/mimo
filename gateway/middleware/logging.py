"""
Structured request/response logging middleware.

Emits one record on entry (event="request_start"), one on exit
(event="request_end"). Errors raised by inner middlewares/handlers are
captured into ``ctx.error`` before re-raising.

The default sink is the stdlib logger; tests can pass a callable that
appends to a list.
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from gateway.core import GatewayError, Middleware, RequestContext

LogSink = Callable[[dict[str, Any]], None]

_default_logger = logging.getLogger("gateway.access")


def _default_sink(record: dict[str, Any]) -> None:
    _default_logger.info("%s", record)


class LoggingMiddleware(Middleware):
    """Emit one structured log entry per request, recording errors on the way."""

    def __init__(self, sink: LogSink | None = None):
        self._sink = sink or _default_sink

    async def process(self, ctx: RequestContext, next_handler):  # type: ignore[override]
        self._sink({"event": "request_start", "req_id": ctx.request_id,
                    "src": ctx.src_protocol, "path": ctx.src_path,
                    "method": ctx.src_method, "model": ctx.model})
        try:
            result = await next_handler(ctx)
        except GatewayError as e:
            ctx.error = f"{e.error_code}: {e.message}"
            self._sink({"event": "request_end", **ctx.to_log()})
            raise
        except Exception as e:
            ctx.error = f"{type(e).__name__}: {e}"
            self._sink({"event": "request_end", **ctx.to_log()})
            raise
        self._sink({"event": "request_end", **ctx.to_log()})
        return result
