"""
Request processing pipeline = ordered chain of middlewares wrapping a terminal
handler.

Middlewares can:
  * inspect / mutate RequestContext before the handler runs
  * short-circuit by raising GatewayError (handled by server.py)
  * wrap the handler call to observe latency, log results, count metrics

This is intentionally simple — no per-chunk async-generator middleware. Stream
lifecycles outlive the middleware boundary, so streaming concerns live inside
the handler (which composes adapter + transport directly).
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any

from .context import RequestContext

Handler = Callable[[RequestContext], Awaitable[Any]]


class Middleware(ABC):
    """Wrap a handler with before/after logic.

    Implementations override ``process``. To continue the chain, await
    ``next_handler(ctx)`` and return its result. To short-circuit, raise a
    GatewayError or return early.
    """

    @abstractmethod
    async def process(self, ctx: RequestContext, next_handler: Handler) -> Any:
        ...


class Pipeline:
    """Compose middlewares + a terminal handler into a single awaitable callable.

    Order matters: the first middleware in the list is outermost.
    """

    def __init__(self, middlewares: list[Middleware], handler: Handler):
        self._middlewares = list(middlewares)
        self._handler = handler

    async def __call__(self, ctx: RequestContext) -> Any:
        # Fold right-to-left so middlewares[0] becomes the outermost wrapper:
        #   chain = mw[0](mw[1](mw[2](... handler ...)))
        chain: Handler = self._handler
        for mw in reversed(self._middlewares):
            chain = self._wrap(mw, chain)
        return await chain(ctx)

    @staticmethod
    def _wrap(mw: Middleware, nxt: Handler) -> Handler:
        async def wrapped(ctx: RequestContext) -> Any:
            return await mw.process(ctx, nxt)
        return wrapped
