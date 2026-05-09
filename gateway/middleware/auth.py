"""
API key authentication middleware.

Pluggable: takes an ``AuthValidator`` (an async callable ``str → Principal``)
that throws AuthError on rejection. The validator implementation lives in
G7's API-key store; tests pass a stub.

Stores:
  * ``ctx.principal`` — whatever the validator returned (typically a record
    with id, label, rate_limit, etc.)
  * ``ctx.api_key_id`` — the authenticated key's identifier (for audit log)
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from gateway.core import AuthError, Middleware, RequestContext

AuthValidator = Callable[[str], Awaitable[Any]]


class APIKeyAuthMiddleware(Middleware):
    """Extract a bearer / x-api-key token and validate it.

    Both Authorization: Bearer <key> and the Anthropic-style x-api-key
    header are accepted. ``ctx.headers`` keys are expected lowercased.
    """

    def __init__(
        self,
        validator: AuthValidator,
        *,
        header: str = "authorization",
        scheme: str = "Bearer",
        x_api_key_header: str = "x-api-key",
    ):
        self._validator = validator
        self._header = header.lower()
        self._scheme = scheme
        self._x_api_key_header = x_api_key_header.lower()

    async def process(self, ctx: RequestContext, next_handler):  # type: ignore[override]
        token = self._extract_token(ctx.headers)
        if not token:
            raise AuthError("Missing API key")
        principal = await self._validator(token)
        ctx.principal = principal
        # Pull a stable identifier off the principal for the audit log.
        ctx.api_key_id = _principal_id(principal) or token[:8]
        ctx.decide(f"auth:ok:{ctx.api_key_id}")
        return await next_handler(ctx)

    def _extract_token(self, headers: dict[str, str]) -> str:
        x_api = headers.get(self._x_api_key_header)
        if x_api:
            return x_api.strip()
        auth = headers.get(self._header)
        if not auth:
            return ""
        parts = auth.strip().split(None, 1)
        if len(parts) != 2:
            return ""
        scheme, token = parts
        if scheme.lower() != self._scheme.lower():
            return ""
        return token.strip()


def _principal_id(principal: Any) -> str:
    if principal is None:
        return ""
    for attr in ("key_id", "id", "name"):
        v = getattr(principal, attr, None)
        if v:
            return str(v)
    if isinstance(principal, dict):
        for k in ("key_id", "id", "name"):
            if principal.get(k):
                return str(principal[k])
    return ""
