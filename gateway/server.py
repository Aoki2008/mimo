"""
Gateway server — FastAPI app composing all the pieces.

Run with::

    uvicorn gateway.server:app --port 8088

Or programmatically::

    from gateway.server import create_app
    app = create_app(config_path="./gateway.yaml")

The /v1/* routes are the only public surface. Each route delegates to a
ProtocolAdapter, runs the full middleware pipeline, then streams or
returns the upstream response. Errors raised anywhere in the pipeline
are converted into the protocol-specific error envelope via
``adapter.error_envelope``.
"""
from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import Response, StreamingResponse

from gateway.adapters import (
    AnthropicAdapter,
    OpenAIChatAdapter,
    OpenAIResponsesAdapter,
    ProtocolAdapter,
)
from gateway.config import APIKeyStore, GatewayConfig, load
from gateway.config.loader import GatewaySettings, ProbeSettings, StorageSettings
from gateway.core import (
    BadRequestError,
    GatewayError,
    Middleware,
    Pipeline,
    RequestContext,
)
from gateway.handler import GatewayHandler
from gateway.middleware import (
    APIKeyAuthMiddleware,
    LoggingMiddleware,
    TimingMiddleware,
    TokenBucketRateLimitMiddleware,
)
from gateway.routing import (
    BackendRegistry,
    InMemoryDecisionLog,
    Router,
)
from gateway.transport import HttpxTransport, UpstreamTransport


# ────────────── factory ──────────────


def create_app(
    *,
    config_path: str | None = None,
    config: GatewayConfig | None = None,
    transport: UpstreamTransport | None = None,
    api_key_store: APIKeyStore | None = None,
    extra_middlewares: list[Middleware] | None = None,
) -> FastAPI:
    """Build the FastAPI app.

    Most parameters are exposed for testability — production callers
    pass only ``config_path``.
    """
    if config is None:
        if config_path is None:
            config_path = os.environ.get("GATEWAY_CONFIG")
        if config_path:
            config = load(config_path)
        else:
            config = GatewayConfig()

    # API key store (file or in-memory).
    if api_key_store is None:
        api_key_store = APIKeyStore(config.storage.api_keys_db)

    # Routing
    registry = BackendRegistry(b.to_backend() for b in config.backends)
    router = Router(registry)
    decision_log = InMemoryDecisionLog(capacity=4096)

    # Upstream transport (httpx by default).
    owned_transport = transport
    if owned_transport is None:
        owned_transport = HttpxTransport()

    handler_obj = GatewayHandler(
        router=router,
        transport=owned_transport,
        decision_log=decision_log,
        metrics=_default_metrics_recorder(),
        upstream_timeout_s=config.gateway.request_timeout_s,
    )

    # Build adapters once — they're stateless.
    adapters: dict[str, ProtocolAdapter] = {
        "openai_chat": OpenAIChatAdapter(),
        "anthropic": AnthropicAdapter(),
        "openai_responses": OpenAIResponsesAdapter(),
    }

    # The terminal pipeline handler reads (adapter, body) off the context.
    async def terminal_handler(ctx: RequestContext) -> tuple[str, Any, bytes]:
        adapter: ProtocolAdapter = ctx_get_adapter(ctx)
        body: dict[str, Any] = ctx_get_body(ctx)
        return await handler_obj.handle(ctx, adapter, body)

    # Default middleware stack — tests can replace by passing extra_middlewares.
    middlewares: list[Middleware] = list(extra_middlewares or [
        LoggingMiddleware(),
        TimingMiddleware(),
        APIKeyAuthMiddleware(api_key_store.validate),
        TokenBucketRateLimitMiddleware(
            default_per_min=config.gateway.default_rate_limit_per_min,
        ),
    ])

    pipeline = Pipeline(middlewares, terminal_handler)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        try:
            yield
        finally:
            await owned_transport.close()
            api_key_store.close()

    app = FastAPI(lifespan=lifespan)
    app.state.config = config
    app.state.registry = registry
    app.state.decision_log = decision_log
    app.state.api_key_store = api_key_store

    async def dispatch(adapter: ProtocolAdapter, request: Request) -> Response:
        try:
            body = await _read_json_body(request)
        except BadRequestError as e:
            return _error_response(adapter, e)

        ctx = _ctx_from_request(request, adapter)
        ctx_set_adapter(ctx, adapter)
        ctx_set_body(ctx, body)

        try:
            content_type, stream_iter, body_bytes = await pipeline(ctx)
        except GatewayError as e:
            return _error_response(adapter, e)
        except Exception as e:
            err = BadRequestError(f"Internal error: {type(e).__name__}: {e}")
            err.error_code = "internal_error"
            err.http_status = 500
            return _error_response(adapter, err)

        if stream_iter is not None:
            return StreamingResponse(stream_iter, media_type=content_type)
        return Response(content=body_bytes, media_type=content_type, status_code=200)

    @app.post("/v1/chat/completions")
    async def openai_chat(request: Request) -> Response:
        return await dispatch(adapters["openai_chat"], request)

    @app.post("/v1/messages")
    async def anthropic_messages(request: Request) -> Response:
        return await dispatch(adapters["anthropic"], request)

    @app.post("/v1/responses")
    async def openai_responses(request: Request) -> Response:
        return await dispatch(adapters["openai_responses"], request)

    @app.get("/health")
    async def health() -> dict[str, Any]:
        backends = [
            {
                "id": b.backend_id, "model": b.model, "health": b.health,
                "open": b.is_open(), "consecutive_failures": b.consecutive_failures,
            }
            for b in registry.all()
        ]
        return {"status": "ok", "backends": backends}

    return app


# ────────────── ctx <-> request helpers ──────────────


def _ctx_from_request(request: Request, adapter: ProtocolAdapter) -> RequestContext:
    headers = {k.lower(): v for k, v in request.headers.items()}
    return RequestContext(
        client_ip=request.client.host if request.client else "",
        user_agent=headers.get("user-agent", ""),
        headers=headers,
        src_protocol=adapter.name,
        src_path=str(request.url.path),
        src_method=request.method,
    )


# Stash adapter+body on the ctx via attribute access. Using a private
# attribute is cheaper than threading two extra args through every
# middleware signature.

_ADAPTER_ATTR = "_gw_adapter"
_BODY_ATTR = "_gw_body"


def ctx_set_adapter(ctx: RequestContext, adapter: ProtocolAdapter) -> None:
    setattr(ctx, _ADAPTER_ATTR, adapter)


def ctx_get_adapter(ctx: RequestContext) -> ProtocolAdapter:
    return getattr(ctx, _ADAPTER_ATTR)


def ctx_set_body(ctx: RequestContext, body: dict[str, Any]) -> None:
    setattr(ctx, _BODY_ATTR, body)


def ctx_get_body(ctx: RequestContext) -> dict[str, Any]:
    return getattr(ctx, _BODY_ATTR)


# ────────────── request body / error helpers ──────────────


async def _read_json_body(request: Request) -> dict[str, Any]:
    raw = await request.body()
    if not raw:
        raise BadRequestError("Empty request body")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise BadRequestError(f"Invalid JSON: {e}") from e
    if not isinstance(data, dict):
        raise BadRequestError("Request body must be a JSON object")
    return data


def _error_response(adapter: ProtocolAdapter, err: GatewayError) -> Response:
    body = adapter.error_envelope(err)
    return Response(
        content=body,
        media_type="application/json",
        status_code=getattr(err, "http_status", 500),
    )


# ────────────── lazy default app for `uvicorn gateway.server:app` ──────────────


def _build_default_app() -> FastAPI:
    """Used by ``uvicorn gateway.server:app``. Reads GATEWAY_CONFIG env var."""
    return create_app()


def _default_metrics_recorder():
    """Plug the SQLite recorder in by default; tests can override via GatewayHandler args."""
    try:
        from gateway.metrics import SQLiteMetricsRecorder
        return SQLiteMetricsRecorder()
    except Exception:
        return None


app = _build_default_app()


# Re-exports of types referenced in the public lifespan API for convenience.
__all__ = [
    "create_app",
    "app",
    "GatewaySettings",
    "ProbeSettings",
    "StorageSettings",
]
