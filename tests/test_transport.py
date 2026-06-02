from __future__ import annotations

import asyncio
import json
import logging

import httpx

from gateway.transport import HttpxTransport
from gateway.ws_tunnel import (
    WebSocketTunnel,
    _Node,
    _account_from_url,
    _path_from_url,
    compose_upstream_url,
)


async def _collect(src):
    out = []
    async for chunk in src:
        out.append(chunk)
    return b"".join(out)


def test_post_json_logs_upstream_400_details(caplog):
    detail = {"error": {"message": "messages[0].content is required", "field": "messages"}}

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json=detail, request=request)

    transport = HttpxTransport()
    transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with caplog.at_level(logging.ERROR, logger="gateway.transport"):
        status, raw = asyncio.run(transport.post_json(
            "https://mimo.example/v1/chat/completions",
            {"model": "m", "messages": [{"role": "user"}]},
        ))

    asyncio.run(transport.close())
    assert status == 400
    assert json.loads(raw) == detail
    assert "Upstream MiMo API returned HTTP 400" in caplog.text
    assert "messages[0].content is required" in caplog.text
    assert "messages_count" in caplog.text


def test_post_stream_logs_and_preserves_upstream_error_body(caplog):
    raw = b'{"error":{"message":"model missing"}}'

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, content=raw, request=request)

    transport = HttpxTransport()
    transport._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with caplog.at_level(logging.ERROR, logger="gateway.transport"):
        status, body_iter = asyncio.run(transport.post_stream(
            "https://mimo.example/v1/chat/completions",
            {"messages": []},
        ))
        body = asyncio.run(_collect(body_iter))

    asyncio.run(transport.close())
    assert status == 400
    assert body == raw
    assert "Upstream MiMo API returned HTTP 400" in caplog.text
    assert "model missing" in caplog.text


def test_ws_backend_url_strips_tunnel_endpoint_prefix():
    assert _path_from_url("ws://gateway.example/ws/v1/chat/completions") == "/v1/chat/completions"
    assert _path_from_url("wss://gateway.example/ws/anthropic/v1/messages?x=1") == "/anthropic/v1/messages?x=1"


def test_ws_path_drops_account_routing_param_but_keeps_others():
    # ?account= is a gateway-side selector — it must not reach the upstream,
    # while any other query the caller passed is preserved.
    assert _path_from_url(
        "wss://gw/ws/v1/chat/completions?account=kuro-aoki"
    ) == "/v1/chat/completions"
    assert _path_from_url(
        "wss://gw/ws/anthropic/v1/messages?account=kuro-aoki&beta=1"
    ) == "/anthropic/v1/messages?beta=1"


def test_account_from_url():
    assert _account_from_url("wss://gw/ws?account=kuro-aoki") == "kuro-aoki"
    assert _account_from_url("wss://gw/ws/v1/chat/completions?account=foo&token=x") == "foo"
    assert _account_from_url("wss://gw/ws") is None
    assert _account_from_url("https://api.example/v1/chat/completions") is None


def test_compose_upstream_url_keeps_account_query_after_path():
    # http backends: plain concat. ws backends: path goes into the path
    # component so the ?account= query survives.
    assert compose_upstream_url(
        "https://api.example/", "/v1/chat/completions"
    ) == "https://api.example/v1/chat/completions"
    assert compose_upstream_url(
        "wss://gw/ws?account=kuro-aoki", "/v1/chat/completions"
    ) == "wss://gw/ws/v1/chat/completions?account=kuro-aoki"


def test_next_node_routes_by_account():
    t = WebSocketTunnel()
    a = _Node(ws=None, label="a", account="kuro-aoki")
    b = _Node(ws=None, label="b", account="other")
    pool = _Node(ws=None, label="pool", account=None)
    t._nodes = [a, b, pool]
    # An account request only ever lands on its own node or the shared pool.
    for _ in range(6):
        assert t._next_node("kuro-aoki") in (a, pool)
    # No node for an unknown account (and no pool-only fallback exclusion).
    t._nodes = [a, b]
    assert t._next_node("ghost") is None
    assert t.has_account("kuro-aoki") and not t.has_account("ghost")
    assert t.online_count_for("kuro-aoki") == 1


def test_transport_delegates_ws_url_to_tunnel(monkeypatch):
    calls = []

    async def fake_request_json(url, body, *, headers=None, timeout_s=60.0):
        calls.append((url, body, headers, timeout_s))
        return 200, b'{"ok":true}'

    monkeypatch.setattr("gateway.ws_tunnel.request_json", fake_request_json)

    transport = HttpxTransport()
    try:
        status, raw = asyncio.run(transport.post_json(
            "ws://gateway.example/ws/v1/chat/completions",
            {"model": "m"},
            headers={"Content-Type": "application/json"},
            timeout_s=12,
        ))
    finally:
        asyncio.run(transport.close())

    assert status == 200
    assert raw == b'{"ok":true}'
    assert calls == [(
        "ws://gateway.example/ws/v1/chat/completions",
        {"model": "m"},
        {"Content-Type": "application/json"},
        12,
    )]
