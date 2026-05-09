"""End-to-end tests for gateway.server.

Uses FastAPI's TestClient against ``create_app`` with a fake upstream
transport so no network calls happen. Covers:

  * /health endpoint shape
  * OpenAI Chat / Anthropic / OpenAI Responses non-stream round-trips
  * Streaming SSE for OpenAI Chat
  * 401 on missing/bad API key (each protocol's error envelope)
  * 400 on bad JSON
  * Upstream 5xx → translated error envelope
  * x-api-key (Anthropic-style) header accepted
"""
from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from gateway.config import APIKeyStore, GatewayConfig
from gateway.config.loader import BackendConfig, GatewaySettings, StorageSettings
from gateway.server import create_app


# ────────────── fakes ──────────────


class FakeTransport:
    """Implements the UpstreamTransport Protocol without touching the network."""

    def __init__(
        self,
        *,
        json_response: tuple[int, bytes] | Exception | None = None,
        stream_response: tuple[int, list[bytes]] | Exception | None = None,
    ):
        self._json = json_response
        self._stream = stream_response
        self.json_calls: list[dict] = []
        self.stream_calls: list[dict] = []
        self.closed = False

    async def post_json(
        self, url: str, body: dict[str, Any], *,
        headers: dict[str, str] | None = None, timeout_s: float = 60.0,
    ) -> tuple[int, bytes]:
        self.json_calls.append({"url": url, "body": body, "headers": headers})
        if isinstance(self._json, Exception):
            raise self._json
        if self._json is None:
            return 200, _default_chat_completion_bytes()
        return self._json

    async def post_stream(
        self, url: str, body: dict[str, Any], *,
        headers: dict[str, str] | None = None, timeout_s: float = 600.0,
    ) -> tuple[int, AsyncIterator[bytes]]:
        self.stream_calls.append({"url": url, "body": body, "headers": headers})
        if isinstance(self._stream, Exception):
            raise self._stream
        chunks = self._stream[1] if self._stream else _default_chat_completion_stream()
        status = self._stream[0] if self._stream else 200

        async def gen() -> AsyncIterator[bytes]:
            for c in chunks:
                yield c

        return status, gen()

    async def close(self) -> None:
        self.closed = True


def _default_chat_completion_bytes() -> bytes:
    return json.dumps({
        "id": "chatcmpl-test",
        "object": "chat.completion",
        "created": 1700000000,
        "model": "MiMo-VL-7B-RL-2508",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "hello world"},
            "finish_reason": "stop",
        }],
        "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7},
    }).encode("utf-8")


def _default_chat_completion_stream() -> list[bytes]:
    """OpenAI-style SSE stream: role chunk, content chunks, terminator."""
    chunks = [
        {"id": "c1", "object": "chat.completion.chunk", "choices": [
            {"index": 0, "delta": {"role": "assistant"}, "finish_reason": None},
        ]},
        {"id": "c1", "object": "chat.completion.chunk", "choices": [
            {"index": 0, "delta": {"content": "hello"}, "finish_reason": None},
        ]},
        {"id": "c1", "object": "chat.completion.chunk", "choices": [
            {"index": 0, "delta": {"content": " world"}, "finish_reason": None},
        ]},
        {"id": "c1", "object": "chat.completion.chunk", "choices": [
            {"index": 0, "delta": {}, "finish_reason": "stop"},
        ]},
    ]
    out = [f"data: {json.dumps(c)}\n\n".encode() for c in chunks]
    out.append(b"data: [DONE]\n\n")
    return out


# ────────────── fixtures ──────────────


def _make_config(*, default_rpm: int = 10_000) -> GatewayConfig:
    """Test config with a single backend and a generous rate limit."""
    return GatewayConfig(
        gateway=GatewaySettings(default_rate_limit_per_min=default_rpm),
        storage=StorageSettings(api_keys_db=":memory:"),
        backends=[BackendConfig(
            id="b1",
            base_url="http://upstream.example",
            model="MiMo-VL-7B-RL-2508",
            api_key="sk-up",
            account_id="acct1",
        )],
    )


@pytest.fixture
def fake_transport():
    return FakeTransport()


@pytest.fixture
def store():
    s = APIKeyStore(":memory:")
    yield s
    s.close()


@pytest.fixture
def api_key(store):
    return store.create(label="test").secret


@pytest.fixture
def client(fake_transport, store):
    app = create_app(
        config=_make_config(),
        transport=fake_transport,
        api_key_store=store,
    )
    with TestClient(app) as tc:
        yield tc


# ────────────── /health ──────────────


def test_health_returns_backend_snapshot(client):
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert len(body["backends"]) == 1
    b = body["backends"][0]
    assert b["id"] == "b1"
    assert b["model"] == "MiMo-VL-7B-RL-2508"
    assert b["health"] in ("alive", "unknown")
    assert b["open"] is False


# ────────────── OpenAI Chat ──────────────


def test_openai_chat_non_stream_round_trip(client, api_key, fake_transport):
    r = client.post(
        "/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "model": "MiMo-VL-7B-RL-2508",
            "messages": [{"role": "user", "content": "hi"}],
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["object"] == "chat.completion"
    assert body["choices"][0]["message"]["content"] == "hello world"
    assert body["choices"][0]["finish_reason"] == "stop"
    # Upstream was called once with the right URL.
    assert len(fake_transport.json_calls) == 1
    assert fake_transport.json_calls[0]["url"] == "http://upstream.example/v1/chat/completions"
    assert fake_transport.json_calls[0]["headers"]["Authorization"] == "Bearer sk-up"


def test_openai_chat_streaming_emits_sse(client, api_key):
    with client.stream(
        "POST", "/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "model": "MiMo-VL-7B-RL-2508",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": True,
        },
    ) as r:
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/event-stream")
        body = b"".join(r.iter_bytes()).decode()
    assert "data: " in body
    assert "[DONE]" in body
    # Content should appear somewhere in a delta.
    assert "hello" in body
    assert "world" in body


# ────────────── Anthropic ──────────────


def test_anthropic_non_stream_round_trip(client, api_key):
    r = client.post(
        "/v1/messages",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "model": "MiMo-VL-7B-RL-2508",
            "max_tokens": 16,
            "messages": [{"role": "user", "content": "hi"}],
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["type"] == "message"
    assert body["role"] == "assistant"
    # Anthropic returns content as a list of blocks.
    assert isinstance(body["content"], list)
    assert body["content"][0]["type"] == "text"
    assert body["content"][0]["text"] == "hello world"
    assert body["stop_reason"] == "end_turn"


def test_anthropic_accepts_x_api_key_header(client, api_key):
    r = client.post(
        "/v1/messages",
        headers={"x-api-key": api_key},
        json={
            "model": "MiMo-VL-7B-RL-2508",
            "max_tokens": 16,
            "messages": [{"role": "user", "content": "hi"}],
        },
    )
    assert r.status_code == 200, r.text


# ────────────── OpenAI Responses ──────────────


def test_responses_non_stream_round_trip(client, api_key):
    r = client.post(
        "/v1/responses",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "model": "MiMo-VL-7B-RL-2508",
            "input": [{"role": "user", "content": [{"type": "input_text", "text": "hi"}]}],
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["object"] == "response"
    # Response has an output array of items; the message should contain our text.
    assert isinstance(body["output"], list)
    text_blob = json.dumps(body["output"])
    assert "hello world" in text_blob


# ────────────── auth ──────────────


def test_missing_auth_returns_openai_error_envelope(client):
    r = client.post(
        "/v1/chat/completions",
        json={"model": "MiMo-VL-7B-RL-2508", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert r.status_code == 401
    err = r.json()
    assert "error" in err
    assert err["error"]["type"]  # OpenAI envelope: {"error":{"type":..,"message":..}}


def test_missing_auth_returns_anthropic_error_envelope(client):
    r = client.post(
        "/v1/messages",
        json={
            "model": "MiMo-VL-7B-RL-2508", "max_tokens": 16,
            "messages": [{"role": "user", "content": "hi"}],
        },
    )
    assert r.status_code == 401
    err = r.json()
    # Anthropic envelope: {"type":"error","error":{"type":..,"message":..}}
    assert err["type"] == "error"
    assert err["error"]["type"]


def test_bad_token_rejected(client):
    r = client.post(
        "/v1/chat/completions",
        headers={"Authorization": "Bearer sk-mimo-not-real"},
        json={"model": "MiMo-VL-7B-RL-2508", "messages": [{"role": "user", "content": "hi"}]},
    )
    assert r.status_code == 401


# ────────────── bad request shapes ──────────────


def test_empty_body_is_bad_request(client, api_key):
    r = client.post(
        "/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        content=b"",
    )
    assert r.status_code == 400


def test_invalid_json_is_bad_request(client, api_key):
    r = client.post(
        "/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        content=b"not-json",
    )
    assert r.status_code == 400


def test_missing_model_is_bad_request(client, api_key):
    # OpenAIChatAdapter.parse_request raises BadRequestError when model is absent.
    r = client.post(
        "/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}"},
        json={"messages": [{"role": "user", "content": "hi"}]},
    )
    assert r.status_code == 400


# ────────────── upstream error translation ──────────────


def test_upstream_5xx_translated_to_protocol_error(api_key, store):
    transport = FakeTransport(json_response=(503, b'{"error":"oops"}'))
    app = create_app(
        config=_make_config(),
        transport=transport,
        api_key_store=store,
    )
    with TestClient(app) as tc:
        r = tc.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": "MiMo-VL-7B-RL-2508",
                "messages": [{"role": "user", "content": "hi"}],
            },
        )
    # UpstreamError → translated by adapter envelope; status carried through.
    assert r.status_code >= 500
    err = r.json()
    assert "error" in err


# ────────────── lifespan ──────────────


def test_lifespan_closes_transport(api_key, store):
    transport = FakeTransport()
    app = create_app(
        config=_make_config(),
        transport=transport,
        api_key_store=store,
    )
    with TestClient(app):
        pass
    assert transport.closed is True
