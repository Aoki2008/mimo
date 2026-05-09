"""Unit tests for gateway.adapters.anthropic.

Covered:
  * matches_path
  * parse_request — string content / blocks / system param / tool_use+tool_result split / images / stop_sequences / tool_choice
  * serialize_response_stream — message_start, content_block_*, message_delta, message_stop
  * serialize_response — non-stream JSON shape with text + tool_use blocks
  * stop_reason mapping — bidirectional
  * error_envelope — Anthropic shape {type:"error", error:{type, message}}
  * Cross-protocol: Anthropic-in / OpenAI-out reuse of IES events
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import pytest

from gateway.adapters.anthropic import (
    AnthropicAdapter,
    _ANTHROPIC_TO_IES,
    _IES_TO_ANTHROPIC,
)
from gateway.adapters.openai_chat import OpenAIChatAdapter
from gateway.core import (
    AuthError,
    BadRequestError,
    ContentBlockEnd,
    ContentBlockStart,
    InternalContent,
    InternalEvent,
    InternalMessage,
    InternalRequest,
    InternalTool,
    MessageEnd,
    MessageStart,
    RateLimitError,
    TextDelta,
    ToolCallDelta,
    UpstreamError,
    Usage,
)


# ───────── helpers ─────────


def _run(coro):
    return asyncio.run(coro)


async def _drain(stream: AsyncIterator[bytes]) -> bytes:
    out = b""
    async for c in stream:
        out += c
    return out


def _adapter() -> AnthropicAdapter:
    return AnthropicAdapter()


def _parse_sse_events(raw: bytes) -> list[tuple[str, dict]]:
    """Parse Anthropic SSE bytes back into [(event_name, payload)]."""
    chunks = [c for c in raw.split(b"\n\n") if c.strip()]
    parsed: list[tuple[str, dict]] = []
    for chunk in chunks:
        lines = chunk.decode().split("\n")
        name = ""
        data = ""
        for line in lines:
            if line.startswith("event:"):
                name = line[len("event:"):].strip()
            elif line.startswith("data:"):
                data = line[len("data:"):].strip()
        parsed.append((name, json.loads(data) if data else {}))
    return parsed


# ───────── matches_path ─────────


def test_matches_path():
    assert AnthropicAdapter.matches_path("/v1/messages")
    assert AnthropicAdapter.matches_path("/anthropic/v1/messages")
    assert not AnthropicAdapter.matches_path("/v1/chat/completions")


# ───────── parse_request ─────────


def test_parse_request_minimal_text():
    body = {
        "model": "claude-3-5-sonnet",
        "max_tokens": 256,
        "messages": [{"role": "user", "content": "hi"}],
    }
    req = _adapter().parse_request(body)
    assert req.model == "claude-3-5-sonnet"
    assert req.max_tokens == 256
    assert len(req.messages) == 1
    assert req.messages[0].role == "user"
    assert req.messages[0].content[0].type == "text"
    assert req.messages[0].content[0].text == "hi"


def test_parse_request_missing_max_tokens_raises():
    with pytest.raises(BadRequestError):
        _adapter().parse_request({
            "model": "m", "messages": [{"role": "user", "content": "x"}],
        })


def test_parse_request_missing_model_raises():
    with pytest.raises(BadRequestError):
        _adapter().parse_request({
            "max_tokens": 1, "messages": [{"role": "user", "content": "x"}],
        })


def test_parse_request_invalid_role_raises():
    with pytest.raises(BadRequestError):
        _adapter().parse_request({
            "model": "m", "max_tokens": 1,
            "messages": [{"role": "system", "content": "x"}],
        })


def test_parse_request_system_string_promoted_to_message():
    body = {
        "model": "m", "max_tokens": 1,
        "system": "You are helpful.",
        "messages": [{"role": "user", "content": "hi"}],
    }
    req = _adapter().parse_request(body)
    assert req.messages[0].role == "system"
    assert req.messages[0].content[0].text == "You are helpful."
    assert req.messages[1].role == "user"


def test_parse_request_system_block_array_promoted():
    body = {
        "model": "m", "max_tokens": 1,
        "system": [
            {"type": "text", "text": "be brief"},
            {"type": "text", "text": "be honest"},
        ],
        "messages": [{"role": "user", "content": "hi"}],
    }
    req = _adapter().parse_request(body)
    sys_msg = req.messages[0]
    assert sys_msg.role == "system"
    assert [c.text for c in sys_msg.content] == ["be brief", "be honest"]


def test_parse_request_user_blocks_text_and_image_base64():
    body = {
        "model": "m", "max_tokens": 1,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "what is this"},
                {"type": "image", "source": {
                    "type": "base64", "media_type": "image/png", "data": "ZZ",
                }},
            ],
        }],
    }
    req = _adapter().parse_request(body)
    blocks = req.messages[0].content
    assert blocks[0].type == "text"
    assert blocks[1].type == "image"
    assert blocks[1].image_mime == "image/png"
    assert blocks[1].image_data == "ZZ"


def test_parse_request_image_url_source_falls_back_to_text():
    body = {
        "model": "m", "max_tokens": 1,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "image", "source": {
                    "type": "url", "url": "https://x.example/y.png",
                }},
            ],
        }],
    }
    req = _adapter().parse_request(body)
    block = req.messages[0].content[0]
    assert block.type == "text"
    assert "https://x.example/y.png" in (block.text or "")


def test_parse_request_assistant_with_tool_use():
    body = {
        "model": "m", "max_tokens": 1,
        "messages": [
            {"role": "user", "content": "search"},
            {
                "role": "assistant",
                "content": [
                    {"type": "text", "text": "let me check"},
                    {"type": "tool_use", "id": "toolu_1",
                     "name": "search", "input": {"q": "weather"}},
                ],
            },
        ],
    }
    req = _adapter().parse_request(body)
    asst = req.messages[1]
    assert asst.role == "assistant"
    assert asst.content[0].type == "text"
    assert asst.content[1].type == "tool_use"
    assert asst.content[1].tool_id == "toolu_1"
    assert asst.content[1].tool_name == "search"
    assert asst.content[1].tool_input == {"q": "weather"}


def test_parse_request_tool_result_in_user_message_split_into_tool_message():
    """Anthropic embeds tool_result inside a user message; we split it out."""
    body = {
        "model": "m", "max_tokens": 1,
        "messages": [
            {"role": "user", "content": "search"},
            {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "tu_1", "name": "f", "input": {}},
                ],
            },
            {
                "role": "user",
                "content": [
                    {"type": "tool_result", "tool_use_id": "tu_1",
                     "content": [{"type": "text", "text": "sunny"}]},
                    {"type": "text", "text": "thanks!"},
                ],
            },
        ],
    }
    req = _adapter().parse_request(body)
    # Last block in input expanded to: tool message + user message
    assert len(req.messages) == 4
    assert req.messages[2].role == "tool"
    tr = req.messages[2].content[0]
    assert tr.type == "tool_result"
    assert tr.tool_id == "tu_1"
    assert tr.tool_output == "sunny"
    assert req.messages[3].role == "user"
    assert req.messages[3].content[0].text == "thanks!"


def test_parse_request_tool_result_string_content_and_is_error():
    body = {
        "model": "m", "max_tokens": 1,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "tool_result", "tool_use_id": "tu_1",
                 "content": "boom", "is_error": True},
            ],
        }],
    }
    req = _adapter().parse_request(body)
    tr = req.messages[0].content[0]
    assert tr.tool_output == "boom"
    assert tr.tool_error is True


def test_parse_request_tool_choice_variants():
    base = {
        "model": "m", "max_tokens": 1,
        "messages": [{"role": "user", "content": "x"}],
    }
    assert _adapter().parse_request(
        {**base, "tool_choice": {"type": "auto"}}).tool_choice == "auto"
    assert _adapter().parse_request(
        {**base, "tool_choice": {"type": "any"}}).tool_choice == "required"
    tc = _adapter().parse_request(
        {**base, "tool_choice": {"type": "tool", "name": "f"}}).tool_choice
    assert isinstance(tc, dict)
    assert tc["function"]["name"] == "f"


def test_parse_request_stop_sequences_normalized():
    body = {
        "model": "m", "max_tokens": 1,
        "messages": [{"role": "user", "content": "x"}],
        "stop_sequences": ["###", "STOP"],
    }
    assert _adapter().parse_request(body).stop == ["###", "STOP"]


def test_parse_request_tools_definition():
    body = {
        "model": "m", "max_tokens": 1,
        "messages": [{"role": "user", "content": "x"}],
        "tools": [{
            "name": "weather",
            "description": "get the weather",
            "input_schema": {"type": "object",
                             "properties": {"loc": {"type": "string"}}},
        }],
    }
    req = _adapter().parse_request(body)
    assert req.tools is not None and len(req.tools) == 1
    t = req.tools[0]
    assert isinstance(t, InternalTool)
    assert t.name == "weather"
    assert t.input_schema["type"] == "object"


# ───────── serialize_response_stream ─────────


def test_serialize_response_stream_text_emits_canonical_anthropic_events():
    async def feed() -> AsyncIterator[InternalEvent]:
        yield MessageStart(message_id="msg_1", model="claude-3-5-sonnet")
        yield ContentBlockStart(index=0, block_type="text")
        yield TextDelta(index=0, text="Hello")
        yield TextDelta(index=0, text=", world")
        yield ContentBlockEnd(index=0)
        yield MessageEnd(finish_reason="stop", usage=Usage(input_tokens=3, output_tokens=4))

    raw = _run(_drain(_adapter().serialize_response_stream(feed())))
    events = _parse_sse_events(raw)
    names = [e[0] for e in events]
    assert names == [
        "message_start",
        "content_block_start",
        "content_block_delta",
        "content_block_delta",
        "content_block_stop",
        "message_delta",
        "message_stop",
    ]
    # message_start carries id + model + initial usage
    ms = events[0][1]
    assert ms["message"]["id"] == "msg_1"
    assert ms["message"]["model"] == "claude-3-5-sonnet"
    # content deltas carry text_delta payload
    d1, d2 = events[2][1], events[3][1]
    assert d1["delta"] == {"type": "text_delta", "text": "Hello"}
    assert d2["delta"]["text"] == ", world"
    # message_delta carries final stop_reason + output_tokens
    md = events[5][1]
    assert md["delta"]["stop_reason"] == "end_turn"
    assert md["usage"]["output_tokens"] == 4


def test_serialize_response_stream_tool_use_uses_input_json_delta():
    async def feed() -> AsyncIterator[InternalEvent]:
        yield MessageStart(message_id="msg_1", model="m")
        yield ContentBlockStart(index=0, block_type="tool_use",
                                tool_id="toolu_1", tool_name="search")
        yield ToolCallDelta(index=0, tool_id="toolu_1", arguments_delta='{"q":')
        yield ToolCallDelta(index=0, tool_id="toolu_1", arguments_delta='"x"}')
        yield ContentBlockEnd(index=0)
        yield MessageEnd(finish_reason="tool_calls",
                         usage=Usage(input_tokens=1, output_tokens=2))

    raw = _run(_drain(_adapter().serialize_response_stream(feed())))
    events = _parse_sse_events(raw)
    # content_block_start carries tool_use with id + name + empty input
    cbs = events[1][1]
    assert cbs["content_block"]["type"] == "tool_use"
    assert cbs["content_block"]["id"] == "toolu_1"
    assert cbs["content_block"]["name"] == "search"
    assert cbs["content_block"]["input"] == {}
    # deltas use input_json_delta
    d1 = events[2][1]
    assert d1["delta"] == {"type": "input_json_delta", "partial_json": '{"q":'}
    # final stop_reason
    md = next(p for n, p in events if n == "message_delta")
    assert md["delta"]["stop_reason"] == "tool_use"


def test_serialize_response_stream_closes_dangling_blocks():
    """If MessageEnd arrives without ContentBlockEnd, we still close the block."""
    async def feed() -> AsyncIterator[InternalEvent]:
        yield MessageStart(message_id="msg_1", model="m")
        yield ContentBlockStart(index=0, block_type="text")
        yield TextDelta(index=0, text="hi")
        yield MessageEnd(finish_reason="stop", usage=Usage())

    raw = _run(_drain(_adapter().serialize_response_stream(feed())))
    events = _parse_sse_events(raw)
    assert any(n == "content_block_stop" and p["index"] == 0 for n, p in events)
    assert events[-1][0] == "message_stop"


# ───────── serialize_response (non-stream) ─────────


def test_serialize_response_non_stream_text():
    events: list[InternalEvent] = [
        MessageStart(message_id="msg_x", model="claude-3-5-sonnet"),
        ContentBlockStart(index=0, block_type="text"),
        TextDelta(index=0, text="hi "),
        TextDelta(index=0, text="there"),
        ContentBlockEnd(index=0),
        MessageEnd(finish_reason="stop", usage=Usage(input_tokens=1, output_tokens=2)),
    ]
    body = json.loads(_adapter().serialize_response(events).decode())
    assert body["id"] == "msg_x"
    assert body["type"] == "message"
    assert body["role"] == "assistant"
    assert body["model"] == "claude-3-5-sonnet"
    assert body["content"] == [{"type": "text", "text": "hi there"}]
    assert body["stop_reason"] == "end_turn"
    assert body["stop_sequence"] is None
    assert body["usage"] == {"input_tokens": 1, "output_tokens": 2}


def test_serialize_response_non_stream_tool_use():
    events: list[InternalEvent] = [
        MessageStart(message_id="msg_x", model="m"),
        ContentBlockStart(index=0, block_type="text"),
        TextDelta(index=0, text="let me check"),
        ContentBlockEnd(index=0),
        ContentBlockStart(index=1, block_type="tool_use",
                          tool_id="toolu_1", tool_name="search"),
        ToolCallDelta(index=1, tool_id="toolu_1", arguments_delta='{"q"'),
        ToolCallDelta(index=1, tool_id="toolu_1", arguments_delta=':"x"}'),
        ContentBlockEnd(index=1),
        MessageEnd(finish_reason="tool_calls", usage=Usage(input_tokens=1, output_tokens=1)),
    ]
    body = json.loads(_adapter().serialize_response(events).decode())
    assert body["stop_reason"] == "tool_use"
    blocks = body["content"]
    assert blocks[0] == {"type": "text", "text": "let me check"}
    assert blocks[1] == {
        "type": "tool_use", "id": "toolu_1",
        "name": "search", "input": {"q": "x"},
    }


# ───────── stop_reason mapping ─────────


def test_stop_reason_bidirectional_table():
    assert _ANTHROPIC_TO_IES["end_turn"] == "stop"
    assert _ANTHROPIC_TO_IES["max_tokens"] == "length"
    assert _ANTHROPIC_TO_IES["tool_use"] == "tool_calls"
    assert _IES_TO_ANTHROPIC["stop"] == "end_turn"
    assert _IES_TO_ANTHROPIC["length"] == "max_tokens"
    assert _IES_TO_ANTHROPIC["tool_calls"] == "tool_use"


# ───────── error_envelope ─────────


def test_error_envelope_anthropic_shape():
    payload = json.loads(_adapter().error_envelope(BadRequestError("bad")).decode())
    assert payload == {
        "type": "error",
        "error": {"type": "invalid_request_error", "message": "bad"},
    }


def test_error_envelope_maps_codes_to_anthropic_types():
    cases = [
        (AuthError("x"), "authentication_error"),
        (RateLimitError("x"), "rate_limit_error"),
        (UpstreamError("x"), "api_error"),
    ]
    for err, expected_type in cases:
        payload = json.loads(_adapter().error_envelope(err).decode())
        assert payload["error"]["type"] == expected_type


# ───────── cross-protocol: Anthropic-in / OpenAI-out ─────────


def test_cross_protocol_anthropic_in_openai_out_round_trip():
    """An Anthropic request body must serialize to something the OpenAI
    upstream codec accepts. This test validates that the IES sits cleanly
    between the two protocols — the most important integration point."""
    body = {
        "model": "claude-3-5-sonnet",
        "max_tokens": 64,
        "system": "Be brief.",
        "messages": [
            {"role": "user", "content": "hi"},
            {"role": "assistant", "content": [
                {"type": "tool_use", "id": "tu1", "name": "f", "input": {"a": 1}},
            ]},
            {"role": "user", "content": [
                {"type": "tool_result", "tool_use_id": "tu1",
                 "content": [{"type": "text", "text": "ok"}]},
            ]},
        ],
    }
    req = AnthropicAdapter().parse_request(body)
    upstream_body = OpenAIChatAdapter().serialize_to_upstream(req)
    msgs = upstream_body["messages"]
    # system promoted to a system message
    assert msgs[0] == {"role": "system", "content": "Be brief."}
    # assistant carries tool_calls
    assert msgs[2]["role"] == "assistant"
    assert msgs[2]["tool_calls"][0]["id"] == "tu1"
    assert msgs[2]["tool_calls"][0]["function"]["name"] == "f"
    # tool_result becomes a tool message
    assert msgs[3] == {"role": "tool", "tool_call_id": "tu1", "content": "ok"}


def test_cross_protocol_openai_event_stream_serialized_to_anthropic_sse():
    """An IES event stream produced by OpenAIChatAdapter.parse_upstream_*
    can be serialized straight out as Anthropic SSE without re-mapping."""
    events = [
        MessageStart(message_id="msg_x", model="m"),
        ContentBlockStart(index=0, block_type="text"),
        TextDelta(index=0, text="Hello"),
        ContentBlockEnd(index=0),
        MessageEnd(finish_reason="stop", usage=Usage(input_tokens=1, output_tokens=1)),
    ]

    async def feed() -> AsyncIterator[InternalEvent]:
        for ev in events:
            yield ev

    raw = _run(_drain(_adapter().serialize_response_stream(feed())))
    parsed = _parse_sse_events(raw)
    assert parsed[0][0] == "message_start"
    assert parsed[-1][0] == "message_stop"
    text_deltas = [
        p["delta"]["text"]
        for n, p in parsed
        if n == "content_block_delta" and p["delta"]["type"] == "text_delta"
    ]
    assert "".join(text_deltas) == "Hello"
