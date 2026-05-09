"""Unit tests for gateway.adapters.openai_responses.

Covered:
  * matches_path
  * parse_request — string input / message items / function_call / function_call_output
                  / instructions / images / tools (flat and legacy-wrapped)
  * serialize_response_stream — canonical event sequence for text + tool calls
  * serialize_response — non-stream JSON with output items
  * error_envelope — Responses-style {error:{type,code,message,param}}
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import pytest

from gateway.adapters.openai_responses import OpenAIResponsesAdapter
from gateway.core import (
    BadRequestError,
    ContentBlockEnd,
    ContentBlockStart,
    InternalEvent,
    MessageEnd,
    MessageStart,
    TextDelta,
    ToolCallDelta,
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


def _adapter() -> OpenAIResponsesAdapter:
    return OpenAIResponsesAdapter()


def _parse_sse(raw: bytes) -> list[tuple[str, dict]]:
    chunks = [c for c in raw.split(b"\n\n") if c.strip()]
    out: list[tuple[str, dict]] = []
    for chunk in chunks:
        name = ""
        data = ""
        for line in chunk.decode().split("\n"):
            if line.startswith("event:"):
                name = line[len("event:"):].strip()
            elif line.startswith("data:"):
                data = line[len("data:"):].strip()
        out.append((name, json.loads(data) if data else {}))
    return out


# ───────── matches_path ─────────


def test_matches_path():
    assert OpenAIResponsesAdapter.matches_path("/v1/responses")
    assert not OpenAIResponsesAdapter.matches_path("/v1/chat/completions")


# ───────── parse_request ─────────


def test_parse_request_string_input():
    body = {"model": "m", "input": "hello"}
    req = _adapter().parse_request(body)
    assert req.messages[0].role == "user"
    assert req.messages[0].content[0].text == "hello"
    assert req.max_tokens == 4096  # default


def test_parse_request_missing_model_raises():
    with pytest.raises(BadRequestError):
        _adapter().parse_request({"input": "x"})


def test_parse_request_missing_input_raises():
    with pytest.raises(BadRequestError):
        _adapter().parse_request({"model": "m"})


def test_parse_request_invalid_input_type_raises():
    with pytest.raises(BadRequestError):
        _adapter().parse_request({"model": "m", "input": 42})


def test_parse_request_empty_message_list_raises():
    with pytest.raises(BadRequestError):
        _adapter().parse_request({"model": "m", "input": []})


def test_parse_request_instructions_promoted_to_system():
    body = {"model": "m", "instructions": "be brief", "input": "hi"}
    req = _adapter().parse_request(body)
    assert req.messages[0].role == "system"
    assert req.messages[0].content[0].text == "be brief"
    assert req.messages[1].role == "user"


def test_parse_request_message_items_with_input_text_and_image():
    body = {
        "model": "m",
        "input": [{
            "type": "message",
            "role": "user",
            "content": [
                {"type": "input_text", "text": "what is this"},
                {"type": "input_image", "image_url": "data:image/png;base64,XXX"},
            ],
        }],
    }
    req = _adapter().parse_request(body)
    blocks = req.messages[0].content
    assert blocks[0].type == "text"
    assert blocks[0].text == "what is this"
    assert blocks[1].type == "image"
    assert blocks[1].image_data == "XXX"
    assert blocks[1].image_mime == "image/png"


def test_parse_request_developer_role_normalizes_to_system():
    body = {
        "model": "m",
        "input": [{"type": "message", "role": "developer", "content": "rules"}],
    }
    req = _adapter().parse_request(body)
    assert req.messages[0].role == "system"


def test_parse_request_function_call_item_becomes_assistant_tool_use():
    body = {
        "model": "m",
        "input": [
            {"type": "message", "role": "user", "content": "search"},
            {"type": "function_call", "id": "fc1", "call_id": "call_1",
             "name": "search", "arguments": '{"q":"weather"}'},
            {"type": "function_call_output", "call_id": "call_1", "output": "sunny"},
        ],
    }
    req = _adapter().parse_request(body)
    assert len(req.messages) == 3
    asst = req.messages[1]
    assert asst.role == "assistant"
    tu = asst.content[0]
    assert tu.type == "tool_use"
    assert tu.tool_id == "call_1"
    assert tu.tool_name == "search"
    assert tu.tool_input == {"q": "weather"}
    tool = req.messages[2]
    assert tool.role == "tool"
    assert tool.content[0].type == "tool_result"
    assert tool.content[0].tool_id == "call_1"
    assert tool.content[0].tool_output == "sunny"


def test_parse_request_function_call_malformed_args_preserved():
    body = {
        "model": "m",
        "input": [{
            "type": "function_call", "call_id": "c1",
            "name": "f", "arguments": "{not json",
        }],
    }
    req = _adapter().parse_request(body)
    tu = req.messages[0].content[0]
    assert tu.tool_input == {"_raw": "{not json"}


def test_parse_request_tool_definition_flat_and_wrapped():
    flat = {
        "model": "m", "input": "x",
        "tools": [{
            "type": "function", "name": "f", "description": "d",
            "parameters": {"type": "object"},
        }],
    }
    req = _adapter().parse_request(flat)
    assert req.tools[0].name == "f"
    assert req.tools[0].input_schema["type"] == "object"

    wrapped = {
        "model": "m", "input": "x",
        "tools": [{"type": "function", "function": {
            "name": "g", "description": "d",
            "parameters": {"type": "object"},
        }}],
    }
    req2 = _adapter().parse_request(wrapped)
    assert req2.tools[0].name == "g"


def test_parse_request_max_output_tokens_used():
    body = {"model": "m", "input": "x", "max_output_tokens": 1024}
    req = _adapter().parse_request(body)
    assert req.max_tokens == 1024


# ───────── serialize_response_stream ─────────


def test_serialize_response_stream_text_emits_canonical_events():
    async def feed() -> AsyncIterator[InternalEvent]:
        yield MessageStart(message_id="resp_1", model="m")
        yield ContentBlockStart(index=0, block_type="text")
        yield TextDelta(index=0, text="Hello")
        yield TextDelta(index=0, text=" world")
        yield ContentBlockEnd(index=0)
        yield MessageEnd(finish_reason="stop", usage=Usage(input_tokens=2, output_tokens=2))

    raw = _run(_drain(_adapter().serialize_response_stream(feed())))
    events = _parse_sse(raw)
    names = [n for n, _ in events]
    assert names[0] == "response.created"
    assert names[1] == "response.in_progress"
    assert "response.output_item.added" in names
    assert "response.content_part.added" in names
    deltas = [p["delta"] for n, p in events if n == "response.output_text.delta"]
    assert deltas == ["Hello", " world"]
    assert "response.output_text.done" in names
    assert "response.content_part.done" in names
    assert "response.output_item.done" in names
    completed = next(p for n, p in events if n == "response.completed")
    assert completed["response"]["status"] == "completed"
    assert completed["response"]["output"][0]["type"] == "message"
    assert completed["response"]["output"][0]["content"][0]["text"] == "Hello world"
    assert completed["response"]["usage"]["total_tokens"] == 4


def test_serialize_response_stream_function_call_events():
    async def feed() -> AsyncIterator[InternalEvent]:
        yield MessageStart(message_id="resp_1", model="m")
        yield ContentBlockStart(index=0, block_type="tool_use",
                                tool_id="call_1", tool_name="search")
        yield ToolCallDelta(index=0, tool_id="call_1", arguments_delta='{"q":')
        yield ToolCallDelta(index=0, tool_id="call_1", arguments_delta='"x"}')
        yield ContentBlockEnd(index=0)
        yield MessageEnd(finish_reason="tool_calls",
                         usage=Usage(input_tokens=1, output_tokens=1))

    raw = _run(_drain(_adapter().serialize_response_stream(feed())))
    events = _parse_sse(raw)
    names = [n for n, _ in events]

    item_added = next(p for n, p in events if n == "response.output_item.added")
    assert item_added["item"]["type"] == "function_call"
    assert item_added["item"]["call_id"] == "call_1"
    assert item_added["item"]["name"] == "search"

    arg_deltas = [
        p["delta"] for n, p in events if n == "response.function_call_arguments.delta"
    ]
    assert arg_deltas == ['{"q":', '"x"}']

    arg_done = next(
        p for n, p in events if n == "response.function_call_arguments.done"
    )
    assert arg_done["arguments"] == '{"q":"x"}'

    item_done = next(p for n, p in events if n == "response.output_item.done")
    assert item_done["item"]["arguments"] == '{"q":"x"}'

    completed = next(p for n, p in events if n == "response.completed")
    assert completed["response"]["output"][0]["arguments"] == '{"q":"x"}'
    # tool_calls finish_reason still maps to completed status
    assert completed["response"]["status"] == "completed"


def test_serialize_response_stream_length_finish_maps_to_incomplete():
    async def feed() -> AsyncIterator[InternalEvent]:
        yield MessageStart(message_id="resp_1", model="m")
        yield ContentBlockStart(index=0, block_type="text")
        yield TextDelta(index=0, text="long...")
        yield ContentBlockEnd(index=0)
        yield MessageEnd(finish_reason="length",
                         usage=Usage(input_tokens=1, output_tokens=4096))

    raw = _run(_drain(_adapter().serialize_response_stream(feed())))
    events = _parse_sse(raw)
    completed = next(p for n, p in events if n == "response.completed")
    assert completed["response"]["status"] == "incomplete"


# ───────── serialize_response (non-stream) ─────────


def test_serialize_response_non_stream_text():
    events: list[InternalEvent] = [
        MessageStart(message_id="resp_x", model="m"),
        ContentBlockStart(index=0, block_type="text"),
        TextDelta(index=0, text="hello "),
        TextDelta(index=0, text="world"),
        ContentBlockEnd(index=0),
        MessageEnd(finish_reason="stop", usage=Usage(input_tokens=1, output_tokens=2)),
    ]
    body = json.loads(_adapter().serialize_response(events).decode())
    assert body["id"] == "resp_x"
    assert body["object"] == "response"
    assert body["status"] == "completed"
    item = body["output"][0]
    assert item["type"] == "message"
    assert item["content"][0]["type"] == "output_text"
    assert item["content"][0]["text"] == "hello world"
    assert body["usage"]["total_tokens"] == 3


def test_serialize_response_non_stream_function_call():
    events: list[InternalEvent] = [
        MessageStart(message_id="resp_x", model="m"),
        ContentBlockStart(index=0, block_type="text"),
        TextDelta(index=0, text="searching"),
        ContentBlockEnd(index=0),
        ContentBlockStart(index=1, block_type="tool_use",
                          tool_id="call_1", tool_name="search"),
        ToolCallDelta(index=1, tool_id="call_1", arguments_delta='{"q"'),
        ToolCallDelta(index=1, tool_id="call_1", arguments_delta=':"x"}'),
        ContentBlockEnd(index=1),
        MessageEnd(finish_reason="tool_calls",
                   usage=Usage(input_tokens=1, output_tokens=1)),
    ]
    body = json.loads(_adapter().serialize_response(events).decode())
    items = body["output"]
    assert len(items) == 2
    assert items[0]["type"] == "message"
    assert items[1]["type"] == "function_call"
    assert items[1]["call_id"] == "call_1"
    assert items[1]["name"] == "search"
    assert items[1]["arguments"] == '{"q":"x"}'


# ───────── error_envelope ─────────


def test_error_envelope_responses_shape():
    payload = json.loads(_adapter().error_envelope(BadRequestError("missing field")).decode())
    assert payload == {
        "error": {
            "type": "invalid_request_error",
            "code": "invalid_request",
            "message": "missing field",
            "param": None,
        }
    }
