"""
OpenAI Responses API adapter — client side only.

Endpoint: ``/v1/responses``. Streaming uses named SSE events
(``event: response.<name>\\ndata: <json>\\n\\n``). The Responses API differs
from Chat Completions in three ways that matter to this adapter:

  1. Top-level ``input`` (not ``messages``) accepts a heterogeneous list of
     input items: ``message``, ``function_call``, ``function_call_output``.
  2. ``instructions`` is the system prompt (a string, not an item).
  3. ``output`` is a list of top-level items. Text and tool calls live as
     siblings — there is no nested ``tool_calls`` array on a message.

This adapter is **client-facing only**: the upstream is always called via
the OpenAI Chat codec. So this class converts:

  * Responses request → InternalRequest      (parse_request)
  * IES events       → Responses SSE bytes   (serialize_response_stream)
  * IES events       → Responses JSON bytes  (serialize_response)
"""
from __future__ import annotations

import json
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

from gateway.core import (
    BadRequestError,
    ContentBlockEnd,
    ContentBlockStart,
    FinishReason,
    GatewayError,
    InternalContent,
    InternalEvent,
    InternalMessage,
    InternalRequest,
    InternalTool,
    MessageEnd,
    MessageStart,
    StreamError,
    TextDelta,
    ToolCallDelta,
    Usage,
)

from .base import ProtocolAdapter


# ────────────── status / finish_reason mapping ──────────────


_IES_TO_RESPONSES_STATUS: dict[FinishReason, str] = {
    "stop": "completed",
    "length": "incomplete",
    "tool_calls": "completed",
    "content_filter": "incomplete",
    "error": "failed",
}


_GATEWAY_TO_RESPONSES_ERR_TYPE: dict[str, str] = {
    "invalid_request": "invalid_request_error",
    "authentication_error": "authentication_error",
    "rate_limit_exceeded": "rate_limit_error",
    "upstream_error": "server_error",
    "backend_unavailable": "server_error",
    "gateway_timeout": "server_error",
    "adapter_error": "server_error",
    "gateway_error": "server_error",
}


# ────────────── Adapter ──────────────


class OpenAIResponsesAdapter(ProtocolAdapter):
    """OpenAI Responses: ``/v1/responses``."""

    name = "openai_responses"

    @classmethod
    def matches_path(cls, path: str) -> bool:
        return path.endswith("/responses")

    # ============ Request side ============

    def parse_request(self, body: dict[str, Any]) -> InternalRequest:
        if not isinstance(body, dict):
            raise BadRequestError("Request body must be a JSON object")
        model = body.get("model")
        if not model:
            raise BadRequestError("Missing 'model'")
        raw_input = body.get("input")
        if raw_input is None:
            raise BadRequestError("Missing 'input'")

        messages: list[InternalMessage] = []

        # instructions = system prompt
        instr = body.get("instructions")
        if isinstance(instr, str) and instr:
            messages.append(InternalMessage(
                role="system",
                content=[InternalContent(type="text", text=instr)],
            ))

        # input may be a bare string OR a list of items.
        if isinstance(raw_input, str):
            messages.append(InternalMessage(
                role="user",
                content=[InternalContent(type="text", text=raw_input)],
            ))
        elif isinstance(raw_input, list):
            for item in raw_input:
                messages.extend(self._parse_input_item(item))
        else:
            raise BadRequestError("'input' must be a string or array")

        if not messages:
            raise BadRequestError("'input' produced no messages")

        tools = None
        if body.get("tools"):
            tools = [self._parse_tool(t) for t in body["tools"]]

        max_tokens = (
            body.get("max_output_tokens")
            or body.get("max_tokens")
            or 4096
        )

        return InternalRequest(
            model=model,
            messages=messages,
            max_tokens=int(max_tokens),
            stream=bool(body.get("stream", False)),
            temperature=body.get("temperature"),
            top_p=body.get("top_p"),
            stop=None,
            tools=tools,
            tool_choice=body.get("tool_choice"),
            metadata={k: v for k, v in body.items() if k not in _CONSUMED_KEYS},
        )

    @staticmethod
    def _parse_tool(t: Any) -> InternalTool:
        """Responses-API tools are flat (no `function` envelope)."""
        if not isinstance(t, dict):
            raise BadRequestError(f"Invalid tool definition: {t}")
        # Some clients still wrap in {function: {...}} (legacy). Accept both.
        if "function" in t and isinstance(t["function"], dict):
            fn = t["function"]
        else:
            fn = t
        name = fn.get("name") or t.get("name")
        if not name:
            raise BadRequestError(f"Tool missing 'name': {t}")
        return InternalTool(
            name=name,
            description=fn.get("description", "") or t.get("description", ""),
            input_schema=(
                fn.get("parameters")
                or fn.get("input_schema")
                or t.get("parameters")
                or t.get("input_schema")
                or {}
            ),
        )

    @classmethod
    def _parse_input_item(cls, item: Any) -> list[InternalMessage]:
        """One Responses input item → 0, 1 or many InternalMessages."""
        if isinstance(item, str):
            return [InternalMessage(
                role="user",
                content=[InternalContent(type="text", text=item)],
            )]
        if not isinstance(item, dict):
            return []

        itype = item.get("type")

        # function_call_output: tool result item.
        if itype == "function_call_output":
            return [InternalMessage(role="tool", content=[
                InternalContent(
                    type="tool_result",
                    tool_id=item.get("call_id", ""),
                    tool_output=str(item.get("output", "")),
                ),
            ])]

        # function_call: assistant invoking a tool.
        if itype == "function_call":
            args = item.get("arguments", "")
            tool_input: dict[str, Any] | None = None
            if isinstance(args, str):
                try:
                    tool_input = json.loads(args) if args else {}
                except json.JSONDecodeError:
                    tool_input = {"_raw": args}
            elif isinstance(args, dict):
                tool_input = args
            return [InternalMessage(role="assistant", content=[
                InternalContent(
                    type="tool_use",
                    tool_id=item.get("call_id") or item.get("id", ""),
                    tool_name=item.get("name", ""),
                    tool_input=tool_input or {},
                ),
            ])]

        # message item (default type is "message").
        if itype is None or itype == "message":
            role = item.get("role", "user")
            if role == "developer":
                role = "system"
            content = item.get("content", "")
            blocks = cls._parse_content(content)
            return [InternalMessage(role=role, content=blocks)]

        return []

    @staticmethod
    def _parse_content(content: Any) -> list[InternalContent]:
        if isinstance(content, str):
            return [InternalContent(type="text", text=content)]
        if not isinstance(content, list):
            return []
        out: list[InternalContent] = []
        for part in content:
            if isinstance(part, str):
                out.append(InternalContent(type="text", text=part))
                continue
            if not isinstance(part, dict):
                continue
            ptype = part.get("type")
            if ptype in ("input_text", "output_text", "text"):
                out.append(InternalContent(type="text", text=part.get("text", "")))
            elif ptype == "input_image":
                url = part.get("image_url", "")
                if isinstance(url, dict):
                    url = url.get("url", "")
                if isinstance(url, str) and url.startswith("data:"):
                    try:
                        header, _, b64 = url.partition(",")
                        mime = header.split(";")[0].split(":", 1)[-1] or "image/png"
                        out.append(InternalContent(
                            type="image", image_data=b64, image_mime=mime,
                        ))
                    except Exception as e:
                        raise BadRequestError(f"Invalid data URL: {e}") from e
                elif isinstance(url, str) and url:
                    out.append(InternalContent(type="text", text=f"[image: {url}]"))
        return out

    # ============ Streaming serialization (IES → Responses SSE) ============

    def serialize_response_stream(
        self, events: AsyncIterator[InternalEvent]
    ) -> AsyncIterator[bytes]:
        return self._serialize_stream(events)

    async def _serialize_stream(
        self, events: AsyncIterator[InternalEvent]
    ) -> AsyncIterator[bytes]:
        response_id = ""
        model = ""
        created = int(time.time())
        # State for output items. A text block becomes a "message" output
        # item; a tool_use block becomes a "function_call" output item.
        output_items: list[dict[str, Any]] = []         # frozen views for response.completed
        block_to_output_idx: dict[int, int] = {}        # IES idx → output_index
        text_accum: dict[int, str] = {}                 # IES idx → joined text so far
        tool_args_accum: dict[int, str] = {}            # IES idx → joined args so far
        tool_meta: dict[int, dict[str, str]] = {}       # IES idx → {item_id, call_id, name}
        message_emitted = False
        final_finish: FinishReason = "stop"
        final_usage = Usage()

        def _next_output_idx() -> int:
            return len(output_items)

        async for ev in events:
            if isinstance(ev, MessageStart):
                response_id = ev.message_id or _gen_resp_id()
                model = ev.model
                base = self._base_response(response_id, model, created)
                yield _sse("response.created", {
                    "type": "response.created",
                    "response": {**base, "status": "in_progress", "output": []},
                })
                yield _sse("response.in_progress", {
                    "type": "response.in_progress",
                    "response": {**base, "status": "in_progress", "output": []},
                })
                message_emitted = True

            elif isinstance(ev, ContentBlockStart):
                if not message_emitted:
                    response_id = response_id or _gen_resp_id()
                    base = self._base_response(response_id, model, created)
                    yield _sse("response.created", {
                        "type": "response.created",
                        "response": {**base, "status": "in_progress", "output": []},
                    })
                    yield _sse("response.in_progress", {
                        "type": "response.in_progress",
                        "response": {**base, "status": "in_progress", "output": []},
                    })
                    message_emitted = True

                output_index = _next_output_idx()
                block_to_output_idx[ev.index] = output_index

                if ev.block_type == "text":
                    item_id = _gen_item_id("msg")
                    item = {
                        "id": item_id,
                        "type": "message",
                        "status": "in_progress",
                        "role": "assistant",
                        "content": [],
                    }
                    output_items.append(item)
                    text_accum[ev.index] = ""
                    yield _sse("response.output_item.added", {
                        "type": "response.output_item.added",
                        "output_index": output_index,
                        "item": item,
                    })
                    yield _sse("response.content_part.added", {
                        "type": "response.content_part.added",
                        "item_id": item_id,
                        "output_index": output_index,
                        "content_index": 0,
                        "part": {"type": "output_text", "text": "", "annotations": []},
                    })
                else:
                    item_id = _gen_item_id("fc")
                    call_id = ev.tool_id or item_id
                    name = ev.tool_name or ""
                    tool_meta[ev.index] = {
                        "item_id": item_id, "call_id": call_id, "name": name,
                    }
                    item = {
                        "id": item_id,
                        "type": "function_call",
                        "status": "in_progress",
                        "call_id": call_id,
                        "name": name,
                        "arguments": "",
                    }
                    output_items.append(item)
                    tool_args_accum[ev.index] = ""
                    yield _sse("response.output_item.added", {
                        "type": "response.output_item.added",
                        "output_index": output_index,
                        "item": item,
                    })

            elif isinstance(ev, TextDelta):
                output_index = block_to_output_idx.get(ev.index)
                if output_index is None:
                    # implicit start
                    output_index = _next_output_idx()
                    block_to_output_idx[ev.index] = output_index
                    item_id = _gen_item_id("msg")
                    item = {
                        "id": item_id, "type": "message", "status": "in_progress",
                        "role": "assistant", "content": [],
                    }
                    output_items.append(item)
                    text_accum[ev.index] = ""
                    yield _sse("response.output_item.added", {
                        "type": "response.output_item.added",
                        "output_index": output_index, "item": item,
                    })
                    yield _sse("response.content_part.added", {
                        "type": "response.content_part.added",
                        "item_id": item_id, "output_index": output_index,
                        "content_index": 0,
                        "part": {"type": "output_text", "text": "", "annotations": []},
                    })
                else:
                    item_id = output_items[output_index]["id"]

                text_accum[ev.index] += ev.text
                yield _sse("response.output_text.delta", {
                    "type": "response.output_text.delta",
                    "item_id": item_id,
                    "output_index": output_index,
                    "content_index": 0,
                    "delta": ev.text,
                })

            elif isinstance(ev, ToolCallDelta):
                output_index = block_to_output_idx.get(ev.index)
                if output_index is None:
                    continue
                item_id = output_items[output_index]["id"]
                tool_args_accum[ev.index] = tool_args_accum.get(ev.index, "") + ev.arguments_delta
                yield _sse("response.function_call_arguments.delta", {
                    "type": "response.function_call_arguments.delta",
                    "item_id": item_id,
                    "output_index": output_index,
                    "delta": ev.arguments_delta,
                })

            elif isinstance(ev, ContentBlockEnd):
                output_index = block_to_output_idx.get(ev.index)
                if output_index is None:
                    continue
                item = output_items[output_index]
                item_id = item["id"]
                if item["type"] == "message":
                    final_text = text_accum.get(ev.index, "")
                    item["content"] = [{
                        "type": "output_text", "text": final_text, "annotations": [],
                    }]
                    item["status"] = "completed"
                    yield _sse("response.output_text.done", {
                        "type": "response.output_text.done",
                        "item_id": item_id,
                        "output_index": output_index,
                        "content_index": 0,
                        "text": final_text,
                    })
                    yield _sse("response.content_part.done", {
                        "type": "response.content_part.done",
                        "item_id": item_id,
                        "output_index": output_index,
                        "content_index": 0,
                        "part": {
                            "type": "output_text",
                            "text": final_text, "annotations": [],
                        },
                    })
                else:  # function_call
                    args = tool_args_accum.get(ev.index, "")
                    item["arguments"] = args
                    item["status"] = "completed"
                    yield _sse("response.function_call_arguments.done", {
                        "type": "response.function_call_arguments.done",
                        "item_id": item_id,
                        "output_index": output_index,
                        "arguments": args,
                    })
                yield _sse("response.output_item.done", {
                    "type": "response.output_item.done",
                    "output_index": output_index,
                    "item": item,
                })

            elif isinstance(ev, MessageEnd):
                final_finish = ev.finish_reason
                final_usage = ev.usage

            elif isinstance(ev, StreamError):
                yield _sse("response.failed", {
                    "type": "response.failed",
                    "response": {
                        "id": response_id or _gen_resp_id(),
                        "object": "response",
                        "status": "failed",
                        "error": {"type": "server_error", "message": ev.message},
                    },
                })
                if not ev.recoverable:
                    return

        if not message_emitted:
            response_id = response_id or _gen_resp_id()
            base = self._base_response(response_id, model, created)
            yield _sse("response.created", {
                "type": "response.created",
                "response": {**base, "status": "in_progress", "output": []},
            })

        yield _sse("response.completed", {
            "type": "response.completed",
            "response": {
                **self._base_response(response_id, model, created),
                "status": _IES_TO_RESPONSES_STATUS.get(final_finish, "completed"),
                "output": output_items,
                "usage": {
                    "input_tokens": final_usage.input_tokens,
                    "output_tokens": final_usage.output_tokens,
                    "total_tokens": final_usage.total_tokens,
                },
            },
        })

    @staticmethod
    def _base_response(response_id: str, model: str, created: int) -> dict[str, Any]:
        return {
            "id": response_id,
            "object": "response",
            "created_at": created,
            "model": model,
            "metadata": {},
        }

    # ============ Non-stream serialization (IES → Responses JSON) ============

    def serialize_response(self, events: list[InternalEvent]) -> bytes:
        response_id = ""
        model = ""
        # Each IES content block → one output item.
        items_by_idx: dict[int, dict[str, Any]] = {}
        item_order: list[int] = []
        text_accum: dict[int, list[str]] = {}
        tool_args_accum: dict[int, list[str]] = {}
        finish: FinishReason = "stop"
        usage = Usage()

        for ev in events:
            if isinstance(ev, MessageStart):
                response_id = ev.message_id or _gen_resp_id()
                model = ev.model
            elif isinstance(ev, ContentBlockStart):
                if ev.index in items_by_idx:
                    continue
                item_order.append(ev.index)
                if ev.block_type == "text":
                    item_id = _gen_item_id("msg")
                    items_by_idx[ev.index] = {
                        "id": item_id, "type": "message",
                        "status": "completed", "role": "assistant",
                        "content": [],
                    }
                    text_accum[ev.index] = []
                else:
                    item_id = _gen_item_id("fc")
                    items_by_idx[ev.index] = {
                        "id": item_id, "type": "function_call",
                        "status": "completed",
                        "call_id": ev.tool_id or item_id,
                        "name": ev.tool_name or "",
                        "arguments": "",
                    }
                    tool_args_accum[ev.index] = []
            elif isinstance(ev, TextDelta):
                text_accum.setdefault(ev.index, []).append(ev.text)
            elif isinstance(ev, ToolCallDelta):
                tool_args_accum.setdefault(ev.index, []).append(ev.arguments_delta)
            elif isinstance(ev, MessageEnd):
                finish = ev.finish_reason
                usage = ev.usage

        output: list[dict[str, Any]] = []
        for idx in item_order:
            item = items_by_idx[idx]
            if item["type"] == "message":
                text = "".join(text_accum.get(idx, []))
                item["content"] = [{
                    "type": "output_text", "text": text, "annotations": [],
                }]
            else:
                item["arguments"] = "".join(tool_args_accum.get(idx, []))
            output.append(item)

        body = {
            "id": response_id or _gen_resp_id(),
            "object": "response",
            "created_at": int(time.time()),
            "status": _IES_TO_RESPONSES_STATUS.get(finish, "completed"),
            "model": model,
            "output": output,
            "usage": {
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "total_tokens": usage.total_tokens,
            },
            "metadata": {},
        }
        return json.dumps(body, ensure_ascii=False).encode()

    # ============ Error envelope ============

    def error_envelope(self, err: GatewayError) -> bytes:
        return json.dumps({
            "error": {
                "type": _GATEWAY_TO_RESPONSES_ERR_TYPE.get(err.error_code, "server_error"),
                "code": err.error_code,
                "message": err.message,
                "param": None,
            },
        }).encode()


# ────────────── helpers ──────────────


def _sse(name: str, payload: dict[str, Any]) -> bytes:
    return f"event: {name}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n".encode()


def _gen_resp_id() -> str:
    return f"resp_{uuid.uuid4().hex[:24]}"


def _gen_item_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:24]}"


_CONSUMED_KEYS = frozenset({
    "model", "input", "instructions", "max_output_tokens", "max_tokens",
    "stream", "temperature", "top_p", "tools", "tool_choice",
    # Things we don't pass through (yet)
    "metadata", "previous_response_id", "store", "reasoning",
    "parallel_tool_calls", "response_format", "user", "service_tier",
})
