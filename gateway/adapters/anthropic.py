"""
Anthropic Messages API adapter — client side only.

Endpoint: ``/v1/messages``. Streaming uses named SSE events
(``event: <name>\\ndata: <json>\\n\\n``).

This adapter is **client-facing only**: the gateway always talks to the
upstream in OpenAI Chat format (handled by OpenAIChatAdapter as UpstreamCodec).
So this class converts:

  * Anthropic request → InternalRequest      (parse_request)
  * IES events       → Anthropic SSE bytes   (serialize_response_stream)
  * IES events       → Anthropic JSON bytes  (serialize_response)

A few protocol quirks worth noting:
  * Anthropic has no ``tool`` role — tool_result blocks live inside ``user``
    messages. We split those out so InternalMessage shapes stay uniform.
  * Anthropic has a top-level ``system`` parameter; we promote it to a
    leading InternalMessage(role="system").
  * Streaming emits an extra ``message_delta`` event right before
    ``message_stop`` carrying the stop_reason and final output token count.
"""
from __future__ import annotations

import json
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


# ────────────── stop_reason mapping ──────────────

_ANTHROPIC_TO_IES: dict[str, FinishReason] = {
    "end_turn": "stop",
    "stop_sequence": "stop",
    "max_tokens": "length",
    "tool_use": "tool_calls",
    "refusal": "content_filter",
}

_IES_TO_ANTHROPIC: dict[FinishReason, str] = {
    "stop": "end_turn",
    "length": "max_tokens",
    "tool_calls": "tool_use",
    "content_filter": "refusal",
    "error": "end_turn",   # Anthropic has no error stop_reason
}


# ────────────── Anthropic-specific error type names ──────────────

_GATEWAY_TO_ANTHROPIC_ERR_TYPE: dict[str, str] = {
    "invalid_request": "invalid_request_error",
    "authentication_error": "authentication_error",
    "rate_limit_exceeded": "rate_limit_error",
    "upstream_error": "api_error",
    "backend_unavailable": "overloaded_error",
    "gateway_timeout": "api_error",
    "adapter_error": "api_error",
    "gateway_error": "api_error",
}


# ────────────── Adapter ──────────────


class AnthropicAdapter(ProtocolAdapter):
    """Anthropic Messages: ``/v1/messages``."""

    name = "anthropic"

    @classmethod
    def matches_path(cls, path: str) -> bool:
        return path.endswith("/messages")

    # ============ Request side ============

    def parse_request(self, body: dict[str, Any]) -> InternalRequest:
        if not isinstance(body, dict):
            raise BadRequestError("Request body must be a JSON object")
        model = body.get("model")
        if not model:
            raise BadRequestError("Missing 'model'")
        max_tokens = body.get("max_tokens")
        if max_tokens is None:
            raise BadRequestError("Missing 'max_tokens'")
        raw_messages = body.get("messages")
        if not isinstance(raw_messages, list) or not raw_messages:
            raise BadRequestError("'messages' must be a non-empty array")

        messages: list[InternalMessage] = []

        # System prompt → leading system message.
        sys = body.get("system")
        if isinstance(sys, str) and sys:
            messages.append(InternalMessage(
                role="system",
                content=[InternalContent(type="text", text=sys)],
            ))
        elif isinstance(sys, list):
            sys_blocks = [
                InternalContent(type="text", text=b.get("text", ""))
                for b in sys
                if isinstance(b, dict) and b.get("type") == "text"
            ]
            if sys_blocks:
                messages.append(InternalMessage(role="system", content=sys_blocks))

        for m in raw_messages:
            messages.extend(self._parse_message(m))

        tools = None
        if body.get("tools"):
            tools = [self._parse_tool(t) for t in body["tools"]]

        return InternalRequest(
            model=model,
            messages=messages,
            max_tokens=int(max_tokens),
            stream=bool(body.get("stream", False)),
            temperature=body.get("temperature"),
            top_p=body.get("top_p"),
            stop=self._parse_stop(body.get("stop_sequences")),
            tools=tools,
            tool_choice=self._parse_tool_choice(body.get("tool_choice")),
            metadata={k: v for k, v in body.items() if k not in _CONSUMED_KEYS},
        )

    @staticmethod
    def _parse_stop(stop: Any) -> list[str] | None:
        if stop is None:
            return None
        if isinstance(stop, list):
            return [str(s) for s in stop]
        return [str(stop)]

    @staticmethod
    def _parse_tool_choice(tc: Any) -> str | dict[str, Any] | None:
        """Anthropic tool_choice → OpenAI-ish form for downstream."""
        if tc is None:
            return None
        if not isinstance(tc, dict):
            return None
        kind = tc.get("type")
        if kind == "auto":
            return "auto"
        if kind == "any":
            return "required"
        if kind == "tool":
            name = tc.get("name", "")
            return {"type": "function", "function": {"name": name}}
        return None

    @staticmethod
    def _parse_tool(t: Any) -> InternalTool:
        if not isinstance(t, dict) or not t.get("name"):
            raise BadRequestError(f"Invalid tool definition: {t}")
        return InternalTool(
            name=t.get("name", ""),
            description=t.get("description", ""),
            input_schema=t.get("input_schema") or {},
        )

    @classmethod
    def _parse_message(cls, m: dict[str, Any]) -> list[InternalMessage]:
        """One Anthropic message → 1 or 2 InternalMessages.

        A user message containing tool_result blocks is split into a
        ``tool`` message (for each tool_result) plus an optional ``user``
        message holding any non-tool content. Order is preserved relative
        to the original block order.
        """
        role = m.get("role")
        if role not in ("user", "assistant"):
            raise BadRequestError(f"Invalid Anthropic role: {role!r}")
        raw = m.get("content")

        # Plain string shorthand for user/assistant.
        if isinstance(raw, str):
            return [InternalMessage(
                role=role,
                content=[InternalContent(type="text", text=raw)],
            )]

        if not isinstance(raw, list):
            return [InternalMessage(role=role, content=[])]

        if role == "assistant":
            blocks = [b for b in (cls._parse_block(b) for b in raw) if b is not None]
            return [InternalMessage(role="assistant", content=blocks)]

        # role == "user": split tool_result vs the rest, preserving order.
        out: list[InternalMessage] = []
        pending_user: list[InternalContent] = []

        def flush_user() -> None:
            if pending_user:
                out.append(InternalMessage(role="user", content=list(pending_user)))
                pending_user.clear()

        for blk in raw:
            if not isinstance(blk, dict):
                continue
            if blk.get("type") == "tool_result":
                flush_user()
                out.append(InternalMessage(
                    role="tool",
                    content=[cls._parse_tool_result(blk)],
                ))
            else:
                parsed = cls._parse_block(blk)
                if parsed is not None:
                    pending_user.append(parsed)
        flush_user()
        return out or [InternalMessage(role="user", content=[])]

    @staticmethod
    def _parse_tool_result(blk: dict[str, Any]) -> InternalContent:
        content = blk.get("content")
        if isinstance(content, list):
            text_parts = [
                c.get("text", "") for c in content
                if isinstance(c, dict) and c.get("type") == "text"
            ]
            text = "".join(text_parts)
        else:
            text = str(content) if content is not None else ""
        return InternalContent(
            type="tool_result",
            tool_id=blk.get("tool_use_id", ""),
            tool_output=text,
            tool_error=bool(blk.get("is_error", False)),
        )

    @staticmethod
    def _parse_block(blk: Any) -> InternalContent | None:
        if isinstance(blk, str):
            return InternalContent(type="text", text=blk)
        if not isinstance(blk, dict):
            return None
        t = blk.get("type")
        if t == "text":
            return InternalContent(type="text", text=blk.get("text", ""))
        if t == "image":
            src = blk.get("source") or {}
            stype = src.get("type")
            if stype == "base64":
                return InternalContent(
                    type="image",
                    image_data=src.get("data", ""),
                    image_mime=src.get("media_type", "image/png"),
                )
            if stype == "url":
                # Anthropic sometimes accepts URL sources; we don't fetch
                # remote images, so degrade to a text reference.
                return InternalContent(
                    type="text",
                    text=f"[image: {src.get('url', '')}]",
                )
            return None
        if t == "tool_use":
            return InternalContent(
                type="tool_use",
                tool_id=blk.get("id", ""),
                tool_name=blk.get("name", ""),
                tool_input=blk.get("input") or {},
            )
        if t == "tool_result":
            # Caller usually handles this directly; fall through for safety.
            content = blk.get("content")
            if isinstance(content, list):
                text = "".join(
                    c.get("text", "") for c in content
                    if isinstance(c, dict) and c.get("type") == "text"
                )
            else:
                text = str(content) if content is not None else ""
            return InternalContent(
                type="tool_result",
                tool_id=blk.get("tool_use_id", ""),
                tool_output=text,
                tool_error=bool(blk.get("is_error", False)),
            )
        return None

    # ============ Streaming serialization (IES → Anthropic SSE) ============

    def serialize_response_stream(
        self, events: AsyncIterator[InternalEvent]
    ) -> AsyncIterator[bytes]:
        return self._serialize_stream(events)

    async def _serialize_stream(
        self, events: AsyncIterator[InternalEvent]
    ) -> AsyncIterator[bytes]:
        message_id = ""
        model = ""
        usage_in = 0
        # Anthropic's message_start needs an initial output_tokens value;
        # we use 1 to match observed behavior, then emit the real total in
        # message_delta.
        message_started = False
        # Track open content blocks for content_block_stop emission.
        open_blocks: set[int] = set()
        final_finish: FinishReason = "stop"
        final_usage = Usage()

        async for ev in events:
            if isinstance(ev, MessageStart):
                message_id = ev.message_id or _gen_msg_id()
                model = ev.model
                yield _sse_event("message_start", {
                    "type": "message_start",
                    "message": {
                        "id": message_id,
                        "type": "message",
                        "role": "assistant",
                        "content": [],
                        "model": model,
                        "stop_reason": None,
                        "stop_sequence": None,
                        "usage": {"input_tokens": usage_in, "output_tokens": 1},
                    },
                })
                message_started = True

            elif isinstance(ev, ContentBlockStart):
                if not message_started:
                    # Be defensive — synthesize a message_start.
                    message_id = message_id or _gen_msg_id()
                    yield _sse_event("message_start", {
                        "type": "message_start",
                        "message": {
                            "id": message_id, "type": "message", "role": "assistant",
                            "content": [], "model": model,
                            "stop_reason": None, "stop_sequence": None,
                            "usage": {"input_tokens": 0, "output_tokens": 1},
                        },
                    })
                    message_started = True
                if ev.block_type == "text":
                    yield _sse_event("content_block_start", {
                        "type": "content_block_start",
                        "index": ev.index,
                        "content_block": {"type": "text", "text": ""},
                    })
                else:
                    yield _sse_event("content_block_start", {
                        "type": "content_block_start",
                        "index": ev.index,
                        "content_block": {
                            "type": "tool_use",
                            "id": ev.tool_id or "",
                            "name": ev.tool_name or "",
                            "input": {},
                        },
                    })
                open_blocks.add(ev.index)

            elif isinstance(ev, TextDelta):
                if ev.index not in open_blocks:
                    # Emit an implicit start for safety.
                    yield _sse_event("content_block_start", {
                        "type": "content_block_start",
                        "index": ev.index,
                        "content_block": {"type": "text", "text": ""},
                    })
                    open_blocks.add(ev.index)
                yield _sse_event("content_block_delta", {
                    "type": "content_block_delta",
                    "index": ev.index,
                    "delta": {"type": "text_delta", "text": ev.text},
                })

            elif isinstance(ev, ToolCallDelta):
                yield _sse_event("content_block_delta", {
                    "type": "content_block_delta",
                    "index": ev.index,
                    "delta": {
                        "type": "input_json_delta",
                        "partial_json": ev.arguments_delta,
                    },
                })

            elif isinstance(ev, ContentBlockEnd):
                if ev.index in open_blocks:
                    yield _sse_event("content_block_stop", {
                        "type": "content_block_stop",
                        "index": ev.index,
                    })
                    open_blocks.discard(ev.index)

            elif isinstance(ev, MessageEnd):
                final_finish = ev.finish_reason
                final_usage = ev.usage

            elif isinstance(ev, StreamError):
                # Anthropic uses a top-level "error" event, but it's only valid
                # before message_start. After streaming has begun, we surface
                # via a synthetic text delta + early end.
                yield _sse_event("error", {
                    "type": "error",
                    "error": {"type": "api_error", "message": ev.message},
                })
                if not ev.recoverable:
                    return

        # Defensive close of any leftover blocks (shouldn't happen if
        # producer is well-behaved, but the protocol requires every start
        # to be matched by a stop).
        for idx in sorted(open_blocks):
            yield _sse_event("content_block_stop", {
                "type": "content_block_stop", "index": idx,
            })

        # message_delta + message_stop, even if MessageEnd never arrived.
        yield _sse_event("message_delta", {
            "type": "message_delta",
            "delta": {
                "stop_reason": _IES_TO_ANTHROPIC.get(final_finish, "end_turn"),
                "stop_sequence": None,
            },
            "usage": {"output_tokens": final_usage.output_tokens},
        })
        yield _sse_event("message_stop", {"type": "message_stop"})

    # ============ Non-stream serialization (IES → Anthropic JSON) ============

    def serialize_response(self, events: list[InternalEvent]) -> bytes:
        message_id = ""
        model = ""
        # We collect per-index data and emit blocks in index order.
        text_by_idx: dict[int, list[str]] = {}
        tool_by_idx: dict[int, dict[str, Any]] = {}
        block_order: list[tuple[int, str]] = []      # [(index, kind)]
        finish: FinishReason = "stop"
        usage = Usage()

        for ev in events:
            if isinstance(ev, MessageStart):
                message_id = ev.message_id or _gen_msg_id()
                model = ev.model
            elif isinstance(ev, ContentBlockStart):
                if ev.block_type == "text":
                    text_by_idx.setdefault(ev.index, [])
                    block_order.append((ev.index, "text"))
                else:
                    tool_by_idx[ev.index] = {
                        "type": "tool_use",
                        "id": ev.tool_id or "",
                        "name": ev.tool_name or "",
                        "input_buf": "",   # accumulate JSON, parse at end
                    }
                    block_order.append((ev.index, "tool_use"))
            elif isinstance(ev, TextDelta):
                text_by_idx.setdefault(ev.index, []).append(ev.text)
                if not any(o == (ev.index, "text") for o in block_order):
                    block_order.append((ev.index, "text"))
            elif isinstance(ev, ToolCallDelta):
                if ev.index in tool_by_idx:
                    tool_by_idx[ev.index]["input_buf"] += ev.arguments_delta
            elif isinstance(ev, MessageEnd):
                finish = ev.finish_reason
                usage = ev.usage

        content_blocks: list[dict[str, Any]] = []
        seen: set[tuple[int, str]] = set()
        for idx, kind in sorted(block_order, key=lambda p: p[0]):
            if (idx, kind) in seen:
                continue
            seen.add((idx, kind))
            if kind == "text":
                text = "".join(text_by_idx.get(idx, []))
                content_blocks.append({"type": "text", "text": text})
            else:
                tu = tool_by_idx[idx]
                buf = tu.pop("input_buf", "")
                try:
                    inp = json.loads(buf) if buf else {}
                except json.JSONDecodeError:
                    inp = {"_raw": buf}
                tu["input"] = inp
                content_blocks.append({
                    "type": "tool_use",
                    "id": tu["id"],
                    "name": tu["name"],
                    "input": inp,
                })

        body = {
            "id": message_id or _gen_msg_id(),
            "type": "message",
            "role": "assistant",
            "content": content_blocks,
            "model": model,
            "stop_reason": _IES_TO_ANTHROPIC.get(finish, "end_turn"),
            "stop_sequence": None,
            "usage": {
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
            },
        }
        return json.dumps(body, ensure_ascii=False).encode()

    # ============ Error envelope ============

    def error_envelope(self, err: GatewayError) -> bytes:
        return json.dumps({
            "type": "error",
            "error": {
                "type": _GATEWAY_TO_ANTHROPIC_ERR_TYPE.get(err.error_code, "api_error"),
                "message": err.message,
            },
        }).encode()


# ────────────── helpers ──────────────


def _sse_event(name: str, payload: dict[str, Any]) -> bytes:
    return f"event: {name}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n".encode()


def _gen_msg_id() -> str:
    return f"msg_{uuid.uuid4().hex[:24]}"


_CONSUMED_KEYS = frozenset({
    "model", "messages", "max_tokens", "stream", "system",
    "temperature", "top_p", "top_k", "stop_sequences",
    "tools", "tool_choice",
    # Things we don't pass through (yet)
    "metadata",
})
