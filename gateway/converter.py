"""
Format conversion: Anthropic Messages API and OpenAI Responses API → OpenAI Chat Completions.

Handles:
  - POST /v1/messages (Anthropic) → /v1/chat/completions (OpenAI)
  - POST /v1/responses (OpenAI Responses) → /v1/chat/completions (OpenAI)
  - POST /v1/chat/completions (OpenAI) → passthrough

Returns (converted_body_dict, source_format).
"""
import json
from typing import Any


def detect_format(body: dict) -> str:
    """Detect API format from request body."""
    if "model" not in body:
        return "unknown"
    # Anthropic: has messages + max_tokens but uses max_tokens (not max_output_tokens)
    # and may have system as top-level key, or use stop_sequences
    if "messages" in body and ("max_tokens_to_sample" in body or "stop_sequences" in body or
                                (isinstance(body.get("system"), (str, list)) and "max_tokens" in body)):
        return "anthropic"
    if "input" in body:
        return "responses"  # OpenAI Responses API
    if "messages" in body:
        return "openai"  # Already OpenAI format
    return "unknown"


def convert_anthropic_to_openai(body: dict) -> dict:
    """
    Convert Anthropic Messages API body to OpenAI Chat Completions format.

    Anthropic format:
    {
        "model": "claude-3-opus-20240229",
        "max_tokens": 1024,
        "system": "You are helpful",
        "messages": [{"role": "user", "content": "Hello"}],
        "stream": true,
        "temperature": 0.7,
        "top_p": 0.9,
        "stop_sequences": ["END"],
        "metadata": {"user_id": "abc"}
    }

    OpenAI format:
    {
        "model": "mimo-v2.5-pro",
        "max_tokens": 1024,
        "messages": [
            {"role": "system", "content": "You are helpful"},
            {"role": "user", "content": "Hello"}
        ],
        "stream": true,
        "temperature": 0.7,
        "top_p": 0.9,
        "stop": ["END"]
    }
    """
    out: dict[str, Any] = {}

    # Model
    out["model"] = body.get("model", "mimo-v2.5-pro")

    # Max tokens
    if "max_tokens" in body:
        out["max_tokens"] = body["max_tokens"]
    if "max_tokens_to_sample" in body:
        out["max_tokens"] = body["max_tokens_to_sample"]

    # Messages: combine system + messages
    messages = []
    system = body.get("system")
    if system:
        if isinstance(system, str):
            messages.append({"role": "system", "content": system})
        elif isinstance(system, list):
            # Anthropic allows system as list of content blocks
            text_parts = []
            for block in system:
                if isinstance(block, dict) and block.get("type") == "text":
                    text_parts.append(block.get("text", ""))
                elif isinstance(block, str):
                    text_parts.append(block)
            if text_parts:
                messages.append({"role": "system", "content": "\n".join(text_parts)})

    for msg in body.get("messages", []):
        role = msg.get("role", "user")
        content = msg.get("content", "")

        # Handle Anthropic content blocks
        if isinstance(content, list):
            text_parts = []
            for block in content:
                if isinstance(block, dict):
                    if block.get("type") == "text":
                        text_parts.append(block.get("text", ""))
                    elif block.get("type") == "image":
                        # Convert to OpenAI image_url format
                        source = block.get("source", {})
                        if source.get("type") == "base64":
                            media = source.get("media_type", "image/png")
                            data = source.get("data", "")
                            text_parts.append(f"[Image: {media}]")
                    elif block.get("type") == "tool_use":
                        text_parts.append(f"[Tool: {block.get('name', 'unknown')}]")
                    elif block.get("type") == "tool_result":
                        tool_content = block.get("content", "")
                        if isinstance(tool_content, list):
                            for tc in tool_content:
                                if isinstance(tc, dict) and tc.get("type") == "text":
                                    text_parts.append(tc.get("text", ""))
                        elif isinstance(tool_content, str):
                            text_parts.append(tool_content)
                elif isinstance(block, str):
                    text_parts.append(block)
            content = "\n".join(text_parts) if text_parts else ""

        if content:
            # Map assistant role
            if role == "assistant":
                role = "assistant"
            elif role == "user":
                role = "user"
            elif role == "system":
                role = "system"
            messages.append({"role": role, "content": content})

    out["messages"] = messages

    # Stream
    if "stream" in body:
        out["stream"] = body["stream"]

    # Sampling params
    if "temperature" in body:
        out["temperature"] = body["temperature"]
    if "top_p" in body:
        out["top_p"] = body["top_p"]
    if "stop_sequences" in body:
        out["stop"] = body["stop_sequences"]

    # Tool definitions (Anthropic → OpenAI function format)
    if "tools" in body:
        openai_tools = []
        for tool in body["tools"]:
            openai_tools.append({
                "type": "function",
                "function": {
                    "name": tool.get("name", ""),
                    "description": tool.get("description", ""),
                    "parameters": tool.get("input_schema", {}),
                }
            })
        out["tools"] = openai_tools

    return out


def convert_responses_to_openai(body: dict) -> dict:
    """
    Convert OpenAI Responses API body to Chat Completions format.

    Responses format:
    {
        "model": "gpt-4o",
        "input": [
            {"role": "user", "content": "Hello"}
        ],
        "stream": true,
        "temperature": 0.7,
        "max_output_tokens": 1024,
        "instructions": "You are helpful"
    }
    """
    out: dict[str, Any] = {}
    out["model"] = body.get("model", "mimo-v2.5-pro")

    # Instructions → system message
    messages = []
    instructions = body.get("instructions")
    if instructions:
        messages.append({"role": "system", "content": instructions})

    # Input → messages
    for item in body.get("input", []):
        if isinstance(item, dict):
            role = item.get("role", "user")
            content = item.get("content", "")
            if isinstance(content, list):
                text_parts = []
                for block in content:
                    if isinstance(block, dict):
                        if block.get("type") == "input_text":
                            text_parts.append(block.get("text", ""))
                        elif block.get("type") == "output_text":
                            text_parts.append(block.get("text", ""))
                        elif block.get("type") == "message":
                            text_parts.append(block.get("text", ""))
                    elif isinstance(block, str):
                        text_parts.append(block)
                content = "\n".join(text_parts) if text_parts else ""
            if content:
                messages.append({"role": role, "content": content})
        elif isinstance(item, str):
            messages.append({"role": "user", "content": item})

    out["messages"] = messages

    if "stream" in body:
        out["stream"] = body["stream"]
    if "temperature" in body:
        out["temperature"] = body["temperature"]
    if "top_p" in body:
        out["top_p"] = body["top_p"]
    if "max_output_tokens" in body:
        out["max_tokens"] = body["max_output_tokens"]
    if "max_tokens" in body:
        out["max_tokens"] = body["max_tokens"]

    return out


def convert_request(body: dict) -> tuple[dict, str]:
    """
    Detect format and convert to OpenAI Chat Completions.
    Returns (converted_body, source_format).
    """
    fmt = detect_format(body)
    if fmt == "anthropic":
        return convert_anthropic_to_openai(body), "anthropic"
    elif fmt == "responses":
        return convert_responses_to_openai(body), "responses"
    return body, "openai"


def convert_anthropic_response_to_openai(resp_body: bytes, stream: bool = False) -> bytes:
    """
    Convert an Anthropic-format SSE response back to OpenAI format.
    For streaming, we just passthrough since the proxy forwards raw bytes.
    This is only needed for non-streaming responses.
    """
    if stream:
        return resp_body
    try:
        data = json.loads(resp_body)
    except (json.JSONDecodeError, ValueError):
        return resp_body

    # If it's already OpenAI format, passthrough
    if "choices" in data:
        return resp_body

    # Convert Anthropic response to OpenAI
    if "content" in data and "role" in data:
        text = ""
        for block in data.get("content", []):
            if isinstance(block, dict) and block.get("type") == "text":
                text += block.get("text", "")
        openai_resp = {
            "id": data.get("id", "chatcmpl-unknown"),
            "object": "chat.completion",
            "created": int(__import__("time").time()),
            "model": data.get("model", "unknown"),
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": data.get("stop_reason", "stop"),
            }],
            "usage": data.get("usage", {}),
        }
        return json.dumps(openai_resp).encode()

    return resp_body
