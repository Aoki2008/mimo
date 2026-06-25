"""MiMo model capability checks used before routing requests upstream."""
from __future__ import annotations

from typing import Any

from gateway.core import BadRequestError, InternalRequest


# Per MiMo's official image-understanding docs, image input is supported only
# by these multimodal models.
IMAGE_INPUT_MODELS = frozenset({
    "mimo-v2.5",
})


def has_image_input(req: InternalRequest) -> bool:
    """Return True if the normalized request contains any image block."""
    return any(
        content.type == "image"
        for message in req.messages
        for content in message.content
    )


def validate_request_capabilities(req: InternalRequest) -> None:
    """Reject requests that use inputs unsupported by the resolved MiMo model."""
    if has_image_input(req):
        validate_image_input_supported(req.model)


def has_anthropic_image_input(body: dict[str, Any]) -> bool:
    """Return True if a raw Anthropic passthrough body contains image blocks."""
    system = body.get("system")
    if isinstance(system, list) and _anthropic_blocks_have_image(system):
        return True
    messages = body.get("messages")
    if not isinstance(messages, list):
        return False
    for message in messages:
        if not isinstance(message, dict):
            continue
        content = message.get("content")
        if isinstance(content, list) and _anthropic_blocks_have_image(content):
            return True
    return False


def validate_anthropic_body_capabilities(model: str, body: dict[str, Any]) -> None:
    """Reject raw Anthropic requests with inputs unsupported by the model."""
    if has_anthropic_image_input(body):
        validate_image_input_supported(model)


def validate_image_input_supported(model: str) -> None:
    if model not in IMAGE_INPUT_MODELS:
        supported = ", ".join(sorted(IMAGE_INPUT_MODELS))
        raise BadRequestError(
            f"该模型不支持多模态输入：{model}。请使用支持图片输入的模型：{supported}",
            details={
                "model": model,
                "unsupported_input": "image",
                "supported_models": sorted(IMAGE_INPUT_MODELS),
            },
        )


def _anthropic_blocks_have_image(blocks: list[Any]) -> bool:
    return any(
        isinstance(block, dict) and block.get("type") == "image"
        for block in blocks
    )
