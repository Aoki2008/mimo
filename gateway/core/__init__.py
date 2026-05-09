"""Gateway core: protocol-agnostic abstractions shared by adapters, routing, middleware."""
from .context import RequestContext
from .errors import (
    AdapterError,
    AuthError,
    BackendUnavailableError,
    BadRequestError,
    GatewayError,
    RateLimitError,
    UpstreamError,
    UpstreamTimeoutError,
)
from .pipeline import Handler, Middleware, Pipeline
from .types import (
    ContentBlockEnd,
    ContentBlockStart,
    ContentType,
    FinishReason,
    InternalContent,
    InternalEvent,
    InternalMessage,
    InternalRequest,
    InternalTool,
    MessageEnd,
    MessageStart,
    Role,
    StreamError,
    TextDelta,
    ToolCallDelta,
    Usage,
)

__all__ = [
    # context
    "RequestContext",
    # errors
    "GatewayError",
    "AuthError",
    "RateLimitError",
    "BadRequestError",
    "UpstreamError",
    "BackendUnavailableError",
    "UpstreamTimeoutError",
    "AdapterError",
    # pipeline
    "Pipeline",
    "Middleware",
    "Handler",
    # types — request side
    "InternalRequest",
    "InternalMessage",
    "InternalContent",
    "InternalTool",
    "Usage",
    "Role",
    "ContentType",
    "FinishReason",
    # types — streaming events
    "InternalEvent",
    "MessageStart",
    "ContentBlockStart",
    "TextDelta",
    "ToolCallDelta",
    "ContentBlockEnd",
    "MessageEnd",
    "StreamError",
]
