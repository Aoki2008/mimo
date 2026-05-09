"""Protocol adapters: client-side encoding/decoding + (where applicable) upstream codec."""
from .anthropic import AnthropicAdapter
from .base import ProtocolAdapter, UpstreamCodec
from .openai_chat import OpenAIChatAdapter, iter_sse_data
from .openai_responses import OpenAIResponsesAdapter

__all__ = [
    "ProtocolAdapter",
    "UpstreamCodec",
    "OpenAIChatAdapter",
    "OpenAIResponsesAdapter",
    "AnthropicAdapter",
    "iter_sse_data",
]
