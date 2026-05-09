"""
Per-request mutable state. Carries identity, audit trail, and result metrics
through the pipeline so any middleware / adapter / router can inspect or record
without threading explicit args.

A RequestContext is created at the entry of /v1/{path} and lives until the
response is fully sent (or aborts). At end-of-life it is serialized into one
log line — the ``decisions`` list is the human-readable trace.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field


@dataclass
class RequestContext:
    request_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16])
    started_at: float = field(default_factory=time.monotonic)

    # Client
    client_ip: str = ""
    user_agent: str = ""
    headers: dict[str, str] = field(default_factory=dict)   # lowercased keys

    # Auth (set by AuthMiddleware)
    api_key_id: str | None = None
    principal: object | None = None

    # Source request
    src_protocol: str = ""        # "anthropic" / "openai_chat" / "openai_responses"
    src_path: str = ""
    src_method: str = ""
    is_stream: bool = False
    model: str = ""

    # Routing (set by Router)
    target_backend_id: str | None = None
    upstream_url: str | None = None

    # Result
    upstream_status: int | None = None
    upstream_latency_ms: float | None = None
    response_chunks: int = 0
    error: str | None = None

    # Audit trail. Append at every notable decision point.
    decisions: list[str] = field(default_factory=list)

    def decide(self, msg: str) -> None:
        self.decisions.append(msg)

    @property
    def total_latency_ms(self) -> float:
        return (time.monotonic() - self.started_at) * 1000

    def to_log(self) -> dict:
        """Render as a structured log record."""
        return {
            "req_id": self.request_id,
            "src": self.src_protocol,
            "path": self.src_path,
            "model": self.model,
            "stream": self.is_stream,
            "key": self.api_key_id,
            "backend": self.target_backend_id,
            "up_status": self.upstream_status,
            "up_lat_ms": self.upstream_latency_ms,
            "total_lat_ms": round(self.total_latency_ms, 1),
            "chunks": self.response_chunks,
            "err": self.error,
            "decisions": list(self.decisions),
        }
