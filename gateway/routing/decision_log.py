"""
Decision log — append-only structured record of routing choices.

Used by middleware to attach a record to each request, and by the
handler when it falls back to a different backend mid-request. The log
is in-memory by default (bounded ring buffer); persistence can be
added later by swapping the writer.
"""
from __future__ import annotations

import json
import threading
from collections import deque
from collections.abc import Iterable
from typing import Any, Protocol

from .router import RoutingDecision


class DecisionLogWriter(Protocol):
    def write(self, decision: RoutingDecision) -> None: ...


class InMemoryDecisionLog:
    """Bounded ring buffer; older entries fall off the back."""

    def __init__(self, capacity: int = 1024):
        self._buf: deque[RoutingDecision] = deque(maxlen=capacity)
        self._lock = threading.Lock()

    def write(self, decision: RoutingDecision) -> None:
        with self._lock:
            self._buf.append(decision)

    def recent(self, n: int = 100) -> list[RoutingDecision]:
        with self._lock:
            data = list(self._buf)
        return data[-n:]

    def filter_by_request(self, request_id: str) -> list[RoutingDecision]:
        with self._lock:
            return [d for d in self._buf if d.request_id == request_id]

    def __len__(self) -> int:
        with self._lock:
            return len(self._buf)


class JSONLDecisionLog:
    """Append routing decisions to a JSON-Lines file. Thread-safe."""

    def __init__(self, path: str):
        self._path = path
        self._lock = threading.Lock()

    def write(self, decision: RoutingDecision) -> None:
        line = json.dumps(decision.to_dict(), ensure_ascii=False)
        with self._lock, open(self._path, "a", encoding="utf-8") as f:
            f.write(line + "\n")


class TeeDecisionLog:
    """Fan a single decision out to multiple writers."""

    def __init__(self, writers: Iterable[DecisionLogWriter]):
        self._writers = list(writers)

    def write(self, decision: RoutingDecision) -> None:
        for w in self._writers:
            try:
                w.write(decision)
            except Exception:
                # Best-effort logging — don't break the request.
                pass


def decision_to_log_record(decision: RoutingDecision) -> dict[str, Any]:
    return {"event": "routing_decision", **decision.to_dict()}
