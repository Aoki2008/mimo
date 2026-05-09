"""
Latency-based router with circuit breaker and weighted round-robin.

Backends are loaded from data/auto_deploy.json — each account's SSH tunnel port
maps to a backend at http://127.0.0.1:{api_port}.
"""
import json
import time
import threading
from collections import deque
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
CONFIG_FILE = DATA_DIR / "auto_deploy.json"

# ────────────── Sliding Window ──────────────

class SlidingWindow:
    """Track last N values for latency averaging."""
    __slots__ = ("_buf",)

    def __init__(self, maxlen: int = 100):
        self._buf: deque[float] = deque(maxlen=maxlen)

    def add(self, value: float):
        self._buf.append(value)

    @property
    def avg(self) -> float:
        if not self._buf:
            return 0.0
        return sum(self._buf) / len(self._buf)

    @property
    def p95(self) -> float:
        if not self._buf:
            return 0.0
        s = sorted(self._buf)
        idx = int(len(s) * 0.95)
        return s[min(idx, len(s) - 1)]

    @property
    def count(self) -> int:
        return len(self._buf)

    def clear(self):
        self._buf.clear()


# ────────────── Circuit Breaker ──────────────

CLOSED, OPEN, HALF_OPEN = "closed", "open", "half-open"
FAILURE_THRESHOLD = 5
RECOVERY_TIMEOUT = 30  # seconds


class CircuitBreaker:
    __slots__ = ("state", "_failures", "_last_failure", "_half_open_calls")

    def __init__(self):
        self.state = CLOSED
        self._failures = 0
        self._last_failure = 0.0
        self._half_open_calls = 0

    def record_success(self):
        if self.state == HALF_OPEN:
            self._half_open_calls += 1
            if self._half_open_calls >= 2:
                self.state = CLOSED
                self._failures = 0
                self._half_open_calls = 0
        elif self.state == CLOSED:
            self._failures = max(0, self._failures - 1)

    def record_failure(self):
        self._last_failure = time.monotonic()
        if self.state == HALF_OPEN:
            self.state = OPEN
            self._half_open_calls = 0
            return
        self._failures += 1
        if self._failures >= FAILURE_THRESHOLD:
            self.state = OPEN

    def allow_request(self) -> bool:
        if self.state == CLOSED:
            return True
        if self.state == OPEN:
            if time.monotonic() - self._last_failure > RECOVERY_TIMEOUT:
                self.state = HALF_OPEN
                self._half_open_calls = 0
                return True
            return False
        return True  # HALF_OPEN allows requests


# ────────────── Backend ──────────────

class Backend:
    __slots__ = ("id", "url", "weight", "latency", "circuit", "enabled",
                 "total_requests", "account_name", "api_port")

    def __init__(self, backend_id: str, url: str, weight: int = 1,
                 account_name: str = "", api_port: int = 0):
        self.id = backend_id
        self.url = url
        self.weight = weight
        self.latency = SlidingWindow()
        self.circuit = CircuitBreaker()
        self.enabled = True
        self.total_requests = 0
        self.account_name = account_name
        self.api_port = api_port

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "url": self.url,
            "healthy": self.circuit.state != OPEN,
            "weight": self.weight,
            "avg_latency_ms": round(self.latency.avg, 1),
            "p95_latency_ms": round(self.latency.p95, 1),
            "circuit": self.circuit.state,
            "total_requests": self.total_requests,
            "enabled": self.enabled,
            "account": self.account_name,
        }


# ────────────── Router ──────────────

class LatencyRouter:
    """Latency-weighted round-robin router with circuit breaker."""

    def __init__(self):
        self._backends: dict[str, Backend] = {}
        self._lock = threading.Lock()
        self._rr_index = 0
        self._total_requests = 0
        self._start_time = time.time()
        self._load_backends()

    def _load_backends(self):
        """Load backends from auto_deploy.json."""
        if not CONFIG_FILE.exists():
            return
        try:
            with open(CONFIG_FILE) as f:
                cfg = json.load(f)
        except (json.JSONDecodeError, ValueError):
            return

        for acc_name, acc_cfg in cfg.get("accounts", {}).items():
            if not acc_cfg.get("enabled", False):
                continue
            api_port = acc_cfg.get("api_port", acc_cfg.get("port", 8800))
            backend_id = acc_name
            url = f"http://127.0.0.1:{api_port}"
            self._backends[backend_id] = Backend(
                backend_id=backend_id,
                url=url,
                weight=1,
                account_name=acc_name,
                api_port=api_port,
            )

    def add_backend(self, backend_id: str, url: str, weight: int = 1):
        with self._lock:
            self._backends[backend_id] = Backend(backend_id, url, weight)

    def get_backend(self) -> Backend | None:
        """Select best backend: weighted by inverse latency, skip circuit-broken."""
        with self._lock:
            candidates = [
                b for b in self._backends.values()
                if b.enabled and b.circuit.allow_request()
            ]
            if not candidates:
                return None

            # If all have zero latency, use round-robin
            if all(b.latency.avg == 0 for b in candidates):
                b = candidates[self._rr_index % len(candidates)]
                self._rr_index += 1
                return b

            # Weighted inverse-latency selection
            scored = []
            for b in candidates:
                lat = max(b.latency.avg, 1.0)  # avoid division by zero
                score = (b.weight * 1000) / lat
                scored.append((score, b))

            scored.sort(key=lambda x: x[0], reverse=True)
            return scored[0][1]

    def record_latency(self, backend_id: str, latency_ms: float, success: bool):
        with self._lock:
            self._total_requests += 1
            b = self._backends.get(backend_id)
            if b:
                b.total_requests += 1
                b.latency.add(latency_ms)
                if success:
                    b.circuit.record_success()
                else:
                    b.circuit.record_failure()

    def get_status(self) -> dict:
        with self._lock:
            total = self._total_requests
            uptime = int(time.time() - self._start_time)
            qps = round(total / max(uptime, 1), 2)
            latencies = [b.latency.avg for b in self._backends.values() if b.latency.avg > 0]
            avg_lat = round(sum(latencies) / len(latencies), 1) if latencies else 0
            healthy = sum(1 for b in self._backends.values()
                         if b.enabled and b.circuit.state != OPEN)
            return {
                "uptime": uptime,
                "total_requests": total,
                "qps": qps,
                "avg_latency_ms": avg_lat,
                "backends_total": len(self._backends),
                "backends_healthy": healthy,
                "pool_idle": 0,  # filled by proxy if available
                "pool_active": 0,
                "pool_reuse_rate": 0,
            }

    def get_all_backends(self) -> list[dict]:
        with self._lock:
            return [b.to_dict() for b in self._backends.values()]

    def toggle_backend(self, backend_id: str) -> dict:
        with self._lock:
            b = self._backends.get(backend_id)
            if not b:
                return {"success": False, "error": f"Backend '{backend_id}' not found"}
            b.enabled = not b.enabled
            return {"success": True, "message": f"Backend '{backend_id}' {'enabled' if b.enabled else 'disabled'}"}


# ────────────── Singleton ──────────────

_router = LatencyRouter()


def get_router() -> LatencyRouter:
    return _router


def get_router_status() -> dict:
    return _router.get_status()


def get_all_backends() -> list[dict]:
    return _router.get_all_backends()


def toggle_backend(backend_id: str) -> dict:
    return _router.toggle_backend(backend_id)
