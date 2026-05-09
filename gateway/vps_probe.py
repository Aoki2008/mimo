"""
VPS probe — periodic TCP connect to every endpoint we care about.

Targets are derived dynamically from ``data/auto_deploy.json`` (each
enabled account contributes its ``ssh_port`` and ``api_port`` on the
jump server) plus a fixed entry for the jump host's own SSH (port 22).
Anything else can be added via ``add_target``.

The probe is intentionally minimal: open a TCP connection with a short
timeout, measure the time to ``EHLO``-equivalent (i.e. socket open),
record latency. No authentication, no payload. That's enough to tell
"tunnel up vs. down" — which is the question the panel actually asks.

Results live in a process-local dict. ``get_status()`` snapshots the
current state for the API; ``start_probe()`` schedules the loop on the
running asyncio loop. Both are idempotent.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

JUMP_HOST = "149.88.90.137"
DEFAULT_INTERVAL_S = 60
DEFAULT_TIMEOUT_S = 5
HISTORY_LIMIT = 60          # ~1h of samples at 60s interval
DATA_PATH = Path(__file__).parent.parent / "data" / "auto_deploy.json"


ProbeKind = Literal["tcp", "http"]
ProbeState = Literal["up", "down", "unknown"]


@dataclass
class Target:
    target_id: str
    name: str
    host: str
    port: int
    kind: ProbeKind = "tcp"
    label: str = ""

    def key(self) -> str:
        return f"{self.host}:{self.port}/{self.kind}"


@dataclass
class ProbeResult:
    target_id: str
    name: str
    host: str
    port: int
    kind: ProbeKind
    state: ProbeState = "unknown"
    latency_ms: float = 0.0
    last_ok_ts: float = 0.0
    last_check_ts: float = 0.0
    last_error: str = ""
    consecutive_failures: int = 0
    history: list[dict] = field(default_factory=list)
    label: str = ""

    def to_dict(self) -> dict:
        return {
            "target_id": self.target_id,
            "name": self.name,
            "host": self.host,
            "port": self.port,
            "kind": self.kind,
            "state": self.state,
            "latency_ms": round(self.latency_ms, 1),
            "last_ok_ts": self.last_ok_ts,
            "last_check_ts": self.last_check_ts,
            "last_error": self.last_error,
            "consecutive_failures": self.consecutive_failures,
            "history": list(self.history),
            "label": self.label,
        }


_results: dict[str, ProbeResult] = {}
_extra_targets: list[Target] = []
_running = False
_task: asyncio.Task | None = None
_lock = asyncio.Lock()


def _build_targets() -> list[Target]:
    """Re-read auto_deploy.json each cycle so newly-enabled accounts show up."""
    targets: list[Target] = [
        Target(
            target_id="jump-ssh",
            name="跳板机 SSH",
            host=JUMP_HOST,
            port=22,
            kind="tcp",
            label="jump",
        ),
    ]
    try:
        cfg = json.loads(DATA_PATH.read_text(encoding="utf-8"))
        for acct, info in (cfg.get("accounts") or {}).items():
            if not info.get("enabled"):
                continue
            ssh_port = info.get("ssh_port")
            api_port = info.get("api_port")
            if ssh_port:
                targets.append(Target(
                    target_id=f"{acct}-ssh",
                    name=f"{acct} 隧道 SSH",
                    host=JUMP_HOST, port=int(ssh_port),
                    kind="tcp", label=acct,
                ))
            if api_port:
                targets.append(Target(
                    target_id=f"{acct}-api",
                    name=f"{acct} API 隧道",
                    host=JUMP_HOST, port=int(api_port),
                    kind="tcp", label=acct,
                ))
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[vps_probe] failed to read auto_deploy.json: {e}")
    targets.extend(_extra_targets)
    return targets


async def _probe_tcp(target: Target, timeout_s: float) -> tuple[bool, float, str]:
    """Open a TCP connection; return (ok, latency_ms, error)."""
    started = time.monotonic()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(target.host, target.port),
            timeout=timeout_s,
        )
    except asyncio.TimeoutError:
        return False, (time.monotonic() - started) * 1000, "timeout"
    except OSError as e:
        return False, (time.monotonic() - started) * 1000, f"{type(e).__name__}: {e}"
    latency_ms = (time.monotonic() - started) * 1000
    try:
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass
    return True, latency_ms, ""


def _update_result(target: Target, ok: bool, latency_ms: float, error: str) -> None:
    res = _results.get(target.target_id)
    if res is None:
        res = ProbeResult(
            target_id=target.target_id, name=target.name,
            host=target.host, port=target.port, kind=target.kind,
            label=target.label,
        )
        _results[target.target_id] = res
    now = time.time()
    res.last_check_ts = now
    res.latency_ms = latency_ms
    if ok:
        res.state = "up"
        res.last_ok_ts = now
        res.last_error = ""
        res.consecutive_failures = 0
    else:
        res.state = "down"
        res.last_error = error
        res.consecutive_failures += 1
    res.history.append({
        "ts": now,
        "ok": ok,
        "latency_ms": round(latency_ms, 1),
    })
    if len(res.history) > HISTORY_LIMIT:
        res.history = res.history[-HISTORY_LIMIT:]
    res.name = target.name
    res.label = target.label


async def probe_once(timeout_s: float = DEFAULT_TIMEOUT_S) -> list[ProbeResult]:
    """Run one full pass; return latest snapshots."""
    targets = _build_targets()
    seen_ids: set[str] = set()
    coros = [_probe_tcp(t, timeout_s) for t in targets]
    outcomes = await asyncio.gather(*coros, return_exceptions=False)
    async with _lock:
        for t, (ok, lat, err) in zip(targets, outcomes):
            seen_ids.add(t.target_id)
            _update_result(t, ok, lat, err)
        # Drop stale targets (e.g. account disabled since last cycle)
        for tid in list(_results.keys()):
            if tid not in seen_ids:
                _results.pop(tid, None)
    return list(_results.values())


async def _probe_loop(interval_s: float, timeout_s: float) -> None:
    while _running:
        try:
            await probe_once(timeout_s=timeout_s)
        except Exception as e:
            print(f"[vps_probe] cycle error: {e}")
        await asyncio.sleep(interval_s)


def start_probe(
    *, interval_s: float = DEFAULT_INTERVAL_S,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> None:
    """Start the background probe loop. Idempotent."""
    global _running, _task
    if _running:
        return
    _running = True
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            _task = asyncio.ensure_future(_probe_loop(interval_s, timeout_s))
        else:
            _task = loop.create_task(_probe_loop(interval_s, timeout_s))
    except RuntimeError:
        _running = False


def stop_probe() -> None:
    global _running, _task
    _running = False
    if _task and not _task.done():
        _task.cancel()
    _task = None


def add_target(target: Target) -> None:
    """Add a custom probe target (won't survive process restart)."""
    if any(t.target_id == target.target_id for t in _extra_targets):
        return
    _extra_targets.append(target)


def get_status() -> dict:
    """Public snapshot of current probe state — safe to expose."""
    items = sorted(_results.values(), key=lambda r: (r.label, r.name))
    summary_total = len(items)
    summary_up = sum(1 for r in items if r.state == "up")
    summary_down = sum(1 for r in items if r.state == "down")
    return {
        "summary": {
            "total": summary_total,
            "up": summary_up,
            "down": summary_down,
            "unknown": summary_total - summary_up - summary_down,
        },
        "targets": [r.to_dict() for r in items],
    }
