#!/usr/bin/env python3
"""Claw-side WebSocket bridge for gateway ``ws://`` backends.

Connects *out* to the public gateway's ``/ws`` endpoint, receives HTTP-like
requests, forwards them to the local OpenClaw/MiMo API, and streams responses
back over WebSocket.  Zero inbound ports required.

Enhanced with: connection pool, logging, timeouts, graceful shutdown, backoff reconnect.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from urllib.parse import urlsplit

import httpx
import websockets

# ────────────── Logging ──────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("ws-bridge")

# ────────────── Config ──────────────

# The deploy substitutes the per-account WS URL into the quoted placeholder
# below (claw/auto_deploy.py:_render_bridge_code). A manual run can instead
# export MIMO_WS_TUNNEL_URL. DO NOT hardcode a real URL here.
_WS_URL_PLACEHOLDER = "__WS_URL__"
CONNECT_DELAY_BASE = float(os.environ.get("MIMO_WS_BRIDGE_RECONNECT_S", "3"))
CONNECT_DELAY_MAX = 60.0
POOL_SIZE = int(os.environ.get("MIMO_WS_BRIDGE_POOL", "50"))
REQUEST_TIMEOUT = float(os.environ.get("MIMO_WS_BRIDGE_TIMEOUT", "300"))
STREAM_MAX_SECONDS = float(os.environ.get("MIMO_WS_BRIDGE_STREAM_TIMEOUT", "600"))


def _resolve_ws_url() -> str:
    env_url = os.environ.get("MIMO_WS_TUNNEL_URL", "").strip()
    if env_url:
        return env_url
    if _WS_URL_PLACEHOLDER.startswith("__") and _WS_URL_PLACEHOLDER.endswith("__"):
        log.error("WS URL not configured — set MIMO_WS_TUNNEL_URL or let deploy substitute __WS_URL__")
        sys.exit(1)
    return _WS_URL_PLACEHOLDER


def _resolve_mimo_config() -> tuple[str, str]:
    key = ""
    ep = ""
    try:
        import subprocess
        gw_pid = subprocess.check_output(
            ["pgrep", "-f", "openclaw-gateway"], text=True
        ).strip().split("\n")[0]
        if gw_pid:
            with open(f"/proc/{gw_pid}/environ", "rb") as f:
                env = dict(
                    kv.split(b"=", 1)
                    for kv in f.read().split(b"\x00")
                    if b"=" in kv
                )
            key = env.get(b"MIMO_API_KEY", b"").decode()
            ep = env.get(b"MIMO_API_ENDPOINT", b"").decode()
            if key or ep:
                log.info("Config source: /proc/%s/environ (Gateway)", gw_pid)
    except Exception:
        pass

    key = key or os.environ.get("MIMO_API_KEY", "")
    ep = ep or os.environ.get("MIMO_API_ENDPOINT", "")
    if not ep:
        ep = "https://api-oc.xiaomimimo.com/v1/chat/completions"
    return key, ep


# ────────────── Init ──────────────

WS_URL = _resolve_ws_url()
API_KEY, API_ENDPOINT = _resolve_mimo_config()
API_BASE = (
    API_ENDPOINT.split("/v1/")[0].rstrip("/")
    if "/v1/" in API_ENDPOINT
    else API_ENDPOINT.rstrip("/")
)

log.info("Backend: %s", API_BASE)
log.info("WS target: %s", WS_URL.split("?")[0])  # hide token in log
log.info("Pool: %d  Timeout: %.0fs  Stream timeout: %.0fs", POOL_SIZE, REQUEST_TIMEOUT, STREAM_MAX_SECONDS)

# Stats
_start_time = time.time()
_total_reqs = 0
_total_errors = 0
# In-flight request tasks, tracked so a shutdown can drain them (and so the
# tasks aren't GC'd mid-flight while only the event loop holds a reference).
_inflight: set[asyncio.Task] = set()


def _target_url(path: str) -> str:
    parsed = urlsplit(path or "/v1/chat/completions")
    target_path = parsed.path or "/v1/chat/completions"
    if target_path == "/v1/messages":
        target_path = "/anthropic/v1/messages"
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{API_BASE}{target_path}{query}"


# ────────────── Connection Pool ──────────────

_client: httpx.AsyncClient | None = None


async def _init_client() -> httpx.AsyncClient:
    global _client
    transport = httpx.AsyncHTTPTransport(
        limits=httpx.Limits(
            max_connections=POOL_SIZE,
            max_keepalive_connections=POOL_SIZE,
            keepalive_expiry=30,
        ),
    )
    _client = httpx.AsyncClient(
        transport=transport,
        timeout=httpx.Timeout(connect=10, read=REQUEST_TIMEOUT, write=30, pool=5),
        trust_env=False,
    )
    log.info("Connection pool initialized (size=%d)", POOL_SIZE)
    return _client


async def _close_client():
    global _client
    if _client:
        await _client.aclose()
        _client = None


# ────────────── WS send helper ──────────────

async def _safe_send(ws, lock: asyncio.Lock, data: dict) -> None:
    async with lock:
        await ws.send(json.dumps(data, ensure_ascii=False))


# ────────────── Request Handler ──────────────

async def _handle_request(ws, req: dict, lock: asyncio.Lock) -> None:
    global _total_reqs, _total_errors
    _total_reqs += 1
    req_id = req.get("req_id")
    t0 = time.monotonic()

    headers = {"Content-Type": "application/json", "Accept": "*/*"}
    if API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"
    for key, value in (req.get("headers") or {}).items():
        lk = str(key).lower()
        if lk in {"anthropic-version", "anthropic-beta", "x-request-id"} and value:
            headers[str(key)] = str(value)

    target = _target_url(req.get("path") or "")
    method = req.get("method") or "POST"

    try:
        async with _client.stream(
            method, target,
            headers=headers,
            content=req.get("body") or "",
        ) as resp:
            await _safe_send(ws, lock, {
                "req_id": req_id,
                "type": "start",
                "status": resp.status_code,
                "headers": dict(resp.headers),
            })

            total_bytes = 0
            deadline = time.monotonic() + STREAM_MAX_SECONDS
            async for chunk in resp.aiter_text():
                if time.monotonic() > deadline:
                    log.warning("[%s] Stream timeout after %.0fs", req_id, STREAM_MAX_SECONDS)
                    await _safe_send(ws, lock, {
                        "req_id": req_id,
                        "type": "error",
                        "body": "Stream timeout",
                    })
                    break
                if chunk:
                    total_bytes += len(chunk)
                    await _safe_send(ws, lock, {
                        "req_id": req_id,
                        "type": "chunk",
                        "body": chunk,
                    })
            else:
                await _safe_send(ws, lock, {"req_id": req_id, "type": "finish"})

        latency_ms = (time.monotonic() - t0) * 1000
        log.info("[%s] %s %s → %d  %dB (%.0fms)",
                 req_id, method, req.get("path", "/"), resp.status_code, total_bytes, latency_ms)

    except Exception as exc:
        _total_errors += 1
        latency_ms = (time.monotonic() - t0) * 1000
        log.error("[%s] %s %s → %s: %s (%.0fms)",
                  req_id, method, req.get("path", "/"), type(exc).__name__, exc, latency_ms)
        await _safe_send(ws, lock, {
            "req_id": req_id,
            "type": "error",
            "body": f"{type(exc).__name__}: {exc}",
        })


# ────────────── Main Loop ──────────────

async def main() -> None:
    await _init_client()

    retry_count = 0
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                max_size=10**8,
                ping_interval=30,
                ping_timeout=10,
            ) as ws:
                retry_count = 0
                log.info("WebSocket connected to %s", WS_URL.split("?")[0])
                lock = asyncio.Lock()
                async for raw in ws:
                    try:
                        req = json.loads(raw)
                    except json.JSONDecodeError as e:
                        log.warning("Bad JSON from WS: %s", e)
                        continue
                    if not isinstance(req, dict):
                        log.warning("Ignoring non-object WS message: %.80s", raw)
                        continue
                    task = asyncio.create_task(_handle_request(ws, req, lock))
                    _inflight.add(task)
                    task.add_done_callback(_inflight.discard)

        except websockets.ConnectionClosed as e:
            retry_count += 1
            delay = min(CONNECT_DELAY_BASE * (2 ** (retry_count - 1)), CONNECT_DELAY_MAX)
            log.warning("WS connection closed (code=%s). Reconnecting in %.1fs...", e.code, delay)
            await asyncio.sleep(delay)

        except Exception as e:
            retry_count += 1
            delay = min(CONNECT_DELAY_BASE * (2 ** (retry_count - 1)), CONNECT_DELAY_MAX)
            log.error("WS error: %s. Reconnecting in %.1fs...", e, delay)
            await asyncio.sleep(delay)


# ────────────── Graceful Shutdown ──────────────

_shutdown_event = asyncio.Event()


def _signal_handler(sig, frame):
    log.info("Received signal %s, shutting down...", signal.Signals(sig).name)
    _shutdown_event.set()


async def _run():
    task = asyncio.create_task(main())
    await _shutdown_event.wait()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    if _inflight:
        log.info("Draining %d in-flight request(s)...", len(_inflight))
        await asyncio.gather(*_inflight, return_exceptions=True)
    await _close_client()
    uptime = time.time() - _start_time
    log.info("Shutdown complete. Uptime=%.0fs  Requests=%d  Errors=%d", uptime, _total_reqs, _total_errors)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    log.info("ws-bridge starting (pid=%d)", os.getpid())
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass
