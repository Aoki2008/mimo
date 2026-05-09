"""
Async health checker for gateway backends.
Pings each backend every 30s, updates circuit breaker state.
"""
import asyncio
import time

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False

CHECK_INTERVAL = 30  # seconds
CHECK_TIMEOUT = 5    # seconds

_running = False
_task: asyncio.Task | None = None


async def _check_backend(client, backend) -> bool:
    """Ping a single backend's health endpoint."""
    try:
        resp = await client.get(f"{backend.url}/", timeout=CHECK_TIMEOUT)
        return resp.status_code == 200
    except Exception:
        return False


async def _health_loop():
    """Periodic health check loop."""
    if not HAS_HTTPX:
        print("[health] httpx not installed, health checks disabled")
        return

    from gateway.router import get_router
    router = get_router()

    async with httpx.AsyncClient(verify=False) as client:
        while _running:
            backends = router.get_all_backends()
            for bd in backends:
                backend_id = bd["id"]
                url = bd["url"]
                try:
                    resp = await client.get(f"{url}/", timeout=CHECK_TIMEOUT)
                    healthy = resp.status_code == 200
                except Exception:
                    healthy = False

                # Update circuit breaker based on health
                with router._lock:
                    b = router._backends.get(backend_id)
                    if b:
                        if healthy:
                            b.circuit.record_success()
                        else:
                            b.circuit.record_failure()

            await asyncio.sleep(CHECK_INTERVAL)


def start_health_checker():
    """Start the background health check task."""
    global _running, _task
    if _running:
        return
    _running = True
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            _task = asyncio.ensure_future(_health_loop())
        else:
            _task = loop.create_task(_health_loop())
    except RuntimeError:
        pass


def stop_health_checker():
    """Stop the health check task."""
    global _running, _task
    _running = False
    if _task and not _task.done():
        _task.cancel()
        _task = None
