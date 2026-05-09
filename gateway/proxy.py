"""
Async proxy forwarding using httpx with raw SSE stream passthrough.
"""
import json
import time

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False

from gateway.router import get_router
from gateway.metrics import record_request
from gateway.converter import convert_request

PROXY_TIMEOUT = httpx.Timeout(connect=10, read=300, write=30, pool=10) if HAS_HTTPX else None

# Shared async client (connection pool reuse)
_client: httpx.AsyncClient | None = None


async def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            verify=False,
            timeout=PROXY_TIMEOUT,
            http2=False,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )
    return _client


async def close_client():
    global _client
    if _client and not _client.is_closed:
        await _client.aclose()
        _client = None


async def proxy_request(method: str, path: str, headers: dict, body: bytes,
                        source_format: str = "openai") -> tuple:
    """
    Proxy a request to the best backend.
    Returns (status_code, response_headers, response_body_or_stream).
    """
    if not HAS_HTTPX:
        return 503, {}, b'{"error":{"message":"httpx not installed"}}'

    router = get_router()
    backend = router.get_backend()
    if not backend:
        return 503, {}, b'{"error":{"message":"No healthy backends available"}}'

    # Parse body for format conversion
    req_body = body
    detected_format = source_format
    if body and method in ("POST", "PUT"):
        try:
            parsed = json.loads(body)
            converted, detected_format = convert_request(parsed)
            req_body = json.dumps(converted).encode()
        except (json.JSONDecodeError, ValueError):
            pass

    # Check if streaming
    is_stream = False
    if req_body:
        try:
            is_stream = json.loads(req_body).get("stream", False)
        except (json.JSONDecodeError, ValueError):
            pass

    # Build upstream headers
    upstream_headers = {
        "Content-Type": headers.get("Content-Type", "application/json"),
        "Accept": headers.get("Accept", "*/*"),
        "Connection": "keep-alive",
    }
    upstream_headers["Authorization"] = "Bearer sk-Aoki-MiMo"

    for key in ("X-Request-Id", "anthropic-version"):
        val = headers.get(key)
        if val:
            upstream_headers[key] = val

    url = f"{backend.url}{path}"
    t0 = time.monotonic()

    try:
        client = await _get_client()

        if is_stream:
            # Streaming: use client.stream() which keeps the response open
            req = client.build_request(method, url, headers=upstream_headers, content=req_body)
            resp = await client.send(req, stream=True)
            latency_ms = (time.monotonic() - t0) * 1000

            if resp.status_code >= 400:
                body_bytes = await resp.aread()
                await resp.aclose()
                router.record_latency(backend.id, latency_ms, False)
                record_request(method, path, backend.id, resp.status_code,
                               latency_ms, detected_format, True,
                               body_bytes.decode(errors="replace")[:200])
                return resp.status_code, dict(resp.headers), body_bytes

            router.record_latency(backend.id, latency_ms, True)
            record_request(method, path, backend.id, resp.status_code,
                           latency_ms, detected_format, True)

            # Build a stream iterator that owns the response lifecycle
            resp_headers = dict(resp.headers)
            resp_headers["Access-Control-Allow-Origin"] = "*"

            async def stream_iter():
                try:
                    async for chunk in resp.aiter_bytes(16384):
                        yield chunk
                finally:
                    await resp.aclose()

            return resp.status_code, resp_headers, stream_iter()

        else:
            # Non-streaming: full response
            resp = await client.request(method, url, headers=upstream_headers, content=req_body)
            latency_ms = (time.monotonic() - t0) * 1000

            success = resp.status_code < 500
            router.record_latency(backend.id, latency_ms, success)
            prompt_t, completion_t = _extract_tokens_from_body(resp.content)
            record_request(method, path, backend.id, resp.status_code,
                           latency_ms, detected_format, False,
                           "" if success else resp.text[:200],
                           prompt_tokens=prompt_t, completion_tokens=completion_t)

            resp_headers = dict(resp.headers)
            resp_headers["Access-Control-Allow-Origin"] = "*"
            return resp.status_code, resp_headers, resp.content

    except Exception as e:
        latency_ms = (time.monotonic() - t0) * 1000
        router.record_latency(backend.id, latency_ms, False)
        record_request(method, path, backend.id, 502, latency_ms,
                       detected_format, is_stream, str(e)[:200])
        error_body = json.dumps({"error": {"message": f"Gateway error: {e}", "type": "proxy_error"}}).encode()
        return 502, {"Content-Type": "application/json"}, error_body


def _extract_tokens_from_body(content: bytes) -> tuple[int, int]:
    """Best-effort prompt/completion token extraction from a non-stream JSON body.

    Returns (0, 0) when the body isn't JSON or has no usage block. Supports
    both OpenAI-style (``usage.prompt_tokens``/``completion_tokens``) and
    Anthropic-style (``usage.input_tokens``/``output_tokens``) shapes.
    """
    if not content:
        return 0, 0
    try:
        obj = json.loads(content)
    except Exception:
        return 0, 0
    usage = obj.get("usage") if isinstance(obj, dict) else None
    if not isinstance(usage, dict):
        return 0, 0
    prompt = usage.get("prompt_tokens") or usage.get("input_tokens") or 0
    completion = usage.get("completion_tokens") or usage.get("output_tokens") or 0
    try:
        return int(prompt), int(completion)
    except (TypeError, ValueError):
        return 0, 0
