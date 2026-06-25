"""Tests for the key-gated public status endpoint."""
from __future__ import annotations

import sys
import types
import time
import importlib

from fastapi.testclient import TestClient


def test_public_status_uses_48_hour_public_series(tmp_path, monkeypatch):
    import gateway.logging_setup

    monkeypatch.setattr(
        gateway.logging_setup,
        "setup_logging",
        lambda base_dir: tmp_path / "logs",
    )
    sys.modules.pop("app", None)
    panel_app = importlib.import_module("app")

    calls: list[int] = []

    fake_metrics = types.ModuleType("gateway.metrics")
    fake_metrics.get_public_totals = lambda: {
        "total_requests": 0,
        "successful_requests": 0,
        "success_rate": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
        "since_ts": 0,
        "since": "",
        "latency": {"p50": 0, "p95": 0, "p99": 0, "avg": 0},
        "ttft": {"p50": 0, "p95": 0, "p99": 0, "avg": 0},
        "status_codes": {},
        "models": [],
        "routes": [],
    }

    def get_public_hourly(*, hours: int = 24):
        calls.append(hours)
        # Minimal series so compute_uptime has something to chew on.
        return [
            {"ts": i, "requests": 1, "success": 1, "fail": 0,
             "success_rate": 100, "status": "operational"}
            for i in range(hours)
        ]

    fake_metrics.get_public_hourly = get_public_hourly

    def get_public_window(*, hours: int = 24):
        return {"hours": hours, "requests": 0, "successful": 0, "errors": 0,
                "success_rate": 0, "prompt_tokens": 0, "completion_tokens": 0,
                "total_tokens": 0, "streaming_requests": 0,
                "non_streaming_requests": 0,
                "latency": {"p50": 0, "p95": 0, "p99": 0, "avg": 0},
                "ttft": {"p50": 0, "p95": 0, "p99": 0, "avg": 0}}

    fake_metrics.get_public_window = get_public_window
    fake_metrics.get_public_daily = lambda *, days=30: []

    def compute_uptime(hourly, hours):
        return 100.0 if hourly else None

    fake_metrics.compute_uptime = compute_uptime
    monkeypatch.setitem(sys.modules, "gateway.metrics", fake_metrics)

    fake_runtime = types.ModuleType("gateway.runtime")
    fake_runtime.get_all_backends = lambda: [
        {"enabled": True, "healthy": True, "lifecycle": "active"}
    ]
    monkeypatch.setitem(sys.modules, "gateway.runtime", fake_runtime)
    monkeypatch.setattr(panel_app._secrets, "status_api_token", "status-test")

    client = TestClient(panel_app.app)
    response = client.get(
        "/api/public/status",
        headers={"X-Status-Key": "status-test"},
    )

    assert response.status_code == 200
    assert calls == [48]
    body = response.json()
    assert len(body["hourly"]) == 48
    assert body["status"] == "operational"
    assert body["backends_online"] == 1
    # Enhanced public fields.
    assert body["uptime"] == {"24h": 100.0, "48h": 100.0}
    assert body["window_24h"]["hours"] == 24
    assert body["daily"] == []
    assert isinstance(body["generated_at"], int) and body["generated_at"] > 0
    assert body["windows"]["hourly"] == "48h"
    assert body["windows"]["daily"] == "30d"


def test_compute_uptime_weights_degraded_half_and_ignores_no_data():
    from gateway.metrics import compute_uptime

    # No traffic at all → None (distinct from a 0% outage).
    assert compute_uptime([{"requests": 0, "status": "no_data"}], 24) is None
    assert compute_uptime([], 24) is None

    series = [
        {"requests": 10, "status": "operational"},   # full credit
        {"requests": 10, "status": "operational"},   # full credit
        {"requests": 10, "status": "degraded"},      # half credit
        {"requests": 10, "status": "major_outage"},  # no credit
        {"requests": 0, "status": "no_data"},        # ignored
    ]
    # (1 + 1 + 0.5 + 0) / 4 tracked buckets = 62.5%
    assert compute_uptime(series, 24) == 62.5
    # Window narrower than the series only looks at the most recent buckets.
    assert compute_uptime(series, 1) is None  # last bucket has no data


def test_get_public_window_and_daily_smoke(tmp_path, monkeypatch):
    db = tmp_path / "m.db"
    sys.modules.pop("gateway.metrics", None)
    import gateway.metrics as metrics
    importlib.reload(metrics)
    metrics._local.conn = None
    metrics.DB_PATH = db
    db.parent.mkdir(parents=True, exist_ok=True)
    metrics._init_db(metrics._get_conn())

    now = time.time()
    for i in range(5):
        metrics.record_request(
            "POST", "/v1/chat/completions",
            backend_id="b1",
            status_code=200,
            latency_ms=100 + i,
            ttft_ms=20 + i,
            source_format="openai_chat",
            is_stream=bool(i % 2),
            prompt_tokens=3,
            completion_tokens=7,
            model="mimo-v2.5-pro",
        )

    window = metrics.get_public_window(hours=24)
    assert window["requests"] == 5
    assert window["successful"] == 5
    assert window["total_tokens"] == 5 * 10
    assert window["latency"]["p95"] > 0

    daily = metrics.get_public_daily(days=7)
    assert len(daily) == 7
    assert daily[-1]["requests"] == 5  # today's bucket
    assert daily[-1]["status"] == "operational"

    if metrics._local.conn is not None:
        metrics._local.conn.close()
        metrics._local.conn = None
    sys.modules.pop("gateway.metrics", None)
