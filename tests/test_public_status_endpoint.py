"""Tests for the key-gated public status endpoint."""
from __future__ import annotations

import sys
import types
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
        return [{"hours": hours}]

    fake_metrics.get_public_hourly = get_public_hourly
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
    assert body["hourly"] == [{"hours": 48}]
    assert body["status"] == "operational"
    assert body["backends_online"] == 1
