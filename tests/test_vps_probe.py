"""Unit tests for gateway.vps_probe."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest


@pytest.fixture
def probe_module(tmp_path, monkeypatch):
    """Reload the module against a tmp auto_deploy.json so each test is isolated."""
    cfg = tmp_path / "auto_deploy.json"
    cfg.write_text(json.dumps({
        "accounts": {
            "alpha": {"enabled": True, "ssh_port": 8022, "api_port": 8800},
            "beta":  {"enabled": True, "ssh_port": 8032, "api_port": 8801},
            "gamma": {"enabled": False, "ssh_port": 8042, "api_port": 8802},
        }
    }), encoding="utf-8")
    import importlib
    import sys
    sys.modules.pop("gateway.vps_probe", None)
    import gateway.vps_probe as v
    importlib.reload(v)
    monkeypatch.setattr(v, "DATA_PATH", cfg)
    v._results.clear()
    v._extra_targets.clear()
    yield v
    v._results.clear()
    v._extra_targets.clear()


def _run(coro):
    return asyncio.run(coro)


def test_build_targets_includes_jump_and_enabled_accounts(probe_module):
    v = probe_module
    targets = v._build_targets()
    ids = {t.target_id for t in targets}
    assert "jump-ssh" in ids
    assert "alpha-ssh" in ids and "alpha-api" in ids
    assert "beta-ssh" in ids and "beta-api" in ids
    # Disabled account is filtered
    assert not any(t.target_id.startswith("gamma") for t in targets)


def test_build_targets_when_config_missing(probe_module, tmp_path, monkeypatch):
    v = probe_module
    monkeypatch.setattr(v, "DATA_PATH", tmp_path / "missing.json")
    targets = v._build_targets()
    assert len(targets) == 1 and targets[0].target_id == "jump-ssh"


def test_add_target_appends_extra(probe_module):
    v = probe_module
    v.add_target(v.Target(target_id="custom", name="自定义",
                          host="1.2.3.4", port=80, kind="tcp"))
    ids = {t.target_id for t in v._build_targets()}
    assert "custom" in ids


def test_probe_once_records_results(probe_module, monkeypatch):
    v = probe_module

    async def fake_probe(target, timeout_s):
        return (target.port % 2 == 0), 12.5, "" if target.port % 2 == 0 else "boom"

    monkeypatch.setattr(v, "_probe_tcp", fake_probe)
    results = _run(v.probe_once(timeout_s=1))
    assert len(results) >= 5
    by_id = {r.target_id: r for r in results}
    # jump SSH (port 22) is even → up
    assert by_id["jump-ssh"].state == "up"
    assert by_id["jump-ssh"].latency_ms == 12.5
    # api ports 8800 even → up; ssh ports 8022 even → up
    assert by_id["alpha-api"].state == "up"
    assert by_id["alpha-ssh"].state == "up"


def test_probe_once_failure_increments_consecutive(probe_module, monkeypatch):
    v = probe_module

    async def always_fail(target, timeout_s):
        return False, 5000.0, "timeout"

    monkeypatch.setattr(v, "_probe_tcp", always_fail)
    _run(v.probe_once(timeout_s=1))
    _run(v.probe_once(timeout_s=1))
    snap = v.get_status()
    jump = next(t for t in snap["targets"] if t["target_id"] == "jump-ssh")
    assert jump["state"] == "down"
    assert jump["consecutive_failures"] == 2
    assert jump["last_error"] == "timeout"


def test_probe_once_drops_stale_targets(probe_module, monkeypatch, tmp_path):
    v = probe_module

    async def ok(target, timeout_s):
        return True, 5.0, ""

    monkeypatch.setattr(v, "_probe_tcp", ok)
    _run(v.probe_once())
    assert any(t.startswith("alpha") for t in v._results)

    # Disable alpha, re-run — its results should be removed.
    cfg = v.DATA_PATH
    cfg.write_text(json.dumps({
        "accounts": {
            "alpha": {"enabled": False, "ssh_port": 8022, "api_port": 8800},
            "beta":  {"enabled": True, "ssh_port": 8032, "api_port": 8801},
        }
    }), encoding="utf-8")
    _run(v.probe_once())
    assert not any(t.startswith("alpha") for t in v._results)
    assert any(t.startswith("beta") for t in v._results)


def test_get_status_summary_counts_states(probe_module, monkeypatch):
    v = probe_module

    async def mixed(target, timeout_s):
        # alpha-* down, everything else up
        if target.target_id.startswith("alpha"):
            return False, 100.0, "refused"
        return True, 10.0, ""

    monkeypatch.setattr(v, "_probe_tcp", mixed)
    _run(v.probe_once())
    snap = v.get_status()
    # 1 jump + 2 alpha (down) + 2 beta (up) = 5
    assert snap["summary"]["total"] == 5
    assert snap["summary"]["up"] == 3   # jump + beta-ssh + beta-api
    assert snap["summary"]["down"] == 2  # alpha-ssh + alpha-api


def test_history_capped(probe_module, monkeypatch):
    v = probe_module
    monkeypatch.setattr(v, "HISTORY_LIMIT", 3)

    async def ok(target, timeout_s):
        return True, 1.0, ""

    monkeypatch.setattr(v, "_probe_tcp", ok)
    for _ in range(5):
        _run(v.probe_once())
    snap = v.get_status()
    for t in snap["targets"]:
        assert len(t["history"]) <= 3


def test_probe_tcp_returns_error_on_refused(probe_module):
    """Probing a definitely-closed port returns down without raising."""
    v = probe_module

    async def go():
        # Port 1 on localhost is reliably closed/forbidden.
        ok, lat, err = await v._probe_tcp(
            v.Target("x", "x", host="127.0.0.1", port=1, kind="tcp"),
            timeout_s=1,
        )
        return ok, err

    ok, err = _run(go())
    assert ok is False
    assert err  # some error string


def test_probe_tcp_succeeds_on_open_socket(probe_module):
    """Probing a port we just opened ourselves succeeds."""
    v = probe_module

    async def run():
        async def handler(reader, writer):
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(handler, host="127.0.0.1", port=0)
        port = server.sockets[0].getsockname()[1]
        try:
            ok, lat, err = await v._probe_tcp(
                v.Target("x", "x", host="127.0.0.1", port=port, kind="tcp"),
                timeout_s=2,
            )
            return ok, err, lat
        finally:
            server.close()
            await server.wait_closed()

    ok, err, lat = _run(run())
    assert ok is True
    assert err == ""
    assert lat >= 0
