from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import pytest

from gateway.routing import Backend, BackendRegistry, Router
from gateway.core import BackendUnavailableError
import gateway.backend_store as backend_store
import gateway.runtime as runtime


@pytest.fixture(autouse=True)
def reset_runtime(monkeypatch, tmp_path):
    path = tmp_path / "backends.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"backends": []}), encoding="utf-8")
    monkeypatch.setattr(backend_store, "DATA_PATH", path)
    for name in (
        "_registry", "_router", "_transport", "_handler",
        "_decision_log", "_probe_task", "_rotation_task",
    ):
        monkeypatch.setattr(runtime, name, None)
    monkeypatch.setattr(runtime, "_adapters", {})
    yield


def _backend(backend_id: str, *, lifecycle: str = "active") -> Backend:
    b = Backend(
        backend_id=backend_id,
        base_url=f"http://{backend_id}.example",
        models=["mimo-v2.5-pro"],
        account_id=backend_id,
        lifecycle=lifecycle,
    )
    b.record_success()
    return b


def test_router_excludes_draining_and_unready_warming_backends():
    active = _backend("active")
    warming_ready = _backend("warming-ready", lifecycle="warming")
    warming_ready.readiness_successes = 1  # passed readiness
    warming_unready = _backend("warming-unready", lifecycle="warming")
    warming_unready.readiness_successes = 0  # no readiness success yet
    draining = _backend("draining", lifecycle="draining")
    router = Router(BackendRegistry([warming_unready, warming_ready, draining, active]))

    chosen, decision = router.choose(request_id="r1", model="mimo-v2.5-pro")

    assert chosen.backend_id in ("active", "warming-ready")
    assert decision.excluded["draining"] == "lifecycle=draining"
    assert decision.excluded["warming-unready"] == "warming, no readiness success yet"


def test_router_raises_when_only_warming_backend_exists():
    b = _backend("warming", lifecycle="warming")
    b.readiness_successes = 0  # no readiness success yet
    router = Router(BackendRegistry([b]))

    with pytest.raises(BackendUnavailableError):
        router.choose(request_id="r1", model="mimo-v2.5-pro")


def test_activate_backend_joins_load_balancing_pool_without_draining_peer(monkeypatch):
    old = _backend("old")
    old.active_since = 100.0
    new = _backend("new", lifecycle="warming")
    reg = BackendRegistry([old, new])
    monkeypatch.setattr(runtime, "_registry", reg)

    result = runtime.activate_backend("new")

    assert result["success"] is True
    assert new.lifecycle == "active"
    assert old.lifecycle == "active"
    assert old.drain_deadline == 0.0


def test_reap_drained_keeps_in_flight_until_deadline(monkeypatch, caplog):
    b = _backend("old", lifecycle="draining")
    b.in_flight = 1
    b.draining_since = 100.0
    b.drain_deadline = 200.0
    reg = BackendRegistry([b])
    monkeypatch.setattr(runtime, "_registry", reg)

    runtime._reap_drained(now=150.0)
    assert reg.get("old") is b

    runtime._reap_drained(now=250.0)
    assert reg.get("old") is None
    assert "Drain deadline reached for backend old" in caplog.text


class FakeTransport:
    def __init__(self):
        self.json_bodies = []
        self.stream_bodies = []

    async def post_json(self, url, body, *, headers=None, timeout_s=60.0):
        self.json_bodies.append(body)
        if body.get("tools"):
            return 200, b'{"choices":[{"message":{"tool_calls":[{"id":"call_1"}]}}]}'
        return 200, b'{"choices":[{"message":{"content":"ok"}}]}'

    async def post_stream(self, url, body, *, headers=None, timeout_s=600.0):
        self.stream_bodies.append(body)

        async def chunks() -> AsyncIterator[bytes]:
            yield b'data: {"choices":[{"delta":{"content":"o"}}]}\n\n'
            yield b'data: [DONE]\n\n'

        return 200, chunks()


def test_readiness_checks_cover_non_stream_stream_and_tool(monkeypatch):
    fake = FakeTransport()
    monkeypatch.setattr(runtime, "_transport", fake)
    backend = _backend("candidate", lifecycle="warming")

    ok, reason, latency_ms = asyncio.run(runtime._run_readiness_checks(backend))

    assert ok is True
    assert reason == "ready"
    assert latency_ms >= 0
    assert len(fake.json_bodies) == 2
    assert len(fake.stream_bodies) == 1
    assert fake.json_bodies[0]["stream"] is False
    assert fake.stream_bodies[0]["stream"] is True
    assert fake.json_bodies[1]["tools"][0]["function"]["name"] == "gateway_readiness_ping"
    assert fake.json_bodies[1]["tool_choice"]["function"]["name"] == "gateway_readiness_ping"


def test_tool_readiness_parses_json_instead_of_raw_byte_search():
    ok, reason = runtime._raw_response_has_tool_call(
        b'{"choices":[{"message":{"tool_calls":[{"id":"call_1"}]}}]}'
    )
    assert ok is True
    assert reason == "ok"

    ok, reason = runtime._raw_response_has_tool_call(
        b'{"choices":[{"message":{"content":"no tool"}}]}'
    )
    assert ok is False
    assert reason == "no tool call in response"


def test_readiness_without_models_fails_explicitly(monkeypatch):
    fake = FakeTransport()
    monkeypatch.setattr(runtime, "_transport", fake)
    backend = _backend("candidate", lifecycle="warming")
    backend.models = []

    ok, reason, _latency_ms = asyncio.run(runtime._run_readiness_checks(backend))

    assert ok is False
    assert "backend has no configured models" in reason
    assert fake.json_bodies == []
    assert fake.stream_bodies == []


def test_persist_backend_runtime_state_logs_failures(monkeypatch, caplog):
    backend = _backend("candidate", lifecycle="warming")

    def fail_update(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(backend_store, "update_backend", fail_update)

    runtime._persist_backend_runtime_state(backend)

    assert "Failed to persist backend state for candidate" in caplog.text


def test_upsert_backend_registers_and_updates_by_account(monkeypatch):
    entry = backend_store.upsert_backend(
        name="alice@8800",
        base_url="http://127.0.0.1:8800",
        account_id="alice.json",
        lifecycle="active",
    )

    again = backend_store.upsert_backend(
        name="alice@8801",
        base_url="http://127.0.0.1:8801",
        account_id="alice",
        lifecycle="active",
    )
    stored = backend_store.list_backends()

    assert len(stored) == 1
    assert again["id"] == entry["id"]
    assert stored[0]["base_url"] == "http://127.0.0.1:8801"
    assert stored[0]["account_id"] == "alice"
    assert "mimo-v2.5-pro" in stored[0]["models"]
    assert stored[0]["api_key"] == "sk-Aoki-MiMo"


def test_concurrent_warm_and_manual_activate_keeps_all_ready_backends_active(monkeypatch):
    old = _backend("old")
    warm_a = _backend("warm-a", lifecycle="warming")
    warm_b = _backend("warm-b", lifecycle="warming")
    warm_a.last_probe_at = 0.0
    warm_b.last_probe_at = 0.0
    reg = BackendRegistry([old, warm_a, warm_b])
    monkeypatch.setattr(runtime, "_registry", reg)
    monkeypatch.setattr(runtime, "_persist_backend_runtime_state", lambda _backend: None)

    entered = asyncio.Event()
    release = asyncio.Event()

    async def fake_readiness(_backend):
        entered.set()
        await release.wait()
        return True, "ready", 1.0

    async def scenario():
        monkeypatch.setattr(runtime, "_run_readiness_checks", fake_readiness)
        task = asyncio.create_task(runtime._warm_ready_backends())
        await entered.wait()
        runtime.activate_backend("warm-b")
        release.set()
        await task

    asyncio.run(scenario())

    active = {b.backend_id for b in reg.all() if b.lifecycle == "active"}
    assert active == {"old", "warm-a", "warm-b"}


def test_prepare_account_deploy_drains_active_backend_when_peer_can_serve(monkeypatch):
    old = _backend("old")
    old.account_id = "alice"
    old.in_flight = 0
    peer = _backend("peer")
    peer.account_id = "bob"
    reg = BackendRegistry([old, peer])
    monkeypatch.setattr(runtime, "_registry", reg)
    monkeypatch.setattr(runtime, "_persist_backend_runtime_state", lambda _backend: None)

    result = runtime.prepare_account_deploy("alice.json")

    assert result["drained"] == ["old"]
    assert result["blocked"] == []
    assert old.lifecycle == "draining"
    assert peer.lifecycle == "active"


def test_prepare_account_deploy_blocks_when_no_peer_exists(monkeypatch):
    only = _backend("only")
    only.account_id = "alice"
    reg = BackendRegistry([only])
    monkeypatch.setattr(runtime, "_registry", reg)
    monkeypatch.setattr(runtime, "_persist_backend_runtime_state", lambda _backend: None)

    result = runtime.prepare_account_deploy("alice")

    assert result["drained"] == []
    assert result["blocked"] == ["only"]
    assert only.lifecycle == "active"


def test_prepare_account_deploy_blocks_when_peer_is_not_selectable(monkeypatch):
    old = _backend("old")
    old.account_id = "alice"
    peer = _backend("peer")
    peer.account_id = "bob"
    peer.in_detection = True
    reg = BackendRegistry([old, peer])
    monkeypatch.setattr(runtime, "_registry", reg)
    monkeypatch.setattr(runtime, "_persist_backend_runtime_state", lambda _backend: None)

    result = runtime.prepare_account_deploy("alice")

    assert result["drained"] == []
    assert result["blocked"] == ["old"]
    assert old.lifecycle == "active"


def test_promote_standby_backends_to_warming_fills_load_balancing_pool(monkeypatch):
    active = _backend("active")
    standby = _backend("standby", lifecycle="standby")
    reg = BackendRegistry([active, standby])
    monkeypatch.setattr(runtime, "_registry", reg)
    monkeypatch.setattr(runtime, "_persist_backend_runtime_state", lambda _backend: None)

    runtime._promote_standby_backends_to_warming()

    assert active.lifecycle == "active"
    assert standby.lifecycle == "warming"


def test_promote_standby_backend_activates_when_no_capacity_exists(monkeypatch):
    standby = _backend("standby", lifecycle="standby")
    reg = BackendRegistry([standby])
    monkeypatch.setattr(runtime, "_registry", reg)
    monkeypatch.setattr(runtime, "_persist_backend_runtime_state", lambda _backend: None)

    runtime._promote_standby_backends_to_warming()

    assert standby.lifecycle == "active"


def test_complete_account_deploy_keeps_verified_backend_active_with_peer(monkeypatch):
    old = _backend("old", lifecycle="draining")
    old.account_id = "alice"
    peer = _backend("peer")
    peer.account_id = "bob"
    reg = BackendRegistry([old, peer])
    monkeypatch.setattr(runtime, "_registry", reg)
    monkeypatch.setattr(runtime, "reload_backends", lambda: len(reg.all()))
    monkeypatch.setattr(runtime, "_persist_backend_runtime_state", lambda _backend: None)

    result = runtime.complete_account_deploy("alice")

    assert result["warmed"] == []
    assert result["activated"] == ["old"]
    assert old.lifecycle == "active"
    assert peer.lifecycle == "active"


def test_complete_account_deploy_activates_when_no_peer_exists(monkeypatch):
    only = _backend("only", lifecycle="draining")
    only.account_id = "alice"
    reg = BackendRegistry([only])
    monkeypatch.setattr(runtime, "_registry", reg)
    monkeypatch.setattr(runtime, "reload_backends", lambda: len(reg.all()))
    monkeypatch.setattr(runtime, "_persist_backend_runtime_state", lambda _backend: None)

    result = runtime.complete_account_deploy("alice")

    assert result["warmed"] == []
    assert result["activated"] == ["only"]
    assert only.lifecycle == "active"


def test_rotation_interval_defaults_to_40_minutes():
    assert runtime._ROTATION_INTERVAL_S == 40 * 60.0
