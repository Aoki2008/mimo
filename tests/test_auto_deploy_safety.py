from __future__ import annotations

from claw import auto_deploy


class MemoryLog:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def log(self, msg: str) -> None:
        self.lines.append(msg)


def test_deploy_start_blocks_without_takeover_backend(monkeypatch):
    def fake_prepare(account_filename: str, *, api_port: int | None = None):
        assert account_filename == "alice.json"
        assert api_port == 8800
        return {"matched": ["only"], "drained": [], "blocked": ["only"]}

    monkeypatch.setattr("gateway.runtime.prepare_account_deploy", fake_prepare)
    log = MemoryLog()

    prepared = auto_deploy._notify_gateway_deploy_start("alice.json", 8800, log)

    assert prepared["safe_to_destroy"] is False
    assert prepared["blocked"] == ["only"]
    assert any("跳过销毁" in line for line in log.lines)


def test_deploy_start_allows_destroy_after_drain(monkeypatch):
    calls: list[str] = []

    def fake_prepare(_account_filename: str, *, api_port: int | None = None):
        return {"matched": ["old"], "drained": ["old"], "blocked": []}

    def fake_wait(_account_filename: str, *, api_port: int | None = None):
        calls.append("wait")
        return {"success": True, "pending": []}

    monkeypatch.setattr("gateway.runtime.prepare_account_deploy", fake_prepare)
    monkeypatch.setattr("gateway.runtime.wait_for_account_drain", fake_wait)
    log = MemoryLog()

    prepared = auto_deploy._notify_gateway_deploy_start("alice.json", 8800, log)

    assert prepared["safe_to_destroy"] is True
    assert prepared["drained"] == ["old"]
    assert calls == ["wait"]
