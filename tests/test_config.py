"""Unit tests for gateway.config: APIKeyStore + YAML loader."""
from __future__ import annotations

import asyncio
import os
import tempfile

import pytest

from gateway.config import (
    APIKeyStore,
    ConfigError,
    GatewayConfig,
    load,
    parse,
)
from gateway.core import AuthError


def _run(coro):
    return asyncio.run(coro)


# ───────── APIKeyStore ─────────


def _store() -> APIKeyStore:
    return APIKeyStore(":memory:")


def test_create_returns_secret_once():
    store = _store()
    created = store.create(label="alice")
    assert created.secret.startswith("sk-mimo-")
    assert created.record.key_id.startswith("k_")
    assert created.record.label == "alice"
    assert created.record.is_active

    # The secret can authenticate.
    rec = store.lookup_by_secret(created.secret)
    assert rec is not None
    assert rec.key_id == created.record.key_id
    # But list() never returns it.
    listed = store.list()
    assert all(not hasattr(r, "secret") for r in listed)


def test_create_persists_rate_limit_and_models():
    store = _store()
    created = store.create(
        label="premium",
        rate_limit_per_min=600,
        allowed_models=["claude-3-5-sonnet", "gpt-4"],
    )
    rec = store.get(created.record.key_id)
    assert rec.rate_limit_per_min == 600
    assert rec.allowed_models == ("claude-3-5-sonnet", "gpt-4")


def test_lookup_by_secret_unknown_returns_none():
    store = _store()
    assert store.lookup_by_secret("sk-mimo-not-real") is None
    assert store.lookup_by_secret("") is None


def test_validate_returns_record_for_active_key():
    store = _store()
    created = store.create()
    rec = _run(store.validate(created.secret))
    assert rec.key_id == created.record.key_id


def test_validate_unknown_key_raises_authentication():
    store = _store()
    with pytest.raises(AuthError, match="Invalid API key"):
        _run(store.validate("sk-mimo-not-real"))


def test_validate_revoked_key_raises_authentication():
    store = _store()
    created = store.create()
    assert store.revoke(created.record.key_id) is True
    with pytest.raises(AuthError, match="revoked"):
        _run(store.validate(created.secret))


def test_revoke_marks_inactive_and_filters_from_list():
    store = _store()
    a = store.create(label="a")
    b = store.create(label="b")
    store.revoke(a.record.key_id)
    active = [r.label for r in store.list()]
    assert active == ["b"]
    all_keys = [r.label for r in store.list(include_revoked=True)]
    assert sorted(all_keys) == ["a", "b"]


def test_revoke_nonexistent_returns_false():
    store = _store()
    assert store.revoke("k_nonexistent") is False


def test_revoke_already_revoked_returns_false():
    store = _store()
    created = store.create()
    assert store.revoke(created.record.key_id) is True
    assert store.revoke(created.record.key_id) is False


def test_delete_removes_row():
    store = _store()
    created = store.create()
    assert store.delete(created.record.key_id) is True
    assert store.get(created.record.key_id) is None


def test_secrets_are_not_stored_plaintext():
    """Direct DB inspection: ensure no plaintext secrets exist."""
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "k.db")
        store = APIKeyStore(path)
        try:
            created = store.create(label="alice")
        finally:
            store.close()
        # Read raw DB bytes — secret must not appear.
        with open(path, "rb") as f:
            blob = f.read()
        assert created.secret.encode() not in blob
        # But the prefix is fine to leak.
        assert created.record.key_prefix.encode() in blob


def test_two_distinct_secrets_have_distinct_hashes():
    store = _store()
    a = store.create()
    b = store.create()
    assert a.secret != b.secret
    rec_a = store.lookup_by_secret(a.secret)
    rec_b = store.lookup_by_secret(b.secret)
    assert rec_a.key_id != rec_b.key_id


# ───────── YAML loader ─────────


_FULL_CONFIG = """
gateway:
  host: 127.0.0.1
  port: 9090
  default_rate_limit_per_min: 100

storage:
  api_keys_db: /tmp/keys.db
  decision_log: /tmp/decisions.jsonl

probe:
  interval_s: 15
  timeout_s: 3
  failure_threshold: 5
  cooldown_s: 60

backends:
  - id: claw-1
    base_url: https://acct1.example
    model: MiMo-VL-7B-RL-2508
    api_key: sk-up-1
    weight: 2
    account_id: acct1
    aliases:
      - claude-3-5-sonnet
      - gpt-4
  - id: claw-2
    base_url: https://acct2.example
    model: MiMo-VL-7B-RL-2508
"""


def test_parse_full_config():
    import yaml as _yaml
    cfg = parse(_yaml.safe_load(_FULL_CONFIG))
    assert isinstance(cfg, GatewayConfig)
    assert cfg.gateway.host == "127.0.0.1"
    assert cfg.gateway.port == 9090
    assert cfg.gateway.default_rate_limit_per_min == 100
    assert cfg.storage.api_keys_db == "/tmp/keys.db"
    assert cfg.probe.interval_s == 15
    assert cfg.probe.failure_threshold == 5
    assert len(cfg.backends) == 2
    assert cfg.backends[0].id == "claw-1"
    assert cfg.backends[0].weight == 2
    assert cfg.backends[0].aliases == ["claude-3-5-sonnet", "gpt-4"]


def test_parse_defaults_when_sections_omitted():
    cfg = parse({})
    assert cfg.gateway.port == 8088
    assert cfg.gateway.host == "0.0.0.0"
    assert cfg.backends == []


def test_parse_backend_to_backend_object():
    import yaml as _yaml
    cfg = parse(_yaml.safe_load(_FULL_CONFIG))
    b = cfg.backends[0].to_backend()
    assert b.backend_id == "claw-1"
    assert b.base_url == "https://acct1.example"
    assert b.model == "MiMo-VL-7B-RL-2508"
    assert b.api_key == "sk-up-1"
    assert b.weight == 2
    assert b.metadata["aliases"] == "claude-3-5-sonnet,gpt-4"


def test_parse_rejects_missing_backend_fields():
    with pytest.raises(ConfigError, match="missing required field"):
        parse({"backends": [{"id": "x", "base_url": "u"}]})  # missing model


def test_parse_rejects_duplicate_backend_id():
    with pytest.raises(ConfigError, match="Duplicate backend"):
        parse({
            "backends": [
                {"id": "x", "base_url": "u", "model": "m"},
                {"id": "x", "base_url": "v", "model": "n"},
            ],
        })


def test_parse_rejects_non_mapping_section():
    with pytest.raises(ConfigError, match="section must be a mapping"):
        parse({"gateway": "not a dict"})


def test_parse_unknown_keys_silently_ignored():
    """Unknown keys forward-compat: ignored without raising."""
    cfg = parse({"gateway": {"port": 8080, "future_field": "ignored"}})
    assert cfg.gateway.port == 8080


def test_load_reads_file():
    with tempfile.TemporaryDirectory() as tmp:
        p = os.path.join(tmp, "config.yaml")
        with open(p, "w", encoding="utf-8") as f:
            f.write(_FULL_CONFIG)
        cfg = load(p)
        assert cfg.gateway.port == 9090
        assert cfg.backends[0].id == "claw-1"


def test_load_missing_file_raises():
    with pytest.raises(ConfigError, match="not found"):
        load("/nonexistent/path/to/config.yaml")


def test_load_empty_file_yields_defaults():
    with tempfile.TemporaryDirectory() as tmp:
        p = os.path.join(tmp, "empty.yaml")
        with open(p, "w", encoding="utf-8") as f:
            f.write("")
        cfg = load(p)
        assert cfg.gateway.port == 8088
        assert cfg.backends == []
