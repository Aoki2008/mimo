"""
YAML configuration layer.

One source of truth for: gateway listen address, default rate limits,
the upstream backend pool, the chat probe schedule, and storage paths.

Example::

    gateway:
      host: 0.0.0.0
      port: 8088
      default_rate_limit_per_min: 60

    storage:
      api_keys_db: ./data/api_keys.db
      decision_log: ./data/decisions.jsonl

    probe:
      interval_s: 30
      timeout_s: 5
      failure_threshold: 3
      cooldown_s: 30

    backends:
      - id: mimo-claw-1
        base_url: https://acct1.aoki.tech
        model: MiMo-VL-7B-RL-2508
        api_key: sk-upstream-1
        weight: 1
        aliases: [claude-3-5-sonnet, gpt-4]
        account_id: acct1
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from gateway.routing import Backend


@dataclass
class GatewaySettings:
    host: str = "0.0.0.0"
    port: int = 8088
    default_rate_limit_per_min: int = 60
    request_timeout_s: float = 600.0


@dataclass
class StorageSettings:
    api_keys_db: str = "./data/api_keys.db"
    decision_log: str = "./data/decisions.jsonl"


@dataclass
class ProbeSettings:
    interval_s: float = 30.0
    timeout_s: float = 5.0
    failure_threshold: int = 3
    cooldown_s: float = 30.0


@dataclass
class BackendConfig:
    id: str
    base_url: str
    model: str
    api_key: str = ""
    weight: int = 1
    aliases: list[str] = field(default_factory=list)
    account_id: str = ""

    def to_backend(self) -> Backend:
        meta: dict[str, str] = {}
        if self.aliases:
            meta["aliases"] = ",".join(self.aliases)
        return Backend(
            backend_id=self.id,
            base_url=self.base_url,
            model=self.model,
            account_id=self.account_id,
            api_key=self.api_key,
            weight=self.weight,
            metadata=meta,
        )


@dataclass
class GatewayConfig:
    gateway: GatewaySettings = field(default_factory=GatewaySettings)
    storage: StorageSettings = field(default_factory=StorageSettings)
    probe: ProbeSettings = field(default_factory=ProbeSettings)
    backends: list[BackendConfig] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)   # passthrough for unknown keys


# ────────────── loading ──────────────


class ConfigError(Exception):
    pass


def load(path: str | Path) -> GatewayConfig:
    """Load and validate a YAML config."""
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"Config file not found: {p}")
    raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ConfigError("Top-level config must be a mapping")
    return parse(raw)


def parse(raw: dict[str, Any]) -> GatewayConfig:
    gateway = _parse_section(raw.get("gateway") or {}, GatewaySettings, "gateway")
    storage = _parse_section(raw.get("storage") or {}, StorageSettings, "storage")
    probe = _parse_section(raw.get("probe") or {}, ProbeSettings, "probe")

    raw_backends = raw.get("backends") or []
    if not isinstance(raw_backends, list):
        raise ConfigError("'backends' must be a list")
    backends: list[BackendConfig] = []
    seen_ids: set[str] = set()
    for i, b in enumerate(raw_backends):
        if not isinstance(b, dict):
            raise ConfigError(f"backends[{i}] must be a mapping")
        for required in ("id", "base_url", "model"):
            if not b.get(required):
                raise ConfigError(f"backends[{i}] missing required field {required!r}")
        bid = b["id"]
        if bid in seen_ids:
            raise ConfigError(f"Duplicate backend id: {bid!r}")
        seen_ids.add(bid)
        aliases = b.get("aliases") or []
        if not isinstance(aliases, list):
            raise ConfigError(f"backends[{i}].aliases must be a list")
        backends.append(BackendConfig(
            id=bid,
            base_url=b["base_url"],
            model=b["model"],
            api_key=b.get("api_key", ""),
            weight=int(b.get("weight", 1)),
            aliases=[str(a) for a in aliases],
            account_id=b.get("account_id", ""),
        ))

    return GatewayConfig(
        gateway=gateway,
        storage=storage,
        probe=probe,
        backends=backends,
        raw=raw,
    )


def _parse_section(section: dict[str, Any], cls: type, name: str):
    if not isinstance(section, dict):
        raise ConfigError(f"{name!r} section must be a mapping, got {type(section).__name__}")
    field_names = {f for f in cls.__dataclass_fields__}
    unknown = set(section) - field_names
    if unknown:
        # Don't fail on unknown — just ignore for forward compat. Could
        # warn here in the future.
        pass
    kwargs = {k: v for k, v in section.items() if k in field_names}
    try:
        return cls(**kwargs)
    except (TypeError, ValueError) as e:
        raise ConfigError(f"Invalid {name!r} section: {e}") from e
