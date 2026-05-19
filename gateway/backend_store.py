"""Persistent backend store — CRUD for data/backends.json.

Each entry is a dict with:
  id, name, base_url, models (list[str]), api_key, weight, enabled, account_id

Legacy format with ``model`` (str) + ``aliases`` (comma-string) is migrated
to ``models`` on read; written entries always use the new shape.

The store is the single source of truth. ``runtime.py`` reloads the
BackendRegistry from it on startup and after every mutation.
"""
from __future__ import annotations

import json
import os
import secrets
import threading
from pathlib import Path
from typing import Any

DATA_PATH = Path(__file__).parent.parent / "data" / "backends.json"

_lock = threading.Lock()

# Mirrors the model list exposed by claw/payload/api-proxy.py so freshly
# deployed backends can be registered without manual model input.
DEFAULT_BACKEND_MODELS: tuple[str, ...] = (
    "mimo-v2.5-pro",
    "mimo-v2.5",
    "mimo-v2-pro",
    "mimo-v2-flash",
    "mimo-v2-omni",
    "mimo-v2-tts",
    "mimo-v2.5-tts",
    "mimo-v2.5-tts-voiceclone",
    "mimo-v2.5-tts-voicedesign",
)


def default_backend_models() -> list[str]:
    return list(DEFAULT_BACKEND_MODELS)


def default_backend_api_key() -> str:
    return (
        os.environ.get("PROXY_AUTH_TOKEN")
        or os.environ.get("MIMO_BACKEND_API_KEY")
        or "sk-Aoki-MiMo"
    ).strip() or "sk-Aoki-MiMo"


def _empty() -> dict:
    return {"backends": []}


def _normalize_models(raw: Any) -> list[str]:
    """Accept list[str], comma-string, or a single str; emit a deduped list."""
    out: list[str] = []
    seen: set[str] = set()
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, str):
        items = raw.split(",")
    else:
        return out
    for m in items:
        if not isinstance(m, str):
            continue
        s = m.strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _normalize_backend_spec(
    *,
    name: str,
    base_url: str,
    models: Any = None,
    api_key: str = "",
    weight: int = 1,
    account_id: str = "",
    model: str = "",
    aliases: str = "",
    default_models: Any = None,
    default_api_key: str = "",
) -> dict[str, Any]:
    name = (name or "").strip()
    base_url = (base_url or "").strip().rstrip("/")
    model_list = _normalize_models(models) if models is not None else []
    if not model_list:
        if model:
            model_list.append(model.strip())
        for a in (aliases or "").split(","):
            a = a.strip()
            if a and a not in model_list:
                model_list.append(a)
    if not model_list and default_models is not None:
        model_list = _normalize_models(list(default_models))
    api_key = (api_key or "").strip()
    if not api_key and default_api_key:
        api_key = default_api_key.strip()
    if not name:
        raise ValueError("name 不能为空")
    if not base_url:
        raise ValueError("base_url 不能为空")
    if not model_list:
        raise ValueError("models 不能为空")
    return {
        "name": name,
        "base_url": base_url,
        "models": model_list,
        "api_key": api_key,
        "weight": max(1, int(weight)),
        "account_id": account_id,
    }


def _migrate_entry(entry: dict) -> dict:
    """If entry uses legacy {model, aliases}, fold them into models[]."""
    if "models" in entry and isinstance(entry["models"], list):
        entry["models"] = _normalize_models(entry["models"])
        entry.pop("model", None)
        entry.pop("aliases", None)
        return entry
    merged: list[str] = []
    primary = entry.pop("model", None)
    if isinstance(primary, str) and primary.strip():
        merged.append(primary.strip())
    aliases = entry.pop("aliases", None)
    if isinstance(aliases, str):
        for a in aliases.split(","):
            a = a.strip()
            if a and a not in merged:
                merged.append(a)
    entry["models"] = merged
    return entry


def _load() -> dict:
    if not DATA_PATH.exists():
        return _empty()
    try:
        data = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _empty()
    for b in data.get("backends") or []:
        _migrate_entry(b)
    return data


def _save(data: dict) -> None:
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, indent=2, ensure_ascii=False)
    tmp_path = DATA_PATH.with_name(f"{DATA_PATH.name}.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(payload)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, DATA_PATH)


def list_backends() -> list[dict[str, Any]]:
    with _lock:
        data = _load()
    return data.get("backends") or []


def add_backend(
    *,
    name: str,
    base_url: str,
    models: Any = None,
    api_key: str = "",
    weight: int = 1,
    account_id: str = "",
    # legacy kwargs accepted for callers that still pass model/aliases
    model: str = "",
    aliases: str = "",
) -> dict[str, Any]:
    base = _normalize_backend_spec(
        name=name,
        base_url=base_url,
        models=models,
        api_key=api_key,
        weight=weight,
        account_id=account_id,
        model=model,
        aliases=aliases,
    )
    entry: dict[str, Any] = {
        "id": secrets.token_hex(6),
        **base,
        "enabled": True,
        "lifecycle": "warming",
    }
    with _lock:
        data = _load()
        for b in data["backends"]:
            if b.get("name") == name:
                raise ValueError(f"后端名 '{name}' 已存在")
        data["backends"].append(entry)
        _save(data)
    return entry


def upsert_backend(
    *,
    name: str,
    base_url: str,
    models: Any = None,
    api_key: str = "",
    weight: int = 1,
    account_id: str = "",
    enabled: bool = True,
    lifecycle: str = "active",
    model: str = "",
    aliases: str = "",
) -> dict[str, Any]:
    """Create or update a backend entry keyed by account_id/base_url/name."""
    base = _normalize_backend_spec(
        name=name,
        base_url=base_url,
        models=models,
        api_key=api_key,
        weight=weight,
        account_id=account_id,
        model=model,
        aliases=aliases,
        default_models=DEFAULT_BACKEND_MODELS,
        default_api_key=default_backend_api_key(),
    )

    def _account_matches(stored: str, requested: str) -> bool:
        stored = (stored or "").strip()
        requested = (requested or "").strip()
        if not stored or not requested:
            return False
        if stored == requested:
            return True
        if stored.endswith(".json") and stored[:-5] == requested:
            return True
        if requested.endswith(".json") and requested[:-5] == stored:
            return True
        return False

    with _lock:
        data = _load()
        target: dict[str, Any] | None = None
        for b in data["backends"]:
            if account_id and _account_matches(str(b.get("account_id") or ""), account_id):
                target = b
                break
        if target is None:
            for b in data["backends"]:
                if (b.get("base_url") or "").rstrip("/") == base["base_url"]:
                    target = b
                    break
        if target is None:
            for b in data["backends"]:
                if b.get("name") == base["name"]:
                    target = b
                    break

        if target is None:
            entry = {
                "id": secrets.token_hex(6),
                **base,
                "enabled": bool(enabled),
                "lifecycle": lifecycle or "active",
            }
            data["backends"].append(entry)
            _save(data)
            return entry

        updated = False
        if target.get("name") != base["name"]:
            target["name"] = base["name"]
            updated = True
        if (target.get("base_url") or "").rstrip("/") != base["base_url"]:
            target["base_url"] = base["base_url"]
            updated = True
        if _normalize_models(target.get("models")) != base["models"]:
            target["models"] = base["models"]
            updated = True
        if base["api_key"] and target.get("api_key") != base["api_key"]:
            target["api_key"] = base["api_key"]
            updated = True
        if int(target.get("weight") or 0) != base["weight"]:
            target["weight"] = base["weight"]
            updated = True
        if (target.get("account_id") or "") != base["account_id"]:
            target["account_id"] = base["account_id"]
            updated = True
        if bool(target.get("enabled", True)) != bool(enabled):
            target["enabled"] = bool(enabled)
            updated = True
        desired_lifecycle = lifecycle or target.get("lifecycle") or "active"
        if target.get("lifecycle") != desired_lifecycle:
            target["lifecycle"] = desired_lifecycle
            updated = True
        if updated:
            _save(data)
        return target


def update_backend(backend_id: str, **fields: Any) -> dict[str, Any] | None:
    allowed = {"name", "base_url", "models", "api_key", "weight",
               "account_id", "enabled", "lifecycle", "generation_id",
               "rotation_failures", "disabled_until",
               "in_detection", "detection_entered_at"}
    # Legacy: caller passes {model, aliases} — fold into models.
    if "model" in fields or "aliases" in fields:
        legacy = []
        m = fields.pop("model", "")
        if isinstance(m, str) and m.strip():
            legacy.append(m.strip())
        a = fields.pop("aliases", "")
        if isinstance(a, str):
            for x in a.split(","):
                x = x.strip()
                if x and x not in legacy:
                    legacy.append(x)
        if legacy and "models" not in fields:
            fields["models"] = legacy

    with _lock:
        data = _load()
        for b in data["backends"]:
            if b["id"] == backend_id:
                for k, v in fields.items():
                    if k not in allowed:
                        continue
                    if k == "base_url" and isinstance(v, str):
                        v = v.rstrip("/")
                    if k == "models":
                        v = _normalize_models(v)
                        if not v:
                            continue  # ignore empty list — keep old
                    b[k] = v
                _save(data)
                return b
    return None


def delete_backend(backend_id: str) -> bool:
    with _lock:
        data = _load()
        before = len(data["backends"])
        data["backends"] = [b for b in data["backends"] if b["id"] != backend_id]
        if len(data["backends"]) == before:
            return False
        _save(data)
    return True
