"""Panel IP allowlist — panel-configurable, stored in data/panel_acl.json.

Empty list = allow all (default, so a fresh install never locks anyone out).
Entries may be exact IPs (``1.2.3.4`` / ``::1``) or CIDR (``1.2.3.0/24``).
Only the panel/admin surface is gated by this; the public API (/v1), /health,
/stats and the token-authed worker/probe channels are NOT affected.
"""
from __future__ import annotations

import ipaddress
import json
import threading
from pathlib import Path

DATA_PATH = Path(__file__).parent.parent / "data" / "panel_acl.json"
_lock = threading.Lock()


def _load() -> dict:
    if not DATA_PATH.exists():
        return {"allowed_ips": []}
    try:
        d = json.loads(DATA_PATH.read_text(encoding="utf-8"))
        if not isinstance(d.get("allowed_ips"), list):
            d["allowed_ips"] = []
        return d
    except (OSError, json.JSONDecodeError):
        return {"allowed_ips": []}


def _save(d: dict) -> None:
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    DATA_PATH.write_text(json.dumps(d, indent=2, ensure_ascii=False), encoding="utf-8")


def validate(ips: list[str]) -> list[str]:
    """Drop empty/invalid entries; keep valid exact IPs and CIDRs."""
    clean = []
    for raw in ips or []:
        s = str(raw).strip()
        if not s:
            continue
        try:
            if "/" in s:
                ipaddress.ip_network(s, strict=False)
            else:
                ipaddress.ip_address(s)
            clean.append(s)
        except ValueError:
            continue
    return clean


def matches(ip: str, entries: list[str]) -> bool:
    """True if entries is empty (no restriction) or ip matches an entry."""
    if not entries:
        return True
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    for entry in entries:
        try:
            if "/" in entry:
                if addr in ipaddress.ip_network(entry, strict=False):
                    return True
            elif addr == ipaddress.ip_address(entry):
                return True
        except ValueError:
            continue
    return False


def list_allowed() -> list[str]:
    with _lock:
        return list(_load().get("allowed_ips", []))


def set_allowed(ips: list[str]) -> list[str]:
    """Validate + persist. Bad entries are dropped. Returns the saved list."""
    clean = validate(ips)
    with _lock:
        _save({"allowed_ips": clean})
    return clean


def is_allowed(ip: str) -> bool:
    """True if the allowlist is empty (no restriction) or ip matches an entry."""
    return matches(ip, list_allowed())
