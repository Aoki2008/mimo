"""
SQLite-backed API key store.

Schema:

  api_keys
    id                INTEGER PRIMARY KEY AUTOINCREMENT
    key_id            TEXT UNIQUE NOT NULL          -- public identifier (k_xxxxx)
    key_hash          TEXT UNIQUE NOT NULL          -- sha256(secret)
    key_prefix        TEXT NOT NULL                 -- first 8 chars of secret, for ops UI
    label             TEXT NOT NULL DEFAULT ''
    rate_limit_per_min INTEGER                       -- NULL = use gateway default
    allowed_models    TEXT NOT NULL DEFAULT ''      -- CSV; '' = all
    created_at        REAL NOT NULL
    revoked_at        REAL                          -- NULL = active

Secrets are NEVER stored in plaintext. ``create()`` returns the secret
once at creation time; thereafter the only retrievable value is the
prefix. Validation goes through ``validate(secret)`` which hashes the
input and looks up by hash.

This is intentionally simple — single-process file DB, one connection
per call (Python's sqlite3 is fine with that). For multi-process
deployments later, swap in an aiosqlite or libsql implementation.
"""
from __future__ import annotations

import hashlib
import secrets
import sqlite3
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass

from gateway.core import AuthError


@dataclass(frozen=True)
class APIKeyRecord:
    """Public-safe view of an API key. ``secret`` is never populated when
    listing — callers see it exactly once via ``CreatedKey.secret``."""

    key_id: str
    key_prefix: str
    label: str
    rate_limit_per_min: int | None
    allowed_models: tuple[str, ...]
    created_at: float
    revoked_at: float | None

    @property
    def is_active(self) -> bool:
        return self.revoked_at is None


@dataclass(frozen=True)
class CreatedKey:
    """Returned by ``create()`` — contains the one-time-visible secret."""

    record: APIKeyRecord
    secret: str


_SCHEMA = """
CREATE TABLE IF NOT EXISTS api_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key_id TEXT UNIQUE NOT NULL,
    key_hash TEXT UNIQUE NOT NULL,
    key_prefix TEXT NOT NULL,
    label TEXT NOT NULL DEFAULT '',
    rate_limit_per_min INTEGER,
    allowed_models TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL,
    revoked_at REAL
);
CREATE INDEX IF NOT EXISTS api_keys_key_hash_idx ON api_keys(key_hash);
"""


class APIKeyStore:
    """File-based SQLite store. Pass ``:memory:`` for tests.

    Holds one long-lived connection (sqlite3 is fine being shared across
    threads when ``check_same_thread=False`` plus our own lock). This
    matches the typical gateway deployment — single process, one writer.
    """

    def __init__(self, path: str):
        self._path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            path, isolation_level=None, check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> "APIKeyStore":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    # ───── CRUD ─────

    def create(
        self,
        *,
        label: str = "",
        rate_limit_per_min: int | None = None,
        allowed_models: Iterable[str] = (),
    ) -> CreatedKey:
        secret = _generate_secret()
        key_id = f"k_{secrets.token_hex(8)}"
        key_hash = _hash_secret(secret)
        key_prefix = secret[:8]
        models_csv = ",".join(allowed_models)
        created_at = time.time()
        with self._lock:
            self._conn.execute(
                "INSERT INTO api_keys "
                "(key_id, key_hash, key_prefix, label, rate_limit_per_min, "
                " allowed_models, created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (key_id, key_hash, key_prefix, label, rate_limit_per_min,
                 models_csv, created_at),
            )
        return CreatedKey(
            record=APIKeyRecord(
                key_id=key_id,
                key_prefix=key_prefix,
                label=label,
                rate_limit_per_min=rate_limit_per_min,
                allowed_models=tuple(allowed_models),
                created_at=created_at,
                revoked_at=None,
            ),
            secret=secret,
        )

    def list(self, *, include_revoked: bool = False) -> list[APIKeyRecord]:
        sql = "SELECT * FROM api_keys"
        if not include_revoked:
            sql += " WHERE revoked_at IS NULL"
        sql += " ORDER BY created_at"
        with self._lock:
            rows = self._conn.execute(sql).fetchall()
        return [_row_to_record(r) for r in rows]

    def get(self, key_id: str) -> APIKeyRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM api_keys WHERE key_id = ?", (key_id,),
            ).fetchone()
        return _row_to_record(row) if row else None

    def revoke(self, key_id: str) -> bool:
        now = time.time()
        with self._lock:
            cur = self._conn.execute(
                "UPDATE api_keys SET revoked_at = ? "
                "WHERE key_id = ? AND revoked_at IS NULL",
                (now, key_id),
            )
            return cur.rowcount > 0

    def delete(self, key_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM api_keys WHERE key_id = ?", (key_id,))
            return cur.rowcount > 0

    # ───── auth-side ─────

    def lookup_by_secret(self, secret: str) -> APIKeyRecord | None:
        if not secret:
            return None
        h = _hash_secret(secret)
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM api_keys WHERE key_hash = ?", (h,),
            ).fetchone()
        return _row_to_record(row) if row else None

    async def validate(self, secret: str) -> APIKeyRecord:
        """AuthValidator-shaped: returns active record or raises AuthError."""
        rec = self.lookup_by_secret(secret)
        if rec is None:
            raise AuthError("Invalid API key")
        if rec.revoked_at is not None:
            raise AuthError("API key has been revoked")
        return rec


# ────────────── helpers ──────────────


def _generate_secret() -> str:
    return f"sk-mimo-{secrets.token_urlsafe(32)}"


def _hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def _row_to_record(row: sqlite3.Row) -> APIKeyRecord:
    models = row["allowed_models"]
    tup = tuple(s for s in models.split(",") if s) if models else ()
    return APIKeyRecord(
        key_id=row["key_id"],
        key_prefix=row["key_prefix"],
        label=row["label"],
        rate_limit_per_min=row["rate_limit_per_min"],
        allowed_models=tup,
        created_at=row["created_at"],
        revoked_at=row["revoked_at"],
    )
