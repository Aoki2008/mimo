from __future__ import annotations

import asyncio
import json

import pytest

from gateway.core import BadRequestError
from gateway.runtime import _read_json_body


class _Request:
    def __init__(self, raw: bytes, *, content_type: str = "application/json"):
        self._raw = raw
        self.headers = {"content-type": content_type}

    async def body(self) -> bytes:
        return self._raw


def _read(raw: bytes, *, content_type: str = "application/json"):
    return asyncio.run(_read_json_body(_Request(raw, content_type=content_type)))


def test_read_json_body_accepts_utf8_json():
    raw = json.dumps({"model": "m", "messages": [{"role": "user", "content": "你好"}]}).encode()

    assert _read(raw)["messages"][0]["content"] == "你好"


def test_read_json_body_uses_declared_gbk_charset():
    raw = json.dumps(
        {"model": "m", "messages": [{"role": "user", "content": "编码测试"}]},
        ensure_ascii=False,
    ).encode("gbk")

    data = _read(raw, content_type="application/json; charset=gbk")

    assert data["messages"][0]["content"] == "编码测试"


def test_read_json_body_falls_back_to_gb18030_for_legacy_clients():
    raw = json.dumps(
        {"model": "m", "messages": [{"role": "user", "content": "中文"}]},
        ensure_ascii=False,
    ).encode("gb18030")

    data = _read(raw)

    assert data["messages"][0]["content"] == "中文"


def test_read_json_body_returns_bad_request_for_undecodable_bytes():
    with pytest.raises(BadRequestError) as exc:
        _read(b'{"model":"m","messages":"\xff\xfe\xff"}')

    assert "Invalid request body encoding" in exc.value.message
