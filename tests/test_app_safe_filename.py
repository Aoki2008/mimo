"""Regression tests for ``app._is_safe_account_filename``.

The validator gates every ``/api/account/{filename}/*`` route AND the
internal ``_load_account_by_filename`` / ``_save_account`` helpers.
A too-strict validator silently breaks legitimate accounts whose
filenames contain ``@``/``.`` (the project's historical convention for
email-derived account ids), so the rules below must be honored:

  * accept email-style and dotted-domain filenames
  * reject path traversal / hidden file / NUL / path separators
  * reject anything that, once joined into ``ACCOUNTS_DIR``, resolves
    outside the directory
"""
from __future__ import annotations

import pytest

from app import _is_safe_account_filename


@pytest.mark.parametrize(
    "name",
    [
        # Legacy/real account filenames in the repo. THIS is the exact
        # regression that motivated the validator rewrite — the previous
        # version rejected every one of these.
        "ASASAAA@baldur.edu.kg",
        "openclaw817449@duckmail.sbs",
        "kuro-aoki",
        # Other realistic shapes.
        "user@example.com",
        "first.last@example.com",
        "user+tag@example.com",
        "alice",
        "alice_bob",
        "user123",
        "a-b-c_d.e@f.g",
    ],
)
def test_accepts_realistic_account_names(name):
    assert _is_safe_account_filename(name) is True


@pytest.mark.parametrize(
    "name",
    [
        "",
        "..",
        ".",
        ".hidden",
        ".env",
        # Anything that starts with a dot is rejected even if its body
        # would resolve OK — leading-dot filenames are universally
        # suspicious (hidden files, partial traversal).
        "..secrets",
        "..%2Fsecrets",
        # Path separators are never valid in a single-segment filename.
        "foo/bar",
        "foo\\bar",
        "../secrets",
        "..\\secrets",
        # NUL bytes truncate file paths on some OSes.
        "foo\x00",
        "foo\x00.json",
    ],
)
def test_rejects_traversal_and_hidden(name):
    assert _is_safe_account_filename(name) is False


def test_rejects_non_string():
    assert _is_safe_account_filename(None) is False  # type: ignore[arg-type]
    assert _is_safe_account_filename(123) is False  # type: ignore[arg-type]
    assert _is_safe_account_filename(b"bytes") is False  # type: ignore[arg-type]
