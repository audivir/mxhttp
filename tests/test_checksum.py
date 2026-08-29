"""Tests for checksum computation and verification module."""

from __future__ import annotations

import hashlib
import io
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

from mxhttp import KNOWN_ALGORITHMS, Checksum, ChecksumMismatchError
from mxhttp.checksum import (
    HEX_CHARS,
    MD5_HEX_LEN,
    SHA256_HEX_LEN,
    SHA512_HEX_LEN,
    compute_file_digest,
    compute_file_digest_async,
    is_checksum_algorithm,
    resolve_checksum,
    verify_checksum,
)

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.anyio


def test_known_algorithms_and_type_guard() -> None:
    assert "sha256" in KNOWN_ALGORITHMS
    assert is_checksum_algorithm("sha256") is True
    assert is_checksum_algorithm("unknown_algo") is False
    assert SHA256_HEX_LEN == 64
    assert SHA512_HEX_LEN == 128
    assert MD5_HEX_LEN == 32
    assert "a" in HEX_CHARS


def test_checksum_constructors() -> None:
    c_256 = Checksum.sha256("abc")
    assert c_256.algorithm == "sha256"
    assert c_256.expected == "abc"
    assert c_256.digest is None

    c_512 = Checksum.sha512("def")
    assert c_512.algorithm == "sha512"
    assert c_512.expected == "def"

    c_384 = Checksum.sha384("ghi")
    assert c_384.algorithm == "sha384"
    assert c_384.expected == "ghi"

    c_224 = Checksum.sha224("jkl")
    assert c_224.algorithm == "sha224"
    assert c_224.expected == "jkl"

    c_1 = Checksum.sha1("mno")
    assert c_1.algorithm == "sha1"
    assert c_1.expected == "mno"

    c_md5 = Checksum.md5("pqr")
    assert c_md5.algorithm == "md5"
    assert c_md5.expected == "pqr"


def test_resolve_checksum() -> None:
    assert resolve_checksum(None) is None

    existing = Checksum.sha256("existing")
    assert resolve_checksum(existing) is existing

    colon_sha = resolve_checksum("sha256:abcd1234")
    assert colon_sha is not None
    assert colon_sha.algorithm == "sha256"
    assert colon_sha.expected == "abcd1234"

    colon_md5 = resolve_checksum("md5:12345678")
    assert colon_md5 is not None
    assert colon_md5.algorithm == "md5"
    assert colon_md5.expected == "12345678"

    colon_empty = resolve_checksum("sha256:")
    assert colon_empty is not None
    assert colon_empty.algorithm == "sha256"
    assert colon_empty.expected is None

    colon_unknown = resolve_checksum("unknown_algo:some_value")
    assert colon_unknown is not None
    assert colon_unknown.algorithm == "sha256"
    assert colon_unknown.expected == "unknown_algo:some_value"

    algo_only = resolve_checksum("sha512")
    assert algo_only is not None
    assert algo_only.algorithm == "sha512"
    assert algo_only.expected is None

    hex_64 = "a" * 64
    inferred_256 = resolve_checksum(hex_64)
    assert inferred_256 is not None
    assert inferred_256.algorithm == "sha256"
    assert inferred_256.expected == hex_64

    hex_128 = "b" * 128
    inferred_512 = resolve_checksum(hex_128)
    assert inferred_512 is not None
    assert inferred_512.algorithm == "sha512"
    assert inferred_512.expected == hex_128

    hex_32 = "c" * 32
    inferred_md5 = resolve_checksum(hex_32)
    assert inferred_md5 is not None
    assert inferred_md5.algorithm == "md5"
    assert inferred_md5.expected == hex_32

    raw_custom = resolve_checksum("custom_expected_hash")
    assert raw_custom is not None
    assert raw_custom.algorithm == "sha256"
    assert raw_custom.expected == "custom_expected_hash"


def test_compute_file_digest() -> None:
    data = b"hello world checksum payload"
    expected = hashlib.sha256(data).hexdigest()

    digest = compute_file_digest(io.BytesIO(data), "sha256")
    assert digest == expected

    with patch("sys.version_info", (3, 10, 0)):
        fallback_digest = compute_file_digest(io.BytesIO(data), "sha256")
        assert fallback_digest == expected

        empty_digest = compute_file_digest(io.BytesIO(b""), "sha256")
        assert empty_digest == hashlib.sha256(b"").hexdigest()


async def test_compute_file_digest_async(tmp_path: Path) -> None:
    file_path = tmp_path / "async_hash.bin"
    file_path.write_bytes(b"async content to digest")
    expected = hashlib.sha256(b"async content to digest").hexdigest()

    import anyio

    digest = await compute_file_digest_async(anyio.Path(file_path), "sha256")
    assert digest == expected


def test_verify_checksum_success_and_callbacks() -> None:
    callback_hashes: list[str] = []

    def on_cb(h: str) -> None:
        callback_hashes.append(h)

    verify_checksum("hash123", None, on_cb)
    assert len(callback_hashes) == 0

    cfg_no_expected = Checksum.sha256(None)
    verify_checksum("computed_digest", cfg_no_expected, None)
    assert cfg_no_expected.digest == "computed_digest"

    cfg = Checksum.sha256("EXPECTED_HASH")
    verify_checksum("expected_hash", cfg, on_cb)
    assert cfg.digest == "expected_hash"
    assert callback_hashes == ["expected_hash"]


def test_verify_checksum_mismatch() -> None:
    cfg = Checksum.sha256("expected_valid_hash")
    with pytest.raises(ChecksumMismatchError) as exc_info:
        verify_checksum("corrupted_hash", cfg)

    err = exc_info.value
    assert err.algorithm == "sha256"
    assert err.expected == "expected_valid_hash"
    assert err.actual == "corrupted_hash"
    assert "sha256 checksum mismatch" in str(err)
