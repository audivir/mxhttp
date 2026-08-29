"""Checksum computation and verification for mxhttp downloads."""

from __future__ import annotations

import hashlib
import hmac
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol, TypeGuard

import msgspec

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

    import anyio

ChecksumAlgorithm = Literal["sha256", "sha512", "sha384", "sha224", "sha1", "md5"]

SHA256_HEX_LEN: int = 64
SHA512_HEX_LEN: int = 128
MD5_HEX_LEN: int = 32
HEX_CHARS: frozenset[str] = frozenset("0123456789abcdefABCDEF")

KNOWN_ALGORITHMS: frozenset[ChecksumAlgorithm] = frozenset(
    {"sha256", "sha512", "sha384", "sha224", "sha1", "md5"}
)


def is_checksum_algorithm(val: str) -> TypeGuard[ChecksumAlgorithm]:
    """Checks whether a string is a supported checksum algorithm."""
    return val in KNOWN_ALGORITHMS


class DigestibleFile(Protocol):
    """File protocol supporting readinto for zero-copy hashing."""

    def readinto(self, buffer: bytearray | memoryview, /) -> int:
        """Reads bytes directly into a buffer."""
        ...

    def readable(self, /) -> bool:
        """Returns True if the file stream can be read."""
        ...


class ChecksumMismatchError(Exception):
    """Raised when downloaded data does not match the expected checksum."""

    def __init__(self, algorithm: ChecksumAlgorithm, expected: str, actual: str) -> None:
        """Initializes checksum mismatch exception with algorithm details."""
        self.algorithm = algorithm
        self.expected = expected
        self.actual = actual
        super().__init__(f"{algorithm} checksum mismatch: expected {expected}, got {actual}")


class Checksum(msgspec.Struct):
    """Checksum verification or calculation specification."""

    algorithm: ChecksumAlgorithm = "sha256"
    expected: str | None = None
    digest: str | None = None

    @classmethod
    def sha256(cls, expected: str | None = None) -> Checksum:
        """Creates a SHA-256 checksum configuration."""
        return cls(algorithm="sha256", expected=expected)

    @classmethod
    def sha512(cls, expected: str | None = None) -> Checksum:
        """Creates a SHA-512 checksum configuration."""
        return cls(algorithm="sha512", expected=expected)

    @classmethod
    def sha384(cls, expected: str | None = None) -> Checksum:
        """Creates a SHA-384 checksum configuration."""
        return cls(algorithm="sha384", expected=expected)

    @classmethod
    def sha224(cls, expected: str | None = None) -> Checksum:
        """Creates a SHA-224 checksum configuration."""
        return cls(algorithm="sha224", expected=expected)

    @classmethod
    def sha1(cls, expected: str | None = None) -> Checksum:
        """Creates a SHA-1 checksum configuration."""
        return cls(algorithm="sha1", expected=expected)

    @classmethod
    def md5(cls, expected: str | None = None) -> Checksum:
        """Creates an MD5 checksum configuration."""
        return cls(algorithm="md5", expected=expected)


ChecksumInput: TypeAlias = "str | Checksum | None"
ChecksumCallback: TypeAlias = "Callable[[str], None]"


def resolve_checksum(checksum: ChecksumInput) -> Checksum | None:  # noqa: PLR0911
    """Normalizes checksum inputs into a Checksum configuration."""
    if checksum is None:
        return None
    if isinstance(checksum, Checksum):
        return checksum

    raw = checksum.strip()
    if ":" in raw:
        algo, _, expected = raw.partition(":")
        algo_clean = algo.strip().lower()
        if is_checksum_algorithm(algo_clean):
            return Checksum(
                algorithm=algo_clean,
                expected=expected.strip() or None,
            )

    algo_lower = raw.lower()
    if is_checksum_algorithm(algo_lower):
        return Checksum(algorithm=algo_lower)

    if len(raw) == SHA256_HEX_LEN and all(c in HEX_CHARS for c in raw):
        return Checksum(algorithm="sha256", expected=raw)
    if len(raw) == SHA512_HEX_LEN and all(c in HEX_CHARS for c in raw):
        return Checksum(algorithm="sha512", expected=raw)
    if len(raw) == MD5_HEX_LEN and all(c in HEX_CHARS for c in raw):
        return Checksum(algorithm="md5", expected=raw)

    return Checksum(algorithm="sha256", expected=raw)


def compute_file_digest(file_obj: DigestibleFile, algorithm: ChecksumAlgorithm = "sha256") -> str:
    """Computes hex digest using hardware acceleration or reusable memory buffer."""
    if sys.version_info >= (3, 11):  # pragma: no cover
        return hashlib.file_digest(file_obj, algorithm).hexdigest()
    else:  # noqa: RET505
        hasher = hashlib.new(algorithm)
        buf = bytearray(1024 * 1024)
        view = memoryview(buf)
        while True:
            bytes_read = file_obj.readinto(view)
            if not bytes_read:
                break
            hasher.update(view[:bytes_read])
        return hasher.hexdigest()


async def compute_file_digest_async(
    file_path: anyio.Path, algorithm: ChecksumAlgorithm = "sha256"
) -> str:
    """Asynchronously computes hex digest of a file path."""
    import anyio

    def _sync_digest() -> str:
        with Path(file_path).open("rb") as f:
            return compute_file_digest(f, algorithm)

    return await anyio.to_thread.run_sync(_sync_digest)


def verify_checksum(
    actual_digest: str,
    config: Checksum | None,
    on_checksum: ChecksumCallback | None = None,
) -> None:
    """Verifies actual digest against expected checksum and invokes callbacks."""
    if config is None:
        return

    config.digest = actual_digest
    if on_checksum is not None:
        on_checksum(actual_digest)

    if config.expected is not None and not hmac.compare_digest(
        actual_digest.lower(), config.expected.lower()
    ):
        raise ChecksumMismatchError(
            algorithm=config.algorithm,
            expected=config.expected,
            actual=actual_digest,
        )
