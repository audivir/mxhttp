"""Deterministic demo payload shared by the example server and client."""

# ruff: noqa: INP001

from __future__ import annotations

import hashlib
import random

DOWNLOAD_SEED = 20260830
DOWNLOAD_SIZE = 2 * 1024 * 1024  # 2 MiB: big enough to exercise multi-part segmentation


def download_payload() -> bytes:
    """Regenerates the deterministic demo file content."""
    return random.Random(DOWNLOAD_SEED).randbytes(DOWNLOAD_SIZE)  # noqa: S311


DOWNLOAD_SHA256 = hashlib.sha256(download_payload()).hexdigest()
