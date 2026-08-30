"""Minimal typed declarative HTTP client."""

from __future__ import annotations

from mxhttp.checksum import (
    KNOWN_ALGORITHMS,
    Checksum,
    ChecksumCallback,
    ChecksumInput,
    ChecksumMismatchError,
)
from mxhttp.concurrency import (
    Concurrency,
    ConcurrencyExceededError,
    ConcurrencyTimeoutError,
    concurrency,
)
from mxhttp.consumer import AsyncConsumer, SyncConsumer, base_url
from mxhttp.download import (
    AsyncDownloader,
    Downloader,
    DownloadIdentityError,
    DownloadLockError,
    DownloadState,
    PartProgressCallback,
    Parts,
    ProgressCallback,
)
from mxhttp.endpoint import delete, endpoint, get, head, patch, post, put
from mxhttp.markers import Body, Cookie, Field, Header, Part, Path, Query, RawBody, RawPath
from mxhttp.progress import TqdmProgress
from mxhttp.ratelimit import RateLimit, RateLimitExceededError, ratelimit
from mxhttp.response import Response, ResumeLostError, response_handler, streaming_response_handler
from mxhttp.retry import Retry, retry
from mxhttp.sse import Event
from mxhttp.types import PartValue

__version__ = "1.6.4"

__all__ = [
    "KNOWN_ALGORITHMS",
    "AsyncConsumer",
    "AsyncDownloader",
    "Body",
    "Checksum",
    "ChecksumCallback",
    "ChecksumInput",
    "ChecksumMismatchError",
    "Concurrency",
    "ConcurrencyExceededError",
    "ConcurrencyTimeoutError",
    "Cookie",
    "DownloadIdentityError",
    "DownloadLockError",
    "DownloadState",
    "Downloader",
    "Event",
    "Field",
    "Header",
    "Part",
    "PartProgressCallback",
    "PartValue",
    "Parts",
    "Path",
    "ProgressCallback",
    "Query",
    "RateLimit",
    "RateLimitExceededError",
    "RawBody",
    "RawPath",
    "Response",
    "ResumeLostError",
    "Retry",
    "SyncConsumer",
    "TqdmProgress",
    "base_url",
    "concurrency",
    "delete",
    "endpoint",
    "get",
    "head",
    "patch",
    "post",
    "put",
    "ratelimit",
    "response_handler",
    "retry",
    "streaming_response_handler",
]
