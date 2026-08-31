"""Minimal typed declarative HTTP client."""

from __future__ import annotations

from mxhttp.auth import ApiKeyAuth, BearerAuth
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
from mxhttp.cookies import CookiesInput, cookies
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
from mxhttp.headers import HeadersInput, headers
from mxhttp.markers import Body, Cookie, Field, Header, Part, Path, Query, RawBody, RawPath
from mxhttp.progress import TqdmProgress
from mxhttp.ratelimit import RateLimit, RateLimitExceededError, ratelimit
from mxhttp.request import RequestSpec, request_handler
from mxhttp.response import Response, ResumeLostError, response_handler, streaming_response_handler
from mxhttp.retry import Retry, retry
from mxhttp.sse import Event
from mxhttp.types import PartValue

__version__ = "1.7.1"

__all__ = [
    "KNOWN_ALGORITHMS",
    "ApiKeyAuth",
    "AsyncConsumer",
    "AsyncDownloader",
    "BearerAuth",
    "Body",
    "Checksum",
    "ChecksumCallback",
    "ChecksumInput",
    "ChecksumMismatchError",
    "Concurrency",
    "ConcurrencyExceededError",
    "ConcurrencyTimeoutError",
    "Cookie",
    "CookiesInput",
    "DownloadIdentityError",
    "DownloadLockError",
    "DownloadState",
    "Downloader",
    "Event",
    "Field",
    "Header",
    "HeadersInput",
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
    "RequestSpec",
    "Response",
    "ResumeLostError",
    "Retry",
    "SyncConsumer",
    "TqdmProgress",
    "base_url",
    "concurrency",
    "cookies",
    "delete",
    "endpoint",
    "get",
    "head",
    "headers",
    "patch",
    "post",
    "put",
    "ratelimit",
    "request_handler",
    "response_handler",
    "retry",
    "streaming_response_handler",
]
