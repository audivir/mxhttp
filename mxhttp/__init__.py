"""Minimal typed declarative HTTP client."""

from __future__ import annotations

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
    ProgressCallback,
)
from mxhttp.endpoint import delete, endpoint, get, head, patch, post, put
from mxhttp.markers import Body, Cookie, Field, Header, Part, Path, Query, RawPath
from mxhttp.progress import TqdmProgress
from mxhttp.ratelimit import RateLimit, RateLimitExceededError, ratelimit
from mxhttp.response import Response, ResumeLostError, response_handler, streaming_response_handler
from mxhttp.retry import Retry, retry
from mxhttp.sse import Event
from mxhttp.types import PartValue

__version__ = "1.6.1"

__all__ = [
    "AsyncConsumer",
    "AsyncDownloader",
    "Body",
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
    "PartValue",
    "Path",
    "ProgressCallback",
    "Query",
    "RateLimit",
    "RateLimitExceededError",
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
