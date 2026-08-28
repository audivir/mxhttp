"""Minimal typed declarative HTTP client."""

from __future__ import annotations

from mxhttp.consumer import AsyncConsumer, SyncConsumer
from mxhttp.download import (
    AsyncDownloader,
    Downloader,
    DownloadIdentityError,
    DownloadState,
    ProgressCallback,
)
from mxhttp.endpoint import delete, endpoint, get, head, patch, post, put
from mxhttp.markers import Body, Cookie, Field, Header, Part, Path, Query, RawPath
from mxhttp.ratelimit import RateLimit, RateLimitExceededError, ratelimit
from mxhttp.response import Response, ResumeLostError, response_handler, streaming_response_handler
from mxhttp.retry import Retry, retry
from mxhttp.sse import Event
from mxhttp.types import PartValue

__version__ = "1.5.6"

__all__ = [
    "AsyncConsumer",
    "AsyncDownloader",
    "Body",
    "Cookie",
    "DownloadIdentityError",
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
