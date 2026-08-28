"""Minimal typed declarative HTTP client."""

from __future__ import annotations

from mxhttp.consumer import AsyncConsumer, SyncConsumer
from mxhttp.endpoint import delete, endpoint, get, head, patch, post, put
from mxhttp.markers import Body, Cookie, Field, Header, Part, Path, Query, RawPath
from mxhttp.response import Response, response_handler, streaming_response_handler
from mxhttp.retry import Retry, retry
from mxhttp.sse import Event
from mxhttp.types import PartValue

__version__ = "1.5.0"

__all__ = [
    "AsyncConsumer",
    "Body",
    "Cookie",
    "Event",
    "Field",
    "Header",
    "Part",
    "PartValue",
    "Path",
    "Query",
    "RawPath",
    "Response",
    "Retry",
    "SyncConsumer",
    "delete",
    "endpoint",
    "get",
    "head",
    "patch",
    "post",
    "put",
    "response_handler",
    "retry",
    "streaming_response_handler",
]
