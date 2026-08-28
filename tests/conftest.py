"""Configuration and fixtures for mxhttp API tests."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Literal, TypeAlias, overload

import httpx
import msgspec
import pytest
from models import ITEM

from mxhttp import AsyncConsumer, SyncConsumer

if TYPE_CHECKING:
    from mxhttp.types import AnyC_T, Parsed_T


RequestHandler: TypeAlias = "Callable[[httpx.Request], httpx.Response]"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@overload
def make_consumer(
    cls: type[AnyC_T],
    response: Callable[[httpx.Request], Parsed_T] | Parsed_T,
    *,
    status_code: int = 200,
    base_url: str = "https://api.example.com",
    track_requests: Literal[False] = False,
) -> AnyC_T: ...
@overload
def make_consumer(
    cls: type[AnyC_T],
    response: Callable[[httpx.Request], Parsed_T] | Parsed_T,
    *,
    status_code: int = 200,
    base_url: str = "https://api.example.com",
    track_requests: Literal[True] = ...,
) -> tuple[AnyC_T, list[httpx.Request]]: ...
def make_consumer(
    cls: type[AnyC_T],
    response: Callable[[httpx.Request], Parsed_T] | Parsed_T,
    *,
    status_code: int = 200,
    base_url: str = "https://api.example.com",
    track_requests: bool = False,
) -> AnyC_T | tuple[AnyC_T, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)

        r = response(request) if isinstance(response, Callable) else response  # type: ignore[operator,arg-type]

        if isinstance(r, httpx.Response):
            http_response: httpx.Response = r
        elif isinstance(r, bytes | str):
            http_response = httpx.Response(status_code, content=r)
        else:
            http_response = httpx.Response(status_code, json=msgspec.to_builtins(r))

        return http_response

    transport = httpx.MockTransport(handler)
    consumer = cls(base_url)

    if issubclass(cls, SyncConsumer):
        consumer._session = httpx.Client(base_url=consumer.base_url, transport=transport)
    elif issubclass(cls, AsyncConsumer):
        consumer._session = httpx.AsyncClient(base_url=consumer.base_url, transport=transport)
    else:
        raise TypeError(f"Unsupported consumer class: {cls.__name__}")
    if track_requests:
        return consumer, seen
    return consumer


def make_stateful_consumer(
    cls: type[AnyC_T], steps: Sequence[int | httpx.Response | Exception]
) -> tuple[AnyC_T, Callable[[], int]]:
    """Builds a consumer whose transport walks through `steps` in order, repeating the last."""
    calls = 0

    def handler(unused_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        step = steps[min(calls, len(steps) - 1)]
        calls += 1
        if isinstance(step, Exception):
            raise step
        if isinstance(step, int):  # pragma: no branch
            step = httpx.Response(step, json=msgspec.to_builtins(ITEM))
        assert isinstance(step, httpx.Response)
        return step

    transport = httpx.MockTransport(handler)
    consumer = cls("https://api.example.com")
    if issubclass(cls, SyncConsumer):
        consumer._session = httpx.Client(base_url=consumer.base_url, transport=transport)
    else:
        consumer._session = httpx.AsyncClient(base_url=consumer.base_url, transport=transport)
    return consumer, lambda: calls
