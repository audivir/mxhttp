"""Configuration and fixtures for mxhttp API tests."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, TypeAlias, overload

import httpx
import msgspec
import pytest

from mxhttp import AsyncConsumer, SyncConsumer

if TYPE_CHECKING:
    from collections.abc import Callable

    from mxhttp.types import AnyC_T, Parsed_T


RequestHandler: TypeAlias = "Callable[[httpx.Request], httpx.Response]"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@overload
def make_consumer(
    cls: type[AnyC_T],
    response: Parsed_T,
    *,
    status_code: int = 200,
    track_requests: Literal[False] = False,
) -> AnyC_T: ...
@overload
def make_consumer(
    cls: type[AnyC_T],
    response: Parsed_T,
    *,
    status_code: int = 200,
    track_requests: Literal[True] = ...,
) -> tuple[AnyC_T, list[httpx.Request]]: ...
def make_consumer(
    cls: type[AnyC_T],
    response: Parsed_T,
    *,
    status_code: int = 200,
    track_requests: bool = False,
) -> AnyC_T | tuple[AnyC_T, list[httpx.Request]]:
    if isinstance(response, httpx.Response):
        http_response: httpx.Response = response
    elif isinstance(response, bytes | str):
        http_response = httpx.Response(status_code, content=response)
    else:
        http_response = httpx.Response(status_code, json=msgspec.to_builtins(response))

    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return http_response

    transport = httpx.MockTransport(handler)
    consumer = cls("https://api.example.com")

    if issubclass(cls, SyncConsumer):
        consumer._session = httpx.Client(base_url=consumer.base_url, transport=transport)
    elif issubclass(cls, AsyncConsumer):
        consumer._session = httpx.AsyncClient(base_url=consumer.base_url, transport=transport)
    else:
        raise TypeError(f"Unsupported consumer class: {cls.__name__}")
    if track_requests:
        return consumer, seen
    return consumer
