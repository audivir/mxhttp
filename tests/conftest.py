"""Configuration and fixtures for mxhttp API tests."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, TypeAlias, overload

import httpx
import msgspec
import pytest
from models import ITEM

from mxhttp import AsyncConsumer, SyncConsumer

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from mxhttp.types import AnyC_T


RequestHandler: TypeAlias = "Callable[[httpx.Request], httpx.Response]"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@overload
def make_consumer(
    cls: type[AnyC_T],
    response: Callable[[httpx.Request], Any] | Any,
    *,
    status_code: int = 200,
    base_url: str | None = None,
    auth: httpx.Auth | tuple[str, str] | None = None,
    track_requests: Literal[False] = False,
) -> AnyC_T: ...
@overload
def make_consumer(
    cls: type[AnyC_T],
    response: Callable[[httpx.Request], Any] | Any,
    *,
    status_code: int = 200,
    base_url: str | None = None,
    auth: httpx.Auth | tuple[str, str] | None = None,
    track_requests: Literal[True] = ...,
) -> tuple[AnyC_T, list[httpx.Request]]: ...
def make_consumer(  # noqa: C901,PLR0913
    cls: type[AnyC_T],
    response: Callable[[httpx.Request], Any] | Any,
    *,
    status_code: int = 200,
    base_url: str | None = None,
    auth: httpx.Auth | tuple[str, str] | None = None,
    track_requests: bool = False,
) -> AnyC_T | tuple[AnyC_T, list[httpx.Request]]:
    import inspect

    seen: list[httpx.Request] = []

    def to_response(r: object) -> httpx.Response:
        if isinstance(r, httpx.Response):
            return r
        if isinstance(r, bytes | str):
            return httpx.Response(status_code, content=r)
        return httpx.Response(status_code, json=msgspec.to_builtins(r))

    def sync_handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        r = response(request) if callable(response) else response
        return to_response(r)

    async def async_handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if callable(response):
            r = response(request)
            if inspect.isawaitable(r):
                r = await r
        else:
            r = response
        return to_response(r)

    consumer = cls(auth=auth)
    if base_url is not None:
        consumer._base_url = base_url
    consumer._base_url = consumer._base_url or "https://api.example.com"

    if issubclass(cls, SyncConsumer):
        consumer._session = httpx.Client(transport=httpx.MockTransport(sync_handler), auth=auth)
    elif issubclass(cls, AsyncConsumer):
        consumer._session = httpx.AsyncClient(
            transport=httpx.MockTransport(async_handler), auth=auth
        )
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
    consumer = cls()
    consumer._base_url = consumer._base_url or "https://api.example.com"
    if issubclass(cls, SyncConsumer):
        consumer._session = httpx.Client(transport=transport)
    else:
        consumer._session = httpx.AsyncClient(transport=transport)
    return consumer, lambda: calls
