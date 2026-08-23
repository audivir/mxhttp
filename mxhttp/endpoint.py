"""Creates the endpoint wrapped function."""

from __future__ import annotations

import functools
import inspect
from collections.abc import AsyncIterator, Callable, Coroutine, Iterator
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    ParamSpec,
    Protocol,
    get_args,
    get_origin,
    overload,
)

from mxhttp.request import RequestSpec, build_plan, build_request
from mxhttp.response import (
    apply_response_handler,
    decode,
    sse_async,
    sse_sync,
    stream_async,
    stream_sync,
)
from mxhttp.sse import Event

if TYPE_CHECKING:
    from mxhttp import AsyncConsumer, SyncConsumer
    from mxhttp.consumer import BaseConsumer
    from mxhttp.types import AnyC_T, AsyncC_T, Method_T, Parsed_T, SyncC_T

P = ParamSpec("P")


class EndpointDecorator(Protocol):
    """Interface for sync/async return invariants based on the context type."""

    @overload
    def __call__(
        self, func: Callable[Concatenate[SyncC_T, P], Parsed_T]
    ) -> Callable[Concatenate[SyncC_T, P], Parsed_T]: ...

    @overload
    def __call__(
        self,
        func: Callable[Concatenate[AsyncC_T, P], Coroutine[Any, Any, Parsed_T]],
    ) -> Callable[Concatenate[AsyncC_T, P], Coroutine[Any, Any, Parsed_T]]: ...

    @overload
    def __call__(
        self, func: Callable[Concatenate[SyncC_T, P], None]
    ) -> Callable[Concatenate[SyncC_T, P], None]: ...

    @overload
    def __call__(
        self,
        func: Callable[Concatenate[AsyncC_T, P], Coroutine[Any, Any, None]],
    ) -> Callable[Concatenate[AsyncC_T, P], Coroutine[Any, Any, None]]: ...

    @overload
    def __call__(
        self, func: Callable[Concatenate[SyncC_T, P], Iterator[bytes]]
    ) -> Callable[Concatenate[SyncC_T, P], Iterator[bytes]]: ...

    @overload
    def __call__(
        self,
        func: Callable[Concatenate[AsyncC_T, P], Coroutine[Any, Any, AsyncIterator[bytes]]],
    ) -> Callable[Concatenate[AsyncC_T, P], Coroutine[Any, Any, AsyncIterator[bytes]]]: ...

    @overload
    def __call__(
        self, func: Callable[Concatenate[SyncC_T, P], Iterator[Event]]
    ) -> Callable[Concatenate[SyncC_T, P], Iterator[Event]]: ...

    @overload
    def __call__(
        self,
        func: Callable[Concatenate[AsyncC_T, P], Coroutine[Any, Any, AsyncIterator[Event]]],
    ) -> Callable[Concatenate[AsyncC_T, P], Coroutine[Any, Any, AsyncIterator[Event]]]: ...


def endpoint(method: Method_T, path: str) -> EndpointDecorator:  # noqa: C901
    """Shared implementation for the HTTP method decorator factories."""

    def decorate(  # noqa: C901
        func: Callable[Concatenate[AnyC_T, P], Parsed_T]
        | Callable[Concatenate[AnyC_T, P], Coroutine[Any, Any, Parsed_T]],
    ) -> (
        Callable[Concatenate[AnyC_T, P], Parsed_T]
        | Callable[Concatenate[AnyC_T, P], Coroutine[Any, Any, Parsed_T]]
    ):
        plan, return_type = build_plan(func, path)
        sig = inspect.signature(func)
        origin = get_origin(return_type)
        stream_item = get_args(return_type)[0] if origin in (Iterator, AsyncIterator) else None
        is_raw_stream = stream_item is bytes
        is_sse_stream = stream_item is Event
        has_cookies = any(p.kind == "cookie" for p in plan)

        def resolve_spec(self: BaseConsumer, *args: P.args, **kwargs: P.kwargs) -> RequestSpec:
            """Binds call arguments against the stub signature and builds the request spec."""
            bound = sig.bind(self, *args, **kwargs)
            bound.apply_defaults()
            jar = dict(self.session.cookies) if has_cookies else None
            return build_request(method, path, plan, bound.arguments, jar=jar)

        if inspect.iscoroutinefunction(func):
            if is_sse_stream:

                @functools.wraps(func)
                async def async_sse_wrapper(
                    self: AsyncConsumer, *args: P.args, **kwargs: P.kwargs
                ) -> AsyncIterator[Event]:
                    spec = resolve_spec(self, *args, **kwargs)
                    return sse_async(self, spec)

                return async_sse_wrapper  # type: ignore[return-value]

            if is_raw_stream:

                @functools.wraps(func)
                async def async_stream_wrapper(
                    self: AsyncConsumer, *args: P.args, **kwargs: P.kwargs
                ) -> AsyncIterator[bytes]:
                    spec = resolve_spec(self, *args, **kwargs)
                    return stream_async(self, spec)

                return async_stream_wrapper  # type: ignore[return-value]

            @functools.wraps(func)
            async def async_wrapper(
                self: AsyncConsumer, *args: P.args, **kwargs: P.kwargs
            ) -> Parsed_T:
                spec = resolve_spec(self, *args, **kwargs)
                response = await self.session.request(spec.method, spec.url, **spec.to_kwargs())
                return decode(apply_response_handler(self, response), return_type)

            return async_wrapper  # type: ignore[return-value]

        if is_sse_stream:

            @functools.wraps(func)
            def sync_sse_wrapper(
                self: SyncConsumer, *args: P.args, **kwargs: P.kwargs
            ) -> Iterator[Event]:
                spec = resolve_spec(self, *args, **kwargs)
                return sse_sync(self, spec)

            return sync_sse_wrapper  # type: ignore[return-value]

        if is_raw_stream:

            @functools.wraps(func)
            def sync_stream_wrapper(
                self: SyncConsumer, *args: P.args, **kwargs: P.kwargs
            ) -> Iterator[bytes]:
                spec = resolve_spec(self, *args, **kwargs)
                return stream_sync(self, spec)

            return sync_stream_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(
            self: SyncConsumer,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> Parsed_T:
            spec = resolve_spec(self, *args, **kwargs)
            response = self.session.request(spec.method, spec.url, **spec.to_kwargs())
            return decode(apply_response_handler(self, response), return_type)

        return sync_wrapper  # type: ignore[return-value]

    return decorate  # type: ignore[return-value]


def get(
    path: str,
) -> EndpointDecorator:
    """Declares a stub method as `GET {path}`."""
    return endpoint("GET", path)


def post(
    path: str,
) -> EndpointDecorator:
    """Declares a stub method as `POST {path}`."""
    return endpoint("POST", path)


def put(
    path: str,
) -> EndpointDecorator:
    """Declares a stub method as `PUT {path}`."""
    return endpoint("PUT", path)


def patch(
    path: str,
) -> EndpointDecorator:
    """Declares a stub method as `PATCH {path}`."""
    return endpoint("PATCH", path)


def delete(
    path: str,
) -> EndpointDecorator:
    """Declares a stub method as `DELETE {path}`."""
    return endpoint("DELETE", path)


def head(
    path: str,
) -> EndpointDecorator:
    """Declares a stub method as `HEAD {path}`."""
    return endpoint("HEAD", path)
