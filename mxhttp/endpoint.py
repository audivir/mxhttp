"""Creates a wrapped endpoint function."""

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

from mxhttp.parse import split_path_template
from mxhttp.ratelimit import gate_async, gate_sync
from mxhttp.request import RequestSpec, build_plan, build_request
from mxhttp.response import (
    apply_response_handler,
    decode,
    sse_async,
    sse_sync,
    stream_async,
    stream_sync,
)
from mxhttp.retry import request_async, request_sync
from mxhttp.sse import Event
from mxhttp.types import MISSING

if TYPE_CHECKING:
    from mxhttp import AsyncConsumer, SyncConsumer
    from mxhttp.consumer import BaseConsumer
    from mxhttp.ratelimit import RateLimit
    from mxhttp.retry import Retry
    from mxhttp.types import AnyC_T, AsyncC_T, Method_T, Missing, Parsed_T, SyncC_T

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


def endpoint(  # noqa: C901
    method: Method_T,
    path: str,
    retry: Retry | Missing | None = MISSING,
    ratelimit: RateLimit | Missing | None = MISSING,
) -> EndpointDecorator:
    """Shared implementation for the HTTP method decorator factories.

    Args:
        method: HTTP method to use to call the endpoint.
        path: The relative endpoint.
        retry: Overrides the class-level `Retry` config for this endpoint only.
            Pass `None` explicitly to disable retries for this endpoint only.
        ratelimit: Overrides the class-level `RateLimit` config for this endpoint only.
            Pass `None` explicitly to disable rate limiting for this endpoint only.
    """

    def decorate(  # noqa: C901
        func: Callable[Concatenate[AnyC_T, P], Parsed_T]
        | Callable[Concatenate[AnyC_T, P], Coroutine[Any, Any, Parsed_T]],
    ) -> (
        Callable[Concatenate[AnyC_T, P], Parsed_T]
        | Callable[Concatenate[AnyC_T, P], Coroutine[Any, Any, Parsed_T]]
    ):
        parsed = split_path_template(path)
        plan, return_type = build_plan(func, parsed)
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
            return build_request(method, parsed, plan, bound.arguments, jar=jar)

        def resolve_retry(self: BaseConsumer) -> Retry | None:
            """Resolves the endpoint retry override, falling back to the consumer default."""
            return self._retry if retry is MISSING else retry

        def resolve_ratelimit(self: BaseConsumer) -> RateLimit | None:
            """Resolves the endpoint rate-limit override, falling back to the consumer default."""
            return self._ratelimit if ratelimit is MISSING else ratelimit

        if inspect.iscoroutinefunction(func):
            if is_sse_stream:

                @functools.wraps(func)
                async def async_sse_wrapper(
                    self: AsyncConsumer, *args: P.args, **kwargs: P.kwargs
                ) -> AsyncIterator[Event]:
                    await gate_async(self, resolve_ratelimit(self))
                    spec = resolve_spec(self, *args, **kwargs)
                    return sse_async(self, spec)

                return async_sse_wrapper  # type: ignore[return-value]

            if is_raw_stream:

                @functools.wraps(func)
                async def async_stream_wrapper(
                    self: AsyncConsumer, *args: P.args, **kwargs: P.kwargs
                ) -> AsyncIterator[bytes]:
                    await gate_async(self, resolve_ratelimit(self))
                    spec = resolve_spec(self, *args, **kwargs)
                    return stream_async(self, spec)

                return async_stream_wrapper  # type: ignore[return-value]

            @functools.wraps(func)
            async def async_wrapper(
                self: AsyncConsumer, *args: P.args, **kwargs: P.kwargs
            ) -> Parsed_T:
                await gate_async(self, resolve_ratelimit(self))
                spec = resolve_spec(self, *args, **kwargs)
                response = await request_async(self, spec, resolve_retry(self))
                return decode(apply_response_handler(self, response), return_type)

            return async_wrapper  # type: ignore[return-value]

        if is_sse_stream:

            @functools.wraps(func)
            def sync_sse_wrapper(
                self: SyncConsumer, *args: P.args, **kwargs: P.kwargs
            ) -> Iterator[Event]:
                gate_sync(self, resolve_ratelimit(self))
                spec = resolve_spec(self, *args, **kwargs)
                return sse_sync(self, spec)

            return sync_sse_wrapper  # type: ignore[return-value]

        if is_raw_stream:

            @functools.wraps(func)
            def sync_stream_wrapper(
                self: SyncConsumer, *args: P.args, **kwargs: P.kwargs
            ) -> Iterator[bytes]:
                gate_sync(self, resolve_ratelimit(self))
                spec = resolve_spec(self, *args, **kwargs)
                return stream_sync(self, spec)

            return sync_stream_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(
            self: SyncConsumer,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> Parsed_T:
            gate_sync(self, resolve_ratelimit(self))
            spec = resolve_spec(self, *args, **kwargs)
            response = request_sync(self, spec, resolve_retry(self))
            return decode(apply_response_handler(self, response), return_type)

        return sync_wrapper  # type: ignore[return-value]

    return decorate  # type: ignore[return-value]


def get(
    path: str,
    retry: Retry | Missing | None = MISSING,
    ratelimit: RateLimit | Missing | None = MISSING,
) -> EndpointDecorator:
    """Declares a stub method as `GET {path}`."""
    return endpoint("GET", path, retry, ratelimit)


def post(
    path: str,
    retry: Retry | Missing | None = MISSING,
    ratelimit: RateLimit | Missing | None = MISSING,
) -> EndpointDecorator:
    """Declares a stub method as `POST {path}`."""
    return endpoint("POST", path, retry, ratelimit)


def put(
    path: str,
    retry: Retry | Missing | None = MISSING,
    ratelimit: RateLimit | Missing | None = MISSING,
) -> EndpointDecorator:
    """Declares a stub method as `PUT {path}`."""
    return endpoint("PUT", path, retry, ratelimit)


def patch(
    path: str,
    retry: Retry | Missing | None = MISSING,
    ratelimit: RateLimit | Missing | None = MISSING,
) -> EndpointDecorator:
    """Declares a stub method as `PATCH {path}`."""
    return endpoint("PATCH", path, retry, ratelimit)


def delete(
    path: str,
    retry: Retry | Missing | None = MISSING,
    ratelimit: RateLimit | Missing | None = MISSING,
) -> EndpointDecorator:
    """Declares a stub method as `DELETE {path}`."""
    return endpoint("DELETE", path, retry, ratelimit)


def head(
    path: str,
    retry: Retry | Missing | None = MISSING,
    ratelimit: RateLimit | Missing | None = MISSING,
) -> EndpointDecorator:
    """Declares a stub method as `HEAD {path}`."""
    return endpoint("HEAD", path, retry, ratelimit)
