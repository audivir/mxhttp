"""Creates a wrapped endpoint function."""

from __future__ import annotations

import functools
import inspect
import uuid
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

from mxhttp.checksum import ChecksumInput, resolve_checksum
from mxhttp.concurrency import Concurrency, gate_concurrency_async, gate_concurrency_sync
from mxhttp.consumer import validate_scheme
from mxhttp.cookies import resolve_cookies
from mxhttp.download import AsyncDownloader, Downloader, Parts, resolve_parts
from mxhttp.headers import resolve_headers
from mxhttp.parse import split_path_template
from mxhttp.ratelimit import gate_async, gate_sync
from mxhttp.request import RequestSpec, build_plan, build_request
from mxhttp.response import (
    apply_response_handler,
    decode,
    resumable_stream_async,
    resumable_stream_sync,
    sse_async,
    sse_sync,
    stream_async,
    stream_sync,
)
from mxhttp.retry import Retry, request_async, request_sync
from mxhttp.sse import Event
from mxhttp.types import MISSING

if TYPE_CHECKING:
    from mxhttp import AsyncConsumer, SyncConsumer
    from mxhttp.consumer import BaseConsumer
    from mxhttp.cookies import CookiesInput
    from mxhttp.headers import HeadersInput
    from mxhttp.ratelimit import RateLimit
    from mxhttp.types import AnyC_T, AsyncC_T, Method_T, Missing, Parsed_T, SyncC_T

P = ParamSpec("P")

DOWNLOAD_DEFAULT_RETRY = Retry(attempts=5, max_delay=60.0)
"""Fallback retry budget for `Downloader`/`AsyncDownloader` when `resumable` isn't given.

Higher than the generic per-request `Retry()` default (3 attempts, 30s max delay): with `parts=`,
several segments retry independently against the same host, so a transient rate limit is far more
likely to be re-tripped by a sibling segment mid-backoff than it would be for a single ordinary
request. More attempts and a longer ceiling give the whole download more headroom to outlast a
shared limit before any one segment exhausts its budget and aborts the rest via the task group.
"""


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

    @overload
    def __call__(
        self, func: Callable[Concatenate[SyncC_T, P], Downloader]
    ) -> Callable[Concatenate[SyncC_T, P], Downloader]: ...

    @overload
    def __call__(
        self,
        func: Callable[Concatenate[AsyncC_T, P], Coroutine[Any, Any, AsyncDownloader]],
    ) -> Callable[Concatenate[AsyncC_T, P], Coroutine[Any, Any, AsyncDownloader]]: ...


def validate_endpoint_kinds(  # noqa: C901, PLR0912, PLR0913
    method: Method_T,
    resumable: Retry | None,
    parts: int | Parts | None,
    checksum: ChecksumInput = None,
    idempotent: bool | Callable[[], str] = False,
    *,
    is_raw_stream: bool,
    is_downloader: bool,
    is_async_downloader: bool,
    is_coroutine: bool,
) -> None:
    """Rejects invalid combinations of resumable, Downloader, parts, checksum, and idempotent."""
    if resumable is not None:
        if method != "GET":
            raise TypeError("resumable is only valid for GET endpoints")
        if not is_raw_stream and not (is_downloader or is_async_downloader):
            raise TypeError(
                "resumable is only valid for Iterator[bytes]/AsyncIterator[bytes]/"
                "Downloader/AsyncDownloader endpoints"
            )

    if idempotent is not False and method not in ("POST", "PUT"):
        raise TypeError("idempotent is only valid for POST/PUT endpoints")

    if parts is not None:
        if method != "GET":
            raise TypeError("parts is only valid for GET endpoints")
        if not (is_downloader or is_async_downloader):
            raise TypeError("parts is only valid for Downloader/AsyncDownloader endpoints")

    if checksum is not None:
        if method != "GET":
            raise TypeError("checksum is only valid for GET endpoints")
        if not (is_downloader or is_async_downloader):
            raise TypeError("checksum is only valid for Downloader/AsyncDownloader endpoints")

    if is_downloader or is_async_downloader:
        if method != "GET":
            raise TypeError("Downloader/AsyncDownloader is only valid for GET endpoints")
        if is_downloader and is_coroutine:
            raise TypeError("an async method must return AsyncDownloader, not Downloader")
        if is_async_downloader and not is_coroutine:
            raise TypeError("a sync method must return Downloader, not AsyncDownloader")


def endpoint(  # noqa: C901, PLR0913, PLR0915, PLR0917
    method: Method_T,
    path: str,
    retry: Retry | Missing | None = MISSING,
    ratelimit: RateLimit | Missing | None = MISSING,
    resumable: Retry | None = None,
    base_url: str | Missing = MISSING,
    concurrency: int | Concurrency | Missing | None = MISSING,
    parts: int | Parts | None = None,
    checksum: ChecksumInput = None,
    headers: HeadersInput | Missing | None = MISSING,
    cookies: CookiesInput | Missing | None = MISSING,
    idempotent: bool | Callable[[], str] = False,
) -> EndpointDecorator:
    """Shared implementation for the HTTP method decorator factories.

    Args:
        method: HTTP method to use to call the endpoint.
        path: The relative endpoint.
        retry: Overrides the class-level `Retry` config for this endpoint only.
            Pass `None` explicitly to disable retries for this endpoint only.
        ratelimit: Overrides the class-level `RateLimit` config for this endpoint only.
            Pass `None` explicitly to disable rate limiting for this endpoint only.
        resumable: Reconnects with `Range` (using this `Retry` for reconnect attempts and
            backoff) if the connection drops mid-stream. Only valid for `GET` endpoints
            returning `Iterator[bytes]`/`AsyncIterator[bytes]`/`Downloader`/`AsyncDownloader`.
            For `Downloader`/`AsyncDownloader`, defaults to `DOWNLOAD_DEFAULT_RETRY` rather than
            disabling reconnects, since resumability is the entire point of that return type.
        base_url: Overrides the class-level base URL for this endpoint only.
        concurrency: Overrides the class-level `Concurrency` config for this endpoint only.
            Pass `None` explicitly to disable concurrency limits for this endpoint only.
        parts: Configures multi-part parallel segmented download for `Downloader`/`AsyncDownloader`.
        checksum: Configures checksum validation for `Downloader`/`AsyncDownloader`.
        headers: Overrides the class-level `@headers` default for this endpoint only, replacing
            it outright rather than merging. Pass `None` explicitly to disable it for this
            endpoint only.
        cookies: Overrides the class-level `@cookies` default for this endpoint only, replacing
            it outright rather than merging. Pass `None` explicitly to disable it for this
            endpoint only.
        idempotent: Attaches an `Idempotency-Key` header, stable across `@retry`'s attempts of
            the same call (generated once per call, before retries begin). `False` (default)
            attaches nothing. `True` generates a fresh `uuid.uuid4()` per call. A `Callable[[],
            str]` generates the key itself, called once per call. Only valid for `POST`/`PUT`.
            `Idempotency-Key` is reserved the same way `Content-Type`/`Cookie` are: it cannot be
            set through a `Header` parameter, a header bag, or `@headers`.
    """
    if path.startswith(("http://", "https://")):
        if base_url is not MISSING:
            raise ValueError("Cannot specify base_url when path is already an absolute URL")
    elif base_url is not MISSING:
        validate_scheme(base_url)

    def decorate(  # noqa: C901, PLR0911, PLR0915
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
        return_type_obj: object = return_type
        is_downloader = return_type_obj is Downloader
        is_async_downloader = return_type_obj is AsyncDownloader
        has_cookies = any(p.kind == "cookie" for p in plan)
        is_coroutine = inspect.iscoroutinefunction(func)

        validate_endpoint_kinds(
            method,
            resumable,
            parts,
            checksum,
            idempotent,
            is_raw_stream=is_raw_stream,
            is_downloader=is_downloader,
            is_async_downloader=is_async_downloader,
            is_coroutine=is_coroutine,
        )

        def resolve_base_url(self: BaseConsumer) -> str | None:
            """Resolves the endpoint base URL override, falling back to the consumer base URL."""
            resolved = self._base_url if base_url is MISSING else base_url
            if resolved is None and not path.startswith(("http://", "https://")):
                raise ValueError(
                    f"Cannot call relative endpoint {path!r} without a base_url configured "
                    f"on {type(self).__name__} or @{method.lower()}()"
                )
            return resolved

        def resolve_class_headers(self: BaseConsumer) -> HeadersInput | None:
            """Resolves the endpoint headers override, falling back to the consumer default."""
            return self._headers if headers is MISSING else headers

        def resolve_class_cookies(self: BaseConsumer) -> CookiesInput | None:
            """Resolves the endpoint cookies override, falling back to the consumer default."""
            return self._cookies if cookies is MISSING else cookies

        def resolve_idempotency_key() -> str | None:
            """Generates a fresh idempotency key for one call, or `None` if not configured.

            Called once per call, from `resolve_spec()`, before `@retry` starts resending the
            built `RequestSpec` — so every retry of one call reuses the same key.
            """
            if idempotent is False:
                return None
            return str(uuid.uuid4()) if idempotent is True else idempotent()

        def resolve_spec(self: BaseConsumer, *args: P.args, **kwargs: P.kwargs) -> RequestSpec:
            """Binds call arguments against the stub signature and builds the request spec."""
            bound = sig.bind(self, *args, **kwargs)
            bound.apply_defaults()
            class_cookies = resolve_class_cookies(self)
            jar = dict(self.session.cookies) if has_cookies or class_cookies is not None else None
            return build_request(
                method,
                parsed,
                plan,
                bound.arguments,
                jar=jar,
                base_url=resolve_base_url(self),
                default_headers=resolve_headers(self, resolve_class_headers(self)),
                default_cookies=resolve_cookies(self, class_cookies),
                idempotency_key=resolve_idempotency_key(),
            )

        def resolve_retry(self: BaseConsumer) -> Retry | None:
            """Resolves the endpoint retry override, falling back to the consumer default."""
            return self._retry if retry is MISSING else retry

        def resolve_ratelimit(self: BaseConsumer) -> RateLimit | None:
            """Resolves the endpoint rate-limit override, falling back to the consumer default."""
            return self._ratelimit if ratelimit is MISSING else ratelimit

        def resolve_concurrency(self: BaseConsumer) -> Concurrency | None:
            """Resolves the endpoint concurrency override, falling back to consumer default."""
            if concurrency is MISSING:
                return self._concurrency
            if isinstance(concurrency, int):
                return Concurrency(limit=concurrency)
            return concurrency

        if is_coroutine:
            if is_async_downloader:

                @functools.wraps(func)
                async def async_downloader_wrapper(
                    self: AsyncConsumer, *args: P.args, **kwargs: P.kwargs
                ) -> AsyncDownloader:
                    spec = resolve_spec(self, *args, **kwargs)
                    return AsyncDownloader(
                        self,
                        spec,
                        resumable or DOWNLOAD_DEFAULT_RETRY,
                        resolve_ratelimit(self),
                        resolve_concurrency(self),
                        resolve_parts(parts),
                        resolve_checksum(checksum),
                    )

                return async_downloader_wrapper  # type: ignore[return-value]

            if is_sse_stream:

                @functools.wraps(func)
                async def async_sse_wrapper(
                    self: AsyncConsumer, *args: P.args, **kwargs: P.kwargs
                ) -> AsyncIterator[Event]:
                    await gate_async(self, resolve_ratelimit(self))
                    spec = resolve_spec(self, *args, **kwargs)
                    return sse_async(self, spec, resolve_concurrency(self))

                return async_sse_wrapper  # type: ignore[return-value]

            if is_raw_stream:

                @functools.wraps(func)
                async def async_stream_wrapper(
                    self: AsyncConsumer, *args: P.args, **kwargs: P.kwargs
                ) -> AsyncIterator[bytes]:
                    await gate_async(self, resolve_ratelimit(self))
                    spec = resolve_spec(self, *args, **kwargs)
                    if resumable is not None:
                        return resumable_stream_async(
                            self, spec, resumable, resolve_concurrency(self)
                        )
                    return stream_async(self, spec, resolve_concurrency(self))

                return async_stream_wrapper  # type: ignore[return-value]

            @functools.wraps(func)
            async def async_wrapper(
                self: AsyncConsumer, *args: P.args, **kwargs: P.kwargs
            ) -> Parsed_T:
                async with gate_concurrency_async(self, resolve_concurrency(self)):
                    await gate_async(self, resolve_ratelimit(self))
                    spec = resolve_spec(self, *args, **kwargs)
                    response = await request_async(self, spec, resolve_retry(self))
                    return decode(apply_response_handler(self, response), return_type)

            return async_wrapper  # type: ignore[return-value]

        if is_downloader:

            @functools.wraps(func)
            def downloader_wrapper(
                self: SyncConsumer, *args: P.args, **kwargs: P.kwargs
            ) -> Downloader:
                spec = resolve_spec(self, *args, **kwargs)
                return Downloader(
                    self,
                    spec,
                    resumable or DOWNLOAD_DEFAULT_RETRY,
                    resolve_ratelimit(self),
                    resolve_concurrency(self),
                    resolve_parts(parts),
                    resolve_checksum(checksum),
                )

            return downloader_wrapper  # type: ignore[return-value]

        if is_sse_stream:

            @functools.wraps(func)
            def sync_sse_wrapper(
                self: SyncConsumer, *args: P.args, **kwargs: P.kwargs
            ) -> Iterator[Event]:
                gate_sync(self, resolve_ratelimit(self))
                spec = resolve_spec(self, *args, **kwargs)
                return sse_sync(self, spec, resolve_concurrency(self))

            return sync_sse_wrapper  # type: ignore[return-value]

        if is_raw_stream:

            @functools.wraps(func)
            def sync_stream_wrapper(
                self: SyncConsumer, *args: P.args, **kwargs: P.kwargs
            ) -> Iterator[bytes]:
                gate_sync(self, resolve_ratelimit(self))
                spec = resolve_spec(self, *args, **kwargs)
                if resumable is not None:
                    return resumable_stream_sync(self, spec, resumable, resolve_concurrency(self))
                return stream_sync(self, spec, resolve_concurrency(self))

            return sync_stream_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def sync_wrapper(
            self: SyncConsumer,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> Parsed_T:
            with gate_concurrency_sync(self, resolve_concurrency(self)):
                gate_sync(self, resolve_ratelimit(self))
                spec = resolve_spec(self, *args, **kwargs)
                response = request_sync(self, spec, resolve_retry(self))
                return decode(apply_response_handler(self, response), return_type)

        return sync_wrapper  # type: ignore[return-value]

    return decorate  # type: ignore[return-value]


def get(  # noqa: PLR0913,PLR0917
    path: str,
    retry: Retry | Missing | None = MISSING,
    ratelimit: RateLimit | Missing | None = MISSING,
    resumable: Retry | None = None,
    base_url: str | Missing = MISSING,
    concurrency: int | Concurrency | Missing | None = MISSING,
    parts: int | Parts | None = None,
    checksum: ChecksumInput = None,
    headers: HeadersInput | Missing | None = MISSING,
    cookies: CookiesInput | Missing | None = MISSING,
) -> EndpointDecorator:
    """Declares a stub method as `GET {path}`."""
    return endpoint(
        "GET",
        path,
        retry,
        ratelimit,
        resumable,
        base_url,
        concurrency,
        parts,
        checksum,
        headers,
        cookies,
    )


def post(  # noqa: PLR0913,PLR0917
    path: str,
    retry: Retry | Missing | None = MISSING,
    ratelimit: RateLimit | Missing | None = MISSING,
    base_url: str | Missing = MISSING,
    concurrency: int | Concurrency | Missing | None = MISSING,
    headers: HeadersInput | Missing | None = MISSING,
    cookies: CookiesInput | Missing | None = MISSING,
    idempotent: bool | Callable[[], str] = False,
) -> EndpointDecorator:
    """Declares a stub method as `POST {path}`."""
    return endpoint(
        "POST",
        path,
        retry,
        ratelimit,
        base_url=base_url,
        concurrency=concurrency,
        headers=headers,
        cookies=cookies,
        idempotent=idempotent,
    )


def put(  # noqa: PLR0913,PLR0917
    path: str,
    retry: Retry | Missing | None = MISSING,
    ratelimit: RateLimit | Missing | None = MISSING,
    base_url: str | Missing = MISSING,
    concurrency: int | Concurrency | Missing | None = MISSING,
    headers: HeadersInput | Missing | None = MISSING,
    cookies: CookiesInput | Missing | None = MISSING,
    idempotent: bool | Callable[[], str] = False,
) -> EndpointDecorator:
    """Declares a stub method as `PUT {path}`."""
    return endpoint(
        "PUT",
        path,
        retry,
        ratelimit,
        base_url=base_url,
        concurrency=concurrency,
        headers=headers,
        cookies=cookies,
        idempotent=idempotent,
    )


def patch(  # noqa: PLR0913,PLR0917
    path: str,
    retry: Retry | Missing | None = MISSING,
    ratelimit: RateLimit | Missing | None = MISSING,
    base_url: str | Missing = MISSING,
    concurrency: int | Concurrency | Missing | None = MISSING,
    headers: HeadersInput | Missing | None = MISSING,
    cookies: CookiesInput | Missing | None = MISSING,
) -> EndpointDecorator:
    """Declares a stub method as `PATCH {path}`."""
    return endpoint(
        "PATCH",
        path,
        retry,
        ratelimit,
        base_url=base_url,
        concurrency=concurrency,
        headers=headers,
        cookies=cookies,
    )


def delete(  # noqa: PLR0913,PLR0917
    path: str,
    retry: Retry | Missing | None = MISSING,
    ratelimit: RateLimit | Missing | None = MISSING,
    base_url: str | Missing = MISSING,
    concurrency: int | Concurrency | Missing | None = MISSING,
    headers: HeadersInput | Missing | None = MISSING,
    cookies: CookiesInput | Missing | None = MISSING,
) -> EndpointDecorator:
    """Declares a stub method as `DELETE {path}`."""
    return endpoint(
        "DELETE",
        path,
        retry,
        ratelimit,
        base_url=base_url,
        concurrency=concurrency,
        headers=headers,
        cookies=cookies,
    )


def head(  # noqa: PLR0913,PLR0917
    path: str,
    retry: Retry | Missing | None = MISSING,
    ratelimit: RateLimit | Missing | None = MISSING,
    base_url: str | Missing = MISSING,
    concurrency: int | Concurrency | Missing | None = MISSING,
    headers: HeadersInput | Missing | None = MISSING,
    cookies: CookiesInput | Missing | None = MISSING,
) -> EndpointDecorator:
    """Declares a stub method as `HEAD {path}`."""
    return endpoint(
        "HEAD",
        path,
        retry,
        ratelimit,
        base_url=base_url,
        concurrency=concurrency,
        headers=headers,
        cookies=cookies,
    )
