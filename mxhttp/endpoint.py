"""Creates a wrapped endpoint function."""

from __future__ import annotations

import functools
import inspect
import uuid
from collections.abc import AsyncIterator, Callable, Coroutine, Iterator, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    Generic,
    ParamSpec,
    Protocol,
    TypeAlias,
    TypeVar,
    get_args,
    get_origin,
    overload,
)

import msgspec
from typing_extensions import Self, TypedDict

from mxhttp.checksum import ChecksumInput, resolve_checksum
from mxhttp.concurrency import Concurrency, gate_concurrency_async, gate_concurrency_sync
from mxhttp.consumer import validate_scheme
from mxhttp.cookies import resolve_cookies
from mxhttp.download import (
    DOWNLOAD_DEFAULT_RETRY,
    AsyncDownloader,
    Downloader,
    Parts,
    resolve_parts,
)
from mxhttp.headers import resolve_headers
from mxhttp.parse import split_path_template
from mxhttp.ratelimit import gate_async, gate_sync
from mxhttp.request import ParamPlan, RequestSpec, build_plan, build_request
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
    from typing_extensions import Unpack

    from mxhttp import AsyncConsumer, SyncConsumer
    from mxhttp.checksum import Checksum
    from mxhttp.consumer import BaseConsumer
    from mxhttp.cookies import CookiesInput
    from mxhttp.headers import HeadersInput
    from mxhttp.parse import ParsedPath
    from mxhttp.ratelimit import RateLimit
    from mxhttp.types import (
        AnyC_T,
        AsyncC_T,
        Method_T,
        Missing,
        Parsed_T,
        RequestHandler,
        ResponseHandler,
        SyncC_T,
    )

P = ParamSpec("P")
Concurrency_T = TypeVar("Concurrency_T", "int | Concurrency", "Concurrency")
Missing_T = TypeVar("Missing_T", "Missing", None)


class BaseEndpointKwargs(TypedDict, Generic[Concurrency_T, Missing_T], total=False):
    """Stores endpoint overrides shared across every HTTP method decorator.

    Attributes:
        retry: Overrides the class-level `Retry` config for this endpoint only.
            Pass `None` explicitly to disable retries for this endpoint only.
        ratelimit: Overrides the class-level `RateLimit` config for this endpoint only.
            Pass `None` explicitly to disable rate limiting for this endpoint only.
        base_url: Overrides the class-level base URL for this endpoint only.
        concurrency: Overrides the class-level `Concurrency` config for this endpoint only.
            Pass `None` explicitly to disable concurrency limits for this endpoint only.
        headers: Overrides the class-level `@headers` default for this endpoint only, replacing
            it outright rather than merging. Pass `None` explicitly to disable it for this
            endpoint only.
        cookies: Overrides the class-level `@cookies` default for this endpoint only, replacing
            it outright rather than merging. Pass `None` explicitly to disable it for this
            endpoint only.
        request_handler: Overrides the class-level `@request_handler` default for this endpoint
            only, replacing it outright rather than composing. Pass `None` explicitly to disable
            it for this endpoint only.
        response_handler: Overrides the class-level `@response_handler` default for this endpoint
            only. Ignored for streaming endpoints; see `streaming_response_handler`.
        streaming_response_handler: Overrides the class-level `@streaming_response_handler`
            default for this endpoint only. Ignored for non-streaming endpoints.
    """

    retry: Retry | Missing_T | None
    ratelimit: RateLimit | Missing_T | None
    base_url: str | Missing_T
    concurrency: Concurrency_T | Missing_T | None
    headers: HeadersInput | Missing_T | None
    cookies: CookiesInput | Missing_T | None
    request_handler: RequestHandler | Missing_T | None
    response_handler: ResponseHandler | Missing_T | None
    streaming_response_handler: ResponseHandler | Missing_T | None


EndpointKwargs: TypeAlias = "BaseEndpointKwargs[int | Concurrency, Missing]"
ResolvedEndpointKwargs: TypeAlias = "BaseEndpointKwargs[Concurrency, None]"


class EndpointConfiguration(msgspec.Struct, frozen=True):
    """Stores the resolved shape of one endpoint: stream/downloader kind and coroutine-ness."""

    is_raw_stream: bool
    is_sse_stream: bool
    is_downloader: bool
    is_async_downloader: bool
    has_cookies: bool
    is_coroutine: bool

    @classmethod
    def build(
        cls,
        func: Callable[Concatenate[AnyC_T, P], Parsed_T]
        | Callable[Concatenate[AnyC_T, P], Coroutine[Any, Any, Parsed_T]],
        plan: Sequence[ParamPlan],
        return_type: type[Parsed_T],
    ) -> Self:
        """Derives the endpoint's shape from its return type annotation and parameter plan."""
        origin = get_origin(return_type)
        stream_item = get_args(return_type)[0] if origin in (Iterator, AsyncIterator) else None
        return_type_obj: object = return_type
        return cls(
            is_raw_stream=stream_item is bytes,
            is_sse_stream=stream_item is Event,
            is_downloader=return_type_obj is Downloader,
            is_async_downloader=return_type_obj is AsyncDownloader,
            has_cookies=any(p.kind == "cookie" for p in plan),
            is_coroutine=inspect.iscoroutinefunction(func),
        )

    @property
    def is_any_downloader(self) -> bool:
        """Whether the endpoint returns `Downloader` or `AsyncDownloader`."""
        return self.is_downloader or self.is_async_downloader

    @property
    def is_any_stream(self) -> bool:
        """Whether the endpoint returns `Iterator` or `AsyncIterator`."""
        return self.is_raw_stream or self.is_sse_stream

    def validate(  # noqa: C901, PLR0912
        self,
        method: Method_T,
        resumable: Retry | None,
        parts: int | Parts | None,
        checksum: ChecksumInput = None,
        idempotent: bool | Callable[[], str] = False,
    ) -> None:
        """Rejects invalid combinations of resumable/Downloader/parts/checksum/idempotent."""
        if resumable is not None:
            if method != "GET":
                raise TypeError("resumable is only valid for GET endpoints")
            if not self.is_raw_stream and not self.is_any_downloader:
                raise TypeError(
                    "resumable is only valid for Iterator[bytes]/AsyncIterator[bytes]/"
                    "Downloader/AsyncDownloader endpoints"
                )

        if idempotent is not False and method not in ("POST", "PUT"):
            raise TypeError("idempotent is only valid for POST/PUT endpoints")

        if parts is not None:
            if method != "GET":
                raise TypeError("parts is only valid for GET endpoints")
            if not self.is_any_downloader:
                raise TypeError("parts is only valid for Downloader/AsyncDownloader endpoints")

        if checksum is not None:
            if method != "GET":
                raise TypeError("checksum is only valid for GET endpoints")
            if not self.is_any_downloader:
                raise TypeError("checksum is only valid for Downloader/AsyncDownloader endpoints")

        if self.is_any_downloader:
            if method != "GET":
                raise TypeError("Downloader/AsyncDownloader is only valid for GET endpoints")
            if self.is_downloader and self.is_coroutine:
                raise TypeError("an async method must return AsyncDownloader, not Downloader")
            if self.is_async_downloader and not self.is_coroutine:
                raise TypeError("a sync method must return Downloader, not AsyncDownloader")


class EndpointRuntime(msgspec.Struct, Generic[P], frozen=True):
    """Stores the per-endpoint invariants computed once at decoration time."""

    method: Method_T
    path: str
    parsed: ParsedPath
    plan: list[ParamPlan]
    sig: inspect.Signature
    conf: EndpointConfiguration
    idempotent: bool | Callable[[], str]
    endpoint_kwargs: EndpointKwargs


def resolve_endpoint_kwargs(
    self: BaseConsumer, runtime: EndpointRuntime[P]
) -> ResolvedEndpointKwargs:
    """Resolves per-call config, falling back to the consumer's class-level defaults."""
    resolved: ResolvedEndpointKwargs = {}
    for key in BaseEndpointKwargs.__required_keys__ | BaseEndpointKwargs.__optional_keys__:
        raw_value = runtime.endpoint_kwargs.get(key, MISSING)
        class_value = self._class_endpoint_kwargs.get(key)
        if key == "base_url":
            # base_url has its own attribute (like _response_handler), not a
            # _class_endpoint_kwargs entry: @base_url must also support the constructor's
            # per-instance base_url= override, which the other five configs don't have.
            class_value = self._base_url
        value = class_value if raw_value is MISSING else raw_value
        if key == "concurrency" and isinstance(value, int):
            value = Concurrency(limit=value)
        elif (
            key == "base_url"
            and value is None
            and not runtime.path.startswith(("http://", "https://"))
        ):
            raise ValueError(
                f"Cannot call relative endpoint {runtime.path!r} without a base_url configured "
                f"on {type(self).__name__} or @{runtime.method.lower()}()"
            )
        resolved[key] = value  # type: ignore[literal-required]
    return resolved


def resolve_idempotency_key(idempotent: bool | Callable[[], str]) -> str | None:
    """Generates a fresh idempotency key for one call, or `None` if not configured."""
    if idempotent is False:
        return None
    return str(uuid.uuid4()) if idempotent is True else idempotent()


def resolve_gate_url(runtime: EndpointRuntime[Any], resolved: ResolvedEndpointKwargs) -> str | None:
    """Picks the per-call host to key rate-limit/concurrency gates by, mirroring join_url()."""
    if runtime.path.startswith(("http://", "https://")):
        return runtime.path
    return resolved["base_url"]


def resolve_and_gate_sync(
    self: BaseConsumer, runtime: EndpointRuntime[P]
) -> ResolvedEndpointKwargs:
    """Resolves per-call config and applies its rate-limit gate, for non-JSON endpoints."""
    resolved = resolve_endpoint_kwargs(self, runtime)
    gate_sync(resolve_gate_url(runtime, resolved), resolved["ratelimit"])
    return resolved


async def resolve_and_gate_async(
    self: BaseConsumer, runtime: EndpointRuntime[P]
) -> ResolvedEndpointKwargs:
    """Resolves per-call config and applies its rate-limit gate, for non-JSON endpoints."""
    resolved = resolve_endpoint_kwargs(self, runtime)
    await gate_async(resolve_gate_url(runtime, resolved), resolved["ratelimit"])
    return resolved


def build_spec(
    self: BaseConsumer,
    runtime: EndpointRuntime[P],
    resolved: ResolvedEndpointKwargs,
    *args: P.args,
    **kwargs: P.kwargs,
) -> RequestSpec:
    """Binds call arguments against the stub signature and builds the request spec."""
    bound = runtime.sig.bind(self, *args, **kwargs)
    bound.apply_defaults()
    jar = (
        dict(self.session.cookies)
        if runtime.conf.has_cookies or resolved["cookies"] is not None
        else None
    )
    spec = build_request(
        runtime.method,
        runtime.parsed,
        runtime.plan,
        bound.arguments,
        jar=jar,
        base_url=resolved["base_url"],
        default_headers=resolve_headers(self, resolved["headers"]),
        default_cookies=resolve_cookies(self, resolved["cookies"]),
        idempotency_key=resolve_idempotency_key(runtime.idempotent),
    )
    handler = resolved["request_handler"]
    return spec if handler is None else handler(spec)


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


def endpoint(  # noqa: C901, PLR0913, PLR0917
    method: Method_T,
    path: str,
    resumable: Retry | None = None,
    parts: int | Parts | None = None,
    checksum: ChecksumInput = None,
    idempotent: bool | Callable[[], str] = False,
    **kwargs: Unpack[EndpointKwargs],
) -> EndpointDecorator:
    """Shared implementation for the HTTP method decorator factories.

    Args:
        method: HTTP method to use to call the endpoint.
        path: The relative endpoint.
        resumable: Reconnects with `Range` (using this `Retry` for reconnect attempts and
            backoff) if the connection drops mid-stream. Only valid for `GET` endpoints
            returning `Iterator[bytes]`/`AsyncIterator[bytes]`/`Downloader`/`AsyncDownloader`.
            For `Downloader`/`AsyncDownloader`, defaults to `DOWNLOAD_DEFAULT_RETRY` rather than
            disabling reconnects, since resumability is the entire point of that return type.
        parts: Configures multi-part parallel segmented download for `Downloader`/`AsyncDownloader`.
        checksum: Configures checksum validation for `Downloader`/`AsyncDownloader`.
        idempotent: Attaches an `Idempotency-Key` header, stable across `@retry`'s attempts of
            the same call (generated once per call, before retries begin). `False` (default)
            attaches nothing. `True` generates a fresh `uuid.uuid4()` per call. A `Callable[[],
            str]` generates the key itself, called once per call. Only valid for `POST`/`PUT`.
            `Idempotency-Key` is reserved the same way `Content-Type`/`Cookie` are: it cannot be
            set through a `Header` parameter, a header bag, or `@headers`.
    """
    base_url = kwargs.get("base_url", MISSING)
    if path.startswith(("http://", "https://")):
        if base_url is not MISSING:
            raise ValueError("Cannot specify base_url when path is already an absolute URL")
    elif base_url is not MISSING:
        validate_scheme(base_url)

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

        conf = EndpointConfiguration.build(func, plan, return_type)
        conf.validate(method, resumable, parts, checksum, idempotent)

        runtime: EndpointRuntime[P] = EndpointRuntime(
            method=method,
            path=path,
            parsed=parsed,
            plan=plan,
            sig=sig,
            conf=conf,
            idempotent=idempotent,
            endpoint_kwargs=kwargs,
        )

        def downloader_tail(
            resolved: ResolvedEndpointKwargs,
        ) -> tuple[
            Retry,
            RateLimit | None,
            Concurrency | None,
            Parts | None,
            Checksum | None,
            ResponseHandler | None,
        ]:
            """Builds the trailing `Downloader`/`AsyncDownloader` constructor args."""
            return (
                resumable or DOWNLOAD_DEFAULT_RETRY,
                resolved["ratelimit"],
                resolved["concurrency"],
                resolve_parts(parts),
                resolve_checksum(checksum),
                resolved["streaming_response_handler"],
            )

        def resolve_call_sync(
            self: BaseConsumer, gate: bool, *args: P.args, **kwargs: P.kwargs
        ) -> tuple[ResolvedEndpointKwargs, RequestSpec]:
            """Resolves per-call config (optionally rate-gated) and builds the request spec."""
            resolved = (
                resolve_and_gate_sync(self, runtime)
                if gate
                else resolve_endpoint_kwargs(self, runtime)
            )
            return resolved, build_spec(self, runtime, resolved, *args, **kwargs)

        async def resolve_call_async(
            self: BaseConsumer, gate: bool, *args: P.args, **kwargs: P.kwargs
        ) -> tuple[ResolvedEndpointKwargs, RequestSpec]:
            """Resolves per-call config (optionally rate-gated) and builds the request spec."""
            resolved = (
                await resolve_and_gate_async(self, runtime)
                if gate
                else resolve_endpoint_kwargs(self, runtime)
            )
            return resolved, build_spec(self, runtime, resolved, *args, **kwargs)

        if conf.is_coroutine:

            @functools.wraps(func)
            async def async_wrapper(
                self: AsyncConsumer, *args: P.args, **kwargs: P.kwargs
            ) -> AsyncDownloader | AsyncIterator[Event] | AsyncIterator[bytes] | Parsed_T:
                if conf.is_async_downloader or conf.is_any_stream:
                    resolved, spec = await resolve_call_async(
                        self, conf.is_any_stream, *args, **kwargs
                    )
                    if conf.is_async_downloader:
                        return AsyncDownloader(self, spec, *downloader_tail(resolved))
                    if conf.is_sse_stream:
                        return sse_async(
                            self,
                            spec,
                            resolved["concurrency"],
                            resolved["streaming_response_handler"],
                        )
                    if resumable is not None:
                        return resumable_stream_async(
                            self,
                            spec,
                            resumable,
                            resolved["concurrency"],
                            resolved["streaming_response_handler"],
                        )
                    return stream_async(
                        self, spec, resolved["concurrency"], resolved["streaming_response_handler"]
                    )
                resolved = resolve_endpoint_kwargs(self, runtime)
                gate_url = resolve_gate_url(runtime, resolved)
                async with gate_concurrency_async(gate_url, resolved["concurrency"]):
                    await gate_async(gate_url, resolved["ratelimit"])
                    spec = build_spec(self, runtime, resolved, *args, **kwargs)
                    response = await request_async(self, spec, resolved["retry"])
                    handler = resolved["response_handler"]
                    return decode(apply_response_handler(handler, response), return_type)

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def wrapper(
            self: SyncConsumer, *args: P.args, **kwargs: P.kwargs
        ) -> Downloader | Iterator[Event] | Iterator[bytes] | Parsed_T:
            if conf.is_downloader or conf.is_any_stream:
                resolved, spec = resolve_call_sync(self, conf.is_any_stream, *args, **kwargs)
                if conf.is_downloader:
                    return Downloader(self, spec, *downloader_tail(resolved))
                if conf.is_sse_stream:
                    return sse_sync(
                        self, spec, resolved["concurrency"], resolved["streaming_response_handler"]
                    )
                if resumable is not None:
                    return resumable_stream_sync(
                        self,
                        spec,
                        resumable,
                        resolved["concurrency"],
                        resolved["streaming_response_handler"],
                    )
                return stream_sync(
                    self, spec, resolved["concurrency"], resolved["streaming_response_handler"]
                )
            resolved = resolve_endpoint_kwargs(self, runtime)
            gate_url = resolve_gate_url(runtime, resolved)
            with gate_concurrency_sync(gate_url, resolved["concurrency"]):
                gate_sync(gate_url, resolved["ratelimit"])
                spec = build_spec(self, runtime, resolved, *args, **kwargs)
                response = request_sync(self, spec, resolved["retry"])
                handler = resolved["response_handler"]
                return decode(apply_response_handler(handler, response), return_type)

        return wrapper  # type: ignore[return-value]

    return decorate  # type: ignore[return-value]


def get(
    path: str,
    resumable: Retry | None = None,
    parts: int | Parts | None = None,
    checksum: ChecksumInput = None,
    **kwargs: Unpack[EndpointKwargs],
) -> EndpointDecorator:
    """Declares a stub method as `GET {path}`."""
    return endpoint("GET", path, resumable=resumable, parts=parts, checksum=checksum, **kwargs)


def post(
    path: str,
    idempotent: bool | Callable[[], str] = False,
    **kwargs: Unpack[EndpointKwargs],
) -> EndpointDecorator:
    """Declares a stub method as `POST {path}`."""
    return endpoint("POST", path, idempotent=idempotent, **kwargs)


def put(
    path: str,
    idempotent: bool | Callable[[], str] = False,
    **kwargs: Unpack[EndpointKwargs],
) -> EndpointDecorator:
    """Declares a stub method as `PUT {path}`."""
    return endpoint("PUT", path, idempotent=idempotent, **kwargs)


def patch(path: str, **kwargs: Unpack[EndpointKwargs]) -> EndpointDecorator:
    """Declares a stub method as `PATCH {path}`."""
    return endpoint("PATCH", path, **kwargs)


def delete(path: str, **kwargs: Unpack[EndpointKwargs]) -> EndpointDecorator:
    """Declares a stub method as `DELETE {path}`."""
    return endpoint("DELETE", path, **kwargs)


def head(path: str, **kwargs: Unpack[EndpointKwargs]) -> EndpointDecorator:
    """Declares a stub method as `HEAD {path}`."""
    return endpoint("HEAD", path, **kwargs)
