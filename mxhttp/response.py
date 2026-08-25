"""Processing steps for a raw `httpx.Response`."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, get_args, get_origin, overload

import msgspec

from mxhttp.sse import Event, SseBuilder
from mxhttp.types import AnyC_T, Parsed_T, ResponseHandler, is_parsed_type

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Iterator
    from types import ModuleType

    import httpx

    from mxhttp.consumer import AsyncConsumer, BaseConsumer, SyncConsumer
    from mxhttp.request import RequestSpec


class Response(msgspec.Struct, Generic[Parsed_T]):
    """Wraps decoded `data` alongside the raw `httpx.Response`."""

    data: Parsed_T
    response: httpx.Response


@overload
def decode(
    response: httpx.Response, return_type: type[Response[Parsed_T]]
) -> Response[Parsed_T]: ...
@overload
def decode(response: httpx.Response, return_type: type[None]) -> None: ...
@overload
def decode(response: httpx.Response, return_type: type[Parsed_T]) -> Parsed_T: ...
def decode(  # noqa: PLR0911
    response: httpx.Response, return_type: type[Parsed_T | None]
) -> Parsed_T | None:
    """Decodes an HTTP response into the specified return type.

    `httpx.Response` passes through, `str` and `bytes` use `.text` and `.content` directly,
    `None` discards the body without decoding it, and `pydantic.BaseModel` subclasses
    validate via `model_validate_json`. All other types decode via `msgspec.json.decode`.
    """
    import httpx

    if not is_parsed_type(return_type):
        return None

    if get_origin(return_type) is Response:
        (inner_type,) = get_args(return_type)
        return Response(decode(response, inner_type), response)  # type: ignore[return-value]
    if isinstance(return_type, type):  # pragma: no branch
        if return_type is httpx.Response:
            return response  # type: ignore[return-value]
        if issubclass(return_type, str):
            return response.text  # type: ignore[return-value]
        if issubclass(return_type, bytes):
            return response.content  # type: ignore[return-value]
        pydantic: ModuleType | None
        try:
            import pydantic
        except ImportError:  # pragma: no cover
            pydantic = None
        if pydantic is not None and issubclass(return_type, pydantic.BaseModel):
            return return_type.model_validate_json(  # type: ignore[attr-defined, no-any-return]
                response.content
            )
    return msgspec.json.decode(response.content, type=return_type)


def apply_response_handler(self: BaseConsumer, response: httpx.Response) -> httpx.Response:
    """Applies the response hook, or calls `raise_for_status` if unset."""
    if self._response_handler:
        return self._response_handler(response)
    return response.raise_for_status()


def apply_streaming_response_handler(
    self: BaseConsumer, response: httpx.Response
) -> httpx.Response:
    """Applies the streaming response hook, or calls `raise_for_status` if unset.

    Only the status line and headers are available at this point.
    """
    if self._streaming_response_handler:
        return self._streaming_response_handler(response)
    return response.raise_for_status()


def response_handler(
    hook: ResponseHandler,
) -> Callable[[type[AnyC_T]], type[AnyC_T]]:
    """Class decorator that runs every raw response through `hook` before decoding.

    Overrides the default hook, which calls `response.raise_for_status()`.
    Does not apply to streaming endpoints. Use `@streaming_response_handler` for those.
    """

    def decorate(cls: type[AnyC_T]) -> type[AnyC_T]:
        cls._response_handler = staticmethod(hook)
        return cls

    return decorate


def streaming_response_handler(
    hook: ResponseHandler,
) -> Callable[[type[AnyC_T]], type[AnyC_T]]:
    """Class decorator that runs every streaming response through `hook` before yielding.

    Applies only to streaming endpoints. Use `@response_handler` for other endpoints.
    Only the status line and headers can be inspected.
    """

    def decorate(cls: type[AnyC_T]) -> type[AnyC_T]:
        cls._streaming_response_handler = staticmethod(hook)
        return cls

    return decorate


def stream_sync(self: SyncConsumer, spec: RequestSpec) -> Iterator[bytes]:
    """Streams the response body in chunks instead of buffering it.

    Runs the `@streaming_response_handler` hook before yielding events.
    Discards any incomplete event when the stream ends.
    """
    with self.session.stream(spec.method, spec.url, **spec.to_kwargs()) as response:
        apply_streaming_response_handler(self, response)
        yield from response.iter_bytes()


async def stream_async(self: AsyncConsumer, spec: RequestSpec) -> AsyncIterator[bytes]:
    """Streams the response body in chunks instead of buffering it.

    Runs the `@streaming_response_handler` hook before yielding events.
    Discards any incomplete event when the stream ends.
    """
    async with self.session.stream(spec.method, spec.url, **spec.to_kwargs()) as response:
        apply_streaming_response_handler(self, response)
        async for chunk in response.aiter_bytes():
            yield chunk


def sse_sync(self: SyncConsumer, spec: RequestSpec) -> Iterator[Event]:
    """Streams the response as parsed Server-Sent Events.

    Runs the `@streaming_response_handler` hook before yielding events.
    Discards any incomplete event when the stream ends.
    """
    builder = SseBuilder()
    with self.session.stream(spec.method, spec.url, **spec.to_kwargs()) as response:
        apply_streaming_response_handler(self, response)
        for line in response.iter_lines():
            event = builder.feed(line)
            if event is not None:
                yield event


async def sse_async(self: AsyncConsumer, spec: RequestSpec) -> AsyncIterator[Event]:
    """Streams the response as parsed Server-Sent Events.

    Runs the `@streaming_response_handler` hook before yielding events.
    Discards any incomplete event when the stream ends.
    """
    builder = SseBuilder()
    async with self.session.stream(spec.method, spec.url, **spec.to_kwargs()) as response:
        apply_streaming_response_handler(self, response)
        async for line in response.aiter_lines():
            event = builder.feed(line)
            if event is not None:
                yield event
