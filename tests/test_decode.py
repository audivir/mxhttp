"""Tests for decoding and response handling."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator  # noqa: TC003

import httpx
import msgspec
import pytest
from conftest import make_consumer
from models import ITEM, AsyncCrudApi, CrudApi, Item, NewItem, PathApi, RawApi

from mxhttp import (
    AsyncConsumer,
    Response,
    SyncConsumer,
    delete,
    get,
    response_handler,
    streaming_response_handler,
)

pytestmark = pytest.mark.anyio


class ApiError(Exception):
    def __init__(self, status_code: int, body: bytes) -> None:
        """Initializes the exception with the HTTP status code and response body."""
        super().__init__(f"HTTP {status_code}: {body.decode(errors='replace')}")
        self.status_code = status_code


class NoneApi(SyncConsumer):
    @delete("/items/{item_id}")
    def delete_item(self, item_id: int) -> None: ...


class AsyncNoneApi(AsyncConsumer):
    @delete("/items/{item_id}")
    async def delete_item(self, item_id: int) -> None: ...


def raise_on_error(response: httpx.Response) -> httpx.Response:
    if response.is_error:
        raise ApiError(response.status_code, response.content)
    return response


@response_handler(raise_on_error)
class StrictApi(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


@response_handler(raise_on_error)
class AsyncStrictApi(AsyncConsumer):
    @get("/items/{item_id}")
    async def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


class WrappedResponseApi(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Response[Item]: ...  # type: ignore[empty-body]


class AsyncWrappedResponseApi(AsyncConsumer):
    @get("/items/{item_id}")
    async def get_item(self, item_id: int) -> Response[Item]: ...  # type: ignore[empty-body]


class StreamApi(SyncConsumer):
    @get("/download/{file_id}")
    def download(self, file_id: int) -> Iterator[bytes]: ...  # type: ignore[empty-body]


class AsyncStreamApi(AsyncConsumer):
    @get("/download/{file_id}")
    async def download(self, file_id: int) -> AsyncIterator[bytes]: ...  # type: ignore[empty-body]


def raise_on_error_status_only(response: httpx.Response) -> httpx.Response:
    if response.is_error:
        raise ApiError(response.status_code, b"")
    return response


@streaming_response_handler(raise_on_error_status_only)
class StrictStreamApi(SyncConsumer):
    @get("/download/{file_id}")
    def download(self, file_id: int) -> Iterator[bytes]: ...  # type: ignore[empty-body]


@streaming_response_handler(raise_on_error_status_only)
class AsyncStrictStreamApi(AsyncConsumer):
    @get("/download/{file_id}")
    async def download(self, file_id: int) -> AsyncIterator[bytes]: ...  # type: ignore[empty-body]


JSON_METHODS = [
    ("GET", "get_item", "/items/7", {"item_id": 7}),
    ("POST", "create_item", "/items", {"item": NewItem(name="Gadget", price=4.5)}),
    (
        "PUT",
        "replace_item",
        "/items/7",
        {"item_id": 7, "item": NewItem(name="Gadget", price=4.5)},
    ),
    (
        "PATCH",
        "update_item",
        "/items/7",
        {"item_id": 7, "item": NewItem(name="Gadget", price=4.5)},
    ),
]


@pytest.mark.parametrize("cls", [CrudApi, AsyncCrudApi], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("http_method", "sync_name", "path", "kwargs"),
    JSON_METHODS,
    ids=[case[0] for case in JSON_METHODS],
)
async def test_json_decoding_http_methods(
    *,
    cls: type[CrudApi | AsyncCrudApi],
    http_method: str,
    sync_name: str,
    path: str,
    kwargs: dict[str, object],
) -> None:
    consumer, seen = make_consumer(cls, ITEM, track_requests=True)

    if isinstance(consumer, AsyncCrudApi):
        call = getattr(consumer, sync_name)
        result = await call(**kwargs)
    else:
        call = getattr(consumer, sync_name)
        result = call(**kwargs)

    assert result == ITEM
    assert seen[0].method == http_method
    assert seen[0].url.path == path


RAW_RESPONSE_METHODS = [
    ("DELETE", "delete_item"),
    ("HEAD", "head_item"),
]


@pytest.mark.parametrize("cls", [CrudApi, AsyncCrudApi], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("http_method", "sync_name"),
    RAW_RESPONSE_METHODS,
    ids=[case[0] for case in RAW_RESPONSE_METHODS],
)
async def test_raw_response_http_methods(
    *, cls: type[CrudApi | AsyncCrudApi], http_method: str, sync_name: str
) -> None:
    consumer, seen = make_consumer(cls, {"error": "boom"}, status_code=204, track_requests=True)

    if isinstance(consumer, AsyncCrudApi):
        call = getattr(consumer, sync_name)
        result = await call(item_id=7)
    else:
        call = getattr(consumer, sync_name)
        result = call(item_id=7)

    assert isinstance(result, httpx.Response)
    assert result.status_code == 204
    assert seen[0].method == http_method
    assert seen[0].url.path == "/items/7"


@pytest.mark.parametrize("cls", [NoneApi, AsyncNoneApi], ids=["sync", "async"])
async def test_none_return_type_discards_body_without_decoding(
    *, cls: type[NoneApi | AsyncNoneApi]
) -> None:
    consumer, seen = make_consumer(cls, b"not json at all", status_code=204, track_requests=True)

    if isinstance(consumer, AsyncNoneApi):
        result = await consumer.delete_item(item_id=7)  # type: ignore[func-returns-value]
    else:
        result = consumer.delete_item(item_id=7)

    assert result is None
    assert seen[0].method == "DELETE"
    assert seen[0].url.path == "/items/7"


@pytest.mark.parametrize("cls", [StrictApi, AsyncStrictApi], ids=["sync", "async"])
@pytest.mark.parametrize("status_code", [404, 500], ids=["not-found", "server-error"])
async def test_response_handler_raises_on_error_status(
    *, cls: type[StrictApi | AsyncStrictApi], status_code: int
) -> None:
    consumer = make_consumer(cls, {"error": "boom"}, status_code=status_code)

    with pytest.raises(ApiError, match=f"HTTP {status_code}") as exc_info:  # noqa: PT012
        if isinstance(consumer, AsyncStrictApi):
            await consumer.get_item(item_id=1)
        else:
            consumer.get_item(item_id=1)

    assert exc_info.value.status_code == status_code


@pytest.mark.parametrize("cls", [StrictApi, AsyncStrictApi], ids=["sync", "async"])
async def test_response_handler_passes_through_success(
    *, cls: type[StrictApi | AsyncStrictApi]
) -> None:
    consumer = make_consumer(cls, ITEM)

    if isinstance(consumer, AsyncStrictApi):
        result = await consumer.get_item(item_id=1)
    else:
        result = consumer.get_item(item_id=1)

    assert result == ITEM


def test_response_handler_transforms_response() -> None:

    def unwrap_envelope(response: httpx.Response) -> httpx.Response:
        envelope = msgspec.json.decode(response.content)
        return httpx.Response(response.status_code, json=envelope["data"], request=response.request)

    @response_handler(unwrap_envelope)
    class EnvelopeApi(SyncConsumer):
        @get("/items/{item_id}")
        def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    consumer = make_consumer(
        EnvelopeApi, {"data": msgspec.to_builtins(ITEM), "meta": {"cached": False}}
    )

    result = consumer.get_item(item_id=1)

    assert result == ITEM


def test_endpoint_response_handler_overrides_class_default() -> None:
    class OtherError(Exception):
        pass

    def raise_other(unused_response: httpx.Response) -> httpx.Response:
        raise OtherError("overridden")

    @response_handler(raise_on_error)
    class MixedApi(SyncConsumer):
        @get("/items/{item_id}", response_handler=raise_other)
        def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    consumer = make_consumer(MixedApi, b"not found", status_code=404)

    with pytest.raises(OtherError):
        consumer.get_item(item_id=1)


def test_endpoint_response_handler_none_disables_class_default() -> None:
    @response_handler(raise_on_error)
    class MixedApi(SyncConsumer):
        @get("/items/{item_id}", response_handler=None)
        def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    consumer = make_consumer(MixedApi, ITEM, status_code=404)

    with pytest.raises(httpx.HTTPStatusError):
        consumer.get_item(item_id=1)


def test_no_response_handler_passes_response_through_unmodified() -> None:
    consumer = make_consumer(PathApi, ITEM)

    result = consumer.get_post(user_id="alice", post_id=1)

    assert result == ITEM


@pytest.mark.parametrize(
    "cls", [WrappedResponseApi, AsyncWrappedResponseApi], ids=["sync", "async"]
)
async def test_wrapped_response_exposes_data_and_raw_response(
    *, cls: type[WrappedResponseApi | AsyncWrappedResponseApi]
) -> None:
    consumer = make_consumer(cls, ITEM, status_code=201)

    if isinstance(consumer, AsyncWrappedResponseApi):
        result = await consumer.get_item(item_id=1)
    else:
        result = consumer.get_item(item_id=1)

    assert isinstance(result, Response)
    assert result.data == ITEM
    assert isinstance(result.response, httpx.Response)
    assert result.response.status_code == 201


@pytest.mark.parametrize("status_code", [404, 500], ids=["not-found", "server-error"])
def test_default_response_handler_raises_for_status(status_code: int) -> None:
    consumer = make_consumer(PathApi, {"error": "boom"}, status_code=status_code)

    with pytest.raises(httpx.HTTPStatusError):
        consumer.get_post(user_id="alice", post_id=1)


def test_return_type_httpx_response_bypasses_json_decode() -> None:
    consumer = make_consumer(RawApi, b"not json at all")

    result = consumer.ping()

    assert isinstance(result, httpx.Response)
    assert result.content == b"not json at all"


def test_union_raises_type_error() -> None:
    with pytest.raises(TypeError, match="Return type must not be a union"):

        class BadApi(SyncConsumer):
            @get("/search")  # type: ignore[type-var]
            def get_union(self) -> Item | None: ...


def test_response_wrapped_union_raises_type_error() -> None:
    with pytest.raises(TypeError, match="Return type must not be a union"):

        class BadApi(SyncConsumer):
            @get("/search")
            def get_wrapped_union(self) -> Response[Item | None]: ...  # type: ignore[type-var, empty-body]


def test_response_wrapped_nested_union_is_allowed() -> None:
    class GetNestedUnionApi(SyncConsumer):
        @get("/values")
        def get_nested_union(self) -> Response[list[int | None]]: ...  # type: ignore[empty-body]

    consumer = make_consumer(GetNestedUnionApi, [1, None, 3])

    result = consumer.get_nested_union()

    assert result.data == [1, None, 3]


def test_missing_return_annotation_raises_value_error() -> None:
    with pytest.raises(ValueError, match="No return type annotated"):

        class BadApi(SyncConsumer):
            @get("/things/{thing_id}")
            def get_thing(self, thing_id: int): ...  # type: ignore[no-untyped-def] # noqa: ANN202


@pytest.mark.parametrize("cls", [StreamApi, AsyncStreamApi], ids=["sync", "async"])
async def test_streaming_response_yields_chunks(*, cls: type[StreamApi | AsyncStreamApi]) -> None:
    consumer = make_consumer(cls, b"streamed file contents")

    if isinstance(consumer, AsyncStreamApi):
        chunks = [chunk async for chunk in await consumer.download(file_id=1)]
    else:
        chunks = list(consumer.download(file_id=1))

    assert b"".join(chunks) == b"streamed file contents"


@pytest.mark.parametrize("cls", [StreamApi, AsyncStreamApi], ids=["sync", "async"])
async def test_streaming_response_raises_for_status(
    *, cls: type[StreamApi | AsyncStreamApi]
) -> None:
    consumer = make_consumer(cls, b"not found", status_code=404)

    with pytest.raises(httpx.HTTPStatusError):  # noqa: PT012
        if isinstance(consumer, AsyncStreamApi):
            [chunk async for chunk in await consumer.download(file_id=1)]
        else:
            list(consumer.download(file_id=1))


@pytest.mark.parametrize("cls", [StrictStreamApi, AsyncStrictStreamApi], ids=["sync", "async"])
async def test_streaming_response_handler_overrides_default(
    *, cls: type[StrictStreamApi | AsyncStrictStreamApi]
) -> None:
    consumer = make_consumer(cls, b"not found", status_code=404)

    with pytest.raises(ApiError, match="HTTP 404") as exc_info:  # noqa: PT012
        if isinstance(consumer, AsyncStrictStreamApi):
            [chunk async for chunk in await consumer.download(file_id=1)]
        else:
            list(consumer.download(file_id=1))

    assert exc_info.value.status_code == 404


@pytest.mark.parametrize("cls", [StrictStreamApi, AsyncStrictStreamApi], ids=["sync", "async"])
async def test_streaming_response_handler_override_passes_through_success(
    *, cls: type[StrictStreamApi | AsyncStrictStreamApi]
) -> None:
    consumer = make_consumer(cls, b"streamed file contents")

    if isinstance(consumer, AsyncStrictStreamApi):
        chunks = [chunk async for chunk in await consumer.download(file_id=1)]
    else:
        chunks = list(consumer.download(file_id=1))

    assert b"".join(chunks) == b"streamed file contents"


def test_endpoint_streaming_response_handler_overrides_class_default() -> None:
    class OtherError(Exception):
        pass

    def raise_other(unused_response: httpx.Response) -> httpx.Response:
        raise OtherError("overridden")

    @streaming_response_handler(raise_on_error_status_only)
    class MixedStreamApi(SyncConsumer):
        @get("/download/{file_id}", streaming_response_handler=raise_other)
        def download(self, file_id: int) -> Iterator[bytes]: ...  # type: ignore[empty-body]

    consumer = make_consumer(MixedStreamApi, b"not found", status_code=404)

    with pytest.raises(OtherError):
        list(consumer.download(file_id=1))


@pytest.mark.parametrize("cls", [StreamApi, AsyncStreamApi], ids=["sync", "async"])
async def test_streaming_response_handler_passes_through_success(
    *, cls: type[StreamApi | AsyncStreamApi]
) -> None:
    consumer = make_consumer(cls, b"streamed file contents")

    if isinstance(consumer, AsyncStreamApi):
        chunks = [chunk async for chunk in await consumer.download(file_id=1)]
    else:
        chunks = list(consumer.download(file_id=1))

    assert b"".join(chunks) == b"streamed file contents"
