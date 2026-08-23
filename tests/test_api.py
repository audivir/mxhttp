"""Tests for the mxhttp HTTP client and its API base classes."""

from __future__ import annotations

from typing import Annotated, Any
from urllib.parse import parse_qsl

import httpx
import msgspec
import pytest
from conftest import make_consumer
from models import (
    ITEM,
    ApiError,
    AsyncBodyApi,
    AsyncCookieApi,
    AsyncCrudApi,
    AsyncFormApi,
    AsyncHeaderApi,
    AsyncNoneApi,
    AsyncPathApi,
    AsyncQueryApi,
    AsyncRawApi,
    AsyncSseApi,
    AsyncStreamApi,
    AsyncStrictApi,
    AsyncStrictStreamApi,
    AsyncUploadApi,
    AsyncWrappedResponseApi,
    BareBytesUploadApi,
    BodyApi,
    CookieApi,
    CrudApi,
    EnvelopeApi,
    FormApi,
    GetNestedUnionApi,
    HeaderApi,
    HeadersUploadApi,
    Item,
    ListFieldApi,
    ListQueryApi,
    NewItem,
    NoneApi,
    OverrideCookieApi,
    PathApi,
    QueryApi,
    RawApi,
    SseApi,
    StreamApi,
    StrictApi,
    StrictStreamApi,
    UploadApi,
    WrappedResponseApi,
)

from mxhttp import (
    Body,
    Cookie,
    Event,
    Field,
    Header,
    Part,
    PartValue,
    Path,
    Query,
    Response,
    SyncConsumer,
    get,
    post,
)
from mxhttp.consumer import BaseConsumer

pytestmark = pytest.mark.anyio


# meta-test
@pytest.mark.parametrize("cls", [RawApi, AsyncRawApi], ids=["sync", "async"])
async def test_make_consumer_accepts_httpx_response_directly(
    *, cls: type[RawApi | AsyncRawApi]
) -> None:
    consumer = make_consumer(cls, httpx.Response(204, headers={"x-marker": "1"}))

    if isinstance(consumer, AsyncRawApi):
        result = await consumer.ping()
    else:
        result = consumer.ping()

    assert result.headers["x-marker"] == "1"


# meta-test
def test_make_consumer_rejects_unsupported_consumer_class() -> None:
    with pytest.raises(TypeError, match="Unsupported consumer class"):
        make_consumer(BaseConsumer, ITEM)


@pytest.mark.parametrize("cls", [PathApi, AsyncPathApi], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("user_id", "post_id", "expected_path"),
    [
        ("alice", 1, "/users/alice/posts/1"),
        ("bob-42", 7, "/users/bob-42/posts/7"),
        ("123", 0, "/users/123/posts/0"),
    ],
)
async def test_path_params(
    *, cls: type[PathApi | AsyncPathApi], user_id: str, post_id: int, expected_path: str
) -> None:
    consumer, seen = make_consumer(cls, ITEM, track_requests=True)

    if isinstance(consumer, AsyncPathApi):
        result = await consumer.get_post(user_id=user_id, post_id=post_id)
    else:
        result = consumer.get_post(user_id=user_id, post_id=post_id)

    assert result == ITEM
    assert seen[0].url.path == expected_path


def test_path_param_slash_is_escaped_not_injected_as_segment() -> None:
    consumer, seen = make_consumer(PathApi, ITEM, track_requests=True)

    result = consumer.get_post(user_id="../admin", post_id=1)

    assert result == ITEM
    # `.path` percent-decodes for display; `.raw_path` is the actually sent data
    assert seen[0].url.raw_path == b"/users/..%2Fadmin/posts/1"


@pytest.mark.parametrize("cls", [QueryApi, AsyncQueryApi], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("kwargs", "expected_params"),
    [
        ({"q": "shoes"}, {"q": "shoes", "limit": "20"}),
        ({"q": "shoes", "limit": 5}, {"q": "shoes", "limit": "5"}),
        ({"q": "shoes", "active": True}, {"q": "shoes", "limit": "20", "active": "true"}),
        ({"q": "shoes", "active": False}, {"q": "shoes", "limit": "20", "active": "false"}),
        ({"q": "shoes", "category": "tech"}, {"q": "shoes", "limit": "20", "cat": "tech"}),
        ({"q": "shoes", "category": None}, {"q": "shoes", "limit": "20"}),
    ],
    ids=["bare", "int-override", "bool-true", "bool-false", "aliased", "explicit-none-omitted"],
)
async def test_query_params(
    *,
    cls: type[QueryApi | AsyncQueryApi],
    kwargs: dict[str, object],
    expected_params: dict[str, str],
) -> None:
    consumer, seen = make_consumer(cls, ITEM, track_requests=True)

    if isinstance(consumer, AsyncQueryApi):
        result = await consumer.search(**kwargs)  # type: ignore[arg-type]
    else:
        result = consumer.search(**kwargs)  # type: ignore[arg-type]

    assert result == ITEM
    assert dict(seen[0].url.params) == expected_params


def test_list_query_param_repeats_key() -> None:
    consumer, seen = make_consumer(ListQueryApi, ITEM, track_requests=True)

    result = consumer.search(tags=["a", "b", "c"])

    assert result == ITEM
    assert seen[0].url.params.multi_items() == [("tags", "a"), ("tags", "b"), ("tags", "c")]


def test_list_field_repeats_key() -> None:
    consumer, seen = make_consumer(ListFieldApi, ITEM, track_requests=True)

    result = consumer.login(tags=[1, 2, 3])

    assert result == ITEM
    assert parse_qsl(seen[0].content.decode()) == [("tags", "1"), ("tags", "2"), ("tags", "3")]


@pytest.mark.parametrize("cls", [FormApi, AsyncFormApi], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("kwargs", "expected_fields"),
    [
        ({"username": "alice", "password": "hunter2"}, {"username": "alice", "pass": "hunter2"}),
        (
            {"username": "alice", "password": "hunter2", "remember": True},
            {"username": "alice", "pass": "hunter2", "remember": "true"},
        ),
    ],
    ids=["required-only", "with-remember"],
)
async def test_form_fields(
    *, cls: type[FormApi | AsyncFormApi], kwargs: dict[str, object], expected_fields: dict[str, str]
) -> None:
    consumer, seen = make_consumer(cls, ITEM, track_requests=True)

    if isinstance(consumer, AsyncFormApi):
        result = await consumer.login(**kwargs)  # type: ignore[arg-type]
    else:
        result = consumer.login(**kwargs)  # type: ignore[arg-type]

    assert result == ITEM
    request = seen[0]
    assert request.headers["content-type"] == "application/x-www-form-urlencoded"
    assert dict(parse_qsl(request.content.decode())) == expected_fields


@pytest.mark.parametrize("cls", [HeaderApi, AsyncHeaderApi], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("kwargs", "expected_header"),
    [
        ({"request_id": "abc"}, "abc"),
        ({"request_id": "abc", "trace": "xyz"}, "abc"),
    ],
    ids=["required-only", "with-trace"],
)
async def test_header_params(
    *, cls: type[HeaderApi | AsyncHeaderApi], kwargs: dict[str, object], expected_header: str
) -> None:
    consumer, seen = make_consumer(cls, ITEM, track_requests=True)

    if isinstance(consumer, AsyncHeaderApi):
        await consumer.get_thing(**kwargs)  # type: ignore[arg-type]
    else:
        consumer.get_thing(**kwargs)  # type: ignore[arg-type]

    request = seen[0]
    assert request.headers["X-Request-Id"] == expected_header
    if "trace" in kwargs:
        assert request.headers["trace"] == kwargs["trace"]
    else:
        assert "trace" not in request.headers


@pytest.mark.parametrize("cls", [CookieApi, AsyncCookieApi], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("kwargs", "expected_cookie"),
    [
        ({"session_id": "abc123"}, "session_id=abc123"),
        ({"session_id": "abc123", "theme": "dark"}, "session_id=abc123; theme=dark"),
    ],
    ids=["required-only", "with-theme"],
)
async def test_cookie_params(
    *, cls: type[CookieApi | AsyncCookieApi], kwargs: dict[str, object], expected_cookie: str
) -> None:
    consumer, seen = make_consumer(cls, ITEM, track_requests=True)

    if isinstance(consumer, AsyncCookieApi):
        await consumer.get_thing(**kwargs)  # type: ignore[arg-type]
    else:
        consumer.get_thing(**kwargs)  # type: ignore[arg-type]

    assert seen[0].headers["cookie"] == expected_cookie


@pytest.mark.parametrize("cls", [CookieApi, AsyncCookieApi], ids=["sync", "async"])
async def test_cookie_defers_to_jar_by_default(*, cls: type[CookieApi | AsyncCookieApi]) -> None:
    consumer, seen = make_consumer(cls, ITEM, track_requests=True)

    if isinstance(consumer, AsyncCookieApi):
        consumer.session.cookies.set("session_id", "from-jar")
        await consumer.get_thing(session_id="explicit")
    else:
        consumer.session.cookies.set("session_id", "from-jar")
        consumer.get_thing(session_id="explicit")

    assert seen[0].headers["cookie"] == "session_id=from-jar"


def test_cookie_override_beats_jar() -> None:
    consumer, seen = make_consumer(OverrideCookieApi, ITEM, track_requests=True)
    consumer.session.cookies.set("session_id", "from-jar")

    consumer.get_thing(session_id="explicit")

    assert seen[0].headers["cookie"] == "session_id=explicit"


@pytest.mark.parametrize("cls", [BodyApi, AsyncBodyApi], ids=["sync", "async"])
@pytest.mark.parametrize(
    "item",
    [NewItem(name="Gadget", price=4.5), NewItem(name="Thing-with-é-unicode", price=0.0)],
    ids=["basic", "unicode-and-zero"],
)
async def test_json_body(*, cls: type[BodyApi | AsyncBodyApi], item: NewItem) -> None:
    consumer, seen = make_consumer(cls, ITEM, track_requests=True)

    if isinstance(consumer, AsyncBodyApi):
        result = await consumer.create_item(item=item)
    else:
        result = consumer.create_item(item=item)

    assert result == ITEM
    request = seen[0]
    assert request.headers["content-type"] == "application/json"
    assert msgspec.json.decode(request.content) == msgspec.to_builtins(item)


@pytest.mark.parametrize("cls", [UploadApi, AsyncUploadApi], ids=["sync", "async"])
@pytest.mark.parametrize(
    ("filename", "content", "content_type"),
    [
        ("hello.txt", b"file-bytes-content", "text/plain"),
        ("data.bin", b"\x00\x01\x02\xff", "application/octet-stream"),
    ],
    ids=["text", "binary"],
)
async def test_multipart_part(
    *, cls: type[UploadApi | AsyncUploadApi], filename: str, content: bytes, content_type: str
) -> None:
    consumer, seen = make_consumer(cls, ITEM, track_requests=True)
    part: PartValue = (filename, content, content_type)

    if isinstance(consumer, AsyncUploadApi):
        result = await consumer.upload(description="a file", file=part)
    else:
        result = consumer.upload(description="a file", file=part)

    assert result == ITEM
    request = seen[0]
    assert request.headers["content-type"].startswith("multipart/form-data")
    body = request.content
    assert b'name="description"' in body
    assert b"a file" in body
    assert f'name="file"; filename="{filename}"'.encode() in body
    assert f"Content-Type: {content_type}".encode() in body
    assert content in body


def test_multipart_bare_bytes() -> None:
    consumer, seen = make_consumer(BareBytesUploadApi, ITEM, track_requests=True)

    result = consumer.upload(file=b"raw file bytes")

    assert result == ITEM
    body = seen[0].content
    assert b'filename="upload"' in body
    assert b"Content-Type: application/octet-stream" in body
    assert b"raw file bytes" in body


def test_multipart_part_with_headers() -> None:
    consumer, seen = make_consumer(HeadersUploadApi, ITEM, track_requests=True)

    part = ("f.bin", b"content", "application/octet-stream", {"X-Foo": "bar"})
    result = consumer.upload(file=part)

    assert result == ITEM
    body = seen[0].content
    assert b'filename="f.bin"' in body
    assert b"X-Foo: bar" in body
    assert b"content" in body


def test_part_scalar_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Part argument 'file' must be bytes \| str \| IO"):

        class BadApi(SyncConsumer):
            @post("/upload")
            def upload(self, file: Annotated[int, Part]) -> httpx.Response: ...  # type: ignore[empty-body]


def test_part_struct_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Part argument 'file' must be bytes \| str \| IO"):

        class BadApi(SyncConsumer):
            @post("/upload")
            def upload(self, file: Annotated[Item, Part]) -> httpx.Response: ...  # type: ignore[empty-body]


def test_part_wrong_tuple_shape_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Part argument 'file' must be bytes \| str \| IO"):

        class BadApi(SyncConsumer):
            @post("/upload")
            def upload(  # type: ignore[empty-body]
                self, file: Annotated[tuple[bytes, bytes], Part]
            ) -> httpx.Response: ...


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
    consumer = make_consumer(
        EnvelopeApi, {"data": msgspec.to_builtins(ITEM), "meta": {"cached": False}}
    )

    result = consumer.get_item(item_id=1)

    assert result == ITEM


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


def test_unbound_parameter_raises_type_error() -> None:
    with pytest.raises(TypeError, match="no Query/Field/Part/Body binding"):

        class BadApi(SyncConsumer):
            @get("/things")
            def get_thing(self, mystery: str) -> Item: ...  # type: ignore[empty-body]


def test_missing_return_annotation_raises_value_error() -> None:
    with pytest.raises(ValueError, match="No return type annotated"):

        class BadApi(SyncConsumer):
            @get("/things/{thing_id}")
            def get_thing(self, thing_id: int): ...  # type: ignore[no-untyped-def] # noqa: ANN202


def test_unknown_extra_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Unexpected extra: typing\.Any"):

        class BadApi(SyncConsumer):
            @get("/things")
            def get_thing(self, thing_id: Annotated[int, Any]) -> httpx.Response: ...  # type: ignore[empty-body]


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
    consumer = make_consumer(GetNestedUnionApi, [1, None, 3])

    result = consumer.get_nested_union()

    assert result.data == [1, None, 3]


def test_path_special_chars() -> None:
    class SpecialCharApi(SyncConsumer):
        @get("/things/{?}")
        def get_thing(self, thing_id: Annotated[int, Path["?"]]) -> httpx.Response: ...  # type: ignore[empty-body]

    consumer = make_consumer(SpecialCharApi, b"0")
    result = consumer.get_thing(thing_id=0)
    assert isinstance(result, httpx.Response)
    assert result.status_code == 200


def test_path_bool_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Path argument 'item_id' must be str \| int \| float"):

        class BadApi(SyncConsumer):
            @get("/things/{item_id}")
            def get_thing(self, item_id: bool) -> httpx.Response: ...  # type: ignore[empty-body]


def test_path_none_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Path argument 'item_id' must be str \| int \| float"):

        class BadApi(SyncConsumer):
            @get("/things/{item_id}")
            def get_thing(self, item_id: None) -> httpx.Response: ...  # type: ignore[empty-body]


def test_path_optional_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Path argument 'item_with_none' must not be optional"):

        class BadApi(SyncConsumer):
            @get("/things/{item_with_none}")
            def get_thing(self, item_with_none: Annotated[int | None, Path]) -> httpx.Response: ...  # type: ignore[empty-body]


def test_path_none_default_raises_type_error() -> None:
    with pytest.raises(TypeError, match="Path argument 'item_id' must not default to None"):

        class BadApi(SyncConsumer):
            @get("/things/{item_id}")
            def get_thing(self, item_id: int = None) -> httpx.Response: ...  # type: ignore[assignment,empty-body] # noqa: RUF013


def test_path_optional_wrapping_annotated_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Path argument 'item_id' must not default to None"):

        class BadApi(SyncConsumer):
            @get("/things/{item_id}")
            def get_thing(  # type: ignore[empty-body]
                self, item_id: Annotated[int, Path] | None = None
            ) -> httpx.Response: ...


def test_path_optional_wrapping_annotated_without_default_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Path argument 'item_id' must not be optional"):

        class BadApi(SyncConsumer):
            @get("/things/{item_id}")
            def get_thing(  # type: ignore[empty-body]
                self, item_id: Annotated[int, Path] | None
            ) -> httpx.Response: ...


def test_path_doubly_optional_raises_type_error() -> None:
    # `get_type_hints` re-wraps `= None` in an outer `Optional[...]`
    with pytest.raises(TypeError, match=r"Path argument 'item_id' must not default to None"):

        class BadApi(SyncConsumer):
            @get("/things/{item_id}")
            def get_thing(  # type: ignore[empty-body]
                self, item_id: Annotated[int | None, Path] = None
            ) -> httpx.Response: ...


def test_query_struct_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Query argument 'thing' must be"):

        class BadApi(SyncConsumer):
            @get("/things")
            def get_thing(  # type: ignore[empty-body]
                self, thing: Annotated[Item, Query]
            ) -> httpx.Response: ...


def test_field_struct_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Field argument 'thing' must be"):

        class BadApi(SyncConsumer):
            @post("/things")
            def create_thing(  # type: ignore[empty-body]
                self, thing: Annotated[Item, Field]
            ) -> httpx.Response: ...


def test_header_struct_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Header argument 'thing' must be"):

        class BadApi(SyncConsumer):
            @get("/things")
            def get_thing(  # type: ignore[empty-body]
                self, thing: Annotated[Item, Header]
            ) -> httpx.Response: ...


def test_cookie_struct_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Cookie argument 'thing' must be"):

        class BadApi(SyncConsumer):
            @get("/things")
            def get_thing(  # type: ignore[empty-body]
                self, thing: Annotated[Item, Cookie]
            ) -> httpx.Response: ...


def test_query_optional_struct_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Query argument 'thing' must be"):

        class BadApi(SyncConsumer):
            @get("/things")
            def get_thing(  # type: ignore[empty-body]
                self, thing: Annotated[Item | None, Query] = None
            ) -> httpx.Response: ...


def test_body_scalar_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Body argument 'item' must not be"):

        class BadApi(SyncConsumer):
            @post("/items")
            def create_item(self, item: Annotated[int, Body]) -> httpx.Response: ...  # type: ignore[empty-body]


def test_body_bytes_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Body argument 'item' must not be"):

        class BadApi(SyncConsumer):
            @post("/items")
            def create_item(self, item: Annotated[bytes, Body]) -> httpx.Response: ...  # type: ignore[empty-body]


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


SSE_BODY = (
    b"event: update\n"
    b"data: line1\n"
    b"data: line2\n"
    b"id: 42\n"
    b"\n"
    b": this is a comment, ignored\n"
    b"data: second event\n"
    b"\n"
    b"retry: 5000\n"
    b"data: third event\n"
    b"\n"
)


@pytest.mark.parametrize("cls", [SseApi, AsyncSseApi], ids=["sync", "async"])
async def test_sse_parses_events(*, cls: type[SseApi | AsyncSseApi]) -> None:
    consumer = make_consumer(cls, SSE_BODY)

    if isinstance(consumer, AsyncSseApi):
        events = [event async for event in await consumer.events()]
    else:
        events = list(consumer.events())

    assert events == [
        Event(data="line1\nline2", event="update", id="42", retry=None),
        Event(data="second event", event="message", id="42", retry=None),
        Event(data="third event", event="message", id="42", retry=5000),
    ]


def test_sse_discards_incomplete_trailing_event() -> None:
    body = b"data: complete\n\ndata: incomplete-no-trailing-blank-line\n"
    consumer = make_consumer(SseApi, body)

    events = list(consumer.events())

    assert events == [Event(data="complete")]


def test_sse_raises_for_status() -> None:
    consumer = make_consumer(SseApi, b"not found", status_code=404)

    with pytest.raises(httpx.HTTPStatusError):
        list(consumer.events())


def test_sse_leading_blank_line_dispatches_nothing() -> None:
    body = b"\ndata: after-leading-blank\n\n"
    consumer = make_consumer(SseApi, body)

    events = list(consumer.events())

    assert events == [Event(data="after-leading-blank")]


def test_sse_unrecognized_field_ignored() -> None:
    body = b"foo: bar\ndata: still-parsed\n\n"
    consumer = make_consumer(SseApi, body)

    events = list(consumer.events())

    assert events == [Event(data="still-parsed")]


def test_sse_non_digit_retry_ignored() -> None:
    body = b"retry: not-a-number\ndata: x\n\n"
    consumer = make_consumer(SseApi, body)

    events = list(consumer.events())

    assert events == [Event(data="x", retry=None)]


def test_consumer_context_manager_closes_session() -> None:
    consumer = make_consumer(CrudApi, ITEM)

    with consumer as entered:
        assert entered is consumer
        assert consumer.get_item(item_id=1) == ITEM

    assert consumer.session.is_closed


async def test_consumer_asnyc_context_manager_closes_session() -> None:
    consumer = make_consumer(AsyncCrudApi, ITEM)

    async with consumer as entered:
        assert entered is consumer
        assert await consumer.get_item(item_id=1) == ITEM

    assert consumer.session.is_closed


def test_part_short_tuple_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Part argument 'file' must be bytes \| str \| IO"):

        class BadApi(SyncConsumer):
            @post("/upload")
            def upload(self, file: Annotated[tuple[str], Part]) -> httpx.Response: ...  # type: ignore[empty-body]


def test_part_five_element_tuple_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Part argument 'file' must be bytes \| str \| IO"):

        class BadApi(SyncConsumer):
            @post("/upload")
            def upload(  # type: ignore[empty-body]
                self, file: Annotated[tuple[str, bytes, str, dict[str, str], int], Part]
            ) -> httpx.Response: ...
