"""Tests for the marker handling functions."""

from __future__ import annotations

import urllib.parse
from typing import Annotated

import httpx  # noqa: TC002
import msgspec
import pytest
from conftest import make_consumer
from models import ITEM, AsyncPathApi, Item, NewItem, PathApi

from mxhttp import (
    AsyncConsumer,
    Body,
    Cookie,
    Field,
    Header,
    Part,
    PartValue,
    Query,
    SyncConsumer,
    get,
    post,
)

pytestmark = pytest.mark.anyio


class QueryApi(SyncConsumer):
    @get("/search")
    def search(  # type: ignore[empty-body]
        self,
        q: Annotated[str, Query],
        limit: Annotated[int, Query] = 20,
        active: Annotated[bool | None, Query] = None,
        category: Annotated[str | None, Query["cat"]] = None,
    ) -> Item: ...


class AsyncQueryApi(AsyncConsumer):
    @get("/search")
    async def search(  # type: ignore[empty-body]
        self,
        q: Annotated[str, Query],
        limit: Annotated[int, Query] = 20,
        active: Annotated[bool | None, Query] = None,
        category: Annotated[str | None, Query["cat"]] = None,
    ) -> Item: ...


class ListQueryApi(SyncConsumer):
    @get("/search")
    def search(self, tags: Annotated[list[str], Query]) -> Item: ...  # type: ignore[empty-body]


class ListFieldApi(SyncConsumer):
    @post("/login")
    def login(self, tags: Annotated[list[int], Field]) -> Item: ...  # type: ignore[empty-body]


class HeaderApi(SyncConsumer):
    @get("/things")
    def get_thing(  # type: ignore[empty-body]
        self,
        request_id: Annotated[str, Header["X-Request-Id"]],
        trace: Annotated[str | None, Header] = None,
    ) -> httpx.Response: ...


class AsyncHeaderApi(AsyncConsumer):
    @get("/things")
    async def get_thing(  # type: ignore[empty-body]
        self,
        request_id: Annotated[str, Header["X-Request-Id"]],
        trace: Annotated[str | None, Header] = None,
    ) -> httpx.Response: ...


class CookieApi(SyncConsumer):
    @get("/things")
    def get_thing(  # type: ignore[empty-body]
        self,
        session_id: Annotated[str, Cookie["session_id"]],
        theme: Annotated[str | None, Cookie] = None,
    ) -> httpx.Response: ...


class AsyncCookieApi(AsyncConsumer):
    @get("/things")
    async def get_thing(  # type: ignore[empty-body]
        self,
        session_id: Annotated[str, Cookie["session_id"]],
        theme: Annotated[str | None, Cookie] = None,
    ) -> httpx.Response: ...


class OverrideCookieApi(SyncConsumer):
    @get("/things")
    def get_thing(  # type: ignore[empty-body]
        self, session_id: Annotated[str, Cookie("session_id", override=True)]
    ) -> httpx.Response: ...


class FormApi(SyncConsumer):
    @post("/login")
    def login(  # type: ignore[empty-body]
        self,
        username: Annotated[str, Field],
        password: Annotated[str, Field["pass"]],
        remember: Annotated[bool | None, Field] = None,
    ) -> Item: ...


class AsyncFormApi(AsyncConsumer):
    @post("/login")
    async def login(  # type: ignore[empty-body]
        self,
        username: Annotated[str, Field],
        password: Annotated[str, Field["pass"]],
        remember: Annotated[bool | None, Field] = None,
    ) -> Item: ...


class BodyApi(SyncConsumer):
    @post("/items")
    def create_item(self, item: Annotated[NewItem, Body]) -> Item: ...  # type: ignore[empty-body]


class AsyncBodyApi(AsyncConsumer):
    @post("/items")
    async def create_item(self, item: Annotated[NewItem, Body]) -> Item: ...  # type: ignore[empty-body]


class UploadApi(SyncConsumer):
    @post("/upload")
    def upload(  # type: ignore[empty-body]
        self,
        description: Annotated[str, Field],
        file: Annotated[PartValue, Part],
    ) -> Item: ...


class AsyncUploadApi(AsyncConsumer):
    @post("/upload")
    async def upload(  # type: ignore[empty-body]
        self,
        description: Annotated[str, Field],
        file: Annotated[PartValue, Part],
    ) -> Item: ...


class BareBytesUploadApi(SyncConsumer):
    @post("/upload")
    def upload(self, file: Annotated[bytes, Part]) -> Item: ...  # type: ignore[empty-body]


class HeadersUploadApi(SyncConsumer):
    @post("/upload")
    def upload(  # type: ignore[empty-body]
        self, file: Annotated[tuple[str, bytes, str, dict[str, str]], Part]
    ) -> Item: ...


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
    assert urllib.parse.parse_qsl(seen[0].content.decode()) == [
        ("tags", "1"),
        ("tags", "2"),
        ("tags", "3"),
    ]


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
    assert dict(urllib.parse.parse_qsl(request.content.decode())) == expected_fields


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
