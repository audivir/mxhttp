"""Tests for `RawBody`, the reserved-header guard, and the sole body-encoding guard."""

from __future__ import annotations

from typing import Annotated

import pytest
from conftest import make_consumer
from models import ITEM, Item, NewItem

from mxhttp import AsyncConsumer, Body, Field, Header, Part, PartValue, RawBody, SyncConsumer, post

pytestmark = pytest.mark.anyio


class RawBodyApi(SyncConsumer):
    @post("/raw")
    def send(self, payload: Annotated[bytes, RawBody]) -> Item: ...  # type: ignore[empty-body]


class AsyncRawBodyApi(AsyncConsumer):
    @post("/raw")
    async def send(self, payload: Annotated[bytes, RawBody]) -> Item: ...  # type: ignore[empty-body]


class RawBodyStrApi(SyncConsumer):
    @post("/raw")
    def send(self, payload: Annotated[str, RawBody]) -> Item: ...  # type: ignore[empty-body]


class RawBodyContentTypeApi(SyncConsumer):
    @post("/raw")
    def send(  # type: ignore[empty-body]
        self, payload: Annotated[bytes, RawBody("application/xml")]
    ) -> Item: ...


class RawBodyContentTypeKeywordApi(SyncConsumer):
    @post("/raw")
    def send(  # type: ignore[empty-body]
        self, payload: Annotated[bytes, RawBody(content_type="application/octet-stream")]
    ) -> Item: ...


@pytest.mark.parametrize("cls", [RawBodyApi, AsyncRawBodyApi], ids=["sync", "async"])
async def test_raw_body_bare_sends_no_content_type(
    *, cls: type[RawBodyApi | AsyncRawBodyApi]
) -> None:
    consumer, seen = make_consumer(cls, ITEM, track_requests=True)

    if isinstance(consumer, AsyncRawBodyApi):
        result = await consumer.send(payload=b"raw-bytes")
    else:
        result = consumer.send(payload=b"raw-bytes")

    assert result == ITEM
    request = seen[0]
    assert request.content == b"raw-bytes"
    assert "content-type" not in request.headers


def test_raw_body_str_payload_round_trips() -> None:
    consumer, seen = make_consumer(RawBodyStrApi, ITEM, track_requests=True)

    result = consumer.send(payload="raw-text")

    assert result == ITEM
    assert seen[0].content == b"raw-text"


def test_raw_body_content_type_positional() -> None:
    consumer, seen = make_consumer(RawBodyContentTypeApi, ITEM, track_requests=True)

    consumer.send(payload=b"<xml/>")

    assert seen[0].headers["content-type"] == "application/xml"
    assert seen[0].content == b"<xml/>"


def test_raw_body_content_type_keyword() -> None:
    consumer, seen = make_consumer(RawBodyContentTypeKeywordApi, ITEM, track_requests=True)

    consumer.send(payload=b"\x00\x01")

    assert seen[0].headers["content-type"] == "application/octet-stream"


def test_raw_body_rejects_non_bytes_str_type() -> None:
    with pytest.raises(TypeError, match=r"RawBody argument 'payload' must be bytes \| str"):

        class BadApi(SyncConsumer):
            @post("/raw")
            def send(self, payload: Annotated[int, RawBody]) -> Item: ...  # type: ignore[empty-body]


def test_header_rejects_content_type_wire_name() -> None:
    with pytest.raises(TypeError, match=r"cannot bind reserved wire name 'content-type'"):

        class BadApi(SyncConsumer):
            @post("/raw")
            def send(  # type: ignore[empty-body]
                self, value: Annotated[str, Header["Content-Type"]]
            ) -> Item: ...


def test_header_rejects_content_type_wire_name_lowercase() -> None:
    with pytest.raises(TypeError, match=r"cannot bind reserved wire name 'content-type'"):

        class BadApi(SyncConsumer):
            @post("/raw")
            def send(  # type: ignore[empty-body]
                self, value: Annotated[str, Header["content-type"]]
            ) -> Item: ...


def test_header_rejects_cookie_wire_name() -> None:
    with pytest.raises(TypeError, match=r"cannot bind reserved wire name 'cookie'"):

        class BadApi(SyncConsumer):
            @post("/raw")
            def send(self, value: Annotated[str, Header["Cookie"]]) -> Item: ...  # type: ignore[empty-body]


def test_raw_body_conflicts_with_body() -> None:
    with pytest.raises(TypeError, match=r"cannot be combined with parameter 'item'"):

        class BadApi(SyncConsumer):
            @post("/raw")
            def send(  # type: ignore[empty-body]
                self, item: Annotated[NewItem, Body], payload: Annotated[bytes, RawBody]
            ) -> Item: ...


def test_raw_body_conflicts_with_field() -> None:
    with pytest.raises(TypeError, match=r"cannot be combined with parameter 'name'"):

        class BadApi(SyncConsumer):
            @post("/raw")
            def send(  # type: ignore[empty-body]
                self, name: Annotated[str, Field], payload: Annotated[bytes, RawBody]
            ) -> Item: ...


def test_raw_body_conflicts_with_part() -> None:
    with pytest.raises(TypeError, match=r"cannot be combined with parameter 'file'"):

        class BadApi(SyncConsumer):
            @post("/raw")
            def send(  # type: ignore[empty-body]
                self, file: Annotated[PartValue, Part], payload: Annotated[bytes, RawBody]
            ) -> Item: ...


def test_body_conflicts_with_field() -> None:
    with pytest.raises(TypeError, match=r"cannot be combined with parameter 'item'"):

        class BadApi(SyncConsumer):
            @post("/raw")
            def send(  # type: ignore[empty-body]
                self, item: Annotated[NewItem, Body], name: Annotated[str, Field]
            ) -> Item: ...


def test_body_conflicts_with_part() -> None:
    with pytest.raises(TypeError, match=r"cannot be combined with parameter 'item'"):

        class BadApi(SyncConsumer):
            @post("/raw")
            def send(  # type: ignore[empty-body]
                self, item: Annotated[NewItem, Body], file: Annotated[PartValue, Part]
            ) -> Item: ...


def test_second_body_marker_raises() -> None:
    with pytest.raises(TypeError, match=r"cannot be combined with parameter 'first'"):

        class BadApi(SyncConsumer):
            @post("/raw")
            def send(  # type: ignore[empty-body]
                self, first: Annotated[NewItem, Body], second: Annotated[bytes, RawBody]
            ) -> Item: ...


def test_field_and_part_together_still_allowed() -> None:
    class OkApi(SyncConsumer):
        @post("/raw")
        def send(  # type: ignore[empty-body]
            self, name: Annotated[str, Field], file: Annotated[PartValue, Part]
        ) -> Item: ...

    consumer, seen = make_consumer(OkApi, ITEM, track_requests=True)

    result = consumer.send(name="a", file=("f.bin", b"content"))

    assert result == ITEM
    assert seen[0].headers["content-type"].startswith("multipart/form-data")
