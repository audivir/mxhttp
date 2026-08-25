"""Tests for all marker types other than `Path`."""

from __future__ import annotations

from typing import Annotated, Any

import httpx  # noqa: TC002
import pytest
from models import Item  # noqa: TC002

from mxhttp import Body, Cookie, Field, Header, Part, Query, SyncConsumer, get, post


def test_unknown_extra_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Unexpected extra: typing\.Any"):

        class BadApi(SyncConsumer):
            @get("/things")
            def get_thing(self, thing_id: Annotated[int, Any]) -> httpx.Response: ...  # type: ignore[empty-body]


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


def test_header_sequence_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Header argument 'thing' must be"):

        class BadApi(SyncConsumer):
            @get("/things")
            def get_thing(  # type: ignore[empty-body]
                self, thing: Annotated[list[str], Header]
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
