"""Tests for the `Path` marker handling."""

from __future__ import annotations

from typing import Annotated, Literal

import httpx
import pytest
from conftest import make_consumer
from models import Item  # noqa: TC002

from mxhttp import Path, Query, SyncConsumer, get


def test_path_special_chars() -> None:
    class SpecialCharApi(SyncConsumer):
        @get("/things/{?}")
        def get_thing(self, thing_id: Annotated[int, Path["?"]]) -> httpx.Response: ...  # type: ignore[empty-body]

    consumer = make_consumer(SpecialCharApi, b"0")
    result = consumer.get_thing(thing_id=0)
    assert isinstance(result, httpx.Response)
    assert result.status_code == 200


def test_field_conversion_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"must not have a format spec or conversion"):

        class BadApi(SyncConsumer):
            @get("/things/{kind!r}")
            def get_thing(self, kind: str) -> Item: ...  # type: ignore[empty-body]


def test_anonymous_field_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Anonymous \('\{\}'\)"):

        class BadApi(SyncConsumer):
            @get("/things/{}")
            def get_thing(self, kind: str) -> Item: ...  # type: ignore[empty-body]


def test_positional_field_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"positional \('\{0\}'\)"):

        class BadApi(SyncConsumer):
            @get("/things/{0}")
            def get_thing(self, kind: str) -> Item: ...  # type: ignore[empty-body]


def test_path_explicit_marker_mismatched_target_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"targets field 'x', but it doesn't appear"):

        class BadApi(SyncConsumer):
            @get("/things/{z}")
            def get_thing(self, z: Annotated[str, Path["x"]]) -> Item: ...  # type: ignore[empty-body]


def test_duplicate_explicit_query_wire_name_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Parameter 'y' reuses query wire name 'x'"):

        class BadApi(SyncConsumer):
            @get("/things")
            def get_thing(  # type: ignore[empty-body]
                self, x: Annotated[str, Query["x"]], y: Annotated[str, Query["x"]]
            ) -> Item: ...


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


def test_path_literal_bool_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Path argument 'item_id' must be str \| int \| float"):

        class BadApi(SyncConsumer):
            @get("/things/{item_id}")
            def get_thing(self, item_id: Literal[True, False]) -> httpx.Response: ...  # type: ignore[empty-body]


def test_path_literal_bytes_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Path argument 'item_id' must be str \| int \| float"):

        class BadApi(SyncConsumer):
            @get("/things/{item_id}")
            def get_thing(self, item_id: Literal[b"a"]) -> httpx.Response: ...  # type: ignore[empty-body]


def test_path_bare_union_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Path argument 'item_id' must be str \| int \| float"):

        class BadApi(SyncConsumer):
            @get("/things/{item_id}")
            def get_thing(self, item_id: Annotated[int | str, Path]) -> httpx.Response: ...  # type: ignore[empty-body]


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


def test_path_unbound_parameter_raises_type_error() -> None:
    with pytest.raises(TypeError, match="no Query/Field/Part/Body binding"):

        class BadApi(SyncConsumer):
            @get("/things")
            def get_thing(self, mystery: str) -> Item: ...  # type: ignore[empty-body]


def test_path_field_reused_by_two_parameters_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Parameter 'b' reuses path wire name 'item_id'"):

        class BadApi(SyncConsumer):
            @get("/things/{item_id}")
            def get_thing(  # type: ignore[empty-body]
                self, a: Annotated[str, Path["item_id"]], b: Annotated[str, Path["item_id"]]
            ) -> Item: ...


def test_path_field_unbound_by_any_parameter_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Path field 'item_id' is not bound by any parameter"):

        class BadApi(SyncConsumer):
            @get("/things/{item_id}")
            def get_thing(self) -> Item: ...  # type: ignore[empty-body]
