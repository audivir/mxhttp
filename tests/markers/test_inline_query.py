"""Tests for the inline query parsing module."""

from __future__ import annotations

from typing import Annotated

import pytest
from conftest import make_consumer
from models import ITEM, Item

from mxhttp import Path, Query, SyncConsumer, get


def test_inline_query_param_is_treated_as_query_not_path() -> None:
    class InlineQueryPathApi(SyncConsumer):
        @get("/things/info?permanentId={thing_id}")
        def get_thing(self, thing_id: str) -> Item: ...  # type: ignore[empty-body]

    consumer, seen = make_consumer(InlineQueryPathApi, ITEM, track_requests=True)

    result = consumer.get_thing(thing_id="abc123")

    assert result == ITEM
    assert seen[0].url == "https://api.example.com/things/info?permanentId=abc123"


def test_inline_query_param_forwards_explicit_query_name() -> None:
    class InlineQueryExplicitQueryApi(SyncConsumer):
        @get("/things/info?permanentId={renamed}")
        def get_thing(  # type: ignore[empty-body]
            self, thing_id: Annotated[str, Query["renamed"]]
        ) -> Item: ...

    consumer, seen = make_consumer(InlineQueryExplicitQueryApi, ITEM, track_requests=True)

    result = consumer.get_thing(thing_id="abc123")

    assert result == ITEM
    assert seen[0].url == "https://api.example.com/things/info?permanentId=abc123"


def test_inline_query_param_malformed_placeholder_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Inline query parameter 'filter' must be"):

        class BadApi(SyncConsumer):
            @get("/things/info?filter=prefix-{kind}")
            def get_thing(self, kind: str) -> Item: ...  # type: ignore[empty-body]


def test_inline_query_param_bare_placeholder_key_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"must supply an explicit key"):

        class BadApi(SyncConsumer):
            @get("/things/info?{kind}")
            def get_thing(self, kind: str) -> Item: ...  # type: ignore[empty-body]


def test_inline_query_field_format_spec_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"must not have a format spec or conversion"):

        class BadApi(SyncConsumer):
            @get("/things/info?permanentId={kind:>10}")
            def get_thing(self, kind: str) -> Item: ...  # type: ignore[empty-body]


def test_inline_query_mismatched_target_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"targets field 'x', but it doesn't appear"):

        class BadApi(SyncConsumer):
            @get("/things/info?permanentId={z}")
            def get_thing(self, z: Annotated[str, Path["x"]]) -> Item: ...  # type: ignore[empty-body]


def test_inline_query_param_and_explicit_query_wire_name_collide_raises_type_error() -> None:
    with pytest.raises(
        TypeError, match=r"Query argument 'inline' targets wire name 'inline' directly"
    ):

        class BadApi(SyncConsumer):
            @get("/things?inline={query_value}")
            def get_thing(  # type: ignore[empty-body]
                self, query_value: str, inline: Annotated[str, Query]
            ) -> Item: ...


def test_inline_query_sequence_bypass_via_literal_wire_name_raises_type_error() -> None:
    with pytest.raises(
        TypeError, match=r"Query argument 'x' targets wire name 'permanentId' directly"
    ):

        class BadApi(SyncConsumer):
            @get("/things/info?permanentId={thing_id}")
            def get_thing(  # type: ignore[empty-body]
                self, x: Annotated[list[str], Query["permanentId"]]
            ) -> Item: ...


def test_inline_query_sequence_type_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Query argument 'tags' must be"):

        class BadApi(SyncConsumer):
            @get("/things?tag={tags}")
            def get_thing(self, tags: list[str]) -> Item: ...  # type: ignore[empty-body]


def test_inline_query_explicit_sequence_type_raises_type_error() -> None:
    with pytest.raises(TypeError, match=r"Query argument 'x' must be"):

        class BadApi(SyncConsumer):
            @get("/things?tag={tags}")
            def get_thing(  # type: ignore[empty-body]
                self, x: Annotated[list[str], Query["tags"]]
            ) -> Item: ...


def test_static_query_and_dynamic_query_wire_name_collide_raises_type_error() -> None:
    with pytest.raises(
        TypeError, match=r"Parameter 'active' reuses query wire name 'active', already set"
    ):

        class BadApi(SyncConsumer):
            @get("/things?active=true")
            def get_thing(  # type: ignore[empty-body]
                self, active: Annotated[str, Query]
            ) -> Item: ...


def test_inline_query_field_reuses_path_field_name_raises_type_error() -> None:
    with pytest.raises(
        TypeError, match=r"Field 'thing_id' is used for both a path segment and an inline query"
    ):

        class BadApi(SyncConsumer):
            @get("/things/{thing_id}?other={thing_id}")
            def get_thing(  # type: ignore[empty-body]
                self, thing_id: str, other_id: Annotated[str, Query["other"]]
            ) -> Item: ...


def test_inline_query_field_unbound_by_any_parameter_raises_type_error() -> None:
    with pytest.raises(
        TypeError, match=r"Inline query field 'thing_id' is not bound by any parameter"
    ):

        class BadApi(SyncConsumer):
            @get("/things/info?permanentId={thing_id}")
            def get_thing(self) -> Item: ...  # type: ignore[empty-body]


def test_static_and_inline_query_params_combine() -> None:
    class StaticAndInlineQueryPathApi(SyncConsumer):
        @get("/things/info?active=true&permanentId={thing_id}")
        def get_thing(self, thing_id: str) -> Item: ...  # type: ignore[empty-body]

    consumer, seen = make_consumer(StaticAndInlineQueryPathApi, ITEM, track_requests=True)

    result = consumer.get_thing(thing_id="abc123")

    assert result == ITEM
    assert dict(seen[0].url.params) == {"active": "true", "permanentId": "abc123"}
