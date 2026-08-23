"""Tests for `Literal[...]` and `Enum` handling."""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Literal

import httpx  # noqa: TC002
import pytest
from conftest import make_consumer
from models import ITEM, Item

from mxhttp import Header, Query, SyncConsumer, get


class Suit(str, Enum):
    HEARTS = "hearts"
    SPADES = "spades"


class Level(Enum):
    LOW = "low"
    HIGH = "high"


class EmptyEnum(Enum):
    pass


class BoolFlag(Enum):
    ON = True
    OFF = False


def test_literal_path_param() -> None:
    class LiteralPathApi(SyncConsumer):
        @get("/things/{kind}")
        def get_thing(  # type: ignore[empty-body]
            self, kind: Literal["a", "b", "c"] = "a"
        ) -> Item: ...

    consumer, seen = make_consumer(LiteralPathApi, ITEM, track_requests=True)

    result = consumer.get_thing(kind="b")

    assert result == ITEM
    assert seen[0].url.path == "/things/b"


def test_str_enum_path_param() -> None:

    class StrEnumPathApi(SyncConsumer):
        @get("/suits/{suit}")
        def get_suit(self, suit: Suit) -> Item: ...  # type: ignore[empty-body]

    consumer, seen = make_consumer(StrEnumPathApi, ITEM, track_requests=True)

    result = consumer.get_suit(suit=Suit.SPADES)

    assert result == ITEM
    assert seen[0].url.path == "/suits/spades"


def test_plain_enum_header_serializes_value_not_repr() -> None:

    class PlainEnumHeaderApi(SyncConsumer):
        @get("/things")
        def get_thing(  # type: ignore[empty-body]
            self, level: Annotated[Level, Header["X-Level"]]
        ) -> httpx.Response: ...

    consumer, seen = make_consumer(PlainEnumHeaderApi, ITEM, track_requests=True)

    consumer.get_thing(level=Level.HIGH)

    assert seen[0].headers["X-Level"] == "high"


def test_plain_enum_path_param_serializes_value_not_repr() -> None:

    class PlainEnumPathApi(SyncConsumer):
        @get("/levels/{level}")
        def get_level(self, level: Level) -> Item: ...  # type: ignore[empty-body]

    consumer, seen = make_consumer(PlainEnumPathApi, ITEM, track_requests=True)

    result = consumer.get_level(level=Level.LOW)

    assert result == ITEM
    assert seen[0].url.path == "/levels/low"


def test_literal_query_param() -> None:
    class LiteralQueryApi(SyncConsumer):
        @get("/search")
        def search(  # type: ignore[empty-body]
            self, sort: Annotated[Literal["asc", "desc"], Query] = "asc"
        ) -> Item: ...

    consumer, seen = make_consumer(LiteralQueryApi, ITEM, track_requests=True)

    result = consumer.search(sort="desc")

    assert result == ITEM
    assert dict(seen[0].url.params) == {"sort": "desc"}


def test_path_empty_enum_raises_type_error() -> None:

    with pytest.raises(TypeError, match=r"Path argument 'item_id' must be str \| int \| float"):

        class BadApi(SyncConsumer):
            @get("/things/{item_id}")
            def get_thing(self, item_id: EmptyEnum) -> httpx.Response: ...  # type: ignore[empty-body]


def test_path_enum_of_bool_raises_type_error() -> None:

    with pytest.raises(TypeError, match=r"Path argument 'item_id' must be str \| int \| float"):

        class BadApi(SyncConsumer):
            @get("/things/{item_id}")
            def get_thing(self, item_id: BoolFlag) -> httpx.Response: ...  # type: ignore[empty-body]
