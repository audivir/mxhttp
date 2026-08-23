"""Tests for decoding HTTP responses into the types declared by `MoreApi`."""

from __future__ import annotations

from conftest import make_consumer
from models import ITEM, AttrsItem, BaseModelItem, DataclassItem, MoreApi


def test_decodes_str() -> None:
    consumer = make_consumer(MoreApi, b"content")

    assert consumer.get_string() == "content"


def test_decodes_bytes() -> None:
    consumer = make_consumer(MoreApi, b"content")

    assert consumer.get_bytes() == b"content"


def test_decodes_int_list() -> None:
    consumer = make_consumer(MoreApi, [1, 2, 3])

    assert consumer.get_int_list() == [1, 2, 3]


def test_decodes_item_list() -> None:
    consumer = make_consumer(MoreApi, [{"id": 1, "name": "Widget", "price": 9.99}])

    assert consumer.get_item_list() == [ITEM]


def test_decodes_str_int_dict() -> None:
    consumer = make_consumer(MoreApi, {"a": 1, "b": 2})

    assert consumer.get_str_int_dict() == {"a": 1, "b": 2}


def test_decodes_typed_dict() -> None:
    consumer = make_consumer(MoreApi, {"id": 1})

    result = consumer.get_typed_dict()
    assert result == {"id": 1}


def test_decodes_dataclass() -> None:
    consumer = make_consumer(MoreApi, {"id": 1})

    assert consumer.get_dataclass() == DataclassItem(id=1)


def test_decodes_attrs_class() -> None:
    consumer = make_consumer(MoreApi, {"id": 1})

    assert consumer.get_attrs() == AttrsItem(id=1)


def test_decodes_pydantic_base_model() -> None:
    consumer = make_consumer(MoreApi, {"id": 1})

    assert consumer.get_base_model() == BaseModelItem(id=1)
