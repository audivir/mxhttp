"""Tests for mxhttp consumer and endpoint decorator type checking."""

from __future__ import annotations

from collections.abc import Coroutine
from typing import Any

import httpx
import pytest
from conftest import make_consumer
from models import (
    ITEM,
    AsyncCrudApi,
    AsyncRawApi,
    BaseModelItem,
    CrudApi,
    DataclassItem,
    Item,
    MoreApi,
    RawApi,
    TypedDictItem,
)
from typing_extensions import assert_type

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize("cls", [RawApi, AsyncRawApi], ids=["sync", "async"])
async def test_inferred_raw_types(*, cls: type[RawApi | AsyncRawApi]) -> None:
    consumer = make_consumer(cls, b"pong", status_code=204)
    if isinstance(consumer, AsyncRawApi):
        coro = consumer.ping()
        assert_type(coro, Coroutine[Any, Any, httpx.Response])  # type: ignore[unused-coroutine]
        result = await coro
    else:
        result = consumer.ping()
        assert_type(result, httpx.Response)

    assert isinstance(result, httpx.Response)
    assert result.content == b"pong"
    assert result.status_code == 204


@pytest.mark.parametrize("cls", [CrudApi, AsyncCrudApi], ids=["sync", "async"])
async def test_inferred_struct_types(*, cls: type[CrudApi | AsyncCrudApi]) -> None:
    consumer = make_consumer(cls, ITEM)
    if isinstance(consumer, AsyncCrudApi):
        coro = consumer.get_item(item_id=7)
        assert_type(coro, Coroutine[Any, Any, Item])  # type: ignore[unused-coroutine]
        result = await coro
    else:
        result = consumer.get_item(item_id=7)
        assert_type(result, Item)

    assert result == ITEM


async def test_str_bytes_types() -> None:
    consumer = make_consumer(MoreApi, b"content")

    str_result = consumer.get_string()
    assert_type(str_result, str)
    assert str_result == "content"

    bytes_result = consumer.get_bytes()
    assert_type(bytes_result, bytes)
    assert bytes_result == b"content"


async def test_int_list_type() -> None:
    consumer = make_consumer(MoreApi, [1, 2, 3])

    result = consumer.get_int_list()
    assert_type(result, list[int])
    assert result == [1, 2, 3]


async def test_item_list_type() -> None:
    consumer = make_consumer(MoreApi, [{"id": 1, "name": "Widget", "price": 9.99}])

    result = consumer.get_item_list()
    assert_type(result, list[Item])
    assert result == [ITEM]


async def test_str_int_dict_type() -> None:
    consumer = make_consumer(MoreApi, {"a": 1})

    result = consumer.get_str_int_dict()
    assert_type(result, dict[str, int])
    assert result == {"a": 1}


async def test_typed_dict_type() -> None:
    consumer = make_consumer(MoreApi, {"id": 1})

    result = consumer.get_typed_dict()
    assert_type(result, TypedDictItem)
    assert result == {"id": 1}


async def test_dataclass_type() -> None:
    consumer = make_consumer(MoreApi, {"id": 1})

    result = consumer.get_dataclass()
    assert_type(result, DataclassItem)
    assert result == DataclassItem(id=1)


async def test_pydantic_base_model_type() -> None:
    consumer = make_consumer(MoreApi, {"id": 1})

    result = consumer.get_base_model()
    assert_type(result, BaseModelItem)
    assert result == BaseModelItem(id=1)
