"""Tests for the consumer closing module."""

from __future__ import annotations

import pytest
from conftest import make_consumer
from models import ITEM, AsyncCrudApi, CrudApi

pytestmark = pytest.mark.anyio


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
