"""Tests for the conftest functions."""

from __future__ import annotations

import httpx
import pytest
from conftest import make_consumer
from models import ITEM, AsyncRawApi, RawApi

from mxhttp.consumer import BaseConsumer

pytestmark = pytest.mark.anyio


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


def test_make_consumer_rejects_unsupported_consumer_class() -> None:
    with pytest.raises(TypeError, match="Unsupported consumer class"):
        make_consumer(BaseConsumer, ITEM)
