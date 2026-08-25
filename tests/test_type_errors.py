"""Tests for the mxhttp type-checking error diagnostics module."""

from __future__ import annotations

import httpx  # noqa: TC002
import pytest
from models import Item  # noqa: TC002

from mxhttp import AsyncConsumer, SyncConsumer, get

pytestmark = pytest.mark.anyio


@pytest.mark.mypy_testing
def test_invalid_consumer_method_combinations() -> None:
    # ruff: noqa: E501
    class SyncWithAsyncMethodApi(SyncConsumer):
        @get(  # E: Value of type variable "Parsed_T" of "__call__" of "EndpointDecorator" cannot be "Coroutine[Any, Any, Response]"  [type-var]
            "/ping"
        )
        async def ping(self, /) -> httpx.Response: ...  # type: ignore[empty-body]

    class AsyncWithSyncMethodApi(AsyncConsumer):
        @get(  # E: Value of type variable "SyncC_T" of "__call__" of "EndpointDecorator" cannot be "AsyncWithSyncMethodApi"
            "/ping"
        )
        def ping(self, /) -> httpx.Response: ...  # type: ignore[empty-body]


@pytest.mark.mypy_testing
def test_undecodable_return_types_rejected() -> None:
    # ruff: noqa: E501
    class FloatApi(SyncConsumer):
        @get(  # E: Value of type variable "Parsed_T" of "__call__" of "EndpointDecorator" cannot be "float"  [type-var]
            "/thing"
        )
        def get_thing(self) -> float: ...  # type: ignore[empty-body]

    class IntApi(SyncConsumer):
        @get(  # E: Value of type variable "Parsed_T" of "__call__" of "EndpointDecorator" cannot be "int"  [type-var]
            "/thing"
        )
        def get_thing(self) -> int: ...  # type: ignore[empty-body]


@pytest.mark.mypy_testing
def test_union_return_type_rejected() -> None:
    # ruff: noqa: E501
    with pytest.raises(TypeError, match="Return type must not be a union"):

        class BadApi(SyncConsumer):
            @get(  # E: Value of type variable "Parsed_T" of "__call__" of "EndpointDecorator" cannot be "Item | None"  [type-var]
                "/search"
            )
            def get_union(self) -> Item | None: ...
