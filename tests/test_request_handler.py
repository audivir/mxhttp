"""Tests for the `@request_handler` class decorator and its per-endpoint override."""

from __future__ import annotations

from typing import TYPE_CHECKING

import httpx
import pytest
from conftest import make_consumer
from models import ITEM, Item

from mxhttp import Retry, SyncConsumer, get, request_handler

if TYPE_CHECKING:
    from mxhttp.request import RequestSpec

pytestmark = pytest.mark.anyio


def add_trace_header(spec: RequestSpec) -> RequestSpec:
    spec.headers = {**(spec.headers or {}), "X-Trace": "abc"}
    return spec


def replace_trace_header(spec: RequestSpec) -> RequestSpec:
    spec.headers = {**(spec.headers or {}), "X-Trace": "override"}
    return spec


@request_handler(add_trace_header)
class TracedApi(SyncConsumer):
    @get("/items")
    def list_items(self) -> Item: ...  # type: ignore[empty-body]


@request_handler(add_trace_header)
class OverriddenEndpointApi(SyncConsumer):
    @get("/items", request_handler=replace_trace_header)
    def list_items(self) -> Item: ...  # type: ignore[empty-body]


@request_handler(add_trace_header)
class DisabledEndpointApi(SyncConsumer):
    @get("/items", request_handler=None)
    def list_items(self) -> Item: ...  # type: ignore[empty-body]


def test_request_handler_mutates_built_request() -> None:
    consumer, seen = make_consumer(TracedApi, ITEM, track_requests=True)

    consumer.list_items()

    assert seen[0].headers["x-trace"] == "abc"


def test_endpoint_request_handler_overrides_class_default() -> None:
    consumer, seen = make_consumer(OverriddenEndpointApi, ITEM, track_requests=True)

    consumer.list_items()

    assert seen[0].headers["x-trace"] == "override"


def test_endpoint_request_handler_none_disables_class_default() -> None:
    consumer, seen = make_consumer(DisabledEndpointApi, ITEM, track_requests=True)

    consumer.list_items()

    assert "x-trace" not in seen[0].headers


def test_request_handler_runs_once_per_call_not_per_retry_attempt() -> None:
    calls: list[None] = []

    def counting_handler(spec: RequestSpec) -> RequestSpec:
        calls.append(None)
        return spec

    @request_handler(counting_handler)
    class RetryingApi(SyncConsumer):
        @get("/items", retry=Retry(attempts=3, backoff=0, on={503}))
        def list_items(self) -> Item: ...  # type: ignore[empty-body]

    attempt_count = 0

    def transport_handler(_: httpx.Request) -> httpx.Response | Item:
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count < 3:
            return httpx.Response(503)
        return ITEM

    consumer, seen = make_consumer(RetryingApi, transport_handler, track_requests=True)

    consumer.list_items()

    assert len(seen) == 3
    assert len(calls) == 1
