"""Tests for the `@headers` class decorator and its per-endpoint override."""

from __future__ import annotations

from typing import Annotated

import pytest
from conftest import make_consumer
from models import ITEM, Item

from mxhttp import Header, SyncConsumer, get, headers

pytestmark = pytest.mark.anyio


@headers({"X-Api-Version": "2"})
class StaticHeadersApi(SyncConsumer):
    @get("/items")
    def list_items(self) -> Item: ...  # type: ignore[empty-body]


@headers(lambda self: {"Authorization": f"Bearer {self.auth_token}"})  # type: ignore[attr-defined]
class ComputedHeadersApi(SyncConsumer):
    auth_token: str = ""

    @get("/items")
    def list_items(self) -> Item: ...  # type: ignore[empty-body]


@headers({"X-Api-Version": "2"})
class OverriddenEndpointApi(SyncConsumer):
    @get("/items", headers={"X-Api-Version": "3"})
    def list_items(self) -> Item: ...  # type: ignore[empty-body]


@headers({"X-Api-Version": "2"})
class DisabledEndpointApi(SyncConsumer):
    @get("/items", headers=None)
    def list_items(self) -> Item: ...  # type: ignore[empty-body]


@headers({"X-Api-Version": "2"})
class CallTimeOverrideApi(SyncConsumer):
    @get("/items")
    def list_items(  # type: ignore[empty-body]
        self, api_version: Annotated[str | None, Header["X-Api-Version"]] = None
    ) -> Item: ...


def test_headers_property_returns_class_default() -> None:
    consumer = StaticHeadersApi()

    assert consumer.headers == {"X-Api-Version": "2"}


def test_headers_property_defaults_to_none() -> None:
    consumer = SyncConsumer()

    assert consumer.headers is None


def test_static_headers_sent_on_every_call() -> None:
    consumer, seen = make_consumer(StaticHeadersApi, ITEM, track_requests=True)

    consumer.list_items()
    consumer.list_items()

    assert seen[0].headers["x-api-version"] == "2"
    assert seen[1].headers["x-api-version"] == "2"


def test_computed_headers_reevaluated_per_call() -> None:
    consumer, seen = make_consumer(ComputedHeadersApi, ITEM, track_requests=True)

    consumer.auth_token = "first"  # noqa: S105
    consumer.list_items()
    consumer.auth_token = "second"  # noqa: S105
    consumer.list_items()

    assert seen[0].headers["authorization"] == "Bearer first"
    assert seen[1].headers["authorization"] == "Bearer second"


def test_endpoint_headers_override_class_default() -> None:
    consumer, seen = make_consumer(OverriddenEndpointApi, ITEM, track_requests=True)

    consumer.list_items()

    assert seen[0].headers["x-api-version"] == "3"


def test_endpoint_headers_none_disables_class_default() -> None:
    consumer, seen = make_consumer(DisabledEndpointApi, ITEM, track_requests=True)

    consumer.list_items()

    assert "x-api-version" not in seen[0].headers


def test_call_time_header_overrides_class_default_without_raising() -> None:
    consumer, seen = make_consumer(CallTimeOverrideApi, ITEM, track_requests=True)

    consumer.list_items(api_version="4")

    assert seen[0].headers["x-api-version"] == "4"


def test_static_headers_none_valued_key_is_omitted() -> None:
    @headers({"X-Api-Version": "2", "X-Skip": None})
    class Api(SyncConsumer):
        @get("/items")
        def list_items(self) -> Item: ...  # type: ignore[empty-body]

    consumer, seen = make_consumer(Api, ITEM, track_requests=True)

    consumer.list_items()

    assert seen[0].headers["x-api-version"] == "2"
    assert "x-skip" not in seen[0].headers


def test_call_time_header_falls_back_to_class_default_when_not_given() -> None:
    consumer, seen = make_consumer(CallTimeOverrideApi, ITEM, track_requests=True)

    consumer.list_items()

    assert seen[0].headers["x-api-version"] == "2"


def test_headers_rejects_static_content_type_at_decoration_time() -> None:
    with pytest.raises(TypeError, match=r"@headers cannot set reserved key 'Content-Type'"):
        headers({"Content-Type": "application/json"})


def test_headers_rejects_callable_cookie_at_call_time() -> None:
    @headers(lambda _: {"Cookie": "a=b"})
    class BadApi(SyncConsumer):
        @get("/items")
        def list_items(self) -> Item: ...  # type: ignore[empty-body]

    consumer, _ = make_consumer(BadApi, ITEM, track_requests=True)

    with pytest.raises(ValueError, match=r"@headers cannot set reserved key 'Cookie'"):
        consumer.list_items()
