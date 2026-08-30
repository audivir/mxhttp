"""Tests for the `@cookies` class decorator, its per-endpoint override, and jar precedence."""

from __future__ import annotations

from typing import Annotated

import pytest
from conftest import make_consumer
from models import ITEM, Item

from mxhttp import Cookie, SyncConsumer, cookies, get

pytestmark = pytest.mark.anyio


@cookies({"tenant": "acme"})
class StaticCookiesApi(SyncConsumer):
    @get("/items")
    def list_items(self) -> Item: ...  # type: ignore[empty-body]


@cookies(lambda self: {"session": self.session_id})  # type: ignore[attr-defined]
class ComputedCookiesApi(SyncConsumer):
    session_id: str = ""

    @get("/items")
    def list_items(self) -> Item: ...  # type: ignore[empty-body]


@cookies({"tenant": "acme"})
class OverriddenEndpointApi(SyncConsumer):
    @get("/items", cookies={"tenant": "other"})
    def list_items(self) -> Item: ...  # type: ignore[empty-body]


@cookies({"tenant": "acme"})
class DisabledEndpointApi(SyncConsumer):
    @get("/items", cookies=None)
    def list_items(self) -> Item: ...  # type: ignore[empty-body]


@cookies({"tenant": "acme"})
class CallTimeOverrideApi(SyncConsumer):
    @get("/items")
    def list_items(  # type: ignore[empty-body]
        self, tenant: Annotated[str | None, Cookie] = None
    ) -> Item: ...


@cookies({"tenant": "acme"})
class NoCookieParamApi(SyncConsumer):
    @get("/items")
    def list_items(self) -> Item: ...  # type: ignore[empty-body]


@cookies({"tenant": "acme"})
class OverrideParamApi(SyncConsumer):
    @get("/items")
    def list_items(  # type: ignore[empty-body]
        self, tenant: Annotated[str | None, Cookie(override=True)] = None
    ) -> Item: ...


def test_cookies_property_returns_class_default() -> None:
    consumer = StaticCookiesApi()

    assert consumer._class_endpoint_kwargs["cookies"] == {"tenant": "acme"}  # noqa: SLF001


def test_cookies_property_defaults_to_none() -> None:
    consumer = SyncConsumer()

    assert consumer._class_endpoint_kwargs.get("cookies") is None  # noqa: SLF001


def test_static_cookies_sent_on_every_call() -> None:
    consumer, seen = make_consumer(StaticCookiesApi, ITEM, track_requests=True)

    consumer.list_items()

    assert "tenant=acme" in seen[0].headers["cookie"]


def test_computed_cookies_reevaluated_per_call() -> None:
    consumer, seen = make_consumer(ComputedCookiesApi, ITEM, track_requests=True)

    consumer.session_id = "first"
    consumer.list_items()
    consumer.session_id = "second"
    consumer.list_items()

    assert "session=first" in seen[0].headers["cookie"]
    assert "session=second" in seen[1].headers["cookie"]


def test_endpoint_cookies_override_class_default() -> None:
    consumer, seen = make_consumer(OverriddenEndpointApi, ITEM, track_requests=True)

    consumer.list_items()

    assert "tenant=other" in seen[0].headers["cookie"]


def test_endpoint_cookies_none_disables_class_default() -> None:
    consumer, seen = make_consumer(DisabledEndpointApi, ITEM, track_requests=True)

    consumer.list_items()

    assert "cookie" not in seen[0].headers


def test_call_time_cookie_overrides_class_default_without_raising() -> None:
    consumer, seen = make_consumer(CallTimeOverrideApi, ITEM, track_requests=True)

    consumer.list_items(tenant="other")

    assert "tenant=other" in seen[0].headers["cookie"]


def test_call_time_cookie_falls_back_to_class_default_when_not_given() -> None:
    consumer, seen = make_consumer(CallTimeOverrideApi, ITEM, track_requests=True)

    consumer.list_items()

    assert "tenant=acme" in seen[0].headers["cookie"]


def test_jar_wins_over_class_default_with_no_matching_parameter() -> None:
    consumer, seen = make_consumer(NoCookieParamApi, ITEM, track_requests=True)
    consumer.session.cookies.set("tenant", "from-jar")

    consumer.list_items()

    assert "tenant=from-jar" in seen[0].headers["cookie"]
    assert "acme" not in seen[0].headers["cookie"]


def test_jar_wins_over_class_default_even_with_override_parameter_unset() -> None:
    consumer, seen = make_consumer(CallTimeOverrideApi, ITEM, track_requests=True)
    consumer.session.cookies.set("tenant", "from-jar")

    consumer.list_items()

    assert "tenant=from-jar" in seen[0].headers["cookie"]


def test_plain_named_cookie_still_loses_to_jar_even_when_given() -> None:
    consumer, seen = make_consumer(CallTimeOverrideApi, ITEM, track_requests=True)
    consumer.session.cookies.set("tenant", "from-jar")

    consumer.list_items(tenant="explicit")

    assert "tenant=from-jar" in seen[0].headers["cookie"]


def test_named_cookie_with_override_wins_over_jar_and_class_default() -> None:
    consumer, seen = make_consumer(OverrideParamApi, ITEM, track_requests=True)
    consumer.session.cookies.set("tenant", "from-jar")

    consumer.list_items(tenant="explicit")

    assert "tenant=explicit" in seen[0].headers["cookie"]
