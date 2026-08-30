"""Tests for `Header`/`Query`/`Field`/`Cookie` bags (dynamic, unnamed-at-definition-time keys)."""

from __future__ import annotations

from typing import Annotated

import pytest
from conftest import make_consumer
from models import ITEM, Item, NewItem

from mxhttp import Body, Cookie, Field, Header, Query, SyncConsumer, cookies, get, headers, post

pytestmark = pytest.mark.anyio


class HeaderBagApi(SyncConsumer):
    @get("/items")
    def list_items(  # type: ignore[empty-body]
        self, extra_headers: Annotated[dict[str, str] | None, Header] = None
    ) -> Item: ...


class QueryBagApi(SyncConsumer):
    @get("/items")
    def list_items(  # type: ignore[empty-body]
        self, extra_query: Annotated[dict[str, str | list[str]] | None, Query] = None
    ) -> Item: ...


class FieldBagApi(SyncConsumer):
    @post("/items")
    def create(  # type: ignore[empty-body]
        self, extra_fields: Annotated[dict[str, str] | None, Field] = None
    ) -> Item: ...


class CookieBagApi(SyncConsumer):
    @get("/items")
    def list_items(  # type: ignore[empty-body]
        self, extra_cookies: Annotated[dict[str, str] | None, Cookie] = None
    ) -> Item: ...


class CookieBagOverrideApi(SyncConsumer):
    @get("/items")
    def list_items(  # type: ignore[empty-body]
        self, extra_cookies: Annotated[dict[str, str] | None, Cookie(override=True)] = None
    ) -> Item: ...


class NamedHeaderApi(SyncConsumer):
    @get("/items")
    def list_items(  # type: ignore[empty-body]
        self,
        x_trace: Annotated[str | None, Header["X-Trace-Id"]] = None,
        extra_headers: Annotated[dict[str, str] | None, Header] = None,
    ) -> Item: ...


class MultiKindBagApi(SyncConsumer):
    @get("/items")
    def list_items(  # type: ignore[empty-body]
        self,
        extra_headers: Annotated[dict[str, str] | None, Header] = None,
        extra_query: Annotated[dict[str, str] | None, Query] = None,
    ) -> Item: ...


@headers({"X-Api-Version": "2"})
class HeaderBagOverClassDefaultApi(SyncConsumer):
    @get("/items")
    def list_items(  # type: ignore[empty-body]
        self, extra_headers: Annotated[dict[str, str] | None, Header] = None
    ) -> Item: ...


@cookies({"tenant": "acme"})
class CookieBagOverClassDefaultApi(SyncConsumer):
    @get("/items")
    def list_items(  # type: ignore[empty-body]
        self, extra_cookies: Annotated[dict[str, str] | None, Cookie] = None
    ) -> Item: ...


def test_header_bag_sends_extra_headers() -> None:
    consumer, seen = make_consumer(HeaderBagApi, ITEM, track_requests=True)

    consumer.list_items(extra_headers={"X-Trace-Id": "abc123", "X-Other": "1"})

    assert seen[0].headers["x-trace-id"] == "abc123"
    assert seen[0].headers["x-other"] == "1"


def test_header_bag_none_omits_entirely() -> None:
    consumer, seen = make_consumer(HeaderBagApi, ITEM, track_requests=True)

    consumer.list_items(extra_headers=None)

    assert "x-trace-id" not in seen[0].headers


def test_header_bag_none_valued_entry_is_dropped() -> None:
    consumer, seen = make_consumer(HeaderBagApi, ITEM, track_requests=True)

    consumer.list_items(extra_headers={"X-Trace-Id": "abc123", "X-Skip": None})  # type: ignore[dict-item]

    assert seen[0].headers["x-trace-id"] == "abc123"
    assert "x-skip" not in seen[0].headers


def test_query_bag_supports_scalar_and_sequence_values() -> None:
    consumer, seen = make_consumer(QueryBagApi, ITEM, track_requests=True)

    consumer.list_items(extra_query={"verbose": "1", "tag": ["a", "b"]})

    assert seen[0].url.params.get("verbose") == "1"
    assert seen[0].url.params.get_list("tag") == ["a", "b"]


def test_field_bag_sends_form_fields() -> None:
    consumer, seen = make_consumer(FieldBagApi, ITEM, track_requests=True)

    consumer.create(extra_fields={"a": "1", "b": "2"})

    body = seen[0].content.decode()
    assert "a=1" in body
    assert "b=2" in body


def test_cookie_bag_loses_to_jar_for_every_key() -> None:
    consumer, seen = make_consumer(CookieBagApi, ITEM, track_requests=True)
    consumer.session.cookies.set("session", "jar-value")

    consumer.list_items(extra_cookies={"session": "bag-value", "theme": "dark"})

    cookie_header = seen[0].headers["cookie"]
    assert "session=jar-value" in cookie_header
    assert "theme=dark" in cookie_header
    assert "bag-value" not in cookie_header


def test_cookie_bag_override_wins_over_jar_for_every_key() -> None:
    consumer, seen = make_consumer(CookieBagOverrideApi, ITEM, track_requests=True)
    consumer.session.cookies.set("session", "jar-value")

    consumer.list_items(extra_cookies={"session": "bag-value"})

    assert "session=bag-value" in seen[0].headers["cookie"]


def test_named_header_and_bag_together_both_work() -> None:
    consumer, seen = make_consumer(NamedHeaderApi, ITEM, track_requests=True)

    consumer.list_items(x_trace="named", extra_headers={"X-Other": "1"})

    assert seen[0].headers["x-trace-id"] == "named"
    assert seen[0].headers["x-other"] == "1"


def test_named_header_and_bag_collide_raises_regardless_of_equal_values() -> None:
    consumer, _ = make_consumer(NamedHeaderApi, ITEM, track_requests=True)

    with pytest.raises(ValueError, match=r"header key 'X-Trace-Id' is set by more than one"):
        consumer.list_items(x_trace="named", extra_headers={"X-Trace-Id": "named"})


def test_header_and_query_bags_on_same_endpoint_both_work() -> None:
    consumer, seen = make_consumer(MultiKindBagApi, ITEM, track_requests=True)

    consumer.list_items(extra_headers={"X-Trace-Id": "abc"}, extra_query={"tag": "x"})

    assert seen[0].headers["x-trace-id"] == "abc"
    assert seen[0].url.params.get("tag") == "x"


def test_header_bag_key_overrides_class_default_without_raising() -> None:
    consumer, seen = make_consumer(HeaderBagOverClassDefaultApi, ITEM, track_requests=True)

    consumer.list_items(extra_headers={"X-Api-Version": "3"})

    assert seen[0].headers["x-api-version"] == "3"


def test_header_bag_falls_back_to_class_default_when_key_not_given() -> None:
    consumer, seen = make_consumer(HeaderBagOverClassDefaultApi, ITEM, track_requests=True)

    consumer.list_items(extra_headers={"X-Other": "1"})

    assert seen[0].headers["x-api-version"] == "2"
    assert seen[0].headers["x-other"] == "1"


def test_cookie_bag_key_overrides_class_default_without_raising() -> None:
    consumer, seen = make_consumer(CookieBagOverClassDefaultApi, ITEM, track_requests=True)

    consumer.list_items(extra_cookies={"tenant": "other"})

    assert "tenant=other" in seen[0].headers["cookie"]


def test_cookie_bag_falls_back_to_class_default_when_key_not_given() -> None:
    consumer, seen = make_consumer(CookieBagOverClassDefaultApi, ITEM, track_requests=True)

    consumer.list_items(extra_cookies={"other": "1"})

    assert "tenant=acme" in seen[0].headers["cookie"]
    assert "other=1" in seen[0].headers["cookie"]


def test_header_bag_rejects_content_type_key() -> None:
    consumer, _ = make_consumer(HeaderBagApi, ITEM, track_requests=True)

    with pytest.raises(ValueError, match=r"cannot bind reserved wire name 'content-type'"):
        consumer.list_items(extra_headers={"Content-Type": "text/plain"})


def test_header_bag_rejects_cookie_key() -> None:
    consumer, _ = make_consumer(HeaderBagApi, ITEM, track_requests=True)

    with pytest.raises(ValueError, match=r"cannot bind reserved wire name 'cookie'"):
        consumer.list_items(extra_headers={"Cookie": "a=b"})


def test_duplicate_header_bag_raises_at_class_definition_time() -> None:
    with pytest.raises(TypeError, match=r"is a second header bag"):

        class BadApi(SyncConsumer):
            @get("/items")
            def list_items(  # type: ignore[empty-body]
                self,
                first: Annotated[dict[str, str] | None, Header] = None,
                second: Annotated[dict[str, str] | None, Header] = None,
            ) -> Item: ...


def test_bracket_on_bag_raises_at_class_definition_time() -> None:
    with pytest.raises(TypeError, match=r"is a bag \(Mapping\) and cannot use bracket syntax"):

        class BadApi(SyncConsumer):
            @get("/items")
            def list_items(  # type: ignore[empty-body]
                self, extra_headers: Annotated[dict[str, str] | None, Header["x"]] = None
            ) -> Item: ...


def test_header_bag_rejects_sequence_value() -> None:
    with pytest.raises(TypeError, match=r"Header argument 'extra_headers' must be"):

        class BadApi(SyncConsumer):
            @get("/items")
            def list_items(  # type: ignore[empty-body]
                self, extra_headers: Annotated[dict[str, list[str]] | None, Header] = None
            ) -> Item: ...


def test_header_bag_rejects_non_str_key_type() -> None:
    with pytest.raises(TypeError, match=r"Header argument 'extra_headers' must be"):

        class BadApi(SyncConsumer):
            @get("/items")
            def list_items(  # type: ignore[empty-body]
                self, extra_headers: Annotated[dict[int, str] | None, Header] = None
            ) -> Item: ...


def test_field_bag_conflicts_with_body() -> None:
    with pytest.raises(TypeError, match=r"cannot be combined with parameter 'item'"):

        class BadApi(SyncConsumer):
            @post("/items")
            def create(  # type: ignore[empty-body]
                self,
                item: Annotated[NewItem, Body],
                extra_fields: Annotated[dict[str, str] | None, Field] = None,
            ) -> Item: ...
