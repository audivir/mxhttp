"""Tests for `idempotent=` on `@post`/`@put` and the `Idempotency-Key` reservation."""

from __future__ import annotations

import uuid
from typing import Annotated

import httpx
import pytest
from conftest import make_consumer
from models import ITEM, Item

from mxhttp import Header, Retry, SyncConsumer, endpoint, get, headers, post, put

pytestmark = pytest.mark.anyio


class DisabledIdempotencyApi(SyncConsumer):
    @post("/items")
    def create(self) -> Item: ...  # type: ignore[empty-body]


class AutoIdempotencyApi(SyncConsumer):
    @put("/items/{item_id}", idempotent=True)
    def update(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


class CustomIdempotencyApi(SyncConsumer):
    @post("/items", idempotent=lambda: "fixed-key")
    def create(self) -> Item: ...  # type: ignore[empty-body]


class RetryingIdempotencyApi(SyncConsumer):
    @post("/items", idempotent=True, retry=Retry(attempts=3, backoff=0, on={503}))
    def create(self) -> Item: ...  # type: ignore[empty-body]


def test_idempotent_disabled_by_default_sends_no_header() -> None:
    consumer, seen = make_consumer(DisabledIdempotencyApi, ITEM, track_requests=True)

    consumer.create()

    assert "idempotency-key" not in seen[0].headers


def test_idempotent_true_sends_a_uuid() -> None:
    consumer, seen = make_consumer(AutoIdempotencyApi, ITEM, track_requests=True)

    consumer.update(item_id=1)

    uuid.UUID(seen[0].headers["idempotency-key"])  # raises ValueError if not a valid UUID


def test_idempotent_true_generates_a_fresh_key_per_call() -> None:
    consumer, seen = make_consumer(AutoIdempotencyApi, ITEM, track_requests=True)

    consumer.update(item_id=1)
    consumer.update(item_id=1)

    assert seen[0].headers["idempotency-key"] != seen[1].headers["idempotency-key"]


def test_idempotent_callable_uses_generator() -> None:
    consumer, seen = make_consumer(CustomIdempotencyApi, ITEM, track_requests=True)

    consumer.create()

    assert seen[0].headers["idempotency-key"] == "fixed-key"


def test_idempotent_key_stable_across_retries_of_one_call() -> None:
    call_count = 0

    def handler(_: httpx.Request) -> httpx.Response | Item:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            return httpx.Response(503)
        return ITEM

    consumer, seen = make_consumer(RetryingIdempotencyApi, handler, track_requests=True)

    consumer.create()

    assert len(seen) == 3
    keys = {request.headers["idempotency-key"] for request in seen}
    assert len(keys) == 1


def test_idempotent_key_differs_across_separate_calls() -> None:
    consumer, seen = make_consumer(RetryingIdempotencyApi, ITEM, track_requests=True)

    consumer.create()
    consumer.create()

    assert seen[0].headers["idempotency-key"] != seen[1].headers["idempotency-key"]


def test_idempotent_rejects_non_post_put_methods() -> None:
    with pytest.raises(TypeError, match=r"idempotent is only valid for POST/PUT endpoints"):

        class BadApi(SyncConsumer):
            @endpoint("GET", "/items", idempotent=True)
            def list_items(self) -> Item: ...  # type: ignore[empty-body]


def test_get_does_not_expose_idempotent() -> None:
    with pytest.raises(TypeError, match=r"unexpected keyword argument 'idempotent'"):
        get("/items", idempotent=True)  # type: ignore[call-arg]


def test_header_rejects_idempotency_key_wire_name() -> None:
    with pytest.raises(TypeError, match=r"cannot bind reserved wire name 'idempotency-key'"):

        class BadApi(SyncConsumer):
            @post("/items")
            def create(  # type: ignore[empty-body]
                self, value: Annotated[str, Header["Idempotency-Key"]]
            ) -> Item: ...


def test_headers_rejects_static_idempotency_key_at_decoration_time() -> None:
    with pytest.raises(TypeError, match=r"@headers cannot set reserved key 'Idempotency-Key'"):
        headers({"Idempotency-Key": "a"})
