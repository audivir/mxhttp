"""Tests for constructor-level authentication."""

from __future__ import annotations

import base64

import httpx
import pytest
from conftest import make_consumer
from models import ITEM, ITEM_BUILTINS, AsyncCrudApi, CrudApi

from mxhttp import AsyncConsumer, SyncConsumer

pytestmark = pytest.mark.anyio


def test_consumer_has_no_auth_by_default() -> None:
    consumer = SyncConsumer("https://api.example.com")
    assert consumer.session.auth is None


def test_consumer_accepts_httpx_auth_instance() -> None:
    auth = httpx.BasicAuth("alice", "secret")
    consumer = SyncConsumer("https://api.example.com", auth=auth)
    assert consumer.session.auth is auth


def test_consumer_accepts_tuple_auth_shorthand() -> None:
    consumer = SyncConsumer("https://api.example.com", auth=("alice", "secret"))
    assert isinstance(consumer.session.auth, httpx.BasicAuth)


def test_async_consumer_accepts_httpx_auth_instance() -> None:
    auth = httpx.BasicAuth("alice", "secret")
    consumer = AsyncConsumer("https://api.example.com", auth=auth)
    assert consumer.session.auth is auth


@pytest.mark.parametrize("cls", [CrudApi, AsyncCrudApi], ids=["sync", "async"])
async def test_basic_auth_sends_authorization_header(*, cls: type[CrudApi | AsyncCrudApi]) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=ITEM_BUILTINS)

    consumer = make_consumer(cls, handler, auth=httpx.BasicAuth("alice", "secret"))
    if isinstance(consumer, AsyncCrudApi):
        assert await consumer.get_item(item_id=1) == ITEM
    else:
        assert consumer.get_item(item_id=1) == ITEM

    expected = "Basic " + base64.b64encode(b"alice:secret").decode()
    assert seen[0].headers["Authorization"] == expected


@pytest.mark.parametrize("cls", [CrudApi, AsyncCrudApi], ids=["sync", "async"])
async def test_tuple_auth_shorthand_authenticates_like_basic_auth(
    *, cls: type[CrudApi | AsyncCrudApi]
) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=ITEM_BUILTINS)

    consumer = make_consumer(cls, handler, auth=("alice", "secret"))
    if isinstance(consumer, AsyncCrudApi):
        assert await consumer.get_item(item_id=1) == ITEM
    else:
        assert consumer.get_item(item_id=1) == ITEM

    expected = "Basic " + base64.b64encode(b"alice:secret").decode()
    assert seen[0].headers["Authorization"] == expected


DIGEST_CHALLENGE = (
    'Digest realm="test-realm", nonce="abcdef0123456789", qop="auth", '
    'opaque="5ccc069c403ebaf9f0171e9517f40e41", algorithm="MD5"'
)


@pytest.mark.parametrize("cls", [CrudApi, AsyncCrudApi], ids=["sync", "async"])
async def test_digest_auth_completes_challenge_response_flow(
    *, cls: type[CrudApi | AsyncCrudApi]
) -> None:
    # httpx mutates the same Request object in place across the retry, so a snapshot of the
    # Authorization header (not the request object itself) is taken on each round trip.
    seen_auth_headers: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_auth_headers.append(request.headers.get("Authorization"))
        if "Authorization" not in request.headers:
            return httpx.Response(401, headers={"WWW-Authenticate": DIGEST_CHALLENGE})
        return httpx.Response(200, json=ITEM_BUILTINS)

    consumer = make_consumer(cls, handler, auth=httpx.DigestAuth("alice", "secret"))
    if isinstance(consumer, AsyncCrudApi):
        assert await consumer.get_item(item_id=1) == ITEM
    else:
        assert consumer.get_item(item_id=1) == ITEM

    # httpx transparently retries with a computed Digest header, mxhttp never sees the 401.
    assert len(seen_auth_headers) == 2
    assert seen_auth_headers[0] is None
    assert seen_auth_headers[1] is not None
    assert seen_auth_headers[1].startswith("Digest ")
