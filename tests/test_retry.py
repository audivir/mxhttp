"""Tests for the automatic request retry module."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from conftest import make_consumer, make_stateful_consumer
from models import ITEM, ITEM_BUILTINS, Item

from mxhttp import AsyncConsumer, Retry, SyncConsumer, get, retry

pytestmark = pytest.mark.anyio


class RetryApi(SyncConsumer):
    @get("/items/{item_id}", retry=Retry(attempts=5, backoff=0))
    def get_item_resilient(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", retry=Retry(attempts=2, backoff=0))
    def get_item_tight(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}")
    def get_item_no_retry(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


class AsyncRetryApi(AsyncConsumer):
    @get("/items/{item_id}", retry=Retry(attempts=5, backoff=0))
    async def get_item_resilient(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", retry=Retry(attempts=2, backoff=0))
    async def get_item_tight(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}")
    async def get_item_no_retry(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


@retry(Retry(attempts=3, backoff=0))
class DecoratedRetryApi(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


@retry(Retry(attempts=5, backoff=0))
class MixedRetryApi(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", retry=Retry(attempts=2, backoff=0))
    def get_item_custom(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", retry=None)
    def get_item_no_retry(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


class RetryOnApi(SyncConsumer):
    @get("/items/{item_id}", retry=Retry(attempts=2, backoff=0, on={418}))
    def get_item_custom_status(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", retry=Retry(attempts=2, backoff=0, on={ValueError}))
    def get_item_custom_exception(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get(
        "/items/{item_id}",
        retry=Retry(attempts=2, backoff=0, on={lambda r: r.headers.get("x-retry-me") == "yes"}),
    )
    def get_item_predicate(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", retry=Retry(attempts=3, backoff=0, on={500, ValueError}))
    def get_item_mixed(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


class RetryTimeoutApi(SyncConsumer):
    @get("/items/{item_id}", retry=Retry(attempts=3, backoff=0, timeout=7))
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


class AsyncRetryTimeoutApi(AsyncConsumer):
    @get("/items/{item_id}", retry=Retry(attempts=3, backoff=0, timeout=7))
    async def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


@pytest.mark.parametrize("cls", [RetryApi, AsyncRetryApi], ids=["sync", "async"])
async def test_retry_recovers_after_transient_status(
    *, cls: type[RetryApi | AsyncRetryApi]
) -> None:
    consumer, calls = make_stateful_consumer(cls, [503, 503, 200])

    if isinstance(consumer, AsyncRetryApi):
        assert await consumer.get_item_resilient(item_id=1) == ITEM
    else:
        assert consumer.get_item_resilient(item_id=1) == ITEM

    assert calls() == 3


@pytest.mark.parametrize("cls", [RetryApi, AsyncRetryApi], ids=["sync", "async"])
async def test_retry_exhausts_attempts_then_raises(*, cls: type[RetryApi | AsyncRetryApi]) -> None:
    consumer, calls = make_stateful_consumer(cls, [500])

    with pytest.raises(httpx.HTTPStatusError):  # noqa: PT012
        if isinstance(consumer, AsyncRetryApi):
            await consumer.get_item_tight(item_id=1)
        else:
            consumer.get_item_tight(item_id=1)
    assert calls() == 2


@pytest.mark.parametrize("cls", [RetryApi, AsyncRetryApi], ids=["sync", "async"])
async def test_retry_on_exception_recovers(*, cls: type[RetryApi | AsyncRetryApi]) -> None:
    consumer, calls = make_stateful_consumer(cls, [httpx.ConnectError("boom"), 200])

    if isinstance(consumer, AsyncRetryApi):
        assert await consumer.get_item_tight(item_id=1) == ITEM
    else:
        assert consumer.get_item_tight(item_id=1) == ITEM
    assert calls() == 2


@pytest.mark.parametrize("cls", [RetryApi, AsyncRetryApi], ids=["sync", "async"])
async def test_retry_on_exception_exhausts_attempts_then_reraises(
    *, cls: type[RetryApi | AsyncRetryApi]
) -> None:
    consumer, calls = make_stateful_consumer(cls, [httpx.ConnectError("boom")])

    with pytest.raises(httpx.ConnectError):  # noqa: PT012
        if isinstance(consumer, AsyncRetryApi):
            await consumer.get_item_tight(item_id=1)
        else:
            consumer.get_item_tight(item_id=1)
    assert calls() == 2


@pytest.mark.parametrize("cls", [RetryApi, AsyncRetryApi], ids=["sync", "async"])
async def test_retry_ignores_non_retryable_status(*, cls: type[RetryApi | AsyncRetryApi]) -> None:
    consumer, calls = make_stateful_consumer(cls, [404])

    with pytest.raises(httpx.HTTPStatusError):  # noqa: PT012
        if isinstance(consumer, AsyncRetryApi):
            await consumer.get_item_tight(item_id=1)
        else:
            consumer.get_item_tight(item_id=1)
    assert calls() == 1


@pytest.mark.parametrize("cls", [RetryApi, AsyncRetryApi], ids=["sync", "async"])
async def test_no_retry_without_config(*, cls: type[RetryApi | AsyncRetryApi]) -> None:
    consumer, calls = make_stateful_consumer(cls, [503, 200])

    with pytest.raises(httpx.HTTPStatusError):  # noqa: PT012
        if isinstance(consumer, AsyncRetryApi):
            await consumer.get_item_no_retry(item_id=1)
        else:
            consumer.get_item_no_retry(item_id=1)
    assert calls() == 1


def test_retry_class_decorator_applies_to_all_instances() -> None:
    consumer, calls = make_stateful_consumer(DecoratedRetryApi, [503, 200])

    assert consumer.get_item(item_id=1) == ITEM
    assert calls() == 2


def test_endpoint_without_override_inherits_class_retry() -> None:
    consumer, calls = make_stateful_consumer(MixedRetryApi, [500, 500, 200])

    assert consumer.get_item(item_id=1) == ITEM
    assert calls() == 3


def test_endpoint_retry_override_replaces_class_retry() -> None:
    consumer, calls = make_stateful_consumer(MixedRetryApi, [500])

    with pytest.raises(httpx.HTTPStatusError):
        consumer.get_item_custom(item_id=1)
    assert calls() == 2  # endpoint override caps at 2 attempts, not the class's 5


def test_endpoint_retry_override_can_disable_retry() -> None:
    consumer, calls = make_stateful_consumer(MixedRetryApi, [500, 200])

    with pytest.raises(httpx.HTTPStatusError):
        consumer.get_item_no_retry(item_id=1)
    assert calls() == 1  # explicit retry=None disables the class's default


def test_retry_on_status_replaces_default_statuses() -> None:
    consumer, calls = make_stateful_consumer(RetryOnApi, [500])

    with pytest.raises(httpx.HTTPStatusError):
        consumer.get_item_custom_status(item_id=1)
    assert calls() == 1  # 500 isn't in the custom `on`, so a default-retryable status is ignored


def test_retry_on_custom_status_code() -> None:
    consumer, calls = make_stateful_consumer(RetryOnApi, [418, 200])

    assert consumer.get_item_custom_status(item_id=1) == ITEM
    assert calls() == 2


def test_retry_on_custom_exception_type() -> None:
    consumer, calls = make_stateful_consumer(RetryOnApi, [ValueError("boom"), 200])

    assert consumer.get_item_custom_exception(item_id=1) == ITEM
    assert calls() == 2


def test_retry_on_exception_not_in_on_reraises_immediately() -> None:
    consumer, calls = make_stateful_consumer(RetryOnApi, [httpx.ConnectError("boom")])

    with pytest.raises(httpx.ConnectError):
        consumer.get_item_custom_exception(item_id=1)
    assert calls() == 1  # ConnectError isn't in the custom `on`, so it isn't caught at all


def test_retry_on_predicate_over_response() -> None:
    consumer, calls = make_stateful_consumer(
        RetryOnApi,
        [
            httpx.Response(200, headers={"x-retry-me": "yes"}, json=ITEM_BUILTINS),
            httpx.Response(200, json=ITEM_BUILTINS),
        ],
    )

    assert consumer.get_item_predicate(item_id=1) == ITEM
    assert calls() == 2  # a 200 still retries because the predicate, not the status, decides


def test_retry_on_mixed_entries_matches_either() -> None:
    consumer, calls = make_stateful_consumer(RetryOnApi, [ValueError("boom"), 500, 200])

    assert consumer.get_item_mixed(item_id=1) == ITEM
    assert calls() == 3


def test_retry_rejects_zero_attempts() -> None:
    with pytest.raises(ValueError, match="attempts must be >= 1"):
        Retry(attempts=0)


def test_retry_delay_grows_exponentially_and_caps() -> None:
    config = Retry(backoff=1.0, exponent=2.0, jitter=False, max_delay=5.0)
    assert config.delay(1) == 1.0
    assert config.delay(2) == 2.0
    assert config.delay(3) == 4.0
    assert config.delay(4) == 5.0  # capped by max_delay


def test_retry_timeout_none_keeps_consumer_default() -> None:
    class TimeoutApi(SyncConsumer):
        @get("/items/{item_id}", retry=Retry(attempts=1, backoff=0))
        def get_item_default_timeout(self, item_id: int) -> dict[str, Any]: ...  # type: ignore[empty-body]

    consumer = make_consumer(TimeoutApi, lambda r: r.extensions)

    extensions = consumer.get_item_default_timeout(item_id=1)
    timeout = extensions.get("timeout")
    assert timeout is not None
    assert timeout == {"connect": 5.0, "read": 5.0, "write": 5.0, "pool": 5.0}


@pytest.mark.parametrize("cls", [RetryTimeoutApi, AsyncRetryTimeoutApi], ids=["sync", "async"])
async def test_retry_timeout_applied_to_every_attempt(
    *, cls: type[RetryTimeoutApi | AsyncRetryTimeoutApi]
) -> None:
    seen_timeouts: list[dict[str, object] | None] = []
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        seen_timeouts.append(request.extensions.get("timeout"))
        calls += 1
        if calls < 2:
            return httpx.Response(503, json=ITEM_BUILTINS)
        return httpx.Response(200, json=ITEM_BUILTINS)

    consumer = make_consumer(cls, handler)
    if isinstance(consumer, AsyncRetryTimeoutApi):
        assert await consumer.get_item(item_id=1) == ITEM
    else:
        assert consumer.get_item(item_id=1) == ITEM

    assert calls == 2
    assert all(t == {"connect": 7, "read": 7, "write": 7, "pool": 7} for t in seen_timeouts)
