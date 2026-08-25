"""Tests for the automatic request retry module."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias

import httpx
import pytest
from models import ITEM, Item

from mxhttp import AsyncConsumer, Retry, SyncConsumer, get, retry

if TYPE_CHECKING:
    from collections.abc import Callable

    from mxhttp.types import AnyC_T

pytestmark = pytest.mark.anyio


class RetryApi(SyncConsumer):
    @get("/items/{item_id}", retry=Retry(attempts=5, backoff=0))
    def get_item_resilient(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", retry=Retry(attempts=3, backoff=0))
    def get_item_limited(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

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


Step: TypeAlias = "httpx.Response | Exception"


def make_stateful_consumer(
    cls: type[AnyC_T], steps: list[Step]
) -> tuple[AnyC_T, Callable[[], int]]:
    """Builds a consumer whose transport walks through `steps` in order, repeating the last."""
    calls = 0

    def handler(unused_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        step = steps[min(calls, len(steps) - 1)]
        calls += 1
        if isinstance(step, Exception):
            raise step
        return step

    transport = httpx.MockTransport(handler)
    consumer = cls("https://api.example.com")
    if issubclass(cls, SyncConsumer):
        consumer._session = httpx.Client(base_url=consumer.base_url, transport=transport)
    else:
        consumer._session = httpx.AsyncClient(base_url=consumer.base_url, transport=transport)
    return consumer, lambda: calls


def item_response(status_code: int = 200) -> httpx.Response:
    import msgspec

    return httpx.Response(status_code, json=msgspec.to_builtins(ITEM))


def test_retry_recovers_after_transient_status() -> None:
    consumer, calls = make_stateful_consumer(
        RetryApi, [item_response(503), item_response(503), item_response(200)]
    )

    assert consumer.get_item_resilient(item_id=1) == ITEM
    assert calls() == 3


async def test_retry_recovers_after_transient_status_async() -> None:
    consumer, calls = make_stateful_consumer(
        AsyncRetryApi, [item_response(503), item_response(200)]
    )

    assert await consumer.get_item_resilient(item_id=1) == ITEM
    assert calls() == 2


def test_retry_exhausts_attempts_then_raises() -> None:
    consumer, calls = make_stateful_consumer(RetryApi, [item_response(500)])

    with pytest.raises(httpx.HTTPStatusError):
        consumer.get_item_limited(item_id=1)
    assert calls() == 3


def test_retry_on_exception_recovers() -> None:
    consumer, calls = make_stateful_consumer(
        RetryApi, [httpx.ConnectError("boom"), item_response(200)]
    )

    assert consumer.get_item_limited(item_id=1) == ITEM
    assert calls() == 2


def test_retry_on_exception_exhausts_attempts_then_reraises() -> None:
    consumer, calls = make_stateful_consumer(RetryApi, [httpx.ConnectError("boom")])

    with pytest.raises(httpx.ConnectError):
        consumer.get_item_tight(item_id=1)
    assert calls() == 2


async def test_retry_on_exception_recovers_async() -> None:
    consumer, calls = make_stateful_consumer(
        AsyncRetryApi, [httpx.ConnectError("boom"), item_response(200)]
    )

    assert await consumer.get_item_resilient(item_id=1) == ITEM
    assert calls() == 2


async def test_retry_on_exception_exhausts_attempts_then_reraises_async() -> None:
    consumer, calls = make_stateful_consumer(AsyncRetryApi, [httpx.ConnectError("boom")])

    with pytest.raises(httpx.ConnectError):
        await consumer.get_item_tight(item_id=1)
    assert calls() == 2


def test_retry_ignores_non_retryable_status() -> None:
    consumer, calls = make_stateful_consumer(RetryApi, [item_response(404)])

    with pytest.raises(httpx.HTTPStatusError):
        consumer.get_item_limited(item_id=1)
    assert calls() == 1


def test_no_retry_without_config() -> None:
    consumer, calls = make_stateful_consumer(RetryApi, [item_response(503), item_response(200)])

    with pytest.raises(httpx.HTTPStatusError):
        consumer.get_item_no_retry(item_id=1)
    assert calls() == 1


async def test_no_retry_without_config_async() -> None:
    consumer, calls = make_stateful_consumer(
        AsyncRetryApi, [item_response(503), item_response(200)]
    )

    with pytest.raises(httpx.HTTPStatusError):
        await consumer.get_item_no_retry(item_id=1)
    assert calls() == 1


def test_retry_class_decorator_applies_to_all_instances() -> None:
    consumer, calls = make_stateful_consumer(
        DecoratedRetryApi, [item_response(503), item_response(200)]
    )

    assert consumer.get_item(item_id=1) == ITEM
    assert calls() == 2


def test_endpoint_without_override_inherits_class_retry() -> None:
    consumer, calls = make_stateful_consumer(
        MixedRetryApi, [item_response(500), item_response(500), item_response(200)]
    )

    assert consumer.get_item(item_id=1) == ITEM
    assert calls() == 3


def test_endpoint_retry_override_replaces_class_retry() -> None:
    consumer, calls = make_stateful_consumer(MixedRetryApi, [item_response(500)])

    with pytest.raises(httpx.HTTPStatusError):
        consumer.get_item_custom(item_id=1)
    assert calls() == 2  # endpoint override caps at 2 attempts, not the class's 5


def test_endpoint_retry_override_can_disable_retry() -> None:
    consumer, calls = make_stateful_consumer(
        MixedRetryApi, [item_response(500), item_response(200)]
    )

    with pytest.raises(httpx.HTTPStatusError):
        consumer.get_item_no_retry(item_id=1)
    assert calls() == 1  # explicit retry=None disables the class's default


def test_retry_rejects_zero_attempts() -> None:
    with pytest.raises(ValueError, match="attempts"):
        Retry(attempts=0)


def test_retry_delay_grows_exponentially_and_caps() -> None:
    config = Retry(backoff=1.0, exponent=2.0, jitter=False, max_delay=5.0)
    assert config.delay(1) == 1.0
    assert config.delay(2) == 2.0
    assert config.delay(3) == 4.0
    assert config.delay(4) == 5.0  # capped by max_delay
