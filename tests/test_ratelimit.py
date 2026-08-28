"""Tests for the rate limiting module."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Iterator  # noqa: TC003
from unittest.mock import MagicMock

import pytest
from conftest import make_consumer
from models import ITEM, Item

from mxhttp import (
    AsyncConsumer,
    Event,
    RateLimit,
    RateLimitExceededError,
    SyncConsumer,
    get,
    ratelimit,
)

pytestmark = pytest.mark.anyio


class FakeClock:
    """Stores a manually advanceable monotonic clock, standing in for `time.monotonic`."""

    def __init__(self, start: float = 0.0) -> None:
        """Initializes the clock at `start` seconds."""
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class LooseLimitApi(SyncConsumer):
    @get("/items/{item_id}", ratelimit=RateLimit(calls=2, period=60, block=False))
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


class TightLimitApi(SyncConsumer):
    @get("/items/{item_id}", ratelimit=RateLimit(calls=1, period=60))
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", ratelimit=RateLimit(calls=1, period=60, block=False))
    def get_item_no_block(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", ratelimit=RateLimit(calls=1, period=60, max_delay=5))
    def get_item_capped(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", ratelimit=RateLimit(calls=1, period=60, block=False))
    def stream_item(self, item_id: int) -> Iterator[bytes]: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", ratelimit=RateLimit(calls=1, period=60, block=False))
    def sse_item(self, item_id: int) -> Iterator[Event]: ...  # type: ignore[empty-body]


class AsyncTightLimitApi(AsyncConsumer):
    @get("/items/{item_id}", ratelimit=RateLimit(calls=1, period=60))
    async def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", ratelimit=RateLimit(calls=1, period=60, max_delay=5))
    async def get_item_capped(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", ratelimit=RateLimit(calls=1, period=60, block=False))
    async def stream_item(self, item_id: int) -> AsyncIterator[bytes]: ...  # type: ignore[empty-body]


@ratelimit(RateLimit(calls=1, period=60, block=False))
class SharedHostLimitApi(SyncConsumer):
    @get("/items/{item_id}")
    def get_item_a(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}")
    def get_item_b(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


@ratelimit(RateLimit(calls=1, period=60, block=False))
class MixedLimitApi(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", ratelimit=RateLimit(calls=5, period=60, block=False))
    def get_item_relaxed(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", ratelimit=None)
    def get_item_unlimited(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


def test_ratelimit_rejects_non_positive_calls() -> None:
    with pytest.raises(ValueError, match="calls must be >= 1"):
        RateLimit(calls=0, period=60)


def test_ratelimit_rejects_non_positive_period() -> None:
    with pytest.raises(ValueError, match="period must be > 0"):
        RateLimit(calls=1, period=0)


def test_ratelimit_allows_calls_within_period(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = FakeClock()
    monkeypatch.setattr(time, "monotonic", clock)
    sleep = MagicMock()
    monkeypatch.setattr(time, "sleep", sleep)

    consumer = make_consumer(LooseLimitApi, ITEM, base_url="https://ratelimit-loose.example.com")
    assert consumer.get_item(item_id=1) == ITEM
    assert consumer.get_item(item_id=2) == ITEM
    sleep.assert_not_called()


def test_ratelimit_raises_when_block_false_and_limit_exceeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    monkeypatch.setattr(time, "monotonic", clock)

    consumer = make_consumer(
        LooseLimitApi, ITEM, base_url="https://ratelimit-loose-exceed.example.com"
    )
    assert consumer.get_item(item_id=1) == ITEM
    assert consumer.get_item(item_id=2) == ITEM
    with pytest.raises(RateLimitExceededError):
        consumer.get_item(item_id=3)


def test_ratelimit_blocks_and_sleeps_when_over_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = FakeClock()
    monkeypatch.setattr(time, "monotonic", clock)
    delays: list[float] = []
    monkeypatch.setattr(time, "sleep", delays.append)

    consumer = make_consumer(TightLimitApi, ITEM, base_url="https://ratelimit-block.example.com")
    assert consumer.get_item(item_id=1) == ITEM
    assert consumer.get_item(item_id=2) == ITEM
    assert delays == [60.0]  # the clock never advances, so the wait is exactly the full period


def test_ratelimit_window_resets_after_period_elapses(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = FakeClock()
    monkeypatch.setattr(time, "monotonic", clock)

    consumer = make_consumer(TightLimitApi, ITEM, base_url="https://ratelimit-reset.example.com")
    assert consumer.get_item_no_block(item_id=1) == ITEM
    with pytest.raises(RateLimitExceededError):
        consumer.get_item_no_block(item_id=2)

    clock.advance(61)
    assert consumer.get_item_no_block(item_id=3) == ITEM  # a fresh window, no longer exceeded


def test_ratelimit_raises_when_wait_exceeds_max_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = FakeClock()
    monkeypatch.setattr(time, "monotonic", clock)
    sleep = MagicMock()
    monkeypatch.setattr(time, "sleep", sleep)

    consumer = make_consumer(TightLimitApi, ITEM, base_url="https://ratelimit-capped.example.com")
    assert consumer.get_item_capped(item_id=1) == ITEM
    with pytest.raises(RateLimitExceededError):
        consumer.get_item_capped(item_id=2)  # the 60s wait exceeds max_delay=5, so it raises
    sleep.assert_not_called()


def test_ratelimit_scoped_per_host(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = FakeClock()
    monkeypatch.setattr(time, "monotonic", clock)

    host_a = make_consumer(LooseLimitApi, ITEM, base_url="https://ratelimit-host-a.example.com")
    # same hostname as host_a, but an explicit non-default port: must be a separate window.
    host_a_alt_port = make_consumer(
        LooseLimitApi, ITEM, base_url="https://ratelimit-host-a.example.com:8443"
    )
    # a different hostname, defaulting to the plain-http port instead of https: also separate.
    host_b = make_consumer(LooseLimitApi, ITEM, base_url="http://ratelimit-host-b.example.com")

    assert host_a.get_item(item_id=1) == ITEM
    assert host_a.get_item(item_id=2) == ITEM
    assert host_a_alt_port.get_item(item_id=1) == ITEM
    assert host_a_alt_port.get_item(item_id=2) == ITEM
    assert host_b.get_item(item_id=1) == ITEM
    assert host_b.get_item(item_id=2) == ITEM

    with pytest.raises(RateLimitExceededError):
        host_a.get_item(item_id=3)
    with pytest.raises(RateLimitExceededError):
        host_a_alt_port.get_item(item_id=3)
    with pytest.raises(RateLimitExceededError):
        host_b.get_item(item_id=3)


def test_ratelimit_class_decorator_shares_budget_across_endpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    monkeypatch.setattr(time, "monotonic", clock)

    consumer = make_consumer(
        SharedHostLimitApi, ITEM, base_url="https://ratelimit-shared.example.com"
    )
    assert consumer.get_item_a(item_id=1) == ITEM
    with pytest.raises(RateLimitExceededError):
        consumer.get_item_b(item_id=1)  # the limit is scoped to the host, not to one endpoint


def test_ratelimit_endpoint_override_replaces_class_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    monkeypatch.setattr(time, "monotonic", clock)

    consumer = make_consumer(MixedLimitApi, ITEM, base_url="https://ratelimit-mixed-a.example.com")
    for item_id in range(1, 6):
        assert consumer.get_item_relaxed(item_id=item_id) == ITEM
    with pytest.raises(RateLimitExceededError):
        consumer.get_item_relaxed(item_id=6)


def test_ratelimit_endpoint_override_can_disable(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = FakeClock()
    monkeypatch.setattr(time, "monotonic", clock)

    consumer = make_consumer(MixedLimitApi, ITEM, base_url="https://ratelimit-mixed-b.example.com")
    for item_id in range(50):
        assert consumer.get_item_unlimited(item_id=item_id) == ITEM


def test_ratelimit_applies_to_streaming_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = FakeClock()
    monkeypatch.setattr(time, "monotonic", clock)

    consumer = make_consumer(
        TightLimitApi, b"chunk", base_url="https://ratelimit-stream.example.com"
    )
    first = consumer.stream_item(item_id=1)
    assert b"".join(first) == b"chunk"
    with pytest.raises(RateLimitExceededError):
        consumer.stream_item(item_id=2)


def test_ratelimit_applies_to_sse_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = FakeClock()
    monkeypatch.setattr(time, "monotonic", clock)

    consumer = make_consumer(
        TightLimitApi, b"data: hi\n\n", base_url="https://ratelimit-sse.example.com"
    )
    first = list(consumer.sse_item(item_id=1))
    assert [e.data for e in first] == ["hi"]
    with pytest.raises(RateLimitExceededError):
        list(consumer.sse_item(item_id=2))


async def test_ratelimit_blocks_and_sleeps_when_over_limit_async(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    monkeypatch.setattr(time, "monotonic", clock)
    delays: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        delays.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    consumer = make_consumer(
        AsyncTightLimitApi, ITEM, base_url="https://ratelimit-block-async.example.com"
    )
    assert await consumer.get_item(item_id=1) == ITEM
    assert await consumer.get_item(item_id=2) == ITEM
    assert delays == [60.0]


async def test_ratelimit_raises_when_wait_exceeds_max_delay_async(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    monkeypatch.setattr(time, "monotonic", clock)
    sleep = MagicMock()
    monkeypatch.setattr(asyncio, "sleep", sleep)

    consumer = make_consumer(
        AsyncTightLimitApi, ITEM, base_url="https://ratelimit-capped-async.example.com"
    )
    assert await consumer.get_item_capped(item_id=1) == ITEM
    with pytest.raises(RateLimitExceededError):
        await consumer.get_item_capped(item_id=2)
    sleep.assert_not_called()


async def test_ratelimit_applies_to_async_streaming_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    monkeypatch.setattr(time, "monotonic", clock)

    consumer = make_consumer(
        AsyncTightLimitApi, b"chunk", base_url="https://ratelimit-stream-async.example.com"
    )
    first = await consumer.stream_item(item_id=1)
    assert b"".join([chunk async for chunk in first]) == b"chunk"
    with pytest.raises(RateLimitExceededError):
        await consumer.stream_item(item_id=2)
