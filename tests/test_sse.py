"""Tests for SSE parsing and error checking."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator  # noqa: TC003

import httpx
import pytest
from conftest import make_consumer

from mxhttp import AsyncConsumer, Event, SyncConsumer, get

pytestmark = pytest.mark.anyio


class SseApi(SyncConsumer):
    @get("/events")
    def events(self) -> Iterator[Event]: ...  # type: ignore[empty-body]


class AsyncSseApi(AsyncConsumer):
    @get("/events")
    async def events(self) -> AsyncIterator[Event]: ...  # type: ignore[empty-body]


SSE_BODY = (
    b"event: update\n"
    b"data: line1\n"
    b"data: line2\n"
    b"id: 42\n"
    b"\n"
    b": this is a comment, ignored\n"
    b"data: second event\n"
    b"\n"
    b"retry: 5000\n"
    b"data: third event\n"
    b"\n"
)


@pytest.mark.parametrize("cls", [SseApi, AsyncSseApi], ids=["sync", "async"])
async def test_sse_parses_events(*, cls: type[SseApi | AsyncSseApi]) -> None:
    consumer = make_consumer(cls, SSE_BODY)

    if isinstance(consumer, AsyncSseApi):
        events = [event async for event in await consumer.events()]
    else:
        events = list(consumer.events())

    assert events == [
        Event(data="line1\nline2", event="update", id="42", retry=None),
        Event(data="second event", event="message", id="42", retry=None),
        Event(data="third event", event="message", id="42", retry=5000),
    ]


def test_sse_discards_incomplete_trailing_event() -> None:
    body = b"data: complete\n\ndata: incomplete-no-trailing-blank-line\n"
    consumer = make_consumer(SseApi, body)

    events = list(consumer.events())

    assert events == [Event(data="complete")]


def test_sse_raises_for_status() -> None:
    consumer = make_consumer(SseApi, b"not found", status_code=404)

    with pytest.raises(httpx.HTTPStatusError):
        list(consumer.events())


def test_sse_leading_blank_line_dispatches_nothing() -> None:
    body = b"\ndata: after-leading-blank\n\n"
    consumer = make_consumer(SseApi, body)

    events = list(consumer.events())

    assert events == [Event(data="after-leading-blank")]


def test_sse_unrecognized_field_ignored() -> None:
    body = b"foo: bar\ndata: still-parsed\n\n"
    consumer = make_consumer(SseApi, body)

    events = list(consumer.events())

    assert events == [Event(data="still-parsed")]


def test_sse_non_digit_retry_ignored() -> None:
    body = b"retry: not-a-number\ndata: x\n\n"
    consumer = make_consumer(SseApi, body)

    events = list(consumer.events())

    assert events == [Event(data="x", retry=None)]
