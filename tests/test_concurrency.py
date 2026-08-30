"""Tests for the concurrency module."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import AsyncIterator, Iterator  # noqa: TC003
from typing import TYPE_CHECKING

import anyio
import httpx
import pytest
from conftest import make_consumer
from models import ITEM, Item
from typing_extensions import override

from mxhttp import (
    AsyncConsumer,
    AsyncDownloader,
    Concurrency,
    ConcurrencyExceededError,
    ConcurrencyTimeoutError,
    Downloader,
    Event,
    SyncConsumer,
    base_url,
    concurrency,
    get,
)
from mxhttp.concurrency import gate_concurrency_async, gate_concurrency_sync

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.anyio


def test_concurrency_struct_validation() -> None:
    with pytest.raises(ValueError, match="limit must be >= 1"):
        Concurrency(limit=0)

    with pytest.raises(ValueError, match="timeout must be >= 0"):
        Concurrency(limit=1, timeout=-0.5)

    c = Concurrency(limit=5, timeout=1.5, block=False, key="test_pool")
    assert c.limit == 5
    assert c.timeout == 1.5
    assert not c.block
    assert c.key == "test_pool"


def test_concurrency_class_decorator() -> None:
    @concurrency(3)
    class ConsumerA(SyncConsumer): ...

    assert ConsumerA._class_endpoint_kwargs["concurrency"] == Concurrency(limit=3)  # noqa: SLF001
    consumer_a = ConsumerA()
    assert consumer_a._class_endpoint_kwargs["concurrency"] == Concurrency(limit=3)  # noqa: SLF001

    @concurrency(Concurrency(limit=2, timeout=1.0))
    class ConsumerB(SyncConsumer): ...

    assert ConsumerB._class_endpoint_kwargs["concurrency"] == Concurrency(limit=2, timeout=1.0)  # noqa: SLF001
    consumer_b = ConsumerB()
    assert consumer_b._class_endpoint_kwargs["concurrency"] == Concurrency(limit=2, timeout=1.0)  # noqa: SLF001


@base_url("https://api.example.com")
@concurrency(Concurrency(limit=1, block=False, key="sync_non_block"))
class SyncNonBlockApi(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", concurrency=None)
    def get_item_unlimited(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", concurrency=Concurrency(limit=2, block=False, key="sync_pool_2"))
    def get_item_pool_2(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", concurrency=2)
    def get_item_int_concurrency(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


def test_sync_concurrency_non_blocking_exceeded() -> None:
    in_flight = threading.Event()
    release_handler = threading.Event()
    calls = 0

    def handler(unused_request: httpx.Request) -> Item:
        nonlocal calls
        calls += 1
        if calls == 1:
            in_flight.set()
            release_handler.wait(timeout=2.0)
        return ITEM

    client = make_consumer(SyncNonBlockApi, handler)

    def run_first() -> None:
        client.get_item(item_id=1)

    t = threading.Thread(target=run_first)
    t.start()
    try:
        assert in_flight.wait(timeout=1.0)
        with pytest.raises(ConcurrencyExceededError, match="concurrency limit of 1 reached"):
            client.get_item(item_id=2)

        unlimited_item = client.get_item_unlimited(item_id=3)
        assert unlimited_item.id == 1

        int_concurrency_item = client.get_item_int_concurrency(item_id=4)
        assert int_concurrency_item.id == 1
    finally:
        release_handler.set()
        t.join(timeout=1.0)


@base_url("https://api.example.com")
@concurrency(Concurrency(limit=1, timeout=0.05, key="sync_timeout"))
class SyncTimeoutApi(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


def test_sync_concurrency_timeout_exceeded() -> None:
    in_flight = threading.Event()
    release_handler = threading.Event()

    def handler(unused_request: httpx.Request) -> Item:
        in_flight.set()
        release_handler.wait(timeout=2.0)
        return ITEM

    client = make_consumer(SyncTimeoutApi, handler)

    def run_first() -> None:
        client.get_item(item_id=1)

    t = threading.Thread(target=run_first)
    t.start()
    try:
        assert in_flight.wait(timeout=1.0)
        with pytest.raises(ConcurrencyTimeoutError, match="timed out waiting for concurrency slot"):
            client.get_item(item_id=2)
    finally:
        release_handler.set()
        t.join(timeout=1.0)


@base_url("https://api.example.com")
@concurrency(Concurrency(limit=1, block=False, key="async_non_block"))
class AsyncNonBlockApi(AsyncConsumer):
    @get("/items/{item_id}")
    async def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", concurrency=None)
    async def get_item_unlimited(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


async def test_async_concurrency_non_blocking_exceeded() -> None:
    in_flight = asyncio.Event()
    release_handler = asyncio.Event()
    calls = 0

    async def handler(unused_request: httpx.Request) -> Item:
        nonlocal calls
        calls += 1
        if calls == 1:
            in_flight.set()
            await release_handler.wait()
        return ITEM

    client = make_consumer(AsyncNonBlockApi, handler)

    task = asyncio.create_task(client.get_item(item_id=1))
    await in_flight.wait()

    try:
        with pytest.raises(ConcurrencyExceededError, match="concurrency limit of 1 reached"):
            await client.get_item(item_id=2)

        unlimited_item = await client.get_item_unlimited(item_id=3)
        assert unlimited_item.id == 1
    finally:
        release_handler.set()
        await task


@base_url("https://api.example.com")
@concurrency(Concurrency(limit=1, timeout=0.05, key="async_timeout"))
class AsyncTimeoutApi(AsyncConsumer):
    @get("/items/{item_id}")
    async def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


async def test_async_concurrency_timeout_exceeded() -> None:
    in_flight = asyncio.Event()
    release_handler = asyncio.Event()

    async def handler(unused_request: httpx.Request) -> Item:
        in_flight.set()
        await release_handler.wait()
        return ITEM

    client = make_consumer(AsyncTimeoutApi, handler)

    task = asyncio.create_task(client.get_item(item_id=1))
    await in_flight.wait()

    try:
        with pytest.raises(ConcurrencyTimeoutError, match="timed out waiting for concurrency slot"):
            await client.get_item(item_id=2)
    finally:
        release_handler.set()
        await task


@base_url("https://api.example.com")
@concurrency(Concurrency(limit=1, block=True, key="sync_blocking"))
class SyncBlockingApi(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


def test_sync_concurrency_blocking_waits_for_slot() -> None:
    in_flight = threading.Event()
    release_handler = threading.Event()
    results: list[Item] = []

    def handler(request: httpx.Request) -> Item:
        if request.url.path == "/items/1":
            in_flight.set()
            release_handler.wait(timeout=2.0)
        return ITEM

    client = make_consumer(SyncBlockingApi, handler)

    def run_first() -> None:
        results.append(client.get_item(item_id=1))

    def run_second() -> None:
        results.append(client.get_item(item_id=2))

    t1 = threading.Thread(target=run_first)
    t2 = threading.Thread(target=run_second)
    t1.start()
    assert in_flight.wait(timeout=1.0)
    t2.start()
    time.sleep(0.02)
    release_handler.set()
    t1.join(timeout=1.0)
    t2.join(timeout=1.0)
    assert len(results) == 2


@base_url("https://api.example.com")
@concurrency(Concurrency(limit=1, block=True, key="async_blocking"))
class AsyncBlockingApi(AsyncConsumer):
    @get("/items/{item_id}")
    async def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


async def test_async_concurrency_blocking_waits_for_slot() -> None:
    in_flight = asyncio.Event()
    release_handler = asyncio.Event()

    async def handler(request: httpx.Request) -> Item:
        if request.url.path == "/items/1":
            in_flight.set()
            await release_handler.wait()
        return ITEM

    client = make_consumer(AsyncBlockingApi, handler)

    task1 = asyncio.create_task(client.get_item(item_id=1))
    await in_flight.wait()

    task2 = asyncio.create_task(client.get_item(item_id=2))
    await asyncio.sleep(0.01)
    release_handler.set()

    res1 = await task1
    res2 = await task2
    assert res1.id == 1
    assert res2.id == 1


class MultiChunkStream(httpx.SyncByteStream, httpx.AsyncByteStream):
    """Test stream yielding chunks with controllable completion."""

    def __init__(self, can_finish: threading.Event) -> None:
        """Stores the completion signaling event."""
        self.can_finish = can_finish

    @override
    def __iter__(self) -> Iterator[bytes]:
        yield b"chunk1"
        self.can_finish.wait(timeout=2.0)
        yield b"chunk2"

    @override
    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"chunk1"
        await anyio.to_thread.run_sync(self.can_finish.wait, 2.0)
        yield b"chunk2"


@base_url("https://api.example.com")
@concurrency(Concurrency(limit=1, block=False, key="stream_sync_concurrency"))
class SyncStreamConcurrencyApi(SyncConsumer):
    @get("/stream")
    def stream_bytes(self) -> Iterator[bytes]: ...  # type: ignore[empty-body]

    @get("/sse")
    def sse_events(self) -> Iterator[Event]: ...  # type: ignore[empty-body]


def test_sync_stream_holds_concurrency_until_consumed() -> None:
    can_finish = threading.Event()
    multi_stream = MultiChunkStream(can_finish)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/stream":
            return httpx.Response(200, stream=multi_stream)
        return httpx.Response(200, content=b"data: hello\n\n")

    client = make_consumer(SyncStreamConcurrencyApi, handler)

    stream = client.stream_bytes()
    chunk1 = next(stream)
    assert chunk1 == b"chunk1"

    try:
        with pytest.raises(ConcurrencyExceededError, match="concurrency limit of 1 reached"):
            next(client.sse_events())
    finally:
        multi_stream.can_finish.set()

    chunk2 = next(stream)
    assert chunk2 == b"chunk2"

    with pytest.raises(StopIteration):
        next(stream)

    events = list(client.sse_events())
    assert len(events) == 1
    assert events[0].data == "hello"


@base_url("https://api.example.com")
@concurrency(Concurrency(limit=1, block=False, key="stream_async_concurrency"))
class AsyncStreamConcurrencyApi(AsyncConsumer):
    @get("/stream")
    async def stream_bytes(self) -> AsyncIterator[bytes]: ...  # type: ignore[empty-body]

    @get("/sse")
    async def sse_events(self) -> AsyncIterator[Event]: ...  # type: ignore[empty-body]


async def test_async_stream_holds_concurrency_until_consumed() -> None:
    can_finish = threading.Event()
    multi_stream = MultiChunkStream(can_finish)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/stream":
            return httpx.Response(200, stream=multi_stream)
        return httpx.Response(200, content=b"data: async-hello\n\n")

    client = make_consumer(AsyncStreamConcurrencyApi, handler)

    stream = await client.stream_bytes()
    chunk = await stream.__anext__()
    assert chunk == b"chunk1"

    try:
        with pytest.raises(ConcurrencyExceededError, match="concurrency limit of 1 reached"):  # noqa: PT012
            events = await client.sse_events()
            await events.__anext__()
    finally:
        multi_stream.can_finish.set()

    chunk2 = await stream.__anext__()
    assert chunk2 == b"chunk2"

    with pytest.raises(StopAsyncIteration):
        await stream.__anext__()

    events_stream = await client.sse_events()
    event = await events_stream.__anext__()
    assert event.data == "async-hello"


@base_url("https://api.example.com")
@concurrency(Concurrency(limit=1, block=False, key="downloader_concurrency"))
class SyncDownloadConcurrencyApi(SyncConsumer):
    @get("/files/{file_id}")
    def download(self, file_id: int) -> Downloader: ...  # type: ignore[empty-body]


def test_sync_downloader_gated_by_concurrency(tmp_path: Path) -> None:
    in_flight = threading.Event()
    release_handler = threading.Event()

    def handler(unused_request: httpx.Request) -> httpx.Response:
        in_flight.set()
        release_handler.wait(timeout=2.0)
        return httpx.Response(200, content=b"content")

    client = make_consumer(SyncDownloadConcurrencyApi, handler)

    dl1 = client.download(file_id=1)
    dl2 = client.download(file_id=2)

    dest1 = tmp_path / "file1.bin"
    dest2 = tmp_path / "file2.bin"

    def run_first() -> None:
        dl1(dest1)

    t = threading.Thread(target=run_first)
    t.start()
    try:
        assert in_flight.wait(timeout=1.0)
        with pytest.raises(ConcurrencyExceededError, match="concurrency limit of 1 reached"):
            dl2(dest2)
    finally:
        release_handler.set()
        t.join(timeout=1.0)


@base_url("https://api.example.com")
@concurrency(Concurrency(limit=1, block=False, key="async_downloader_concurrency"))
class AsyncDownloadConcurrencyApi(AsyncConsumer):
    @get("/files/{file_id}")
    async def download_async_def(self, file_id: int) -> AsyncDownloader: ...  # type: ignore[empty-body]


async def test_async_downloader_gated_by_concurrency(tmp_path: Path) -> None:
    in_flight = asyncio.Event()
    release_handler = asyncio.Event()

    async def handler(unused_request: httpx.Request) -> httpx.Response:
        in_flight.set()
        await release_handler.wait()
        return httpx.Response(200, content=b"content")

    client = make_consumer(AsyncDownloadConcurrencyApi, handler)

    dl_async_def1 = await client.download_async_def(file_id=1)
    assert isinstance(dl_async_def1, AsyncDownloader)

    dl_async_def2 = await client.download_async_def(file_id=2)
    assert isinstance(dl_async_def2, AsyncDownloader)

    dest1 = tmp_path / "file1.bin"
    dest2 = tmp_path / "file2.bin"

    task = asyncio.create_task(dl_async_def1(dest1))
    await in_flight.wait()

    try:
        with pytest.raises(ConcurrencyExceededError, match="concurrency limit of 1 reached"):
            await dl_async_def2(dest2)
    finally:
        release_handler.set()
        await task


def test_noop_concurrency_contexts() -> None:
    with gate_concurrency_sync(None, None):
        pass

    async def run_async() -> None:
        async with gate_concurrency_async(None, None):
            pass

    asyncio.run(run_async())
