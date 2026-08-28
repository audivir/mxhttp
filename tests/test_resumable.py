"""Tests for automatically resumable (Range-reconnecting) streaming downloads."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator  # noqa: TC003

import httpx
import pytest
from conftest import make_consumer
from models import Item  # noqa: TC002
from typing_extensions import override

from mxhttp import AsyncConsumer, Event, ResumeLostError, Retry, SyncConsumer, get

pytestmark = pytest.mark.anyio


class FailingSyncStream(httpx.SyncByteStream):
    """Yields `chunks` then raises `exc`, simulating a connection dropping mid-download."""

    def __init__(self, chunks: list[bytes], exc: Exception) -> None:
        """Stores the chunks to yield before `exc` is raised."""
        self.chunks = chunks
        self.exc = exc

    @override
    def __iter__(self) -> Iterator[bytes]:
        yield from self.chunks
        raise self.exc


class FailingAsyncStream(httpx.AsyncByteStream):
    """Yields `chunks` then raises `exc`, simulating a connection dropping mid-download."""

    def __init__(self, chunks: list[bytes], exc: Exception) -> None:
        """Stores the chunks to yield before `exc` is raised."""
        self.chunks = chunks
        self.exc = exc

    @override
    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self.chunks:
            yield chunk
        raise self.exc


class ResumableApi(SyncConsumer):
    @get("/files/{file_id}", resumable=Retry(attempts=3, backoff=0))
    def download(self, file_id: int) -> Iterator[bytes]: ...  # type: ignore[empty-body]

    @get("/files/{file_id}", resumable=Retry(attempts=1, backoff=0))
    def download_one_shot(self, file_id: int) -> Iterator[bytes]: ...  # type: ignore[empty-body]

    @get("/files/{file_id}", resumable=Retry(attempts=3, backoff=0, on={ValueError}))
    def download_narrow_on(self, file_id: int) -> Iterator[bytes]: ...  # type: ignore[empty-body]


class AsyncResumableApi(AsyncConsumer):
    @get("/files/{file_id}", resumable=Retry(attempts=3, backoff=0))
    async def download(self, file_id: int) -> AsyncIterator[bytes]: ...  # type: ignore[empty-body]

    @get("/files/{file_id}", resumable=Retry(attempts=1, backoff=0))
    async def download_one_shot(self, file_id: int) -> AsyncIterator[bytes]: ...  # type: ignore[empty-body]


def test_resumable_rejects_non_get_methods() -> None:
    from mxhttp import endpoint

    with pytest.raises(TypeError, match="only valid for GET"):

        class _Api(SyncConsumer):
            @endpoint("POST", "/files/{file_id}", resumable=Retry())
            def upload(self, file_id: int) -> Iterator[bytes]: ...  # type: ignore[empty-body]


def test_resumable_rejects_non_byte_stream_return_types() -> None:
    with pytest.raises(TypeError, match="Iterator\\[bytes\\]"):

        class _RegularApi(SyncConsumer):
            @get("/files/{file_id}", resumable=Retry())
            def download(self, file_id: int) -> Item: ...  # type: ignore[empty-body]

    with pytest.raises(TypeError, match="Iterator\\[bytes\\]"):

        class _SseApi(SyncConsumer):
            @get("/files/{file_id}", resumable=Retry())
            def events(self, file_id: int) -> Iterator[Event]: ...  # type: ignore[empty-body]


def test_resumable_completes_without_interruption() -> None:
    calls = 0

    def handler(unused_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=b"hello world")

    consumer = make_consumer(ResumableApi, handler)
    assert b"".join(consumer.download(file_id=1)) == b"hello world"
    assert calls == 1


def test_resumable_reconnects_after_transport_error_sync() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if "range" not in request.headers:
            return httpx.Response(
                200,
                headers={"ETag": '"v1"'},
                stream=FailingSyncStream([b"hello "], httpx.ReadError("boom")),
            )
        return httpx.Response(206, content=b"world")

    consumer = make_consumer(ResumableApi, handler)
    assert b"".join(consumer.download(file_id=1)) == b"hello world"

    assert len(seen) == 2
    assert "range" not in seen[0].headers
    assert seen[1].headers["range"] == "bytes=6-"
    assert seen[1].headers["if-range"] == '"v1"'


async def test_resumable_reconnects_after_transport_error_async() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if "range" not in request.headers:
            return httpx.Response(
                200,
                headers={"ETag": '"v1"'},
                stream=FailingAsyncStream([b"hello "], httpx.ReadError("boom")),
            )
        return httpx.Response(206, content=b"world")

    consumer = make_consumer(AsyncResumableApi, handler)
    stream = await consumer.download(file_id=1)
    assert b"".join([chunk async for chunk in stream]) == b"hello world"

    assert len(seen) == 2
    assert "range" not in seen[0].headers
    assert seen[1].headers["range"] == "bytes=6-"
    assert seen[1].headers["if-range"] == '"v1"'


def test_resumable_reconnect_without_validator_omits_if_range() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if "range" not in request.headers:
            return httpx.Response(
                200, stream=FailingSyncStream([b"hello "], httpx.ReadError("boom"))
            )
        return httpx.Response(206, content=b"world")

    consumer = make_consumer(ResumableApi, handler)
    assert b"".join(consumer.download(file_id=1)) == b"hello world"

    assert seen[1].headers["range"] == "bytes=6-"
    assert "if-range" not in seen[1].headers


def test_resumable_raises_resume_lost_when_server_ignores_range() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "range" not in request.headers:
            return httpx.Response(
                200, stream=FailingSyncStream([b"hello "], httpx.ReadError("boom"))
            )
        return httpx.Response(200, content=b"full body again")

    consumer = make_consumer(ResumableApi, handler)
    with pytest.raises(ResumeLostError):
        b"".join(consumer.download(file_id=1))


def test_resumable_exhausts_attempts_then_reraises() -> None:
    def handler(unused_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=FailingSyncStream([b"partial"], httpx.ReadError("boom")))

    consumer = make_consumer(ResumableApi, handler)
    with pytest.raises(httpx.ReadError):
        b"".join(consumer.download_one_shot(file_id=1))


def test_resumable_only_retries_configured_exception_types() -> None:
    calls = 0

    def handler(unused_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, stream=FailingSyncStream([b"partial"], httpx.ReadError("boom")))

    consumer = make_consumer(ResumableApi, handler)
    with pytest.raises(httpx.ReadError):
        b"".join(consumer.download_narrow_on(file_id=1))
    assert calls == 1  # ReadError isn't in `on={ValueError}`, so it isn't caught at all


async def test_resumable_raises_resume_lost_when_server_ignores_range_async() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "range" not in request.headers:
            return httpx.Response(
                200, stream=FailingAsyncStream([b"hello "], httpx.ReadError("boom"))
            )
        return httpx.Response(200, content=b"full body again")

    consumer = make_consumer(AsyncResumableApi, handler)
    stream = await consumer.download(file_id=1)
    with pytest.raises(ResumeLostError):
        b"".join([chunk async for chunk in stream])


async def test_resumable_exhausts_attempts_then_reraises_async() -> None:
    def handler(unused_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=FailingAsyncStream([b"partial"], httpx.ReadError("boom")))

    consumer = make_consumer(AsyncResumableApi, handler)
    stream = await consumer.download_one_shot(file_id=1)
    with pytest.raises(httpx.ReadError):
        b"".join([chunk async for chunk in stream])


@pytest.mark.parametrize("cls", [ResumableApi, AsyncResumableApi], ids=["sync", "async"])
async def test_resumable_retries_transient_status_code(
    cls: type[ResumableApi | AsyncResumableApi],
) -> None:
    calls = 0

    def handler(unused_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return (
                httpx.Response(
                    200, stream=FailingSyncStream([b"hello "], httpx.ReadError("boom"))
                )
                if issubclass(cls, SyncConsumer)
                else httpx.Response(
                    200, stream=FailingAsyncStream([b"hello "], httpx.ReadError("boom"))
                )
            )
        if calls == 2:
            return httpx.Response(503, headers={"Retry-After": "0"})
        return httpx.Response(206, content=b"world")

    consumer = make_consumer(cls, handler)
    if isinstance(consumer, AsyncResumableApi):
        stream = await consumer.download(file_id=1)
        chunks = [chunk async for chunk in stream]
    else:
        chunks = list(consumer.download(file_id=1))

    assert b"".join(chunks) == b"hello world"
    assert calls == 3


@pytest.mark.parametrize("cls", [ResumableApi, AsyncResumableApi], ids=["sync", "async"])
async def test_resumable_unretryable_status_code_raises_immediately(
    cls: type[ResumableApi | AsyncResumableApi],
) -> None:
    calls = 0

    def handler(unused_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(404)

    consumer = make_consumer(cls, handler)
    with pytest.raises(httpx.HTTPStatusError):  # noqa: PT012
        if isinstance(consumer, AsyncResumableApi):
            stream = await consumer.download(file_id=1)
            _ = [chunk async for chunk in stream]
        else:
            _ = list(consumer.download(file_id=1))

    assert calls == 1
