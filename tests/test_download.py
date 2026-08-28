"""Tests for resumable-from-disk downloads (Downloader/AsyncDownloader)."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path  # noqa: TC003
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import httpx
import msgspec
import pytest
from conftest import make_consumer
from typing_extensions import override

from mxhttp import (
    AsyncConsumer,
    AsyncDownloader,
    Downloader,
    DownloadIdentityError,
    DownloadLockError,
    DownloadState,
    RateLimit,
    RateLimitExceededError,
    ResumeLostError,
    Retry,
    SyncConsumer,
    get,
)
from mxhttp.download import extract_total_size, part_paths

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

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


class DownloadApi(SyncConsumer):
    @get("/files/{file_id}", resumable=Retry(attempts=3, backoff=0))
    def download(self, file_id: int) -> Downloader: ...  # type: ignore[empty-body]

    @get("/files/{file_id}", resumable=Retry(attempts=1, backoff=0))
    def download_one_shot(self, file_id: int) -> Downloader: ...  # type: ignore[empty-body]

    @get("/files/{file_id}")
    def download_default_retry(self, file_id: int) -> Downloader: ...  # type: ignore[empty-body]


class AsyncDownloadApi(AsyncConsumer):
    @get("/files/{file_id}", resumable=Retry(attempts=3, backoff=0))
    async def download(self, file_id: int) -> AsyncDownloader: ...  # type: ignore[empty-body]

    @get("/files/{file_id}", resumable=Retry(attempts=1, backoff=0))
    async def download_one_shot(self, file_id: int) -> AsyncDownloader: ...  # type: ignore[empty-body]


class RateLimitedDownloadApi(SyncConsumer):
    @get("/files/{file_id}", ratelimit=RateLimit(calls=1, period=60, block=False))
    def download(self, file_id: int) -> Downloader: ...  # type: ignore[empty-body]


class AsyncRateLimitedDownloadApi(AsyncConsumer):
    @get("/files/{file_id}", ratelimit=RateLimit(calls=1, period=60, block=False))
    async def download(self, file_id: int) -> AsyncDownloader: ...  # type: ignore[empty-body]


def test_download_rejects_non_get_methods() -> None:
    from mxhttp import endpoint

    with pytest.raises(TypeError, match="only valid for GET"):

        class _Api(SyncConsumer):
            @endpoint("POST", "/files/{file_id}")
            def upload(self, file_id: int) -> Downloader: ...  # type: ignore[empty-body]


def test_download_rejects_sync_method_returning_async_downloader() -> None:
    with pytest.raises(TypeError, match="must return Downloader"):

        class _Api(SyncConsumer):
            @get("/files/{file_id}")
            def download(self, file_id: int) -> AsyncDownloader: ...  # type: ignore[empty-body]


def test_download_rejects_async_method_returning_downloader() -> None:
    with pytest.raises(TypeError, match="must return AsyncDownloader"):

        class _Api(AsyncConsumer):
            @get("/files/{file_id}")
            async def download(self, file_id: int) -> Downloader: ...  # type: ignore[empty-body]


@pytest.mark.parametrize("cls", [DownloadApi, AsyncDownloadApi], ids=["sync", "async"])
async def test_download_completes_and_returns_path(
    tmp_path: Path, *, cls: type[DownloadApi | AsyncDownloadApi]
) -> None:
    def handler(unused_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"hello world")

    consumer = make_consumer(cls, handler)
    target = tmp_path / "file.bin"
    if isinstance(consumer, AsyncDownloadApi):
        async_downloader = await consumer.download(file_id=1)
        result = await async_downloader(target)
    else:
        sync_downloader = consumer.download(file_id=1)
        result = sync_downloader(target)

    assert result == target
    assert target.read_bytes() == b"hello world"
    part_path, state_path, _ = part_paths(target)
    assert not part_path.exists()
    assert not state_path.exists()


def test_download_resumes_across_separate_calls(tmp_path: Path) -> None:
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

    consumer = make_consumer(DownloadApi, handler)
    target = tmp_path / "file.bin"
    part_path, state_path, _ = part_paths(target)

    with pytest.raises(httpx.ReadError):
        consumer.download_one_shot(file_id=1)(target)

    assert part_path.read_bytes() == b"hello "
    assert state_path.exists()
    assert not target.exists()

    # a fresh Downloader, as a new process would build after a crash, resumes from disk.
    result = consumer.download_one_shot(file_id=1)(target)

    assert result == target
    assert target.read_bytes() == b"hello world"
    assert not part_path.exists()
    assert not state_path.exists()
    assert len(seen) == 2
    assert seen[1].headers["range"] == "bytes=6-"
    assert seen[1].headers["if-range"] == '"v1"'


@pytest.mark.parametrize("cls", [DownloadApi, AsyncDownloadApi], ids=["sync", "async"])
async def test_download_identity_mismatch_raises(
    tmp_path: Path, *, cls: type[DownloadApi | AsyncDownloadApi]
) -> None:
    # the identity check is local and pre-flight, so a mismatch must never reach the network.
    handler = MagicMock()

    consumer = make_consumer(cls, handler)
    target = tmp_path / "file.bin"
    part_path, state_path, _ = part_paths(target)
    part_path.write_bytes(b"stale partial data")
    state_path.write_bytes(
        msgspec.json.encode(DownloadState(url="/different/file", etag=None, last_modified=None))
    )

    if isinstance(consumer, AsyncDownloadApi):
        async_downloader = await consumer.download(file_id=1)
        with pytest.raises(DownloadIdentityError):
            await async_downloader(target)
    else:
        downloader = consumer.download(file_id=1)
        with pytest.raises(DownloadIdentityError):
            downloader(target)

    handler.assert_not_called()
    # the mismatched files are left in place rather than silently discarded.
    assert part_path.read_bytes() == b"stale partial data"


@pytest.mark.parametrize("cls", [DownloadApi, AsyncDownloadApi], ids=["sync", "async"])
async def test_download_overwrite_discards_mismatched_part_file(
    tmp_path: Path, *, cls: type[DownloadApi | AsyncDownloadApi]
) -> None:
    def handler(unused_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"fresh content")

    consumer = make_consumer(cls, handler)
    target = tmp_path / "file.bin"
    part_path, state_path, _ = part_paths(target)
    part_path.write_bytes(b"stale partial data")
    state_path.write_bytes(
        msgspec.json.encode(DownloadState(url="/different/file", etag=None, last_modified=None))
    )

    if isinstance(consumer, AsyncDownloadApi):
        async_downloader = await consumer.download(file_id=1)
        result = await async_downloader(target, overwrite=True)
    else:
        result = consumer.download(file_id=1)(target, overwrite=True)

    assert result == target
    assert target.read_bytes() == b"fresh content"


def test_download_raises_resume_lost_when_server_ignores_range(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "range" not in request.headers:
            return httpx.Response(
                200, stream=FailingSyncStream([b"hello "], httpx.ReadError("boom"))
            )
        return httpx.Response(200, content=b"full body again")

    consumer = make_consumer(DownloadApi, handler)
    target = tmp_path / "file.bin"

    with pytest.raises(ResumeLostError):
        consumer.download(file_id=1)(target)  # attempts=3, so this reconnects internally first


def test_download_defaults_to_retrying_without_explicit_resumable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(time, "sleep", lambda unused_seconds: None)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if "range" not in request.headers:
            return httpx.Response(
                200, stream=FailingSyncStream([b"hello "], httpx.ReadError("boom"))
            )
        return httpx.Response(206, content=b"world")

    consumer = make_consumer(DownloadApi, handler)
    target = tmp_path / "file.bin"
    result = consumer.download_default_retry(file_id=1)(target)

    assert result == target
    assert target.read_bytes() == b"hello world"
    assert calls == 2


async def test_async_download_resumes_across_separate_calls(tmp_path: Path) -> None:
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

    consumer = make_consumer(AsyncDownloadApi, handler)
    target = tmp_path / "file.bin"
    part_path, state_path, _ = part_paths(target)

    first_downloader = await consumer.download_one_shot(file_id=1)
    with pytest.raises(httpx.ReadError):
        await first_downloader(target)

    assert part_path.read_bytes() == b"hello "

    # a fresh AsyncDownloader, as a new process would build after a crash, resumes from disk.
    second_downloader = await consumer.download_one_shot(file_id=1)
    result = await second_downloader(target)

    assert result == target
    assert target.read_bytes() == b"hello world"
    assert not part_path.exists()
    assert not state_path.exists()
    assert len(seen) == 2


async def test_async_download_reconnects_internally_then_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fake_sleep(unused_seconds: float) -> None:
        return

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if "range" not in request.headers:
            return httpx.Response(
                200, stream=FailingAsyncStream([b"hello "], httpx.ReadError("boom"))
            )
        return httpx.Response(206, content=b"world")

    consumer = make_consumer(AsyncDownloadApi, handler)
    target = tmp_path / "file.bin"
    downloader = await consumer.download(file_id=1)  # attempts=3, reconnects internally
    result = await downloader(target)

    assert result == target
    assert target.read_bytes() == b"hello world"
    assert len(seen) == 2


async def test_async_download_raises_resume_lost_when_server_ignores_range(
    tmp_path: Path,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if "range" not in request.headers:
            return httpx.Response(
                200, stream=FailingAsyncStream([b"hello "], httpx.ReadError("boom"))
            )
        return httpx.Response(200, content=b"full body again")

    consumer = make_consumer(AsyncDownloadApi, handler)
    target = tmp_path / "file.bin"
    downloader = await consumer.download(file_id=1)

    with pytest.raises(ResumeLostError):
        await downloader(target)


@pytest.mark.parametrize(
    ("cls", "base_url"),
    [
        (RateLimitedDownloadApi, "https://ratelimit-download.example.com"),
        (AsyncRateLimitedDownloadApi, "https://ratelimit-async-download.example.com"),
    ],
    ids=["sync", "async"],
)
async def test_download_rate_limit_applies_at_call_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    cls: type[RateLimitedDownloadApi | AsyncRateLimitedDownloadApi],
    base_url: str,
) -> None:
    from test_ratelimit import FakeClock

    clock = FakeClock()
    monkeypatch.setattr(time, "monotonic", clock)

    def handler(unused_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"content")

    consumer = make_consumer(cls, handler, base_url=base_url)
    target1 = tmp_path / "file1.bin"
    target2 = tmp_path / "file2.bin"

    if isinstance(consumer, AsyncRateLimitedDownloadApi):
        # creating downloader instances does not consume the rate limit budget.
        async_dl1 = await consumer.download(file_id=1)
        async_dl2 = await consumer.download(file_id=2)
        assert await async_dl1(target1) == target1
        with pytest.raises(RateLimitExceededError):
            await async_dl2(target2)
    else:
        # creating downloader instances does not consume the rate limit budget.
        sync_dl1 = consumer.download(file_id=1)
        sync_dl2 = consumer.download(file_id=2)
        assert sync_dl1(target1) == target1
        with pytest.raises(RateLimitExceededError):
            sync_dl2(target2)


def test_extract_total_size() -> None:
    resp_range = httpx.Response(206, headers={"Content-Range": "bytes 0-9/100"})
    assert extract_total_size(resp_range, 0) == 100

    resp_range_wildcard = httpx.Response(206, headers={"Content-Range": "bytes 0-9/*"})
    assert extract_total_size(resp_range_wildcard, 0) is None

    resp_length = httpx.Response(200, headers={"Content-Length": "80"})
    assert extract_total_size(resp_length, 20) == 100

    resp_invalid_length = httpx.Response(200, headers={"Content-Length": "invalid"})
    assert extract_total_size(resp_invalid_length, 0) is None

    resp_empty = httpx.Response(200)
    assert extract_total_size(resp_empty, 0) is None


@pytest.mark.parametrize("cls", [DownloadApi, AsyncDownloadApi], ids=["sync", "async"])
async def test_download_reports_progress(
    tmp_path: Path, *, cls: type[DownloadApi | AsyncDownloadApi]
) -> None:
    events: list[tuple[int, int | None]] = []

    def handler(unused_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"Content-Length": "11"}, content=b"hello world")

    consumer = make_consumer(cls, handler)
    target = tmp_path / "file.bin"
    if isinstance(consumer, AsyncDownloadApi):
        async_dl = await consumer.download(file_id=1)
        await async_dl(target, on_progress=lambda r, t: events.append((r, t)))
    else:
        sync_dl = consumer.download(file_id=1)
        sync_dl(target, on_progress=lambda r, t: events.append((r, t)))

    assert events == [(0, 11), (11, 11)]
    assert target.read_bytes() == b"hello world"


def test_download_reports_progress_on_resume(tmp_path: Path) -> None:
    events: list[tuple[int, int | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if "range" not in request.headers:
            return httpx.Response(
                200,
                headers={"ETag": '"v1"', "Content-Length": "11"},
                stream=FailingSyncStream([b"hello "], httpx.ReadError("boom")),
            )
        return httpx.Response(
            206, headers={"Content-Range": "bytes 6-10/11"}, content=b"world"
        )

    consumer = make_consumer(DownloadApi, handler)
    target = tmp_path / "file.bin"

    with pytest.raises(httpx.ReadError):
        consumer.download_one_shot(file_id=1)(
            target, on_progress=lambda r, t: events.append((r, t))
        )

    assert events == [(0, 11), (6, 11)]

    events.clear()
    consumer.download_one_shot(file_id=1)(
        target, on_progress=lambda r, t: events.append((r, t))
    )

    assert events == [(6, 11), (11, 11)]
    assert target.read_bytes() == b"hello world"


@pytest.mark.parametrize("cls", [DownloadApi, AsyncDownloadApi], ids=["sync", "async"])
async def test_download_raises_lock_error_when_locked(
    tmp_path: Path, *, cls: type[DownloadApi | AsyncDownloadApi]
) -> None:
    from filelock import FileLock

    target = tmp_path / "file.bin"
    _, _, lock_path = part_paths(target)
    handler = MagicMock()
    consumer = make_consumer(cls, handler)

    with FileLock(lock_path):
        if isinstance(consumer, AsyncDownloadApi):
            async_dl = await consumer.download(file_id=1)
            with pytest.raises(DownloadLockError):
                await async_dl(target)
        else:
            sync_dl = consumer.download(file_id=1)
            with pytest.raises(DownloadLockError):
                sync_dl(target)

    handler.assert_not_called()


@pytest.mark.parametrize("cls", [DownloadApi, AsyncDownloadApi], ids=["sync", "async"])
async def test_download_retries_transient_status_code(
    tmp_path: Path, *, cls: type[DownloadApi | AsyncDownloadApi]
) -> None:
    calls = 0

    def handler(unused_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return (
                httpx.Response(
                    200,
                    headers={"ETag": '"v1"'},
                    stream=FailingSyncStream([b"hello "], httpx.ReadError("boom")),
                )
                if issubclass(cls, SyncConsumer)
                else httpx.Response(
                    200,
                    headers={"ETag": '"v1"'},
                    stream=FailingAsyncStream([b"hello "], httpx.ReadError("boom")),
                )
            )
        if calls == 2:
            return httpx.Response(503, headers={"Retry-After": "0"})
        return httpx.Response(206, content=b"world")

    consumer = make_consumer(cls, handler)
    target = tmp_path / "file.bin"
    if isinstance(consumer, AsyncDownloadApi):
        async_dl = await consumer.download(file_id=1)
        assert await async_dl(target) == target
    else:
        sync_dl = consumer.download(file_id=1)
        assert sync_dl(target) == target

    assert target.read_bytes() == b"hello world"
    assert calls == 3


@pytest.mark.parametrize("cls", [DownloadApi, AsyncDownloadApi], ids=["sync", "async"])
async def test_download_unretryable_status_code_raises_immediately(
    tmp_path: Path, *, cls: type[DownloadApi | AsyncDownloadApi]
) -> None:
    calls = 0

    def handler(unused_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(404)

    consumer = make_consumer(cls, handler)
    target = tmp_path / "file.bin"
    with pytest.raises(httpx.HTTPStatusError):  # noqa: PT012
        if isinstance(consumer, AsyncDownloadApi):
            async_dl = await consumer.download(file_id=1)
            await async_dl(target)
        else:
            sync_dl = consumer.download(file_id=1)
            sync_dl(target)

    assert calls == 1
