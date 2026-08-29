"""Tests for resumable-from-disk downloads (Downloader/AsyncDownloader)."""

from __future__ import annotations

import asyncio
import hashlib
import io
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
    Checksum,
    ChecksumMismatchError,
    Downloader,
    DownloadIdentityError,
    DownloadLockError,
    DownloadState,
    Parts,
    RateLimit,
    RateLimitExceededError,
    ResumeLostError,
    Retry,
    SyncConsumer,
    TqdmProgress,
    base_url,
    get,
)
from mxhttp.download import (
    MultiPartState,
    PartState,
    compute_ranges,
    extract_total_size,
    part_paths,
    resolve_parts,
)

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


class MultiPartDownloadApi(SyncConsumer):
    @get("/files/{file_id}", parts=Parts(count=2, min_part_size=10))
    def download(self, file_id: int) -> Downloader: ...  # type: ignore[empty-body]

    @get("/files/{file_id}", parts=2)
    def download_int_parts(self, file_id: int) -> Downloader: ...  # type: ignore[empty-body]


class AsyncMultiPartDownloadApi(AsyncConsumer):
    @get("/files/{file_id}", parts=Parts(count=2, min_part_size=10))
    async def download(self, file_id: int) -> AsyncDownloader: ...  # type: ignore[empty-body]

    @get("/files/{file_id}", parts=2)
    async def download_int_parts(self, file_id: int) -> AsyncDownloader: ...  # type: ignore[empty-body]


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
        return httpx.Response(206, headers={"Content-Range": "bytes 6-10/11"}, content=b"world")

    consumer = make_consumer(DownloadApi, handler)
    target = tmp_path / "file.bin"

    with pytest.raises(httpx.ReadError):
        consumer.download_one_shot(file_id=1)(
            target, on_progress=lambda r, t: events.append((r, t))
        )

    assert events == [(0, 11), (6, 11)]

    events.clear()
    consumer.download_one_shot(file_id=1)(target, on_progress=lambda r, t: events.append((r, t)))

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


def test_parts_struct_validation() -> None:
    p = Parts()
    assert p.count == 4
    assert p.min_part_size == 5 * 1024 * 1024
    assert p.max_parts == 16

    with pytest.raises(ValueError, match="count must be >= 1"):
        Parts(count=0)

    with pytest.raises(ValueError, match="min_part_size must be >= 1"):
        Parts(min_part_size=0)

    with pytest.raises(ValueError, match="max_parts must be >= count"):
        Parts(count=10, max_parts=5)


def test_parts_resolve_count() -> None:
    p = Parts(count=4, min_part_size=1000, max_parts=10)
    assert p.resolve_count(500) == 1
    assert p.resolve_count(1000) == 1
    assert p.resolve_count(2500) == 2
    assert p.resolve_count(4000) == 4
    assert p.resolve_count(10000) == 4

    p2 = Parts(count=8, min_part_size=1000, max_parts=8)
    assert p2.resolve_count(50000) == 8


def test_resolve_parts_helper() -> None:
    assert resolve_parts(None) is None
    p = resolve_parts(4)
    assert isinstance(p, Parts)
    assert p.count == 4
    assert resolve_parts(p) is p


def test_compute_ranges() -> None:
    assert compute_ranges(100, 1) == [(0, 99)]
    assert compute_ranges(100, 0) == [(0, 99)]
    assert compute_ranges(1000, 4) == [(0, 249), (250, 499), (500, 749), (750, 999)]
    assert compute_ranges(10, 3) == [(0, 2), (3, 5), (6, 9)]


def test_endpoint_parts_validation() -> None:
    from mxhttp import endpoint

    with pytest.raises(TypeError, match="parts is only valid for GET endpoints"):

        class _PostPartsApi(SyncConsumer):
            @endpoint("POST", "/files", parts=2)
            def create(self) -> Downloader: ...  # type: ignore[empty-body]

    with pytest.raises(
        TypeError, match="parts is only valid for Downloader/AsyncDownloader endpoints"
    ):

        class _GetNonDownloaderApi(SyncConsumer):
            @get("/files", parts=2)  # type: ignore[type-var]
            def get_files(self) -> int: ...  # type: ignore[empty-body]


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_success(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    events: list[tuple[int, int | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers.get("Range")
        if range_header == "bytes=0-0":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-0/20", "ETag": '"v1"'},
                content=b"0",
            )
        if range_header == "bytes=0-9":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-9/20", "ETag": '"v1"'},
                content=b"0123456789",
            )
        assert range_header == "bytes=10-19"
        return httpx.Response(
            206,
            headers={"Content-Range": "bytes 10-19/20", "ETag": '"v1"'},
            content=b"abcdefghij",
        )

    consumer = make_consumer(cls, handler)
    target = tmp_path / "result.bin"

    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        res = await async_dl(target, on_progress=lambda r, t: events.append((r, t)))
    else:
        sync_dl = consumer.download(file_id=1)
        res = sync_dl(target, on_progress=lambda r, t: events.append((r, t)))

    assert res == target
    assert target.read_bytes() == b"0123456789abcdefghij"
    assert not (tmp_path / "result.bin.part.0").exists()
    assert not (tmp_path / "result.bin.part.1").exists()
    assert not (tmp_path / "result.bin.part.json").exists()
    assert events[-1] == (20, 20)


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_fallback_when_probe_returns_200(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    def handler(unused_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"full non-range response")

    consumer = make_consumer(cls, handler)
    target = tmp_path / "fallback.bin"

    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        await async_dl(target)
    else:
        sync_dl = consumer.download(file_id=1)
        sync_dl(target)

    assert target.read_bytes() == b"full non-range response"


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_fallback_when_size_below_min(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers.get("Range")
        if range_header == "bytes=0-0":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-0/5", "ETag": '"v1"'},
                content=b"h",
            )
        return httpx.Response(200, content=b"hello")

    consumer = make_consumer(cls, handler)
    target = tmp_path / "small.bin"

    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        await async_dl(target)
    else:
        sync_dl = consumer.download(file_id=1)
        sync_dl(target)

    assert target.read_bytes() == b"hello"


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_resumes_existing_parts(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    target = tmp_path / "resume_parts.bin"
    seg0 = tmp_path / "resume_parts.bin.part.0"
    seg1 = tmp_path / "resume_parts.bin.part.1"
    state_file = tmp_path / "resume_parts.bin.part.json"

    seg0.write_bytes(b"0123456789")
    seg1.write_bytes(b"abc")

    url = "https://api.example.com/files/1"
    state = MultiPartState(
        url=url,
        etag='"v1"',
        last_modified=None,
        total_size=20,
        parts=[
            PartState(index=0, start=0, end=9, received=10),
            PartState(index=1, start=10, end=19, received=3),
        ],
    )
    state_file.write_bytes(msgspec.json.encode(state))

    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers.get("Range")
        assert range_header == "bytes=13-19"
        return httpx.Response(
            206,
            headers={"Content-Range": "bytes 13-19/20", "ETag": '"v1"'},
            content=b"defghij",
        )

    consumer = make_consumer(cls, handler)
    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        await async_dl(target)
    else:
        sync_dl = consumer.download(file_id=1)
        sync_dl(target)

    assert target.read_bytes() == b"0123456789abcdefghij"


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_overwrite(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    target = tmp_path / "overwrite.bin"
    seg0 = tmp_path / "overwrite.bin.part.0"
    state_file = tmp_path / "overwrite.bin.part.json"

    seg0.write_bytes(b"corrupt")
    state_file.write_bytes(b"{}")

    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers.get("Range")
        if range_header == "bytes=0-0":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-0/20", "ETag": '"v1"'},
                content=b"0",
            )
        if range_header == "bytes=0-9":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-9/20", "ETag": '"v1"'},
                content=b"0123456789",
            )
        assert range_header == "bytes=10-19"
        return httpx.Response(
            206,
            headers={"Content-Range": "bytes 10-19/20", "ETag": '"v1"'},
            content=b"abcdefghij",
        )

    consumer = make_consumer(cls, handler)
    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        await async_dl(target, overwrite=True)
    else:
        sync_dl = consumer.download(file_id=1)
        sync_dl(target, overwrite=True)

    assert target.read_bytes() == b"0123456789abcdefghij"


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_identity_error(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    target = tmp_path / "ident.bin"
    state_file = tmp_path / "ident.bin.part.json"

    state = MultiPartState(
        url="https://api.example.com/different/resource",
        etag='"v1"',
        last_modified=None,
        total_size=20,
        parts=[PartState(0, 0, 9, 0), PartState(1, 10, 19, 0)],
    )
    state_file.write_bytes(msgspec.json.encode(state))

    consumer = make_consumer(cls, lambda _: httpx.Response(200))
    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        with pytest.raises(DownloadIdentityError):
            await async_dl(target)
    else:
        sync_dl = consumer.download(file_id=1)
        with pytest.raises(DownloadIdentityError):
            sync_dl(target)


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_resume_lost_error_when_server_returns_200(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    target = tmp_path / "resume_lost.bin"
    seg0 = tmp_path / "resume_lost.bin.part.0"
    state_file = tmp_path / "resume_lost.bin.part.json"

    seg0.write_bytes(b"01234")
    state = MultiPartState(
        url="https://api.example.com/files/1",
        etag='"v1"',
        last_modified=None,
        total_size=20,
        parts=[PartState(0, 0, 9, 5), PartState(1, 10, 19, 0)],
    )
    state_file.write_bytes(msgspec.json.encode(state))

    def handler(unused_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"full content changed")

    consumer = make_consumer(cls, handler)
    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        with pytest.raises(
            Exception, match=r"ResumeLostError|server ignored Range|unhandled errors in a TaskGroup"
        ):
            await async_dl(target)
    else:
        sync_dl = consumer.download(file_id=1)
        with pytest.raises(ResumeLostError):
            sync_dl(target)


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_corrupted_state_restarts(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    target = tmp_path / "corrupt_state.bin"
    state_file = tmp_path / "corrupt_state.bin.part.json"
    state_file.write_bytes(b"invalid-json")

    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers.get("Range")
        if range_header == "bytes=0-0":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-0/20", "ETag": '"v1"'},
                content=b"0",
            )
        if range_header == "bytes=0-9":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-9/20", "ETag": '"v1"'},
                content=b"0123456789",
            )
        assert range_header == "bytes=10-19"
        return httpx.Response(
            206,
            headers={"Content-Range": "bytes 10-19/20", "ETag": '"v1"'},
            content=b"abcdefghij",
        )

    consumer = make_consumer(cls, handler)
    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        await async_dl(target)
    else:
        sync_dl = consumer.download(file_id=1)
        sync_dl(target)

    assert target.read_bytes() == b"0123456789abcdefghij"


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_retries_transient_error(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    part1_attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal part1_attempts
        range_header = request.headers.get("Range")
        if range_header == "bytes=0-0":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-0/20", "ETag": '"v1"'},
                content=b"0",
            )
        if range_header == "bytes=0-9":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-9/20", "ETag": '"v1"'},
                content=b"0123456789",
            )
        assert range_header == "bytes=10-19"
        part1_attempts += 1
        if part1_attempts == 1:
            return httpx.Response(503, headers={"Retry-After": "0"})
        return httpx.Response(
            206,
            headers={"Content-Range": "bytes 10-19/20", "ETag": '"v1"'},
            content=b"abcdefghij",
        )

    consumer = make_consumer(cls, handler)
    target = tmp_path / "retry_part.bin"
    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        await async_dl(target)
    else:
        sync_dl = consumer.download(file_id=1)
        sync_dl(target)

    assert target.read_bytes() == b"0123456789abcdefghij"
    assert part1_attempts == 2


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_exceeds_retry_attempts(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers.get("Range")
        if range_header == "bytes=0-0":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-0/20", "ETag": '"v1"'},
                content=b"0",
            )
        if range_header == "bytes=0-9":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-9/20", "ETag": '"v1"'},
                content=b"0123456789",
            )
        assert range_header == "bytes=10-19"
        return httpx.Response(503, headers={"Retry-After": "0"})

    consumer = make_consumer(cls, handler)
    target = tmp_path / "exceed_retry.bin"
    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        with pytest.raises(Exception, match=r"503|HTTPStatusError|TaskGroup"):
            await async_dl(target)
    else:
        sync_dl = consumer.download(file_id=1)
        with pytest.raises(httpx.HTTPStatusError):
            sync_dl(target)


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_unretryable_part_error(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers.get("Range")
        if range_header == "bytes=0-0":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-0/20", "ETag": '"v1"'},
                content=b"0",
            )
        return httpx.Response(404)

    consumer = make_consumer(cls, handler)
    target = tmp_path / "unretryable_part.bin"
    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        with pytest.raises(Exception, match=r"404|HTTPStatusError|TaskGroup"):
            await async_dl(target)
    else:
        sync_dl = consumer.download(file_id=1)
        with pytest.raises(httpx.HTTPStatusError):
            sync_dl(target)


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_call_time_override(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers.get("Range")
        if range_header == "bytes=0-0":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-0/30", "ETag": '"v1"'},
                content=b"0",
            )
        if range_header == "bytes=0-9":
            return httpx.Response(206, content=b"0123456789")
        if range_header == "bytes=10-19":
            return httpx.Response(206, content=b"abcdefghij")
        assert range_header == "bytes=20-29"
        return httpx.Response(206, content=b"KLMNOPQRST")

    consumer = make_consumer(cls, handler)
    target = tmp_path / "override_parts.bin"

    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        await async_dl(target, parts=Parts(count=3, min_part_size=10))
    else:
        sync_dl = consumer.download(file_id=1)
        sync_dl(target, parts=Parts(count=3, min_part_size=10))

    assert target.read_bytes() == b"0123456789abcdefghijKLMNOPQRST"


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_int_parts(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    def handler(unused_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"hello")

    consumer = make_consumer(cls, handler)
    target = tmp_path / "int_parts.bin"

    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download_int_parts(file_id=1)
        await async_dl(target)
    else:
        sync_dl = consumer.download_int_parts(file_id=1)
        sync_dl(target)

    assert target.read_bytes() == b"hello"


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_on_part_progress(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    part_events: list[tuple[int, int, int]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers.get("Range")
        if range_header == "bytes=0-0":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-0/20", "ETag": '"v1"'},
                content=b"0",
            )
        if range_header == "bytes=0-9":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-9/20", "ETag": '"v1"'},
                content=b"0123456789",
            )
        assert range_header == "bytes=10-19"
        return httpx.Response(
            206,
            headers={"Content-Range": "bytes 10-19/20", "ETag": '"v1"'},
            content=b"abcdefghij",
        )

    consumer = make_consumer(cls, handler)
    target = tmp_path / "part_progress.bin"

    def part_cb(p_idx: int, p_recv: int, p_tot: int) -> None:
        part_events.append((p_idx, p_recv, p_tot))

    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        await async_dl(target, on_part_progress=part_cb)
    else:
        sync_dl = consumer.download(file_id=1)
        sync_dl(target, on_part_progress=part_cb)

    assert target.read_bytes() == b"0123456789abcdefghij"
    assert any(p_idx == 0 and p_recv == 10 for p_idx, p_recv, _ in part_events)
    assert any(p_idx == 1 and p_recv == 10 for p_idx, p_recv, _ in part_events)


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_tqdm_per_part_integration(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers.get("Range")
        if range_header == "bytes=0-0":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-0/20", "ETag": '"v1"'},
                content=b"0",
            )
        if range_header == "bytes=0-9":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-9/20", "ETag": '"v1"'},
                content=b"0123456789",
            )
        assert range_header == "bytes=10-19"
        return httpx.Response(
            206,
            headers={"Content-Range": "bytes 10-19/20", "ETag": '"v1"'},
            content=b"abcdefghij",
        )

    consumer = make_consumer(cls, handler)
    target = tmp_path / "tqdm_per_part.bin"
    buf = io.StringIO()
    progress = TqdmProgress(desc="Multi parts", file=buf, per_part=True)

    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        await async_dl(target, on_progress=progress)
    else:
        sync_dl = consumer.download(file_id=1)
        sync_dl(target, on_progress=progress)

    assert target.read_bytes() == b"0123456789abcdefghij"
    assert progress.progress_bar is None
    assert len(progress.part_progress_bars) == 0


@pytest.mark.parametrize("cls", [DownloadApi, AsyncDownloadApi], ids=["sync", "async"])
async def test_single_stream_download_checksum_valid(
    tmp_path: Path, *, cls: type[DownloadApi | AsyncDownloadApi]
) -> None:
    data = b"hello stream with checksum validation"
    expected_hash = hashlib.sha256(data).hexdigest()

    def handler(unused_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=data)

    consumer = make_consumer(cls, handler)
    target = tmp_path / "valid_checksum.bin"

    if isinstance(consumer, AsyncDownloadApi):
        async_dl = await consumer.download(file_id=1)
        res_path = await async_dl(target, checksum=expected_hash)
    else:
        sync_dl = consumer.download(file_id=1)
        res_path = sync_dl(target, checksum=expected_hash)

    assert res_path == target
    assert target.read_bytes() == data


@pytest.mark.parametrize("cls", [DownloadApi, AsyncDownloadApi], ids=["sync", "async"])
async def test_single_stream_download_checksum_mismatch(
    tmp_path: Path, *, cls: type[DownloadApi | AsyncDownloadApi]
) -> None:
    data = b"hello corrupted stream"

    def handler(unused_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=data)

    consumer = make_consumer(cls, handler)
    target = tmp_path / "mismatch_checksum.bin"

    if isinstance(consumer, AsyncDownloadApi):
        async_dl = await consumer.download(file_id=1)
        with pytest.raises(ChecksumMismatchError):
            await async_dl(target, checksum="0" * 64)
    else:
        sync_dl = consumer.download(file_id=1)
        with pytest.raises(ChecksumMismatchError):
            sync_dl(target, checksum="0" * 64)

    assert not target.exists()


@pytest.mark.parametrize("cls", [DownloadApi, AsyncDownloadApi], ids=["sync", "async"])
async def test_single_stream_download_checksum_container_and_callback(
    tmp_path: Path, *, cls: type[DownloadApi | AsyncDownloadApi]
) -> None:
    data = b"container and callback stream data"
    expected_hash = hashlib.sha256(data).hexdigest()
    recorded: list[str] = []

    def handler(unused_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=data)

    consumer = make_consumer(cls, handler)
    target = tmp_path / "container_cb.bin"
    cs = Checksum.sha256()

    if isinstance(consumer, AsyncDownloadApi):
        async_dl = await consumer.download(file_id=1)
        await async_dl(target, checksum=cs, on_checksum=recorded.append)
    else:
        sync_dl = consumer.download(file_id=1)
        sync_dl(target, checksum=cs, on_checksum=recorded.append)

    assert cs.digest == expected_hash
    assert recorded == [expected_hash]
    assert target.read_bytes() == data


@pytest.mark.parametrize("cls", [DownloadApi, AsyncDownloadApi], ids=["sync", "async"])
async def test_single_stream_download_checksum_with_resume(
    tmp_path: Path, *, cls: type[DownloadApi | AsyncDownloadApi]
) -> None:
    full_data = b"0123456789abcdefghij"
    expected_hash = hashlib.sha256(full_data).hexdigest()
    target = tmp_path / "resume_checksum.bin"
    part_path = tmp_path / "resume_checksum.bin.part"
    part_json = tmp_path / "resume_checksum.bin.part.json"

    part_path.write_bytes(b"0123456789")
    part_json.write_bytes(
        msgspec.json.encode(
            DownloadState(url="https://api.example.com/files/1", etag='"v1"', last_modified=None)
        )
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("Range") == "bytes=10-"
        return httpx.Response(
            206,
            headers={"Content-Range": "bytes 10-19/20", "ETag": '"v1"'},
            content=b"abcdefghij",
        )

    consumer = make_consumer(cls, handler)

    if isinstance(consumer, AsyncDownloadApi):
        async_dl = await consumer.download(file_id=1)
        await async_dl(target, checksum=expected_hash)
    else:
        sync_dl = consumer.download(file_id=1)
        sync_dl(target, checksum=expected_hash)

    assert target.read_bytes() == full_data


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_checksum_valid(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    full_data = b"0123456789abcdefghij"
    expected_hash = hashlib.sha256(full_data).hexdigest()

    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers.get("Range")
        if range_header == "bytes=0-0":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-0/20", "ETag": '"v1"'},
                content=b"0",
            )
        if range_header == "bytes=0-9":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-9/20", "ETag": '"v1"'},
                content=b"0123456789",
            )
        assert range_header == "bytes=10-19"
        return httpx.Response(
            206,
            headers={"Content-Range": "bytes 10-19/20", "ETag": '"v1"'},
            content=b"abcdefghij",
        )

    consumer = make_consumer(cls, handler)
    target = tmp_path / "mp_valid_checksum.bin"

    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        res_path = await async_dl(target, checksum=expected_hash)
    else:
        sync_dl = consumer.download(file_id=1)
        res_path = sync_dl(target, checksum=expected_hash)

    assert res_path == target
    assert target.read_bytes() == full_data


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_checksum_mismatch(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers.get("Range")
        if range_header == "bytes=0-0":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-0/20", "ETag": '"v1"'},
                content=b"0",
            )
        if range_header == "bytes=0-9":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-9/20", "ETag": '"v1"'},
                content=b"0123456789",
            )
        assert range_header == "bytes=10-19"
        return httpx.Response(
            206,
            headers={"Content-Range": "bytes 10-19/20", "ETag": '"v1"'},
            content=b"abcdefghij",
        )

    consumer = make_consumer(cls, handler)
    target = tmp_path / "mp_mismatch_checksum.bin"

    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        with pytest.raises(ChecksumMismatchError):
            await async_dl(target, checksum="f" * 64)
    else:
        sync_dl = consumer.download(file_id=1)
        with pytest.raises(ChecksumMismatchError):
            sync_dl(target, checksum="f" * 64)

    assert not target.exists()


@pytest.mark.parametrize(
    "cls", [MultiPartDownloadApi, AsyncMultiPartDownloadApi], ids=["sync", "async"]
)
async def test_multipart_download_checksum_container_and_callback(
    tmp_path: Path, *, cls: type[MultiPartDownloadApi | AsyncMultiPartDownloadApi]
) -> None:
    full_data = b"0123456789abcdefghij"
    expected_hash = hashlib.sha256(full_data).hexdigest()
    recorded: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers.get("Range")
        if range_header == "bytes=0-0":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-0/20", "ETag": '"v1"'},
                content=b"0",
            )
        if range_header == "bytes=0-9":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-9/20", "ETag": '"v1"'},
                content=b"0123456789",
            )
        assert range_header == "bytes=10-19"
        return httpx.Response(
            206,
            headers={"Content-Range": "bytes 10-19/20", "ETag": '"v1"'},
            content=b"abcdefghij",
        )

    consumer = make_consumer(cls, handler)
    target = tmp_path / "mp_container_cb.bin"
    cs = Checksum.sha256()

    if isinstance(consumer, AsyncMultiPartDownloadApi):
        async_dl = await consumer.download(file_id=1)
        await async_dl(target, checksum=cs, on_checksum=recorded.append)
    else:
        sync_dl = consumer.download(file_id=1)
        sync_dl(target, checksum=cs, on_checksum=recorded.append)

    assert cs.digest == expected_hash
    assert recorded == [expected_hash]
    assert target.read_bytes() == full_data


@base_url("https://api.example.com")
class ChecksumEndpointApi(SyncConsumer):
    @get("/files/{file_id}", parts=Parts(count=2, min_part_size=1), checksum="sha256")
    def download(self, file_id: int) -> Downloader: ...  # type: ignore[empty-body]


@base_url("https://api.example.com")
class AsyncChecksumEndpointApi(AsyncConsumer):
    @get("/files/{file_id}", parts=Parts(count=2, min_part_size=1), checksum="sha256")
    async def download(self, file_id: int) -> AsyncDownloader: ...  # type: ignore[empty-body]


@pytest.mark.parametrize(
    "cls", [ChecksumEndpointApi, AsyncChecksumEndpointApi], ids=["sync", "async"]
)
async def test_endpoint_level_checksum_configuration(
    tmp_path: Path, *, cls: type[ChecksumEndpointApi | AsyncChecksumEndpointApi]
) -> None:
    full_data = b"0123456789abcdefghij"
    expected_hash = hashlib.sha256(full_data).hexdigest()
    recorded: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        range_header = request.headers.get("Range")
        if range_header == "bytes=0-0":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-0/20", "ETag": '"v1"'},
                content=b"0",
            )
        if range_header == "bytes=0-9":
            return httpx.Response(
                206,
                headers={"Content-Range": "bytes 0-9/20", "ETag": '"v1"'},
                content=b"0123456789",
            )
        assert range_header == "bytes=10-19"
        return httpx.Response(
            206,
            headers={"Content-Range": "bytes 10-19/20", "ETag": '"v1"'},
            content=b"abcdefghij",
        )

    consumer = make_consumer(cls, handler)
    target = tmp_path / "endpoint_cs.bin"

    if isinstance(consumer, AsyncChecksumEndpointApi):
        async_dl = await consumer.download(file_id=1)
        assert async_dl.checksum is not None
        assert async_dl.checksum.algorithm == "sha256"
        await async_dl(target, on_checksum=recorded.append)
    else:
        sync_dl = consumer.download(file_id=1)
        assert sync_dl.checksum is not None
        assert sync_dl.checksum.algorithm == "sha256"
        sync_dl(target, on_checksum=recorded.append)

    assert recorded == [expected_hash]
    assert target.read_bytes() == full_data


def test_endpoint_checksum_validation_errors() -> None:
    from mxhttp.endpoint import validate_endpoint_kinds

    with pytest.raises(TypeError, match="checksum is only valid for GET endpoints"):
        validate_endpoint_kinds(
            "POST",
            None,
            None,
            checksum="sha256",
            is_raw_stream=False,
            is_downloader=False,
            is_async_downloader=False,
            is_coroutine=False,
        )

    with pytest.raises(TypeError, match="checksum is only valid for Downloader/AsyncDownloader"):
        validate_endpoint_kinds(
            "GET",
            None,
            None,
            checksum="sha256",
            is_raw_stream=False,
            is_downloader=False,
            is_async_downloader=False,
            is_coroutine=False,
        )
