"""Resumable-from-disk downloads: `Downloader`/`AsyncDownloader`."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import os
import threading
import time
from collections.abc import AsyncIterator, Callable, Iterator  # noqa: TC003
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias

import msgspec

from mxhttp.checksum import (
    Checksum,
    ChecksumCallback,
    ChecksumInput,
    compute_file_digest,
    compute_file_digest_async,
    resolve_checksum,
    verify_checksum,
)
from mxhttp.concurrency import Concurrency, gate_concurrency_async, gate_concurrency_sync
from mxhttp.consumer import AsyncConsumer, SyncConsumer  # noqa: TC001
from mxhttp.ratelimit import RateLimit, gate_async, gate_sync
from mxhttp.request import RequestSpec  # noqa: TC001
from mxhttp.response import ResumeLostError, apply_streaming_response_handler, resume_headers
from mxhttp.retry import (
    Retry,
    extract_response,
    is_retryable_exception,
    resolve_delay,
    retry_call_async,
    retry_call_sync,
    retryable_exceptions,
)
from mxhttp.types import ResponseHandler  # noqa: TC001

if TYPE_CHECKING:
    import anyio
    import httpx
    from _typeshed import StrPath

logger = logging.getLogger(__name__)

DOWNLOAD_DEFAULT_RETRY = Retry(attempts=5, max_delay=60.0)
"""Fallback retry budget for `Downloader`/`AsyncDownloader` when `resumable` isn't given."""

ProgressCallback: TypeAlias = "Callable[[int, int | None], None]"
PartProgressCallback: TypeAlias = "Callable[[int, int, int], None]"


def notify(callback: ProgressCallback | None, received: int, total: int | None) -> None:
    """Invokes the synchronous progress callback if provided."""
    if callback is not None:
        callback(received, total)


def notify_part(  # noqa: PLR0913, PLR0917
    callback: ProgressCallback | None,
    part_callback: PartProgressCallback | None,
    part_idx: int,
    part_received: int,
    part_total: int,
    current_total: int,
    total_size: int | None,
) -> None:
    """Invokes progress callbacks for a specific part and overall total."""
    if part_callback is not None:
        part_callback(part_idx, part_received, part_total)
    if callback is not None:
        if hasattr(callback, "update_part"):
            callback.update_part(part_idx, part_received, part_total, current_total, total_size)
        else:
            callback(current_total, total_size)


class DownloadIdentityError(Exception):
    """An existing `.part` file belongs to a different resource than the one being downloaded."""


class DownloadInterruptedError(KeyboardInterrupt, Exception):
    """The download was interrupted or cancelled by the user."""


DownloadInterrupted: TypeAlias = DownloadInterruptedError


def cleanup_progress(callback: object) -> None:
    """Cleans up or closes progress callback if supported, suppressing any error."""
    if callback is None:
        return
    with contextlib.suppress(Exception):
        if hasattr(callback, "cleanup"):
            callback.cleanup()
        elif hasattr(callback, "close"):
            callback.close()


class DownloadLockError(Exception):
    """Another process or task is actively downloading to the target path."""


class Parts(msgspec.Struct, frozen=True):
    """Configures multi-part parallel download behavior."""

    count: int = 4
    min_part_size: int = 5 * 1024 * 1024
    max_parts: int = 16

    def __post_init__(self) -> None:
        if self.count < 1:
            raise ValueError("count must be >= 1")
        if self.min_part_size < 1:
            raise ValueError("min_part_size must be >= 1")
        if self.max_parts < self.count:
            raise ValueError("max_parts must be >= count")

    def resolve_count(self, total_size: int) -> int:
        """Calculates effective number of parts for a given file size."""
        if total_size <= self.min_part_size:
            return 1
        possible = total_size // self.min_part_size
        return max(1, min(self.count, possible, self.max_parts))


def resolve_parts(parts: int | Parts | None) -> Parts | None:
    """Converts an integer or Parts to a Parts instance or None."""
    if parts is None:
        return None
    if isinstance(parts, int):
        return Parts(count=parts)
    return parts


def compute_ranges(total_size: int, num_parts: int) -> list[tuple[int, int]]:
    """Calculates disjoint [start, end] inclusive byte ranges for num_parts."""
    if num_parts <= 1:
        return [(0, total_size - 1)]
    part_len = total_size // num_parts
    ranges: list[tuple[int, int]] = []
    for i in range(num_parts):
        start = i * part_len
        end = (i + 1) * part_len - 1 if i < num_parts - 1 else total_size - 1
        ranges.append((start, end))
    return ranges


class PartState(msgspec.Struct):
    """Tracks state of an individual download segment."""

    index: int
    start: int
    end: int
    received: int


class MultiPartState(msgspec.Struct):
    """Stores metadata for a multi-part segmented download."""

    url: str
    etag: str | None
    last_modified: str | None
    total_size: int
    parts: list[PartState]


class DownloadState(msgspec.Struct):
    """Stores the resource identity a `.part` file was downloaded from, for resume validation."""

    url: str
    etag: str | None
    last_modified: str | None


def resolve_lock_path(target: Path) -> Path:
    """Returns lock file path for `target`, placed in `MXHTTP_LOCK_DIR` if set."""
    if lock_dir := os.getenv("MXHTTP_LOCK_DIR"):
        logger.info("Using external lock dir: %s", lock_dir)
        lock_dir_path = Path(lock_dir)
        lock_dir_path.mkdir(parents=True, exist_ok=True)
        safe_name = str(target.resolve()).replace(":", "_").replace(os.sep, "_")
        return lock_dir_path / f"{safe_name}.part.lock"
    return target.with_name(target.name + ".part.lock")


def part_paths(path: Path) -> tuple[Path, Path, Path]:
    """Returns the staging file, sidecar metadata, and lock paths for a download at `path`."""
    return (
        path.with_name(path.name + ".part"),
        path.with_name(path.name + ".part.json"),
        resolve_lock_path(path),
    )


@contextlib.contextmanager
def hold_download_lock(target: Path) -> Iterator[None]:
    """Holds the lock for a download to `target` and unlinks its lock file before releasing it.

    filelock keeps the lock file on Unix, so it is unlinked here, after the download work and while
    still held: a waiter that already opened it locks a dead inode, which filelock>=3.20.4 discards
    and retries. This needs `fallback_to_soft=False`, since a soft lock is released by unlinking
    its file. On Windows, filelock removes the file itself after release.

    Raises:
        DownloadLockError: If another process holds the lock.
    """
    from filelock import FileLock, Timeout

    lock_path = part_paths(target)[2]
    lock = FileLock(lock_path, timeout=0, fallback_to_soft=False)
    try:
        lock.acquire()
    except Timeout as e:
        raise DownloadLockError(f"Download to {target} is locked by another process") from e
    try:
        yield
    finally:
        try:
            # on Windows, release already deletes the lock file
            if os.name != "nt":  # pragma: no branch
                lock_path.unlink(missing_ok=True)
        finally:
            lock.release()


@contextlib.asynccontextmanager
async def hold_download_lock_async(target: anyio.Path) -> AsyncIterator[None]:
    """Async `hold_download_lock`.

    Raises:
        DownloadLockError: If another process holds the lock.
    """
    import anyio
    from filelock import AsyncFileLock, Timeout

    resolved_lock = resolve_lock_path(Path(target))
    lock_path = anyio.Path(resolved_lock)
    lock = AsyncFileLock(resolved_lock, timeout=0, fallback_to_soft=False)
    try:
        await lock.acquire()
    except Timeout as e:
        raise DownloadLockError(f"Download to {target} is locked by another process") from e
    try:
        yield
    finally:
        try:
            # on Windows, release already deletes the lock file
            if os.name != "nt":  # pragma: no branch
                await lock_path.unlink(missing_ok=True)
        finally:
            await lock.release()


def cleanup_staging_files(target: Path) -> None:
    """Removes all staging fragments and sidecar metadata for target."""
    parent = target.parent
    name = target.name
    for p in parent.glob(f"{name}.part*"):
        if p.name == f"{name}.part.lock":
            continue
        p.unlink(missing_ok=True)


async def cleanup_staging_files_async(target: anyio.Path) -> None:
    """Removes all staging fragments and sidecar metadata for target asynchronously."""
    parent = target.parent
    name = target.name
    async for p in parent.glob(f"{name}.part*"):
        if p.name == f"{name}.part.lock":
            continue
        await p.unlink(missing_ok=True)


def extract_total_size(response: httpx.Response, received: int) -> int | None:
    """Calculates the total expected download size from response headers."""
    content_range = response.headers.get("content-range")
    if content_range:
        _, _, total_str = content_range.partition("/")
        if total_str.isdigit():
            return int(total_str)
    content_length = response.headers.get("content-length")
    if content_length and content_length.isdigit():
        return received + int(content_length)
    return None


def load_resume_state(
    part_path: Path, state_path: Path, url: str, *, overwrite: bool
) -> tuple[int, str | None]:
    """Checks for a resumable `.part` file, returning its size and identity validator.

    Raises:
        DownloadIdentityError: If a `.part` file exists for a different resource.
    """
    if not part_path.exists() or not state_path.exists():
        return 0, None
    state = msgspec.json.decode(state_path.read_bytes(), type=DownloadState)
    if state.url != url:
        if not overwrite:
            raise DownloadIdentityError(
                f"{part_path} belongs to a different resource; pass overwrite=True to restart"
            )
        return 0, None
    return part_path.stat().st_size, state.etag or state.last_modified


def write_state(state_path: Path, url: str, response: httpx.Response) -> str | None:
    """Persists the resource identity from `response` and returns the resulting validator."""
    etag = response.headers.get("etag")
    last_modified = response.headers.get("last-modified")
    state = DownloadState(url=url, etag=etag, last_modified=last_modified)
    state_path.write_bytes(msgspec.json.encode(state))
    validator: str | None = etag or last_modified
    return validator


async def load_resume_state_async(
    part_path: anyio.Path, state_path: anyio.Path, url: str, *, overwrite: bool
) -> tuple[int, str | None]:
    """Checks for a resumable `.part` file, returning its size and identity validator.

    Raises:
        DownloadIdentityError: If a `.part` file exists for a different resource.
    """
    if not await part_path.exists() or not await state_path.exists():
        return 0, None
    state = msgspec.json.decode(await state_path.read_bytes(), type=DownloadState)
    if state.url != url:
        if not overwrite:
            raise DownloadIdentityError(
                f"{part_path} belongs to a different resource; pass overwrite=True to restart"
            )
        return 0, None
    stat = await part_path.stat()
    return stat.st_size, state.etag or state.last_modified


async def write_state_async(
    state_path: anyio.Path, url: str, response: httpx.Response
) -> str | None:
    """Persists the resource identity from `response` and returns the resulting validator."""
    etag = response.headers.get("etag")
    last_modified = response.headers.get("last-modified")
    state = DownloadState(url=url, etag=etag, last_modified=last_modified)
    await state_path.write_bytes(msgspec.json.encode(state))
    validator: str | None = etag or last_modified
    return validator


class ProbeResult(msgspec.Struct, frozen=True):
    """Stores status, total size, and validators from a `Range: bytes=0-0` probe."""

    status_code: int
    total_size: int | None
    etag: str | None = None
    last_modified: str | None = None


def probe_remote(
    consumer: SyncConsumer,
    spec: RequestSpec,
    ratelimit: RateLimit | None,
    retry: Retry,
    streaming_response_handler: ResponseHandler | None,
) -> ProbeResult:
    """Probes remote resource with `Range: bytes=0-0`, retrying transient failures."""

    def send_probe() -> ProbeResult:
        gate_sync(spec.url, ratelimit)
        kwargs = spec.to_kwargs()
        headers = dict(kwargs.get("headers") or {})
        headers["Range"] = "bytes=0-0"
        kwargs["headers"] = headers
        with consumer.session.stream(spec.method, spec.url, **kwargs) as response:
            total_size = extract_total_size(response, 0)
            # a 416 carrying a total size answers the probe for an empty resource.
            if not (
                response.status_code == HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE
                and total_size is not None
            ):
                apply_streaming_response_handler(streaming_response_handler, response)
            return ProbeResult(
                status_code=response.status_code,
                total_size=total_size,
                etag=response.headers.get("etag"),
                last_modified=response.headers.get("last-modified"),
            )

    return retry_call_sync(send_probe, retry)


async def probe_remote_async(
    consumer: AsyncConsumer,
    spec: RequestSpec,
    ratelimit: RateLimit | None,
    retry: Retry,
    streaming_response_handler: ResponseHandler | None,
) -> ProbeResult:
    """Asynchronously probes remote resource with `Range: bytes=0-0`, retrying failures."""

    async def send_probe() -> ProbeResult:
        await gate_async(spec.url, ratelimit)
        kwargs = spec.to_kwargs()
        headers = dict(kwargs.get("headers") or {})
        headers["Range"] = "bytes=0-0"
        kwargs["headers"] = headers
        async with consumer.session.stream(spec.method, spec.url, **kwargs) as response:
            total_size = extract_total_size(response, 0)
            # a 416 carrying a total size answers the probe for an empty resource.
            if not (
                response.status_code == HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE
                and total_size is not None
            ):
                apply_streaming_response_handler(streaming_response_handler, response)
            return ProbeResult(
                status_code=response.status_code,
                total_size=total_size,
                etag=response.headers.get("etag"),
                last_modified=response.headers.get("last-modified"),
            )

    return await retry_call_async(send_probe, retry)


def check_existing_target(  # noqa: PLR0913, PLR0917
    target: Path,
    spec: RequestSpec,
    checksum_config: Checksum | None,
    consumer: SyncConsumer,
    ratelimit: RateLimit | None,
    retry: Retry,
    streaming_response_handler: ResponseHandler | None,
    *,
    on_checksum: ChecksumCallback | None,
    on_progress: ProgressCallback | None,
) -> None:
    """Validates existing destination file against checksum or remote size.

    Raises:
        ChecksumMismatchError: If the existing file digest does not match expected checksum.
        DownloadIdentityError: If the existing file size does not match remote resource size.
        IsADirectoryError: If target is a directory.
    """
    if target.is_dir():
        raise IsADirectoryError(f"{target} is a directory")

    if checksum_config is not None and checksum_config.expected is not None:
        with target.open("rb") as f:
            digest = compute_file_digest(f, checksum_config.algorithm)
        verify_checksum(digest, checksum_config, on_checksum)
        file_size = target.stat().st_size
        notify(on_progress, file_size, file_size)
        cleanup_staging_files(target)
        return

    probe = probe_remote(consumer, spec, ratelimit, retry, streaming_response_handler)
    local_size = target.stat().st_size
    if probe.total_size is not None and local_size == probe.total_size:
        if checksum_config is not None:
            with target.open("rb") as f:
                digest = compute_file_digest(f, checksum_config.algorithm)
            verify_checksum(digest, checksum_config, on_checksum)
        notify(on_progress, local_size, probe.total_size)
        cleanup_staging_files(target)
        return

    size_desc = f"{probe.total_size} bytes" if probe.total_size is not None else "unknown size"
    raise DownloadIdentityError(
        f"{target} already exists with size {local_size} bytes, "
        f"but remote resource has size {size_desc}; "
        "pass overwrite=True to restart"
    )


async def check_existing_target_async(  # noqa: PLR0913, PLR0917
    target: anyio.Path,
    spec: RequestSpec,
    checksum_config: Checksum | None,
    consumer: AsyncConsumer,
    ratelimit: RateLimit | None,
    retry: Retry,
    streaming_response_handler: ResponseHandler | None,
    *,
    on_checksum: ChecksumCallback | None,
    on_progress: ProgressCallback | None,
) -> None:
    """Asynchronously validates existing destination file against checksum or remote size.

    Raises:
        ChecksumMismatchError: If the existing file digest does not match expected checksum.
        DownloadIdentityError: If the existing file size does not match remote resource size.
        IsADirectoryError: If target is a directory.
    """
    if await target.is_dir():
        raise IsADirectoryError(f"{target} is a directory")

    if checksum_config is not None and checksum_config.expected is not None:
        digest = await compute_file_digest_async(target, checksum_config.algorithm)
        verify_checksum(digest, checksum_config, on_checksum)
        stat = await target.stat()
        notify(on_progress, stat.st_size, stat.st_size)
        await cleanup_staging_files_async(target)
        return

    probe = await probe_remote_async(consumer, spec, ratelimit, retry, streaming_response_handler)
    stat = await target.stat()
    local_size = stat.st_size
    if probe.total_size is not None and local_size == probe.total_size:
        if checksum_config is not None:
            digest = await compute_file_digest_async(target, checksum_config.algorithm)
            verify_checksum(digest, checksum_config, on_checksum)
        notify(on_progress, local_size, probe.total_size)
        await cleanup_staging_files_async(target)
        return

    size_desc = f"{probe.total_size} bytes" if probe.total_size is not None else "unknown size"
    raise DownloadIdentityError(
        f"{target} already exists with size {local_size} bytes, "
        f"but remote resource has size {size_desc}; "
        "pass overwrite=True to restart"
    )


class Downloader(msgspec.Struct, frozen=True):
    """Bound, not-yet-executed resumable download, returned by a `-> Downloader` GET endpoint."""

    consumer: SyncConsumer
    spec: RequestSpec
    retry: Retry
    ratelimit: RateLimit | None = None
    concurrency: Concurrency | None = None
    parts: Parts | None = None
    checksum: Checksum | None = None
    streaming_response_handler: ResponseHandler | None = None

    def __call__(  # noqa: PLR0913
        self,
        path: StrPath,
        *,
        overwrite: bool = False,
        on_progress: ProgressCallback | None = None,
        on_part_progress: PartProgressCallback | None = None,
        parts: int | Parts | None = None,
        checksum: ChecksumInput = None,
        on_checksum: ChecksumCallback | None = None,
    ) -> Path:
        """Downloads (or resumes) to `path`, returning it once the download completes cleanly.

        Raises:
            ChecksumMismatchError: If the downloaded or existing file digest does not match
                expected checksum.
            DownloadIdentityError: If a `.part` file exists for a different resource, or if `path`
                already exists with a different size than the remote resource. Pass `overwrite=True`
                to discard it and start over.
            ResumeLostError: If a reconnect gets a full response instead of a partial one.
            IsADirectoryError: If `path` is an existing directory.
        """
        target = Path(path)
        try:
            with gate_concurrency_sync(self.spec.url, self.concurrency):
                part_path, state_path, _ = part_paths(target)
                with hold_download_lock(target):
                    parts_config = resolve_parts(self.parts if parts is None else parts)
                    checksum_config = resolve_checksum(
                        self.checksum if checksum is None else checksum
                    )
                    if not overwrite and target.exists():
                        check_existing_target(
                            target,
                            self.spec,
                            checksum_config,
                            self.consumer,
                            self.ratelimit,
                            self.retry,
                            self.streaming_response_handler,
                            on_checksum=on_checksum,
                            on_progress=on_progress,
                        )
                        return target
                    if parts_config is None or parts_config.count == 1:
                        return self._download_single_stream(
                            target,
                            part_path,
                            state_path,
                            overwrite=overwrite,
                            on_progress=on_progress,
                            checksum_config=checksum_config,
                            on_checksum=on_checksum,
                        )
                    return self._download_multi_part(
                        target,
                        part_path,
                        state_path,
                        parts_config=parts_config,
                        overwrite=overwrite,
                        on_progress=on_progress,
                        on_part_progress=on_part_progress,
                        checksum_config=checksum_config,
                        on_checksum=on_checksum,
                    )
        except KeyboardInterrupt as e:
            cleanup_progress(on_progress)
            cleanup_progress(on_part_progress)
            raise DownloadInterruptedError(f"Download to {target} was interrupted") from e
        except BaseException:
            cleanup_progress(on_progress)
            cleanup_progress(on_part_progress)
            raise

    def _download_single_stream(  # noqa: C901, PLR0913
        self,
        target: Path,
        part_path: Path,
        state_path: Path,
        *,
        overwrite: bool,
        on_progress: ProgressCallback | None,
        checksum_config: Checksum | None = None,
        on_checksum: ChecksumCallback | None = None,
    ) -> Path:
        received, validator = load_resume_state(
            part_path, state_path, self.spec.url, overwrite=overwrite
        )

        hasher = hashlib.new(checksum_config.algorithm) if checksum_config is not None else None
        if hasher is not None and received > 0 and part_path.exists():
            with part_path.open("rb") as pfh:
                while True:
                    buf = pfh.read(1024 * 1024)
                    if not buf:
                        break
                    hasher.update(buf)

        attempt = 0
        with part_path.open("ab" if received else "wb") as fh:
            while True:
                gate_sync(self.spec.url, self.ratelimit)
                kwargs = self.spec.to_kwargs()
                kwargs["headers"] = resume_headers(self.spec, received, validator) or None
                try:
                    with self.consumer.session.stream(
                        self.spec.method, self.spec.url, **kwargs
                    ) as response:
                        apply_streaming_response_handler(self.streaming_response_handler, response)
                        if received and response.status_code != HTTPStatus.PARTIAL_CONTENT:
                            raise ResumeLostError("server ignored Range or the resource changed")
                        if validator is None:
                            validator = write_state(state_path, self.spec.url, response)
                        total = extract_total_size(response, received)
                        notify(on_progress, received, total)
                        for chunk in response.iter_bytes():
                            fh.write(chunk)
                            if hasher is not None:
                                hasher.update(chunk)
                            fh.flush()
                            os.fsync(fh.fileno())
                            received += len(chunk)
                            notify(on_progress, received, total)
                    break
                except retryable_exceptions(self.retry.on) as e:
                    if not is_retryable_exception(e, self.retry):
                        raise
                    attempt += 1
                    if attempt >= self.retry.attempts:
                        raise
                    time.sleep(resolve_delay(self.retry, attempt, extract_response(e)))

        if checksum_config is not None and hasher is not None:
            verify_checksum(hasher.hexdigest(), checksum_config, on_checksum)

        part_path.replace(target)
        cleanup_staging_files(target)
        return target

    def _download_multi_part(  # noqa: C901, PLR0912, PLR0913, PLR0915
        self,
        target: Path,
        part_path: Path,
        state_path: Path,
        *,
        parts_config: Parts,
        overwrite: bool,
        on_progress: ProgressCallback | None,
        on_part_progress: PartProgressCallback | None,
        checksum_config: Checksum | None = None,
        on_checksum: ChecksumCallback | None = None,
    ) -> Path:
        saved_state: MultiPartState | None = None
        if state_path.exists():
            try:
                saved_state = msgspec.json.decode(state_path.read_bytes(), type=MultiPartState)
            except (msgspec.DecodeError, OSError):
                saved_state = None

        if saved_state is not None and saved_state.url != self.spec.url:
            if not overwrite:
                msg = f"{part_path} belongs to a different resource; pass overwrite=True to restart"
                raise DownloadIdentityError(msg)
            saved_state = None

        if saved_state is None:
            cleanup_staging_files(target)
            probe = probe_remote(
                self.consumer,
                self.spec,
                self.ratelimit,
                self.retry,
                self.streaming_response_handler,
            )
            if (
                probe.status_code != HTTPStatus.PARTIAL_CONTENT
                or probe.total_size is None
                or parts_config.resolve_count(probe.total_size) <= 1
            ):
                cleanup_staging_files(target)
                return self._download_single_stream(
                    target,
                    part_path,
                    state_path,
                    overwrite=overwrite,
                    on_progress=on_progress,
                    checksum_config=checksum_config,
                    on_checksum=on_checksum,
                )

            total_size = probe.total_size
            etag = probe.etag
            last_modified = probe.last_modified
            validator = etag or last_modified

            num_parts = parts_config.resolve_count(total_size)
            ranges = compute_ranges(total_size, num_parts)
            parts_state = [
                PartState(index=i, start=start, end=end, received=0)
                for i, (start, end) in enumerate(ranges)
            ]
            saved_state = MultiPartState(
                url=self.spec.url,
                etag=etag,
                last_modified=last_modified,
                total_size=total_size,
                parts=parts_state,
            )
            state_path.write_bytes(msgspec.json.encode(saved_state))
        else:
            total_size = saved_state.total_size
            validator = saved_state.etag or saved_state.last_modified
            num_parts = len(saved_state.parts)
            parts_state = saved_state.parts

        received_per_part = [0] * num_parts
        for i in range(num_parts):
            seg_path = target.with_name(f"{target.name}.part.{i}")
            if seg_path.exists():
                parts_state[i].received = seg_path.stat().st_size
            else:
                parts_state[i].received = 0
            received_per_part[i] = parts_state[i].received

        lock = threading.Lock()
        with lock:
            current_total = sum(received_per_part)
        for i, p_info in enumerate(parts_state):
            p_total = p_info.end - p_info.start + 1
            notify_part(
                on_progress,
                on_part_progress,
                i,
                p_info.received,
                p_total,
                current_total,
                total_size,
            )

        cancel_event = threading.Event()
        active_responses: dict[int, httpx.Response] = {}

        def download_segment(part_idx: int) -> None:  # noqa: C901
            if cancel_event.is_set():
                return
            part_info = parts_state[part_idx]
            seg_path = target.with_name(f"{target.name}.part.{part_idx}")
            needed = part_info.end - part_info.start + 1
            if part_info.received >= needed:
                return

            attempt = 0
            while True:
                gate_sync(self.spec.url, self.ratelimit)
                part_kwargs = self.spec.to_kwargs()
                range_start = part_info.start + part_info.received
                part_headers = resume_headers(self.spec, range_start, validator) or {}
                part_headers["Range"] = f"bytes={range_start}-{part_info.end}"
                part_kwargs["headers"] = part_headers
                try:
                    with self.consumer.session.stream(
                        self.spec.method, self.spec.url, **part_kwargs
                    ) as resp:
                        active_responses[part_idx] = resp
                        try:
                            apply_streaming_response_handler(self.streaming_response_handler, resp)
                            if (
                                resp.status_code != HTTPStatus.PARTIAL_CONTENT
                                and (part_info.start + part_info.received) > 0
                            ):
                                raise ResumeLostError(
                                    "server ignored Range or the resource changed"
                                )
                            with seg_path.open("ab" if part_info.received else "wb") as sfh:
                                for chunk in resp.iter_bytes():
                                    if cancel_event.is_set():
                                        return
                                    sfh.write(chunk)
                                    sfh.flush()
                                    part_info.received += len(chunk)
                                    with lock:
                                        received_per_part[part_idx] = part_info.received
                                        current_total = sum(received_per_part)
                                    part_total = part_info.end - part_info.start + 1
                                    notify_part(
                                        on_progress,
                                        on_part_progress,
                                        part_idx,
                                        part_info.received,
                                        part_total,
                                        current_total,
                                        total_size,
                                    )
                        finally:
                            active_responses.pop(part_idx, None)
                    break
                except retryable_exceptions(self.retry.on) as e:
                    if not is_retryable_exception(e, self.retry):
                        raise
                    attempt += 1
                    if attempt >= self.retry.attempts:
                        raise
                    delay = resolve_delay(self.retry, attempt, extract_response(e))
                    if cancel_event.wait(delay):
                        return

        segment_errors: dict[int, Exception] = {}

        def run_worker(part_idx: int) -> None:
            try:
                download_segment(part_idx)
            except Exception as e:  # noqa: BLE001
                segment_errors[part_idx] = e

        threads = [
            threading.Thread(target=run_worker, args=(i,), daemon=True) for i in range(num_parts)
        ]
        for t in threads:
            t.start()

        try:
            for t in threads:
                while t.is_alive():
                    t.join(timeout=0.1)
        except BaseException:
            cancel_event.set()
            for resp in list(active_responses.values()):
                with contextlib.suppress(Exception):
                    resp.close()
            raise

        if segment_errors:
            chosen_idx = min(segment_errors)
            if len(segment_errors) == 1:
                raise segment_errors[chosen_idx]
            failed_parts = ", ".join(str(i) for i in sorted(segment_errors))
            raise segment_errors[chosen_idx] from RuntimeError(
                f"{len(segment_errors)} of {num_parts} segments failed (parts {failed_parts})"
            )

        hasher = hashlib.new(checksum_config.algorithm) if checksum_config is not None else None

        with part_path.open("wb") as out_fh:
            for i in range(num_parts):
                seg_path = target.with_name(f"{target.name}.part.{i}")
                with seg_path.open("rb") as in_fh:
                    while True:
                        buf = in_fh.read(1024 * 1024)
                        if not buf:
                            break
                        out_fh.write(buf)
                        if hasher is not None:
                            hasher.update(buf)
                seg_path.unlink(missing_ok=True)
            out_fh.flush()
            os.fsync(out_fh.fileno())

        if checksum_config is not None and hasher is not None:
            verify_checksum(hasher.hexdigest(), checksum_config, on_checksum)

        part_path.replace(target)
        cleanup_staging_files(target)
        return target


class AsyncDownloader(msgspec.Struct, frozen=True):
    """Bound, not-yet-executed resumable download, returned by a `-> AsyncDownloader` endpoint."""

    consumer: AsyncConsumer
    spec: RequestSpec
    retry: Retry
    ratelimit: RateLimit | None = None
    concurrency: Concurrency | None = None
    parts: Parts | None = None
    checksum: Checksum | None = None
    streaming_response_handler: ResponseHandler | None = None

    async def __call__(  # noqa: PLR0913
        self,
        path: StrPath,
        *,
        overwrite: bool = False,
        on_progress: ProgressCallback | None = None,
        on_part_progress: PartProgressCallback | None = None,
        parts: int | Parts | None = None,
        checksum: ChecksumInput = None,
        on_checksum: ChecksumCallback | None = None,
    ) -> Path:
        """Downloads (or resumes) to `path`, returning it once the download completes cleanly.

        Raises:
            ChecksumMismatchError: If the downloaded or existing file digest does not match
                expected checksum.
            DownloadIdentityError: If a `.part` file exists for a different resource, or if `path`
                already exists with a different size than the remote resource. Pass `overwrite=True`
                to discard it and start over.
            ResumeLostError: If a reconnect gets a full response instead of a partial one.
            IsADirectoryError: If `path` is an existing directory.
        """
        import anyio

        target = anyio.Path(path)
        try:
            async with gate_concurrency_async(self.spec.url, self.concurrency):
                part_path = target.with_name(target.name + ".part")
                state_path = target.with_name(target.name + ".part.json")
                async with hold_download_lock_async(target):
                    parts_config = resolve_parts(self.parts if parts is None else parts)
                    checksum_config = resolve_checksum(
                        self.checksum if checksum is None else checksum
                    )
                    if not overwrite and await target.exists():
                        await check_existing_target_async(
                            target,
                            self.spec,
                            checksum_config,
                            self.consumer,
                            self.ratelimit,
                            self.retry,
                            self.streaming_response_handler,
                            on_checksum=on_checksum,
                            on_progress=on_progress,
                        )
                        return Path(target)
                    if parts_config is None or parts_config.count == 1:
                        return await self._download_single_stream_async(
                            target,
                            part_path,
                            state_path,
                            overwrite=overwrite,
                            on_progress=on_progress,
                            checksum_config=checksum_config,
                            on_checksum=on_checksum,
                        )
                    return await self._download_multi_part_async(
                        target,
                        part_path,
                        state_path,
                        parts_config=parts_config,
                        overwrite=overwrite,
                        on_progress=on_progress,
                        on_part_progress=on_part_progress,
                        checksum_config=checksum_config,
                        on_checksum=on_checksum,
                    )
        except KeyboardInterrupt as e:
            cleanup_progress(on_progress)
            cleanup_progress(on_part_progress)
            raise DownloadInterruptedError(f"Download to {target} was interrupted") from e
        except BaseException:
            cleanup_progress(on_progress)
            cleanup_progress(on_part_progress)
            raise

    async def _download_single_stream_async(  # noqa: C901, PLR0913
        self,
        target: anyio.Path,
        part_path: anyio.Path,
        state_path: anyio.Path,
        *,
        overwrite: bool,
        on_progress: ProgressCallback | None,
        checksum_config: Checksum | None = None,
        on_checksum: ChecksumCallback | None = None,
    ) -> Path:
        received, validator = await load_resume_state_async(
            part_path, state_path, self.spec.url, overwrite=overwrite
        )

        hasher = hashlib.new(checksum_config.algorithm) if checksum_config is not None else None
        if hasher is not None and received > 0 and await part_path.exists():
            async with await part_path.open("rb") as pfh:
                while True:
                    buf = await pfh.read(1024 * 1024)
                    if not buf:
                        break
                    hasher.update(buf)

        attempt = 0
        async with await part_path.open("ab" if received else "wb") as fh:
            while True:
                await gate_async(self.spec.url, self.ratelimit)
                kwargs = self.spec.to_kwargs()
                kwargs["headers"] = resume_headers(self.spec, received, validator) or None
                try:
                    async with self.consumer.session.stream(
                        self.spec.method, self.spec.url, **kwargs
                    ) as response:
                        apply_streaming_response_handler(self.streaming_response_handler, response)
                        if received and response.status_code != HTTPStatus.PARTIAL_CONTENT:
                            raise ResumeLostError("server ignored Range or the resource changed")
                        if validator is None:
                            validator = await write_state_async(state_path, self.spec.url, response)
                        total = extract_total_size(response, received)
                        notify(on_progress, received, total)
                        async for chunk in response.aiter_bytes():
                            await fh.write(chunk)
                            await fh.flush()
                            if hasher is not None:
                                hasher.update(chunk)
                            received += len(chunk)
                            notify(on_progress, received, total)
                    break
                except retryable_exceptions(self.retry.on) as e:
                    if not is_retryable_exception(e, self.retry):
                        raise
                    attempt += 1
                    if attempt >= self.retry.attempts:
                        raise
                    await fh.flush()
                    await asyncio.sleep(resolve_delay(self.retry, attempt, extract_response(e)))

        if checksum_config is not None and hasher is not None:
            verify_checksum(hasher.hexdigest(), checksum_config, on_checksum)

        await part_path.replace(target)
        await cleanup_staging_files_async(target)
        return Path(target)

    async def _download_multi_part_async(  # noqa: C901, PLR0912, PLR0913, PLR0915
        self,
        target: anyio.Path,
        part_path: anyio.Path,
        state_path: anyio.Path,
        *,
        parts_config: Parts,
        overwrite: bool,
        on_progress: ProgressCallback | None,
        on_part_progress: PartProgressCallback | None,
        checksum_config: Checksum | None = None,
        on_checksum: ChecksumCallback | None = None,
    ) -> Path:
        import anyio

        saved_state: MultiPartState | None = None
        if await state_path.exists():
            try:
                saved_state = msgspec.json.decode(
                    await state_path.read_bytes(), type=MultiPartState
                )
            except (msgspec.DecodeError, OSError):
                saved_state = None

        if saved_state is not None and saved_state.url != self.spec.url:
            if not overwrite:
                msg = f"{part_path} belongs to a different resource; pass overwrite=True to restart"
                raise DownloadIdentityError(msg)
            saved_state = None

        if saved_state is None:
            await cleanup_staging_files_async(target)
            probe = await probe_remote_async(
                self.consumer,
                self.spec,
                self.ratelimit,
                self.retry,
                self.streaming_response_handler,
            )
            if (
                probe.status_code != HTTPStatus.PARTIAL_CONTENT
                or probe.total_size is None
                or parts_config.resolve_count(probe.total_size) <= 1
            ):
                await cleanup_staging_files_async(target)
                return await self._download_single_stream_async(
                    target,
                    part_path,
                    state_path,
                    overwrite=overwrite,
                    on_progress=on_progress,
                    checksum_config=checksum_config,
                    on_checksum=on_checksum,
                )

            total_size = probe.total_size
            etag = probe.etag
            last_modified = probe.last_modified
            validator = etag or last_modified

            num_parts = parts_config.resolve_count(total_size)
            ranges = compute_ranges(total_size, num_parts)
            parts_state = [
                PartState(index=i, start=start, end=end, received=0)
                for i, (start, end) in enumerate(ranges)
            ]
            saved_state = MultiPartState(
                url=self.spec.url,
                etag=etag,
                last_modified=last_modified,
                total_size=total_size,
                parts=parts_state,
            )
            await state_path.write_bytes(msgspec.json.encode(saved_state))
        else:
            total_size = saved_state.total_size
            validator = saved_state.etag or saved_state.last_modified
            num_parts = len(saved_state.parts)
            parts_state = saved_state.parts

        received_per_part = [0] * num_parts
        for i in range(num_parts):
            seg_path = target.with_name(f"{target.name}.part.{i}")
            if await seg_path.exists():
                parts_state[i].received = (await seg_path.stat()).st_size
            else:
                parts_state[i].received = 0
            received_per_part[i] = parts_state[i].received

        current_total = sum(received_per_part)
        for i, p_info in enumerate(parts_state):
            p_total = p_info.end - p_info.start + 1
            notify_part(
                on_progress,
                on_part_progress,
                i,
                p_info.received,
                p_total,
                current_total,
                total_size,
            )

        segment_errors: dict[int, BaseException] = {}

        async def download_segment_async(part_idx: int) -> None:
            # recording exceptions per part, instead of raising directly
            part_info = parts_state[part_idx]
            seg_path = target.with_name(f"{target.name}.part.{part_idx}")
            needed = part_info.end - part_info.start + 1
            if part_info.received >= needed:
                return

            attempt = 0
            while True:
                await gate_async(self.spec.url, self.ratelimit)
                part_kwargs = self.spec.to_kwargs()
                range_start = part_info.start + part_info.received
                part_headers = resume_headers(self.spec, range_start, validator) or {}
                part_headers["Range"] = f"bytes={range_start}-{part_info.end}"
                part_kwargs["headers"] = part_headers
                try:
                    async with self.consumer.session.stream(
                        self.spec.method, self.spec.url, **part_kwargs
                    ) as resp:
                        apply_streaming_response_handler(self.streaming_response_handler, resp)
                        if (
                            resp.status_code != HTTPStatus.PARTIAL_CONTENT
                            and (part_info.start + part_info.received) > 0
                        ):
                            raise ResumeLostError("server ignored Range or the resource changed")
                        async with await seg_path.open("ab" if part_info.received else "wb") as sfh:
                            async for chunk in resp.aiter_bytes():
                                await sfh.write(chunk)
                                await sfh.flush()
                                part_info.received += len(chunk)
                                received_per_part[part_idx] = part_info.received
                                current_total = sum(received_per_part)
                                part_total = part_info.end - part_info.start + 1
                                notify_part(
                                    on_progress,
                                    on_part_progress,
                                    part_idx,
                                    part_info.received,
                                    part_total,
                                    current_total,
                                    total_size,
                                )
                    return
                except retryable_exceptions(self.retry.on) as e:
                    if not is_retryable_exception(e, self.retry):
                        segment_errors[part_idx] = e
                        return
                    attempt += 1
                    if attempt >= self.retry.attempts:
                        segment_errors[part_idx] = e
                        return
                    await asyncio.sleep(resolve_delay(self.retry, attempt, extract_response(e)))

        async with anyio.create_task_group() as tg:
            for i in range(num_parts):
                _ = tg.start_soon(download_segment_async, i)

        if segment_errors:
            chosen_idx = min(segment_errors)
            if len(segment_errors) == 1:
                raise segment_errors[chosen_idx]
            failed_parts = ", ".join(str(i) for i in sorted(segment_errors))
            raise segment_errors[chosen_idx] from RuntimeError(
                f"{len(segment_errors)} of {num_parts} segments failed (parts {failed_parts})"
            )

        hasher = hashlib.new(checksum_config.algorithm) if checksum_config is not None else None

        async with await part_path.open("wb") as out_fh:
            for i in range(num_parts):
                seg_path = target.with_name(f"{target.name}.part.{i}")
                async with await seg_path.open("rb") as in_fh:
                    while True:
                        buf = await in_fh.read(1024 * 1024)
                        if not buf:
                            break
                        await out_fh.write(buf)
                        if hasher is not None:
                            hasher.update(buf)
                await seg_path.unlink(missing_ok=True)
            await out_fh.flush()

        if checksum_config is not None and hasher is not None:
            verify_checksum(hasher.hexdigest(), checksum_config, on_checksum)

        await part_path.replace(target)
        await cleanup_staging_files_async(target)
        return Path(target)
