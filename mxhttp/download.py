"""Resumable-from-disk downloads: `Downloader`/`AsyncDownloader`."""

from __future__ import annotations

import asyncio
import concurrent.futures
import os
import threading
import time
from collections.abc import Callable  # noqa: TC003
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias

import msgspec

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
    retryable_exceptions,
)

if TYPE_CHECKING:
    import anyio
    import httpx
    from _typeshed import StrPath

ProgressCallback: TypeAlias = "Callable[[int, int | None], None]"


def _notify(callback: ProgressCallback | None, received: int, total: int | None) -> None:
    """Invokes the synchronous progress callback if provided."""
    if callback is not None:
        callback(received, total)


class DownloadIdentityError(Exception):
    """An existing `.part` file belongs to a different resource than the one being downloaded."""


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


def part_paths(path: Path) -> tuple[Path, Path, Path]:
    """Returns the staging file, sidecar metadata, and lock paths for a download at `path`."""
    return (
        path.with_name(path.name + ".part"),
        path.with_name(path.name + ".part.json"),
        path.with_name(path.name + ".part.lock"),
    )


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
    if overwrite or not part_path.exists() or not state_path.exists():
        return 0, None
    state = msgspec.json.decode(state_path.read_bytes(), type=DownloadState)
    if state.url != url:
        raise DownloadIdentityError(
            f"{part_path} belongs to a different resource; pass overwrite=True to restart"
        )
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
    if overwrite or not await part_path.exists() or not await state_path.exists():
        return 0, None
    state = msgspec.json.decode(await state_path.read_bytes(), type=DownloadState)
    if state.url != url:
        raise DownloadIdentityError(
            f"{part_path} belongs to a different resource; pass overwrite=True to restart"
        )
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


class Downloader(msgspec.Struct, frozen=True):
    """Bound, not-yet-executed resumable download, returned by a `-> Downloader` GET endpoint."""

    consumer: SyncConsumer
    spec: RequestSpec
    retry: Retry
    ratelimit: RateLimit | None = None
    concurrency: Concurrency | None = None
    parts: Parts | None = None

    def __call__(
        self,
        path: StrPath,
        *,
        overwrite: bool = False,
        on_progress: ProgressCallback | None = None,
        parts: int | Parts | None = None,
    ) -> Path:
        """Downloads (or resumes) to `path`, returning it once the download completes cleanly.

        Raises:
            DownloadIdentityError: If a `.part` file exists for a different resource. Pass
                `overwrite=True` to discard it and start over.
            ResumeLostError: If a reconnect gets a full response instead of a partial one.
        """
        from filelock import FileLock, Timeout

        with gate_concurrency_sync(self.consumer, self.concurrency):
            target = Path(path)
            part_path, state_path, lock_path = part_paths(target)
            try:
                with FileLock(lock_path, timeout=0, fallback_to_soft=False):
                    parts_config = resolve_parts(self.parts if parts is None else parts)
                    if parts_config is None or parts_config.count == 1:
                        return self._download_single_stream(
                            target,
                            part_path,
                            state_path,
                            overwrite=overwrite,
                            on_progress=on_progress,
                        )
                    return self._download_multi_part(
                        target,
                        part_path,
                        state_path,
                        parts_config=parts_config,
                        overwrite=overwrite,
                        on_progress=on_progress,
                    )
            except Timeout as e:
                raise DownloadLockError(f"Download to {target} is locked by another process") from e

    def _download_single_stream(
        self,
        target: Path,
        part_path: Path,
        state_path: Path,
        *,
        overwrite: bool,
        on_progress: ProgressCallback | None,
    ) -> Path:
        received, validator = load_resume_state(
            part_path, state_path, self.spec.url, overwrite=overwrite
        )

        attempt = 0
        with part_path.open("ab" if received else "wb") as fh:
            while True:
                gate_sync(self.consumer, self.ratelimit)
                kwargs = self.spec.to_kwargs()
                kwargs["headers"] = resume_headers(self.spec, received, validator) or None
                try:
                    with self.consumer.session.stream(
                        self.spec.method, self.spec.url, **kwargs
                    ) as response:
                        apply_streaming_response_handler(self.consumer, response)
                        if received and response.status_code != HTTPStatus.PARTIAL_CONTENT:
                            raise ResumeLostError("server ignored Range or the resource changed")
                        if validator is None:
                            validator = write_state(state_path, self.spec.url, response)
                        total = extract_total_size(response, received)
                        _notify(on_progress, received, total)
                        for chunk in response.iter_bytes():
                            fh.write(chunk)
                            fh.flush()
                            os.fsync(fh.fileno())
                            received += len(chunk)
                            _notify(on_progress, received, total)
                    break
                except retryable_exceptions(self.retry.on) as e:
                    if not is_retryable_exception(e, self.retry):
                        raise
                    attempt += 1
                    if attempt >= self.retry.attempts:
                        raise
                    time.sleep(resolve_delay(self.retry, attempt, extract_response(e)))

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
    ) -> Path:
        saved_state: MultiPartState | None = None
        if not overwrite and state_path.exists():
            try:
                saved_state = msgspec.json.decode(state_path.read_bytes(), type=MultiPartState)
            except (msgspec.DecodeError, OSError):
                saved_state = None

        if saved_state is not None and saved_state.url != self.spec.url:
            msg = f"{part_path} belongs to a different resource; pass overwrite=True to restart"
            raise DownloadIdentityError(msg)

        if saved_state is None:
            cleanup_staging_files(target)
            gate_sync(self.consumer, self.ratelimit)
            probe_kwargs = self.spec.to_kwargs()
            probe_headers = dict(probe_kwargs.get("headers") or {})
            probe_headers["Range"] = "bytes=0-0"
            probe_kwargs["headers"] = probe_headers

            with self.consumer.session.stream(
                self.spec.method, self.spec.url, **probe_kwargs
            ) as probe_resp:
                apply_streaming_response_handler(self.consumer, probe_resp)
                if probe_resp.status_code != HTTPStatus.PARTIAL_CONTENT:
                    cleanup_staging_files(target)
                    validator = write_state(state_path, self.spec.url, probe_resp)
                    total = extract_total_size(probe_resp, 0)
                    received = 0
                    _notify(on_progress, received, total)
                    with part_path.open("wb") as fh:
                        for chunk in probe_resp.iter_bytes():
                            fh.write(chunk)
                            received += len(chunk)
                            _notify(on_progress, received, total)
                        fh.flush()
                        os.fsync(fh.fileno())
                    part_path.replace(target)
                    cleanup_staging_files(target)
                    return target

                total_size = extract_total_size(probe_resp, 0)
                etag = probe_resp.headers.get("etag")
                last_modified = probe_resp.headers.get("last-modified")
                validator = etag or last_modified

            if total_size is None or parts_config.resolve_count(total_size) <= 1:
                cleanup_staging_files(target)
                return self._download_single_stream(
                    target, part_path, state_path, overwrite=overwrite, on_progress=on_progress
                )

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
        _notify(on_progress, sum(received_per_part), total_size)

        def download_segment(part_idx: int) -> None:
            part_info = parts_state[part_idx]
            seg_path = target.with_name(f"{target.name}.part.{part_idx}")
            needed = part_info.end - part_info.start + 1
            if part_info.received >= needed:
                return

            attempt = 0
            while True:
                gate_sync(self.consumer, self.ratelimit)
                part_kwargs = self.spec.to_kwargs()
                range_start = part_info.start + part_info.received
                part_headers = resume_headers(self.spec, range_start, validator) or {}
                part_headers["Range"] = f"bytes={range_start}-{part_info.end}"
                part_kwargs["headers"] = part_headers
                try:
                    with self.consumer.session.stream(
                        self.spec.method, self.spec.url, **part_kwargs
                    ) as resp:
                        apply_streaming_response_handler(self.consumer, resp)
                        if (
                            resp.status_code != HTTPStatus.PARTIAL_CONTENT
                            and (part_info.start + part_info.received) > 0
                        ):
                            raise ResumeLostError("server ignored Range or the resource changed")
                        with seg_path.open("ab" if part_info.received else "wb") as sfh:
                            for chunk in resp.iter_bytes():
                                sfh.write(chunk)
                                sfh.flush()
                                part_info.received += len(chunk)
                                with lock:
                                    received_per_part[part_idx] = part_info.received
                                    current_total = sum(received_per_part)
                                _notify(on_progress, current_total, total_size)
                    break
                except retryable_exceptions(self.retry.on) as e:
                    if not is_retryable_exception(e, self.retry):
                        raise
                    attempt += 1
                    if attempt >= self.retry.attempts:
                        raise
                    time.sleep(resolve_delay(self.retry, attempt, extract_response(e)))

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_parts) as executor:
            futures = [executor.submit(download_segment, i) for i in range(num_parts)]
            for future in concurrent.futures.as_completed(futures):
                future.result()

        with part_path.open("wb") as out_fh:
            for i in range(num_parts):
                seg_path = target.with_name(f"{target.name}.part.{i}")
                with seg_path.open("rb") as in_fh:
                    while True:
                        buf = in_fh.read(1024 * 1024)
                        if not buf:
                            break
                        out_fh.write(buf)
                seg_path.unlink(missing_ok=True)
            out_fh.flush()
            os.fsync(out_fh.fileno())

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

    async def __call__(
        self,
        path: StrPath,
        *,
        overwrite: bool = False,
        on_progress: ProgressCallback | None = None,
        parts: int | Parts | None = None,
    ) -> Path:
        """Downloads (or resumes) to `path`, returning it once the download completes cleanly.

        Raises:
            DownloadIdentityError: If a `.part` file exists for a different resource. Pass
                `overwrite=True` to discard it and start over.
            ResumeLostError: If a reconnect gets a full response instead of a partial one.
        """
        import anyio
        from filelock import AsyncFileLock, Timeout

        async with gate_concurrency_async(self.consumer, self.concurrency):
            target = anyio.Path(path)
            part_path = target.with_name(target.name + ".part")
            state_path = target.with_name(target.name + ".part.json")
            lock_path = target.with_name(target.name + ".part.lock")
            try:
                async with AsyncFileLock(lock_path, timeout=0, fallback_to_soft=False):
                    parts_config = resolve_parts(self.parts if parts is None else parts)
                    if parts_config is None or parts_config.count == 1:
                        return await self._download_single_stream_async(
                            target,
                            part_path,
                            state_path,
                            overwrite=overwrite,
                            on_progress=on_progress,
                        )
                    return await self._download_multi_part_async(
                        target,
                        part_path,
                        state_path,
                        parts_config=parts_config,
                        overwrite=overwrite,
                        on_progress=on_progress,
                    )
            except Timeout as e:
                raise DownloadLockError(f"Download to {target} is locked by another process") from e

    async def _download_single_stream_async(
        self,
        target: anyio.Path,
        part_path: anyio.Path,
        state_path: anyio.Path,
        *,
        overwrite: bool,
        on_progress: ProgressCallback | None,
    ) -> Path:
        received, validator = await load_resume_state_async(
            part_path, state_path, self.spec.url, overwrite=overwrite
        )

        attempt = 0
        async with await part_path.open("ab" if received else "wb") as fh:
            while True:
                await gate_async(self.consumer, self.ratelimit)
                kwargs = self.spec.to_kwargs()
                kwargs["headers"] = resume_headers(self.spec, received, validator) or None
                try:
                    async with self.consumer.session.stream(
                        self.spec.method, self.spec.url, **kwargs
                    ) as response:
                        apply_streaming_response_handler(self.consumer, response)
                        if received and response.status_code != HTTPStatus.PARTIAL_CONTENT:
                            raise ResumeLostError("server ignored Range or the resource changed")
                        if validator is None:
                            validator = await write_state_async(state_path, self.spec.url, response)
                        total = extract_total_size(response, received)
                        _notify(on_progress, received, total)
                        async for chunk in response.aiter_bytes():
                            await fh.write(chunk)
                            received += len(chunk)
                            _notify(on_progress, received, total)
                    await fh.flush()
                    break
                except retryable_exceptions(self.retry.on) as e:
                    if not is_retryable_exception(e, self.retry):
                        raise
                    attempt += 1
                    if attempt >= self.retry.attempts:
                        raise
                    await fh.flush()
                    await asyncio.sleep(resolve_delay(self.retry, attempt, extract_response(e)))

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
    ) -> Path:
        import anyio

        saved_state: MultiPartState | None = None
        if not overwrite and await state_path.exists():
            try:
                saved_state = msgspec.json.decode(
                    await state_path.read_bytes(), type=MultiPartState
                )
            except (msgspec.DecodeError, OSError):
                saved_state = None

        if saved_state is not None and saved_state.url != self.spec.url:
            msg = f"{part_path} belongs to a different resource; pass overwrite=True to restart"
            raise DownloadIdentityError(msg)

        if saved_state is None:
            await cleanup_staging_files_async(target)
            await gate_async(self.consumer, self.ratelimit)
            probe_kwargs = self.spec.to_kwargs()
            probe_headers = dict(probe_kwargs.get("headers") or {})
            probe_headers["Range"] = "bytes=0-0"
            probe_kwargs["headers"] = probe_headers

            async with self.consumer.session.stream(
                self.spec.method, self.spec.url, **probe_kwargs
            ) as probe_resp:
                apply_streaming_response_handler(self.consumer, probe_resp)
                if probe_resp.status_code != HTTPStatus.PARTIAL_CONTENT:
                    await cleanup_staging_files_async(target)
                    validator = await write_state_async(state_path, self.spec.url, probe_resp)
                    total = extract_total_size(probe_resp, 0)
                    received = 0
                    _notify(on_progress, received, total)
                    async with await part_path.open("wb") as fh:
                        async for chunk in probe_resp.aiter_bytes():
                            await fh.write(chunk)
                            received += len(chunk)
                            _notify(on_progress, received, total)
                        await fh.flush()
                    await part_path.replace(target)
                    await cleanup_staging_files_async(target)
                    return Path(target)

                total_size = extract_total_size(probe_resp, 0)
                etag = probe_resp.headers.get("etag")
                last_modified = probe_resp.headers.get("last-modified")
                validator = etag or last_modified

            if total_size is None or parts_config.resolve_count(total_size) <= 1:
                await cleanup_staging_files_async(target)
                return await self._download_single_stream_async(
                    target, part_path, state_path, overwrite=overwrite, on_progress=on_progress
                )

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

        _notify(on_progress, sum(received_per_part), total_size)

        async def download_segment_async(part_idx: int) -> None:
            part_info = parts_state[part_idx]
            seg_path = target.with_name(f"{target.name}.part.{part_idx}")
            needed = part_info.end - part_info.start + 1
            if part_info.received >= needed:
                return

            attempt = 0
            while True:
                await gate_async(self.consumer, self.ratelimit)
                part_kwargs = self.spec.to_kwargs()
                range_start = part_info.start + part_info.received
                part_headers = resume_headers(self.spec, range_start, validator) or {}
                part_headers["Range"] = f"bytes={range_start}-{part_info.end}"
                part_kwargs["headers"] = part_headers
                try:
                    async with self.consumer.session.stream(
                        self.spec.method, self.spec.url, **part_kwargs
                    ) as resp:
                        apply_streaming_response_handler(self.consumer, resp)
                        if (
                            resp.status_code != HTTPStatus.PARTIAL_CONTENT
                            and (part_info.start + part_info.received) > 0
                        ):
                            raise ResumeLostError("server ignored Range or the resource changed")
                        async with await seg_path.open("ab" if part_info.received else "wb") as sfh:
                            async for chunk in resp.aiter_bytes():
                                await sfh.write(chunk)
                                part_info.received += len(chunk)
                                received_per_part[part_idx] = part_info.received
                                current_total = sum(received_per_part)
                                _notify(on_progress, current_total, total_size)
                            await sfh.flush()
                    break
                except retryable_exceptions(self.retry.on) as e:
                    if not is_retryable_exception(e, self.retry):
                        raise
                    attempt += 1
                    if attempt >= self.retry.attempts:
                        raise
                    await asyncio.sleep(resolve_delay(self.retry, attempt, extract_response(e)))

        async with anyio.create_task_group() as tg:
            for i in range(num_parts):
                _ = tg.start_soon(download_segment_async, i)

        async with await part_path.open("wb") as out_fh:
            for i in range(num_parts):
                seg_path = target.with_name(f"{target.name}.part.{i}")
                async with await seg_path.open("rb") as in_fh:
                    while True:
                        buf = await in_fh.read(1024 * 1024)
                        if not buf:
                            break
                        await out_fh.write(buf)
                await seg_path.unlink(missing_ok=True)
            await out_fh.flush()

        await part_path.replace(target)
        await cleanup_staging_files_async(target)
        return Path(target)
