"""Resumable-from-disk downloads: `Downloader`/`AsyncDownloader`."""

from __future__ import annotations

import os
import time
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, TypeAlias

import msgspec
from filelock import FileLock, Timeout

from mxhttp.ratelimit import gate_async, gate_sync
from mxhttp.response import ResumeLostError, apply_streaming_response_handler, resume_headers
from mxhttp.retry import (
    extract_response,
    is_retryable_exception,
    resolve_delay,
    retryable_exceptions,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    import anyio
    import httpx
    from _typeshed import StrPath

    from mxhttp.consumer import AsyncConsumer, SyncConsumer
    from mxhttp.ratelimit import RateLimit
    from mxhttp.request import RequestSpec
    from mxhttp.retry import Retry

ProgressCallback: TypeAlias = "Callable[[int, int | None], None]"


def _notify(
    callback: ProgressCallback | None, received: int, total: int | None
) -> None:
    """Invokes the synchronous progress callback if provided."""
    if callback is not None:
        callback(received, total)


class DownloadIdentityError(Exception):
    """An existing `.part` file belongs to a different resource than the one being downloaded."""


class DownloadLockError(Exception):
    """Another process or task is actively downloading to the target path."""


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

    def __call__(
        self,
        path: StrPath,
        *,
        overwrite: bool = False,
        on_progress: ProgressCallback | None = None,
    ) -> Path:
        """Downloads (or resumes) to `path`, returning it once the download completes cleanly.

        Raises:
            DownloadIdentityError: If a `.part` file exists for a different resource. Pass
                `overwrite=True` to discard it and start over.
            ResumeLostError: If a reconnect gets a full response instead of a partial one.
        """
        target = Path(path)
        part_path, state_path, lock_path = part_paths(target)
        try:
            with FileLock(lock_path, timeout=0, fallback_to_soft=False):
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
                                    raise ResumeLostError(
                                        "server ignored Range or the resource changed"
                                    )
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
                        except retryable_exceptions(self.retry.on) as exc:
                            if not is_retryable_exception(exc, self.retry):
                                raise
                            attempt += 1
                            if attempt >= self.retry.attempts:
                                raise
                            time.sleep(
                                resolve_delay(self.retry, attempt, extract_response(exc))
                            )

                part_path.replace(target)
                state_path.unlink(missing_ok=True)
                return target
        except Timeout as exc:
            raise DownloadLockError(
                f"Download to {target} is locked by another process"
            ) from exc


class AsyncDownloader(msgspec.Struct, frozen=True):
    """Bound, not-yet-executed resumable download, returned by a `-> AsyncDownloader` endpoint."""

    consumer: AsyncConsumer
    spec: RequestSpec
    retry: Retry
    ratelimit: RateLimit | None = None

    async def __call__(
        self,
        path: StrPath,
        *,
        overwrite: bool = False,
        on_progress: ProgressCallback | None = None,
    ) -> Path:
        """Downloads (or resumes) to `path`, returning it once the download completes cleanly.

        Raises:
            DownloadIdentityError: If a `.part` file exists for a different resource. Pass
                `overwrite=True` to discard it and start over.
            ResumeLostError: If a reconnect gets a full response instead of a partial one.
        """
        import asyncio

        import anyio
        from filelock import AsyncFileLock

        target = anyio.Path(path)
        part_path = target.with_name(target.name + ".part")
        state_path = target.with_name(target.name + ".part.json")
        lock_path = target.with_name(target.name + ".part.lock")
        try:
            async with AsyncFileLock(lock_path, timeout=0, fallback_to_soft=False):
                received, validator = await load_resume_state_async(
                    part_path, state_path, self.spec.url, overwrite=overwrite
                )

                attempt = 0
                async with await part_path.open("ab" if received else "wb") as fh:
                    while True:
                        await gate_async(self.consumer, self.ratelimit)
                        kwargs = self.spec.to_kwargs()
                        kwargs["headers"] = (
                            resume_headers(self.spec, received, validator) or None
                        )
                        try:
                            async with self.consumer.session.stream(
                                self.spec.method, self.spec.url, **kwargs
                            ) as response:
                                apply_streaming_response_handler(self.consumer, response)
                                if (
                                    received
                                    and response.status_code != HTTPStatus.PARTIAL_CONTENT
                                ):
                                    raise ResumeLostError(
                                        "server ignored Range or the resource changed"
                                    )
                                if validator is None:
                                    validator = await write_state_async(
                                        state_path, self.spec.url, response
                                    )
                                total = extract_total_size(response, received)
                                _notify(on_progress, received, total)
                                async for chunk in response.aiter_bytes():
                                    await fh.write(chunk)
                                    received += len(chunk)
                                    _notify(on_progress, received, total)
                            await fh.flush()
                            break
                        except retryable_exceptions(self.retry.on) as exc:
                            if not is_retryable_exception(exc, self.retry):
                                raise
                            attempt += 1
                            if attempt >= self.retry.attempts:
                                raise
                            await fh.flush()
                            await asyncio.sleep(
                                resolve_delay(self.retry, attempt, extract_response(exc))
                            )

                await part_path.replace(target)
                await state_path.unlink(missing_ok=True)
                return Path(path)
        except Timeout as exc:
            raise DownloadLockError(
                f"Download to {target} is locked by another process"
            ) from exc
