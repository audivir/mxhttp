"""Resumable-from-disk downloads: `Downloader`/`AsyncDownloader`."""

from __future__ import annotations

import os
import time
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from mxhttp.ratelimit import gate_async, gate_sync
from mxhttp.response import ResumeLostError, apply_streaming_response_handler, resume_headers
from mxhttp.retry import exception_types, resolve_delay

if TYPE_CHECKING:
    import anyio
    import httpx
    from _typeshed import StrPath

    from mxhttp.consumer import AsyncConsumer, SyncConsumer
    from mxhttp.ratelimit import RateLimit
    from mxhttp.request import RequestSpec
    from mxhttp.retry import Retry


class DownloadIdentityError(Exception):
    """An existing `.part` file belongs to a different resource than the one being downloaded."""


class DownloadState(msgspec.Struct):
    """Stores the resource identity a `.part` file was downloaded from, for resume validation."""

    url: str
    etag: str | None
    last_modified: str | None


def part_paths(path: Path) -> tuple[Path, Path]:
    """Returns the staging file and sidecar metadata paths for a download at `path`."""
    return path.with_name(path.name + ".part"), path.with_name(path.name + ".part.json")


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

    def __call__(self, path: StrPath, *, overwrite: bool = False) -> Path:
        """Downloads (or resumes) to `path`, returning it once the download completes cleanly.

        Raises:
            DownloadIdentityError: If a `.part` file exists for a different resource. Pass
                `overwrite=True` to discard it and start over.
            ResumeLostError: If a reconnect gets a full response instead of a partial one.
        """
        target = Path(path)
        part_path, state_path = part_paths(target)
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
                        for chunk in response.iter_bytes():
                            fh.write(chunk)
                            fh.flush()
                            os.fsync(fh.fileno())
                            received += len(chunk)
                    break
                except exception_types(self.retry.on):
                    attempt += 1
                    if attempt >= self.retry.attempts:
                        raise
                    time.sleep(resolve_delay(self.retry, attempt, None))

        part_path.replace(target)
        state_path.unlink(missing_ok=True)
        return target


class AsyncDownloader(msgspec.Struct, frozen=True):
    """Bound, not-yet-executed resumable download, returned by a `-> AsyncDownloader` endpoint."""

    consumer: AsyncConsumer
    spec: RequestSpec
    retry: Retry
    ratelimit: RateLimit | None = None

    async def __call__(self, path: StrPath, *, overwrite: bool = False) -> Path:
        """Downloads (or resumes) to `path`, returning it once the download completes cleanly.

        Raises:
            DownloadIdentityError: If a `.part` file exists for a different resource. Pass
                `overwrite=True` to discard it and start over.
            ResumeLostError: If a reconnect gets a full response instead of a partial one.
        """
        import asyncio

        import anyio

        target = anyio.Path(path)
        part_path = target.with_name(target.name + ".part")
        state_path = target.with_name(target.name + ".part.json")
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
                            validator = await write_state_async(
                                state_path, self.spec.url, response
                            )
                        async for chunk in response.aiter_bytes():
                            await fh.write(chunk)
                            received += len(chunk)
                    await fh.flush()
                    break
                except exception_types(self.retry.on):
                    attempt += 1
                    if attempt >= self.retry.attempts:
                        raise
                    await fh.flush()
                    await asyncio.sleep(resolve_delay(self.retry, attempt, None))

        await part_path.replace(target)
        await state_path.unlink(missing_ok=True)
        return Path(path)
