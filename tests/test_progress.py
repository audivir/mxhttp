"""Tests for the progress module."""

from __future__ import annotations

import asyncio
import io
import itertools
import threading
import time
from typing import TYPE_CHECKING

import httpx
import pytest
from conftest import make_consumer

from mxhttp import Downloader, SyncConsumer, TqdmProgress, base_url, get

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.anyio


def test_tqdm_progress_unstarted_close() -> None:
    progress = TqdmProgress()
    assert progress.progress_bar is None
    progress.close()
    assert progress.progress_bar is None


def test_tqdm_progress_lifecycle() -> None:
    buf = io.StringIO()
    progress = TqdmProgress(desc="Downloading asset", file=buf)
    progress(current=0, total=1000)
    pb = progress.progress_bar
    assert pb is not None
    assert pb.n == 0
    assert pb.total == 1000

    progress(current=400, total=1000)
    assert pb.n == 400

    progress(current=400, total=1000)
    assert pb.n == 400

    progress(current=1000, total=1000)
    assert progress.progress_bar is None
    assert "Downloading asset" in buf.getvalue()


def test_tqdm_progress_late_total_discovery() -> None:
    buf = io.StringIO()
    progress = TqdmProgress(desc="Dynamic stream", file=buf)
    progress(current=100, total=None)
    assert progress.progress_bar is not None

    progress(current=250, total=500)
    pb = progress.progress_bar
    assert pb is not None
    assert pb.total == 500
    assert pb.n == 250

    progress.close()
    assert progress.progress_bar is None


def test_tqdm_progress_context_manager() -> None:
    buf = io.StringIO()
    with TqdmProgress(desc="Managed context", file=buf) as progress:
        progress(current=10, total=100)
        assert progress.progress_bar is not None

    assert progress.progress_bar is None


@base_url("https://api.example.com")
class DownloadProgressApi(SyncConsumer):
    @get("/files/{file_id}")
    def download(self, file_id: int) -> Downloader: ...  # type: ignore[empty-body]


def test_tqdm_progress_integration_with_downloader(tmp_path: Path) -> None:
    def handler(unused_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"hello world")

    client = make_consumer(DownloadProgressApi, handler)
    dl = client.download(file_id=1)
    dest = tmp_path / "out.txt"

    buf = io.StringIO()
    progress = TqdmProgress(desc="Downloading file", file=buf)
    dl(dest, on_progress=progress)

    assert dest.read_bytes() == b"hello world"
    assert progress.progress_bar is None


def test_monotonic_progress_single_thread_sync() -> None:
    buf = io.StringIO()
    progress = TqdmProgress(desc="Mono sync", file=buf)
    history: list[int] = []

    chunks = [50, 150, 200, 300, 300]
    current = 0
    total = sum(chunks)

    progress(0, total)
    history.append(0)

    for chunk in chunks:
        current += chunk
        progress(current, total)
        history.append(current)

    assert all(a <= b for a, b in itertools.pairwise(history))
    assert history[-1] == total
    assert progress.progress_bar is None


async def test_monotonic_progress_async_multi_parts() -> None:
    buf = io.StringIO()
    progress = TqdmProgress(desc="Async multi-part", file=buf)
    num_parts = 4
    chunks_per_part = [50, 50, 50, 50, 50]
    total_per_part = sum(chunks_per_part)
    total_size = num_parts * total_per_part

    received_per_part = [0] * num_parts
    history: list[int] = []

    progress(0, total_size)
    history.append(0)

    async def download_part(part_id: int) -> None:
        for chunk in chunks_per_part:
            await asyncio.sleep(0.001 * ((part_id + 1) % 3))
            received_per_part[part_id] += chunk
            current = sum(received_per_part)
            progress(current, total_size)
            history.append(current)

    await asyncio.gather(*(download_part(i) for i in range(num_parts)))

    assert all(a <= b for a, b in itertools.pairwise(history))
    assert history[-1] == total_size
    assert progress.progress_bar is None


def test_monotonic_progress_multi_threaded_sync_multi_parts() -> None:
    buf = io.StringIO()
    progress = TqdmProgress(desc="Multi-thread sync", file=buf)
    num_parts = 4
    chunks_per_part = [50, 50, 50, 50, 50]
    total_per_part = sum(chunks_per_part)
    total_size = num_parts * total_per_part

    received_per_part = [0] * num_parts
    lock = threading.Lock()
    history: list[int] = []

    progress(0, total_size)
    history.append(0)

    def worker(part_id: int) -> None:
        for chunk in chunks_per_part:
            time.sleep(0.001 * ((part_id + 1) % 3))
            with lock:
                received_per_part[part_id] += chunk
                current = sum(received_per_part)
                history.append(current)
            progress(current, total_size)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_parts)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=2.0)

    assert all(a <= b for a, b in itertools.pairwise(history))
    assert history[-1] == total_size
    assert progress.progress_bar is None


def test_tqdm_progress_per_part_disabled_delegates_to_call() -> None:
    buf = io.StringIO()
    progress = TqdmProgress(desc="Single stream", file=buf, per_part=False)
    progress.update_part(
        part_idx=0,
        part_received=50,
        part_total=100,
        current_total=50,
        total_size=200,
    )
    assert progress.progress_bar is not None
    assert progress.progress_bar.n == 50
    assert len(progress.part_progress_bars) == 0

    progress.update_part(
        part_idx=0,
        part_received=100,
        part_total=100,
        current_total=200,
        total_size=200,
    )
    assert progress.progress_bar is None


def test_tqdm_progress_per_part_multi_bar_lifecycle() -> None:
    buf = io.StringIO()
    progress = TqdmProgress(desc="Multi part", file=buf, per_part=True)
    assert len(progress.part_progress_bars) == 0

    progress.update_part(
        part_idx=0,
        part_received=0,
        part_total=100,
        current_total=0,
        total_size=200,
    )
    pb0 = progress.progress_bar
    assert pb0 is not None
    assert pb0.n == 0
    assert 0 in progress.part_progress_bars
    assert progress.part_progress_bars[0].n == 0

    progress.update_part(
        part_idx=1,
        part_received=0,
        part_total=100,
        current_total=0,
        total_size=200,
    )
    assert 1 in progress.part_progress_bars

    progress.update_part(
        part_idx=0,
        part_received=50,
        part_total=100,
        current_total=50,
        total_size=200,
    )
    progress.update_part(
        part_idx=0,
        part_received=50,
        part_total=100,
        current_total=50,
        total_size=200,
    )
    pb1 = progress.progress_bar
    assert pb1 is not None
    assert pb1.n == 50
    assert progress.part_progress_bars[0].n == 50

    progress.update_part(
        part_idx=0,
        part_received=100,
        part_total=100,
        current_total=100,
        total_size=200,
    )

    progress.update_part(
        part_idx=1,
        part_received=100,
        part_total=100,
        current_total=200,
        total_size=200,
    )
    assert progress.progress_bar is None
    assert len(progress.part_progress_bars) == 0


def test_tqdm_progress_per_part_late_total_and_context_manager() -> None:
    buf = io.StringIO()
    with TqdmProgress(desc="Context multi", file=buf, per_part=True) as progress:
        progress.update_part(
            part_idx=0,
            part_received=10,
            part_total=50,
            current_total=10,
            total_size=None,
        )
        pb = progress.progress_bar
        assert pb is not None
        assert pb.total is None

        progress.update_part(
            part_idx=0,
            part_received=20,
            part_total=50,
            current_total=20,
            total_size=100,
        )
        pb2 = progress.progress_bar
        assert pb2 is not None
        assert pb2.total == 100

    assert progress.progress_bar is None
    assert len(progress.part_progress_bars) == 0
