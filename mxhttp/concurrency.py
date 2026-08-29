"""Limits simultaneous in-flight requests per consumer or endpoint."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, TypeAlias

import msgspec

from mxhttp.parse import host_port

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractAsyncContextManager, AbstractContextManager
    from types import TracebackType

    import anyio

    from mxhttp.consumer import BaseConsumer
    from mxhttp.types import AnyC_T


class ConcurrencyExceededError(Exception):
    """Raised when a non-blocking concurrency limit is exceeded."""


class ConcurrencyTimeoutError(Exception):
    """Raised when waiting for a concurrency slot exceeds the timeout."""


class Concurrency(msgspec.Struct, frozen=True):
    """Configures maximum concurrent in-flight requests."""

    limit: int
    timeout: float | None = None
    block: bool = True
    key: str | None = None

    def __post_init__(self) -> None:
        if self.limit < 1:
            raise ValueError("limit must be >= 1")
        if self.timeout is not None and self.timeout < 0:
            raise ValueError("timeout must be >= 0")


def concurrency(
    config: int | Concurrency,
) -> Callable[[type[AnyC_T]], type[AnyC_T]]:
    """Class decorator setting maximum concurrent in-flight requests."""
    resolved = Concurrency(limit=config) if isinstance(config, int) else config

    def decorate(cls: type[AnyC_T]) -> type[AnyC_T]:
        cls._concurrency = resolved
        return cls

    return decorate


SemaphoreKey: TypeAlias = tuple[str, int, str | None, int]

sync_semaphores: dict[SemaphoreKey, threading.BoundedSemaphore] = {}
sync_semaphores_guard = threading.Lock()

async_semaphores: dict[SemaphoreKey, anyio.Semaphore] = {}


def get_sync_semaphore(host: str, port: int, config: Concurrency) -> threading.BoundedSemaphore:
    """Retrieves or creates the sync semaphore for the given host and concurrency config."""
    key: SemaphoreKey = (host, port, config.key, config.limit)
    with sync_semaphores_guard:
        sem = sync_semaphores.get(key)
        if sem is None:
            sem = threading.BoundedSemaphore(config.limit)
            sync_semaphores[key] = sem
        return sem


def get_async_semaphore(host: str, port: int, config: Concurrency) -> anyio.Semaphore:
    """Retrieves or creates the async semaphore for the given host and concurrency config."""
    import anyio

    key: SemaphoreKey = (host, port, config.key, config.limit)
    sem = async_semaphores.get(key)
    if sem is None:
        sem = anyio.Semaphore(config.limit)
        async_semaphores[key] = sem
    return sem


class SyncConcurrencyContext:
    """Context manager acquiring and releasing a sync concurrency semaphore."""

    def __init__(self, sem: threading.BoundedSemaphore, config: Concurrency) -> None:
        """Stores the semaphore and concurrency configuration."""
        self.sem = sem
        self.config = config

    def __enter__(self) -> None:
        if not self.config.block:
            acquired = self.sem.acquire(blocking=False)
            if not acquired:
                raise ConcurrencyExceededError(f"concurrency limit of {self.config.limit} reached")
            return
        if self.config.timeout is not None:
            acquired = self.sem.acquire(timeout=self.config.timeout)
            if not acquired:
                raise ConcurrencyTimeoutError(
                    f"timed out waiting for concurrency slot after {self.config.timeout}s"
                )
            return
        self.sem.acquire()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.sem.release()


class AsyncConcurrencyContext:
    """Context manager acquiring and releasing an async concurrency semaphore."""

    def __init__(self, sem: anyio.Semaphore, config: Concurrency) -> None:
        """Stores the semaphore and concurrency configuration."""
        self.sem = sem
        self.config = config

    async def __aenter__(self) -> None:
        import anyio

        if not self.config.block:
            try:
                self.sem.acquire_nowait()
            except anyio.WouldBlock as err:
                raise ConcurrencyExceededError(
                    f"concurrency limit of {self.config.limit} reached"
                ) from err
            return
        if self.config.timeout is not None:
            try:
                with anyio.fail_after(self.config.timeout):
                    await self.sem.acquire()
            except TimeoutError as err:
                raise ConcurrencyTimeoutError(
                    f"timed out waiting for concurrency slot after {self.config.timeout}s"
                ) from err
            return
        await self.sem.acquire()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.sem.release()


class NoopSyncContext:
    """No-op context manager when concurrency is not configured."""

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass


class NoopAsyncContext:
    """No-op async context manager when concurrency is not configured."""

    async def __aenter__(self) -> None:
        pass

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass


NOOP_SYNC = NoopSyncContext()
NOOP_ASYNC = NoopAsyncContext()


def gate_concurrency_sync(
    self: BaseConsumer, config: Concurrency | None
) -> AbstractContextManager[None]:
    """Applies concurrency limits to the current sync call."""
    if config is None:
        return NOOP_SYNC
    host, port = host_port(self.base_url or "")
    return SyncConcurrencyContext(get_sync_semaphore(host, port, config), config)


def gate_concurrency_async(
    self: BaseConsumer, config: Concurrency | None
) -> AbstractAsyncContextManager[None]:
    """Applies concurrency limits to the current async call."""
    if config is None:
        return NOOP_ASYNC
    host, port = host_port(self.base_url or "")
    return AsyncConcurrencyContext(get_async_semaphore(host, port, config), config)
