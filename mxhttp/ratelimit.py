"""Limits how often a consumer class calls a given host."""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import msgspec

if TYPE_CHECKING:
    from collections.abc import Callable

    from mxhttp.consumer import BaseConsumer
    from mxhttp.types import AnyC_T


class RateLimitExceededError(Exception):
    """A call would have to wait past `RateLimit.max_delay`, or `RateLimit.block` is `False`."""


class RateLimit(msgspec.Struct, frozen=True):
    """Configures a maximum call rate, shared across consumers of the same host, for a class."""

    calls: int
    period: float
    block: bool = True
    max_delay: float | None = None
    """Raises `RateLimitExceededError` instead of sleeping past this many seconds."""

    def __post_init__(self) -> None:
        if self.calls < 1:
            raise ValueError("calls must be >= 1")
        if self.period <= 0:
            raise ValueError("period must be > 0")


class Window:
    """Stores the call count and reset time of one fixed rate-limit window."""

    __slots__ = ("count", "lock", "reset_at")

    def __init__(self) -> None:
        """Initializes an unstarted window: no calls counted, no reset time set yet."""
        self.count = 0
        self.reset_at = 0.0
        self.lock = threading.Lock()


_windows: dict[tuple[str, int], Window] = {}
_windows_guard = threading.Lock()


def get_window(host: str, port: int) -> Window:
    """Returns the shared rate-limit window for `(host, port)`, creating it on first use."""
    key = (host, port)
    with _windows_guard:
        window = _windows.get(key)
        if window is None:
            window = Window()
            _windows[key] = window
        return window


def host_port(url: str) -> tuple[str, int]:
    """Extracts the `(host, port)` pair a rate limit is scoped to, defaulting the port by scheme."""
    parts = urlsplit(url)
    port = parts.port or (443 if parts.scheme == "https" else 80)
    return parts.hostname or "", port


def wait_time(window: Window, config: RateLimit) -> float:
    """Reserves a call slot in `window`, returning the delay in seconds before it may proceed."""
    with window.lock:
        now = time.monotonic()
        if now >= window.reset_at:
            window.reset_at = now + config.period
            window.count = 0
        window.count += 1
        if window.count <= config.calls:
            return 0.0
        return window.reset_at - now


def ratelimit(config: RateLimit) -> Callable[[type[AnyC_T]], type[AnyC_T]]:
    """Class decorator that limits outbound requests to `config.calls` per `config.period`."""

    def decorate(cls: type[AnyC_T]) -> type[AnyC_T]:
        cls._ratelimit = config
        return cls

    return decorate


def acquire_sync(config: RateLimit, base_url: str) -> None:
    """Blocks, or raises `RateLimitExceededError`, until a call slot for the host is free."""
    delay = wait_time(get_window(*host_port(base_url)), config)
    if delay <= 0:
        return
    if not config.block:
        raise RateLimitExceededError(f"rate limit exceeded, retry after {delay:.2f}s")
    if config.max_delay is not None and delay > config.max_delay:
        raise RateLimitExceededError(f"rate limit wait of {delay:.2f}s exceeds max_delay")
    time.sleep(delay)


async def acquire_async(config: RateLimit, base_url: str) -> None:
    """Blocks, or raises `RateLimitExceededError`, until a call slot for the host is free."""
    import asyncio

    delay = wait_time(get_window(*host_port(base_url)), config)
    if delay <= 0:
        return
    if not config.block:
        raise RateLimitExceededError(f"rate limit exceeded, retry after {delay:.2f}s")
    if config.max_delay is not None and delay > config.max_delay:
        raise RateLimitExceededError(f"rate limit wait of {delay:.2f}s exceeds max_delay")
    await asyncio.sleep(delay)


def gate_sync(self: BaseConsumer, config: RateLimit | None) -> None:
    """Applies `config` to the current call, if a rate limit is configured."""
    if config is not None:
        acquire_sync(config, self.base_url)


async def gate_async(self: BaseConsumer, config: RateLimit | None) -> None:
    """Applies `config` to the current call, if a rate limit is configured."""
    if config is not None:
        await acquire_async(config, self.base_url)
