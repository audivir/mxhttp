"""Limits how often a consumer class calls a given host."""

from __future__ import annotations

import asyncio
import threading
import time
from typing import TYPE_CHECKING, TypeAlias

import msgspec

from mxhttp.parse import host_port

if TYPE_CHECKING:
    from collections.abc import Callable

    from mxhttp.types import AnyC_T


class RateLimitExceededError(Exception):
    """The call exceeded the rate limit without blocking, or the wait exceeded `max_delay`."""


class RateLimit(msgspec.Struct, frozen=True):
    """Configures a maximum call rate, shared across consumers of the same host, for a class."""

    calls: int
    period: float
    block: bool = True
    max_delay: float | None = None
    """Raises `RateLimitExceededError` instead of sleeping past this many seconds."""
    key: str | None = None

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


WindowKey: TypeAlias = tuple[str, int, str | None, int, float]

windows: dict[WindowKey, Window] = {}
windows_guard = threading.Lock()


def get_window(host: str, port: int, config: RateLimit) -> Window:
    """Returns the shared rate-limit window for `(host, port, key, calls, period)`."""
    key: WindowKey = (host, port, config.key, config.calls, config.period)
    with windows_guard:
        window = windows.get(key)
        if window is None:
            window = Window()
            windows[key] = window
        return window


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
        cls._class_endpoint_kwargs = {**cls._class_endpoint_kwargs, "ratelimit": config}
        return cls

    return decorate


def acquire_sync(config: RateLimit, base_url: str | None) -> None:
    """Blocks, or raises `RateLimitExceededError`, until a call slot for the host is free."""
    host, port = host_port(base_url or "")
    delay = wait_time(get_window(host, port, config), config)
    if delay <= 0:
        return
    if not config.block:
        raise RateLimitExceededError(f"rate limit exceeded, retry after {delay:.2f}s")
    if config.max_delay is not None and delay > config.max_delay:
        raise RateLimitExceededError(f"rate limit wait of {delay:.2f}s exceeds max_delay")
    time.sleep(delay)


async def acquire_async(config: RateLimit, base_url: str | None) -> None:
    """Blocks, or raises `RateLimitExceededError`, until a call slot for the host is free."""
    host, port = host_port(base_url or "")
    delay = wait_time(get_window(host, port, config), config)
    if delay <= 0:
        return
    if not config.block:
        raise RateLimitExceededError(f"rate limit exceeded, retry after {delay:.2f}s")
    if config.max_delay is not None and delay > config.max_delay:
        raise RateLimitExceededError(f"rate limit wait of {delay:.2f}s exceeds max_delay")
    await asyncio.sleep(delay)


def gate_sync(url: str | None, config: RateLimit | None) -> None:
    """Applies `config` to the current call, keyed by the actual host `url` targets."""
    if config is not None:
        acquire_sync(config, url)


async def gate_async(url: str | None, config: RateLimit | None) -> None:
    """Applies `config` to the current call, keyed by the actual host `url` targets."""
    if config is not None:
        await acquire_async(config, url)
