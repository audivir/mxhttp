"""Retries failed HTTP requests with exponential backoff."""

from __future__ import annotations

import random
import time
from http import HTTPStatus
from typing import TYPE_CHECKING

import msgspec

if TYPE_CHECKING:
    from collections.abc import Callable, Collection

    import httpx

    from mxhttp.consumer import AsyncConsumer, SyncConsumer
    from mxhttp.request import RequestSpec
    from mxhttp.types import AnyC_T

DEFAULT_STATUSES = frozenset(
    {
        HTTPStatus.TOO_MANY_REQUESTS,
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.BAD_GATEWAY,
        HTTPStatus.SERVICE_UNAVAILABLE,
        HTTPStatus.GATEWAY_TIMEOUT,
    }
)


def build_default_exceptions() -> tuple[type[Exception], ...]:
    """Builds the default exceptions to catch."""
    import httpx

    return (httpx.TransportError,)


class Retry(msgspec.Struct, frozen=True):
    """Configures automatic retries with exponential backoff for a consumer class."""

    attempts: int = 3
    statuses: Collection[int] = DEFAULT_STATUSES
    exceptions: Collection[type[Exception]] = msgspec.field(
        default_factory=build_default_exceptions
    )
    backoff: float = 1.0
    exponent: float = 2.0
    jitter: bool = True
    max_delay: float = 30.0
    timeout: float | httpx.Timeout | None = None

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("attempts must be >= 1")

    def delay(self, attempt: int) -> float:
        """Computes the sleep duration in seconds before the given (1-indexed) retry."""
        raw = self.backoff * (self.exponent ** (attempt - 1))
        if self.jitter:
            raw *= 1 + random.random()  # noqa: S311
        return min(raw, self.max_delay)


def retry(config: Retry) -> Callable[[type[AnyC_T]], type[AnyC_T]]:
    """Class decorator that retries failed requests according to `config`."""

    def decorate(cls: type[AnyC_T]) -> type[AnyC_T]:
        cls._retry = config
        return cls

    return decorate


def send_sync(self: SyncConsumer, spec: RequestSpec, config: Retry) -> httpx.Response:
    """Sends `spec` once, applying `config.timeout` if one is set."""
    if config.timeout is None:
        return self.session.request(spec.method, spec.url, **spec.to_kwargs())
    return self.session.request(spec.method, spec.url, timeout=config.timeout, **spec.to_kwargs())


async def send_async(self: AsyncConsumer, spec: RequestSpec, config: Retry) -> httpx.Response:
    """Sends `spec` once, applying `config.timeout` if one is set."""
    if config.timeout is None:
        return await self.session.request(spec.method, spec.url, **spec.to_kwargs())
    return await self.session.request(
        spec.method, spec.url, timeout=config.timeout, **spec.to_kwargs()
    )


def request_sync(self: SyncConsumer, spec: RequestSpec, config: Retry | None) -> httpx.Response:
    """Sends `spec`, retrying according to `config` if one is given."""
    if config is None:
        return self.session.request(spec.method, spec.url, **spec.to_kwargs())

    attempt = 0
    while True:
        try:
            response = send_sync(self, spec, config)
        except tuple(config.exceptions):
            attempt += 1
            if attempt >= config.attempts:
                raise
            time.sleep(config.delay(attempt))
            continue
        attempt += 1
        if response.status_code not in config.statuses or attempt >= config.attempts:
            return response
        time.sleep(config.delay(attempt))


async def request_async(
    self: AsyncConsumer, spec: RequestSpec, config: Retry | None
) -> httpx.Response:
    """Sends `spec`, retrying according to `config` if one is given."""
    import asyncio

    if config is None:
        return await self.session.request(spec.method, spec.url, **spec.to_kwargs())

    attempt = 0
    while True:
        try:
            response = await send_async(self, spec, config)
        except tuple(config.exceptions):
            attempt += 1
            if attempt >= config.attempts:
                raise
            await asyncio.sleep(config.delay(attempt))
            continue
        attempt += 1
        if response.status_code not in config.statuses or attempt >= config.attempts:
            return response
        await asyncio.sleep(config.delay(attempt))
