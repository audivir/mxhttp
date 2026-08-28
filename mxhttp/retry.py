"""Retries failed HTTP requests with exponential backoff."""

from __future__ import annotations

import email.utils
import random
import time
from datetime import datetime, timezone
from http import HTTPStatus
from typing import TYPE_CHECKING, TypeAlias

import msgspec

if TYPE_CHECKING:
    from collections.abc import Callable, Collection

    import httpx

    from mxhttp.consumer import AsyncConsumer, SyncConsumer
    from mxhttp.request import RequestSpec
    from mxhttp.types import AnyC_T

RetryOn: TypeAlias = "int | type[BaseException] | Callable[[httpx.Response], bool]"

DEFAULT_ON_STATUSES = frozenset(
    {
        HTTPStatus.TOO_MANY_REQUESTS,
        HTTPStatus.INTERNAL_SERVER_ERROR,
        HTTPStatus.BAD_GATEWAY,
        HTTPStatus.SERVICE_UNAVAILABLE,
        HTTPStatus.GATEWAY_TIMEOUT,
    }
)


def build_default_on() -> tuple[RetryOn, ...]:
    """Builds the default retry conditions: transient-failure statuses and transport errors."""
    import httpx

    return (*DEFAULT_ON_STATUSES, httpx.TransportError)


def exception_types(on: Collection[RetryOn]) -> tuple[type[BaseException], ...]:
    """Extracts the exception-type entries from `on`, usable as an `except` clause tuple."""
    return tuple(entry for entry in on if isinstance(entry, type))


def retryable_exceptions(on: Collection[RetryOn]) -> tuple[type[BaseException], ...]:
    """Returns all exception types to catch, including `HTTPStatusError` for status-code checks."""
    import httpx

    return (httpx.HTTPStatusError, *exception_types(on))


def matches_response(on: Collection[RetryOn], response: httpx.Response) -> bool:
    """Checks whether `response` satisfies a status-code or predicate entry in `on`."""
    for entry in on:
        if isinstance(entry, type):
            continue
        if isinstance(entry, int):
            if response.status_code == entry:
                return True
        elif entry(response):
            return True
    return False


class Retry(msgspec.Struct, frozen=True):
    """Configures automatic retries with exponential backoff for a consumer class."""

    attempts: int = 3
    on: Collection[RetryOn] = msgspec.field(default_factory=build_default_on)
    """Status codes, exception types, or response predicates that trigger a retry."""
    backoff: float = 1.0
    exponent: float = 2.0
    jitter: bool = True
    max_delay: float = 30.0
    respect_retry_after: bool = True
    """Uses the response `Retry-After` header, when present, as a floor for the computed backoff."""
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


def parse_retry_after(value: str) -> float | None:
    """Parses a `Retry-After` header value as a delay in seconds, from seconds or an HTTP-date."""
    cleaned = value.strip()
    if cleaned.isdigit():
        return float(cleaned)
    try:
        target = email.utils.parsedate_to_datetime(cleaned)
    except (TypeError, ValueError):
        return None
    if target.tzinfo is None:
        target = target.replace(tzinfo=timezone.utc)
    return (target - datetime.now(timezone.utc)).total_seconds()


def is_retryable_exception(exc: BaseException, config: Retry) -> bool:
    """Checks whether an exception or its enclosed response matches the retry config."""
    import httpx

    if isinstance(exc, httpx.HTTPStatusError):
        return matches_response(config.on, exc.response)
    return isinstance(exc, exception_types(config.on))


def extract_response(exc: BaseException) -> httpx.Response | None:
    """Extracts the underlying HTTP response from an exception if present."""
    import httpx

    return exc.response if isinstance(exc, httpx.HTTPStatusError) else None


def resolve_delay(config: Retry, attempt: int, response: httpx.Response | None) -> float:
    """Computes the sleep duration before the next attempt, honoring `Retry-After` when present."""
    computed = config.delay(attempt)
    if not config.respect_retry_after or response is None:
        return computed
    header = response.headers.get("Retry-After")
    if header is None:
        return computed
    header_delay = parse_retry_after(header)
    if header_delay is None or header_delay < 0:
        return computed
    return min(max(computed, header_delay), config.max_delay)


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
        except exception_types(config.on):
            attempt += 1
            if attempt >= config.attempts:
                raise
            time.sleep(resolve_delay(config, attempt, None))
            continue
        attempt += 1
        if not matches_response(config.on, response) or attempt >= config.attempts:
            return response
        time.sleep(resolve_delay(config, attempt, response))


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
        except exception_types(config.on):
            attempt += 1
            if attempt >= config.attempts:
                raise
            await asyncio.sleep(resolve_delay(config, attempt, None))
            continue
        attempt += 1
        if not matches_response(config.on, response) or attempt >= config.attempts:
            return response
        await asyncio.sleep(resolve_delay(config, attempt, response))
