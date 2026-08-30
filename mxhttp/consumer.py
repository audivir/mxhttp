"""Base classes for the declarative HTTP consumers."""

from __future__ import annotations

import urllib.parse
from typing import TYPE_CHECKING

from typing_extensions import Self, override

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import TracebackType

    import httpx

    from mxhttp.concurrency import Concurrency
    from mxhttp.cookies import CookiesInput
    from mxhttp.headers import HeadersInput
    from mxhttp.ratelimit import RateLimit
    from mxhttp.retry import Retry
    from mxhttp.types import AnyC_T, ResponseHandler


def validate_scheme(url: str) -> str:
    """Ensures url has an http:// or https:// scheme."""
    cleaned = url.rstrip("/")
    scheme = urllib.parse.urlsplit(cleaned).scheme
    if scheme not in ("http", "https"):
        raise ValueError(f"base_url must start with 'http://' or 'https://', got: {url!r}")
    return cleaned


def base_url(url: str) -> Callable[[type[AnyC_T]], type[AnyC_T]]:
    """Class decorator setting the default base URL for the consumer class."""
    validated = validate_scheme(url)

    def decorate(cls: type[AnyC_T]) -> type[AnyC_T]:
        cls._base_url = validated
        return cls

    return decorate


class BaseConsumer:
    """Base class for the sync and async declarative API client."""

    _base_url: str | None = None
    _response_handler: ResponseHandler | None = None
    _streaming_response_handler: ResponseHandler | None = None
    _retry: Retry | None = None
    _ratelimit: RateLimit | None = None
    _concurrency: Concurrency | None = None
    _headers: HeadersInput | None = None
    _cookies: CookiesInput | None = None

    def __init__(
        self,
        *,
        use_async: bool = False,
        timeout: float | httpx.Timeout = 5.0,
        auth: httpx.Auth | tuple[str, str] | None = None,
        base_url: str | None = None,
    ) -> None:
        """Initializes the client.

        Args:
            use_async: Whether to use an `httpx.AsyncClient` instead of `httpx.Client`.
            timeout: Default timeout for every request.
            auth: `httpx` authentication to attach to every request.
            base_url: Sets the base URL for this instance only, overriding the class-level
                `@base_url` default (if any). Prefer `@base_url` for a URL shared by every
                instance; use this for a URL that varies per instance.
        """
        import httpx

        if base_url is not None:
            self._base_url = validate_scheme(base_url)
        self._session = (
            httpx.AsyncClient(timeout=timeout, auth=auth)
            if use_async
            else httpx.Client(timeout=timeout, auth=auth)
        )

    @override
    def __repr__(self) -> str:  # pragma: no cover
        return f"{type(self).__name__}<{self._base_url}>"

    @property
    def base_url(self) -> str | None:
        """The base URL configured for the consumer class."""
        return self._base_url

    @property
    def concurrency(self) -> Concurrency | None:
        """The concurrency configuration for the consumer class."""
        return self._concurrency

    @property
    def headers(self) -> HeadersInput | None:
        """The default headers configuration for the consumer class."""
        return self._headers

    @property
    def cookies(self) -> CookiesInput | None:
        """The default cookies configuration for the consumer class."""
        return self._cookies

    @property
    def session(self) -> httpx.Client | httpx.AsyncClient:
        """The HTTP client used for outbound requests."""
        return self._session


class SyncConsumer(BaseConsumer):
    """Base class for the synchronous declarative API client."""

    def __init__(
        self,
        *,
        timeout: float | httpx.Timeout = 5.0,
        auth: httpx.Auth | tuple[str, str] | None = None,
        base_url: str | None = None,
    ) -> None:
        """Initializes the synchronous client."""
        super().__init__(use_async=False, timeout=timeout, auth=auth, base_url=base_url)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.session.close()

    if TYPE_CHECKING:

        @override
        @property
        def session(self) -> httpx.Client:
            """The synchronous HTTP client."""
            return self._session  # type: ignore[return-value]


class AsyncConsumer(BaseConsumer):
    """Base class for the asynchronous declarative API client."""

    def __init__(
        self,
        *,
        timeout: float | httpx.Timeout = 5.0,
        auth: httpx.Auth | tuple[str, str] | None = None,
        base_url: str | None = None,
    ) -> None:
        """Initializes the asynchronous client."""
        super().__init__(use_async=True, timeout=timeout, auth=auth, base_url=base_url)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.session.aclose()

    if TYPE_CHECKING:

        @override
        @property
        def session(self) -> httpx.AsyncClient:
            """The asynchronous HTTP client."""
            return self._session  # type: ignore[return-value]
