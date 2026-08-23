"""Base classes for the declarative HTTP consumers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import Self, override

if TYPE_CHECKING:
    from types import TracebackType

    import httpx

    from mxhttp.types import ResponseHandler


class BaseConsumer:
    """Base class for the sync and async declarative API client."""

    _response_handler: ResponseHandler | None = None
    _streaming_response_handler: ResponseHandler | None = None

    def __init__(self, base_url: str, *, use_async: bool = False) -> None:
        """Initializes the client bound to `base_url` with an empty header dict."""
        import httpx

        self.base_url = base_url.rstrip("/")
        self._session = (
            httpx.AsyncClient(base_url=self.base_url)
            if use_async
            else httpx.Client(base_url=self.base_url)
        )

    @override
    def __repr__(self) -> str:  # pragma: no cover
        return f"{type(self).__name__}<{self.base_url}>"

    @property
    def session(self) -> httpx.Client | httpx.AsyncClient:
        """The HTTP client used for outbound requests."""
        return self._session


class SyncConsumer(BaseConsumer):
    """Base class for the synchronous declarative API client."""

    def __init__(self, base_url: str) -> None:
        """Initializes the client bound to `base_url` with an empty header dict."""
        super().__init__(base_url, use_async=False)

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

    def __init__(self, base_url: str) -> None:
        """Initializes the client bound to `base_url` with an empty header dict."""
        super().__init__(base_url, use_async=True)

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
