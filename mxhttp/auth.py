"""Header-based authentication for `httpx`."""

from __future__ import annotations

from typing import TYPE_CHECKING

import httpx
from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Generator


class BearerAuth(httpx.Auth):
    """Attaches `Authorization: Bearer <token>` to every request."""

    def __init__(self, token: str) -> None:
        """Stores the token to attach to every request."""
        self.token = token

    @override
    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        """Adds the `Authorization` header before the request is sent."""
        request.headers["Authorization"] = f"Bearer {self.token}"
        yield request


class ApiKeyAuth(httpx.Auth):
    """Attaches an API key header, such as `X-API-Key`, to every request."""

    def __init__(self, key: str, header: str = "X-API-Key") -> None:
        """Stores the header name and key to attach to every request."""
        self.key = key
        self.header = header

    @override
    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        """Adds the API key header before the request is sent."""
        request.headers[self.header] = self.key
        yield request
