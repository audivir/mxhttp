"""Class-level default cookies, merged into every request of a consumer."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias

from mxhttp.request import scalar_str

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from mxhttp.consumer import BaseConsumer
    from mxhttp.types import AnyC_T

CookieValue: TypeAlias = str | int | float | bool
CookiesInput: TypeAlias = (
    "Mapping[str, CookieValue | None] | Callable[[BaseConsumer], Mapping[str, CookieValue | None]]"
)
"""A value of `None` omits that key, matching every other header/query/field/cookie."""


def cookies(config: CookiesInput) -> Callable[[type[AnyC_T]], type[AnyC_T]]:
    """Class decorator setting default cookies merged into every request."""

    def decorate(cls: type[AnyC_T]) -> type[AnyC_T]:
        # A plain callable stored as a class attribute is auto-bound as a method when read via
        # `self._cookies`, which would pass `self` twice once `resolve_cookies()` calls it.
        # `staticmethod` prevents that binding while still reading back as the plain callable.
        resolved_config = staticmethod(config) if callable(config) else config
        cls._class_endpoint_kwargs = {**cls._class_endpoint_kwargs, "cookies": resolved_config}
        return cls

    return decorate


def resolve_cookies(self: BaseConsumer, config: CookiesInput | None) -> dict[str, str]:
    """Resolves a cookies config, static or computed, into a plain string-keyed dict."""
    if config is None:
        return {}
    resolved = config(self) if callable(config) else config
    return {key: scalar_str(value) for key, value in resolved.items() if value is not None}
