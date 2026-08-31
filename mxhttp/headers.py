"""Class-level default headers, merged into every request of a consumer."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeAlias

from mxhttp.markers import Header
from mxhttp.request import scalar_str

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from mxhttp.consumer import BaseConsumer
    from mxhttp.types import AnyC_T

HeaderValue: TypeAlias = str | int | float | bool
HeadersInput: TypeAlias = (
    "Mapping[str, HeaderValue | None] | Callable[[BaseConsumer], Mapping[str, HeaderValue | None]]"
)


def headers(config: HeadersInput) -> Callable[[type[AnyC_T]], type[AnyC_T]]:
    """Class decorator setting default headers merged into every request."""
    if not callable(config):
        for key in config:
            if key.lower() in Header.RESERVED_WIRE_NAMES:
                raise TypeError(
                    f"@headers cannot set reserved key {key!r}: "
                    f"{Header.RESERVED_WIRE_HINTS[key.lower()]}"
                )

    def decorate(cls: type[AnyC_T]) -> type[AnyC_T]:
        resolved_config = staticmethod(config) if callable(config) else config
        cls._class_endpoint_kwargs = {**cls._class_endpoint_kwargs, "headers": resolved_config}
        return cls

    return decorate


def resolve_headers(self: BaseConsumer, config: HeadersInput | None) -> dict[str, str]:
    """Resolves a headers config, static or computed, into a plain string-keyed dict."""
    if config is None:
        return {}
    resolved = config(self) if callable(config) else config
    out: dict[str, str] = {}
    for key, value in resolved.items():
        if value is None:  # None omits the key, matching every other header/query/field/cookie
            continue
        if key.lower() in Header.RESERVED_WIRE_NAMES:
            hint = Header.RESERVED_WIRE_HINTS[key.lower()]
            raise ValueError(f"@headers cannot set reserved key {key!r}: {hint}")
        out[key] = scalar_str(value)
    return out
