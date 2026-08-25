"""Defines models and types for the HTTP client."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from enum import Enum, auto
from typing import IO, TYPE_CHECKING, Literal, TypeAlias, TypeVar

from typing_extensions import TypeIs

if TYPE_CHECKING:
    import httpx
    import msgspec
    from _typeshed import DataclassInstance
    from attrs import AttrsInstance

    from mxhttp.consumer import AsyncConsumer, BaseConsumer, SyncConsumer  # cyclic


class Missing(Enum):
    """Sentinel value representing a missing or absent field."""

    SENTINEL = auto()


MISSING = Missing.SENTINEL
"""Sentinel value representing a missing or absent field."""

AnyC_T = TypeVar("AnyC_T", bound="BaseConsumer")
SyncC_T = TypeVar("SyncC_T", bound="SyncConsumer")
AsyncC_T = TypeVar("AsyncC_T", bound="AsyncConsumer")

JsonValue: TypeAlias = bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"] | None
TypedDictLike: TypeAlias = "Mapping[str, object]"
DataclassLike: TypeAlias = "msgspec.Struct | DataclassInstance | AttrsInstance | TypedDictLike"
JsonDecodable: TypeAlias = "JsonValue | DataclassLike"
Parsed_T = TypeVar(
    "Parsed_T",
    bound=(
        "httpx.Response | str | bytes | DataclassLike | Sequence[JsonDecodable] | Mapping[str, JsonDecodable]"  # noqa: E501
    ),
)
"""Raw response, raw content, dataclass-like structures, or JSON-decodable types."""

Param_T: TypeAlias = Literal["path", "body", "query", "field", "part", "header", "cookie"]
"""HTTP request parameter names."""

Method_T: TypeAlias = Literal["DELETE", "GET", "HEAD", "PATCH", "POST", "PUT"]
"""HTTP methods."""

SseField_T: TypeAlias = Literal["data", "event", "id", "retry"]
"""Accepted SSE line prefixes, others are ignored."""

ValidPath_T: TypeAlias = str | int | float
"""Allowed types for a path argument."""

FileContent: TypeAlias = bytes | str | IO[bytes]
"""The content of a multipart file part: in-memory bytes/text, or a file-like object."""

PartValue: TypeAlias = (
    FileContent
    | tuple[str | None, FileContent]
    | tuple[str | None, FileContent, str | None]
    | tuple[str | None, FileContent, str | None, Mapping[str, str]]
)
"""A multipart file part: bare content, or a (filename, content, content-type, headers) tuple."""

ParamValue = str | int | float | bool
"""A scalar query, field, header, or cookie value accepted by `httpx`."""

QueryValue: TypeAlias = "ParamValue | Sequence[ParamValue]"
"""A `ParamValue` scalar or a sequence of those, repeated as `key=a&key=b&...`."""

ResponseHandler: TypeAlias = "Callable[[httpx.Response], httpx.Response]"


def is_parsed_type(return_type: type[Parsed_T | None]) -> TypeIs[type[Parsed_T]]:
    """Checks whether `return_type` is not `NoneType`."""
    return return_type is not type(None)
