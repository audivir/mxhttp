"""Markers for HTTP request parameters inside `Annotated[...]`."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from enum import Enum
from types import UnionType
from typing import TYPE_CHECKING, Literal, Union, get_args, get_origin

from typing_extensions import Self, override

from mxhttp.types import ParamValue, ValidPath_T

if TYPE_CHECKING:
    from inspect import Parameter


def scalar_value_types(hint: type | None) -> tuple[type, ...] | None:
    """Returns the concrete runtime types backing a `Literal[...]` or `Enum`, else `None`."""
    if get_origin(hint) is Literal:
        return tuple({type(v) for v in get_args(hint)})
    if isinstance(hint, type) and issubclass(hint, Enum):
        return tuple({type(member.value) for member in hint})
    return None


def is_valid_scalar(
    hint: type | None, allowed: type | UnionType, *, forbid_bool: bool = False
) -> bool:
    """Checks whether `hint` (a type, `Literal[...]`, or `Enum` subclass) matches `allowed`."""
    value_types = scalar_value_types(hint)
    if value_types is None:
        if not isinstance(hint, type) or get_origin(hint) is not None:
            return False
        value_types = (hint,)
    if not value_types:
        return False
    if forbid_bool and any(issubclass(t, bool) for t in value_types):
        return False
    return all(issubclass(t, allowed) for t in value_types)


def is_scalar_sequence(hint: type | None) -> bool:
    """Checks whether `hint` is a `list`, `tuple`, or `Sequence` of scalar `ParamValue` objects."""
    if get_origin(hint) not in (list, tuple, Sequence):
        return False
    item_types = [a for a in get_args(hint) if a is not Ellipsis]
    return bool(item_types) and all(is_valid_scalar(a, ParamValue) for a in item_types)


def validate_scalar_arg(
    py_name: str, scalar_type: type | None, marker_name: str, *, allow_sequence: bool = False
) -> None:
    """Verifies that `Query`, `Field`, `Header`, or `Cookie` arguments have a scalar type."""
    if allow_sequence and is_scalar_sequence(scalar_type):
        return
    if not is_valid_scalar(scalar_type, ParamValue):
        allowed = "str | int | float | bool"
        if allow_sequence:
            allowed += " | Sequence[str | int | float | bool]"
        raise TypeError(f"{marker_name} argument {py_name!r} must be {allowed}")


def is_file_content(hint: type | None) -> bool:
    """Checks whether `hint` is bytes/str/file-like, or a `Union` of those, like `FileContent`."""
    if get_origin(hint) in (Union, UnionType):
        return all(is_file_content(a) for a in get_args(hint))
    return hint in (bytes, str) or callable(getattr(hint, "read", None))


def unwrap_optional(hint: type | None) -> tuple[type | None, bool]:
    """Removes the `Optional` wrapper from a `None`-defaulted parameter annotation."""
    if get_origin(hint) not in (UnionType, Union):
        return hint, hint is None
    args = get_args(hint)
    non_none = [a for a in args if a is not type(None)]
    return non_none[0] if len(non_none) == 1 else hint, len(args) > len(non_none)


def is_optional_str(hint: type | None) -> bool:
    """Checks whether `hint` is `str` or `str | None`."""
    resolved, _ = unwrap_optional(hint)
    return resolved is str


def is_part_tuple(hint: type | None) -> bool:  # noqa: PLR0911
    """Checks whether `hint` is a multipart file part tuple shape."""
    if get_origin(hint) is not tuple:
        return False
    args = get_args(hint)
    if len(args) < 2:  # noqa: PLR2004
        return False
    filename, content, *rest = args
    if not is_optional_str(filename) or not is_file_content(content):
        return False
    match rest:
        case []:
            return True
        case [content_type]:
            return is_optional_str(content_type)
        case [content_type, headers]:
            return is_optional_str(content_type) and get_origin(headers) in (Mapping, dict)
        case _:
            return False


class Marker:
    """Base class for parameter-binding annotations inside `Annotated[...]`."""

    __slots__ = ("name",)

    def __init__(self, name: str | None = None) -> None:
        """Initializes the tracker with an optional name."""
        self.name = name

    def __class_getitem__(cls, name: str) -> Self:
        return cls(name)


class Path(Marker):
    """Binds a parameter to a URL path parameter."""

    @staticmethod
    def validate(py_name: str, path_type: type | None, is_optional: bool, param: Parameter) -> None:
        """Verifies that path arguments have the correct type."""
        if not is_valid_scalar(path_type, ValidPath_T, forbid_bool=True):
            raise TypeError(f"Path argument {py_name!r} must be str | int | float")
        if param.default is None:
            raise TypeError(f"Path argument {py_name!r} must not default to None")
        if is_optional:
            raise TypeError(f"Path argument {py_name!r} must not be optional")


class RawPath(Path):
    """Binds a `str` path parameter without percent-encoding it.

    The value is spliced into the URL as-is, so `/`, `?`, and `#` are treated as URL
    structure rather than literal text. Use only for values that are already encoded
    relative paths or query fragments.
    """

    @override
    @staticmethod
    def validate(py_name: str, path_type: type | None, is_optional: bool, param: Parameter) -> None:
        """Verifies that a `RawPath` argument is `str`."""
        if path_type is not str:
            raise TypeError(f"RawPath argument {py_name!r} must be str")
        if param.default is None:
            raise TypeError(f"RawPath argument {py_name!r} must not default to None")
        if is_optional:
            raise TypeError(f"RawPath argument {py_name!r} must not be optional")


class Query(Marker):
    """Binds a parameter to a URL query string parameter, omitting `None` values."""

    @classmethod
    def validate(
        cls, name: str, resolved_hint: type | None, *, allow_sequence: bool = True
    ) -> None:
        """Verifies that a `Query` argument is a scalar or a sequence of scalars."""
        validate_scalar_arg(name, resolved_hint, cls.__name__, allow_sequence=allow_sequence)


class Field(Marker):
    """Binds a parameter to a form field, omitting `None` values.

    Encoded as `application/x-www-form-urlencoded`, unless the same call also has a `Part`
    parameter, in which case the whole body becomes multipart instead.
    """

    @classmethod
    def validate(cls, name: str, resolved_hint: type | None) -> None:
        """Verifies that a `Field` argument is a scalar or a sequence of scalars."""
        validate_scalar_arg(name, resolved_hint, cls.__name__, allow_sequence=True)


class Part(Marker):
    """Binds a parameter to a multipart file part.

    Converts the request body to multipart format, making any `Field` parameters on the
    same call multipart fields instead of urlencoded.
    """

    @staticmethod
    def validate(py_name: str, part_type: type | None) -> None:
        """Verifies that a `Part` argument matches one of the file-upload shapes `httpx` accepts."""
        candidates = (
            get_args(part_type) if get_origin(part_type) in (Union, UnionType) else [part_type]
        )
        if candidates and all(is_file_content(c) or is_part_tuple(c) for c in candidates):
            return
        raise TypeError(
            f"Part argument {py_name!r} must be bytes | str | IO, optionally wrapped in a "
            "(filename, content), (filename, content, content_type), or "
            "(filename, content, content_type, headers) tuple"
        )


class Header(Marker):
    """Binds a parameter to an HTTP request header and omits `None` values."""

    @classmethod
    def validate(cls, name: str, resolved_hint: type | None) -> None:
        """Verifies that a `Header` argument is a scalar and not a sequence."""
        validate_scalar_arg(name, resolved_hint, cls.__name__)


class Cookie(Marker):
    """Binds a parameter to a request cookie, omitting `None` values.

    If the cookie jar already holds a cookie with this name, the jar value is sent
    instead of this parameter value, unless `override=True` is set.
    """

    __slots__ = ("override",)

    def __init__(self, name: str | None = None, *, override: bool = False) -> None:
        """Initializes the cookie binding with an optional name and jar-override flag."""
        super().__init__(name)
        self.override = override

    @classmethod
    def validate(cls, name: str, resolved_hint: type | None) -> None:
        """Verifies that a `Cookie` argument is a scalar and not a sequence."""
        validate_scalar_arg(name, resolved_hint, cls.__name__)


class Body:
    """Binds a JSON-encodable parameter as the JSON request body."""

    @staticmethod
    def validate(py_name: str, body_type: type | None) -> None:
        """Verifies that a `Body` argument is an object, not a scalar."""
        if isinstance(body_type, type) and issubclass(body_type, (*get_args(ParamValue), bytes)):
            raise TypeError(
                f"Body argument {py_name!r} must not be str | int | float | bool | bytes"
            )
