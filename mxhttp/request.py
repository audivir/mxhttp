"""Builds an `httpx.Request`."""

from __future__ import annotations

import inspect
import string
import urllib.parse
from enum import Enum
from types import UnionType
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Concatenate,
    ParamSpec,
    TypedDict,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

import msgspec

from mxhttp.markers import (
    Body,
    Cookie,
    Field,
    Header,
    Marker,
    Part,
    Path,
    Query,
    RawBody,
    RawPath,
    unwrap_optional,
)
from mxhttp.response import Response
from mxhttp.types import MISSING, AnyC_T, JsonValue, Param_T, Parsed_T, PartValue, QueryValue

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Coroutine, Mapping
    from inspect import Parameter

    from mxhttp.parse import ParsedPath
P = ParamSpec("P")


class ParamPlan(msgspec.Struct):
    """Stores the binding location for a single bound parameter in a request."""

    py_name: str
    wire_name: str
    kind: Param_T
    cookie_override: bool = False
    raw_path: bool = False
    raw_body: bool = False
    raw_body_content_type: str | None = None


class RequestKwargs(TypedDict):
    """Stores keyword arguments shared by every `session.request` or `session.stream` call."""

    params: dict[str, QueryValue] | None
    headers: dict[str, str] | None
    data: dict[str, object] | None
    files: dict[str, PartValue] | None
    json: JsonValue
    content: bytes | str | None


class RequestSpec(msgspec.Struct):
    """Stores a fully resolved request, ready to pass to `httpx`."""

    method: str
    url: str
    params: dict[str, QueryValue] | None = None
    headers: dict[str, str] | None = None
    data: dict[str, object] | None = None
    files: dict[str, PartValue] | None = None
    json: JsonValue = None
    content: bytes | str | None = None

    def to_kwargs(self) -> RequestKwargs:
        """Builds the keyword arguments shared by every `session.request`/`session.stream` call."""
        return {
            "params": self.params,
            "headers": self.headers,
            "data": self.data,
            "files": self.files,
            "json": self.json,
            "content": self.content,
        }


def scalar_str(value: object) -> str:
    """Converts a scalar value to a string, using the `.value` attribute of `Enum` objects."""
    if isinstance(value, Enum):
        value = value.value
    return str(value)


def unwrap_hint(hint: type | None) -> tuple[type | None, bool, list[object]]:
    """Unwraps nested `Optional` and `Annotated` layers in any order.

    `get_type_hints` re-wraps `= None` parameters in `Optional[...]`, so the layers can nest
    in either order and repeat.

    Returns:
        Tuple of the innermost type, whether `None` was found, and all markers.
    """
    is_optional = False
    markers: list[object] = []
    while True:
        if get_origin(hint) is Annotated:
            args = get_args(hint)
            hint = args[0]
            markers.extend(args[1:])
            continue
        unwrapped, hint_is_optional = unwrap_optional(hint)
        is_optional = is_optional or hint_is_optional
        if unwrapped is hint:
            return hint, is_optional, markers
        hint = unwrapped


def resolve_inline_query(
    marker: Query, name: str, resolved_hint: type | None, inline_query_names: Mapping[str, str]
) -> tuple[str, Marker] | None:
    """Resolves an explicit `Query[...]` marker against an inline query field, if it names one.

    Returns:
        The resolved `(wire_name, marker)` pair, or `None` if the marker does not name an inline
        query field.

    Raises:
        TypeError: If the marker targets the wire name of an inline field directly instead of its
            placeholder name, which would bypass validation of the field.
    """
    lookup_name = marker.name or name
    if lookup_name in inline_query_names:
        marker.validate(name, resolved_hint, allow_sequence=False)
        return (inline_query_names[lookup_name], marker)
    if lookup_name in inline_query_names.values():
        raise TypeError(
            f"Query argument {name!r} targets wire name {lookup_name!r} directly"
            " which is reserved by an inline query"
        )
    return None


def classify(  # noqa: C901
    name: str,
    hint: type | None,
    path_parts: Collection[str],
    inline_query_names: Mapping[str, str],
    param: Parameter,
) -> tuple[str, Marker | type[Body] | RawBody]:
    """Resolves the binding of a parameter via an explicit marker or implicit path/inline query."""
    resolved_hint, is_optional, markers = unwrap_hint(hint)
    for extra in markers:  # pragma: no branch
        marker = (
            extra()
            if extra in (Path, RawPath, Query, Field, Part, Header, Cookie, RawBody)
            else extra
        )
        if isinstance(marker, Path):
            lookup_name = marker.name or name
            if lookup_name not in path_parts:
                raise TypeError(
                    f"Path argument {name!r} targets field {lookup_name!r}, but it doesn't "
                    "appear in the path template"
                )
            marker.validate(name, resolved_hint, is_optional, param)
        if isinstance(marker, Query):
            resolved = resolve_inline_query(marker, name, resolved_hint, inline_query_names)
            if resolved is not None:
                return resolved
        if isinstance(marker, (Query, Field, Header, Cookie, Part)):
            marker.validate(name, resolved_hint)
        if isinstance(marker, Marker):
            return (marker.name or name, marker)
        if isinstance(marker, RawBody):
            RawBody.validate(name, resolved_hint)
            return (name, marker)
        if extra is Body:
            Body.validate(name, resolved_hint)
            return (name, Body)
        raise TypeError(f"Unexpected extra: {extra}")
    if name in inline_query_names:
        Query.validate(name, resolved_hint, allow_sequence=False)
        return (inline_query_names[name], Query())
    if name in path_parts:
        Path.validate(name, resolved_hint, is_optional, param)
        return (name, Path())
    raise TypeError(f"Parameter {name!r} has no Query/Field/Part/Body binding or path match")


def rejects_union(return_type: type) -> bool:
    """Checks whether a bare union, or `Response[...]` wrapping one, is not decodable."""
    if get_origin(return_type) in (Union, UnionType):
        return True
    if get_origin(return_type) is Response:
        (inner_type,) = get_args(return_type)
        return get_origin(inner_type) in (Union, UnionType)
    return False


def build_plan(  # noqa: C901, PLR0912, PLR0915
    func: Callable[Concatenate[AnyC_T, P], Parsed_T]
    | Callable[Concatenate[AnyC_T, P], Coroutine[Any, Any, Parsed_T]],
    parsed: ParsedPath,
) -> tuple[list[ParamPlan], type[Parsed_T]]:
    """Builds a parameter plan and return type for a callable."""
    hints: dict[str, type] = get_type_hints(func, include_extras=True)
    return_type = hints.get("return", MISSING)
    if return_type is MISSING:
        raise ValueError("No return type annotated")
    if rejects_union(return_type):
        raise TypeError(f"Return type must not be a union: {return_type!r}")

    path_parts = {name for _, name, _, _ in string.Formatter().parse(parsed.path_only) if name}

    sig = inspect.signature(func)
    plan: list[ParamPlan] = []
    wire_names_seen: dict[Param_T, set[str]] = {
        "path": set(),
        "query": set(parsed.static_query),
        "field": set(),
        "header": set(),
        "cookie": set(),
    }
    body_group: str | None = None
    body_owner: str = ""
    body_owner_label: str = ""
    kind: Param_T
    for py_name, param in sig.parameters.items():
        if py_name == "self":
            continue
        wire_name, marker = classify(
            py_name, hints.get(py_name), path_parts, parsed.inline_query_names, param
        )
        if marker is Body or isinstance(marker, RawBody):
            kind = "body"
        elif isinstance(marker, Path):
            kind = "path"
        elif isinstance(marker, Query):
            kind = "query"
        elif isinstance(marker, Field):
            kind = "field"
        elif isinstance(marker, Header):
            kind = "header"
        elif isinstance(marker, Cookie):
            kind = "cookie"
        else:
            kind = "part"
        if kind in wire_names_seen:
            if wire_name in wire_names_seen[kind]:
                if kind == "query" and wire_name in parsed.static_query:
                    raise TypeError(
                        f"Parameter {py_name!r} reuses query wire name {wire_name!r}, already "
                        "set as a static value in the path template"
                    )
                raise TypeError(
                    f"Parameter {py_name!r} reuses {kind} wire name {wire_name!r}, already "
                    "bound by another parameter"
                )
            wire_names_seen[kind].add(wire_name)
        if kind in ("body", "field", "part"):
            group = "body" if kind == "body" else "form"
            if isinstance(marker, RawBody):
                label = "RawBody"
            elif kind == "body":
                label = "Body"
            else:
                label = kind.capitalize()
            if body_group is not None and (group != body_group or group == "body"):
                raise TypeError(
                    f"Parameter {py_name!r} ({label}) cannot be combined with parameter "
                    f"{body_owner!r} ({body_owner_label}): a request can only have one body "
                    "encoding"
                )
            if body_group is None:
                body_group, body_owner, body_owner_label = group, py_name, label
        cookie_override = marker.override if isinstance(marker, Cookie) else False
        plan.append(
            ParamPlan(
                py_name=py_name,
                wire_name=wire_name,
                kind=kind,
                cookie_override=cookie_override,
                raw_path=isinstance(marker, RawPath),
                raw_body=isinstance(marker, RawBody),
                raw_body_content_type=marker.content_type if isinstance(marker, RawBody) else None,
            )
        )
    bound_path_names = {p.wire_name for p in plan if p.kind == "path"}
    for field_name in sorted(path_parts - bound_path_names):
        raise TypeError(f"Path field {field_name!r} is not bound by any parameter")
    bound_query_names = {p.wire_name for p in plan if p.kind == "query"}
    for field_name, wire_name in sorted(parsed.inline_query_names.items()):
        if wire_name not in bound_query_names:
            raise TypeError(f"Inline query field {field_name!r} is not bound by any parameter")
    return plan, return_type


def join_url(base_url: str | None, path: str) -> str:
    """Combines base URL and path into a fully qualified absolute URL."""
    if path.startswith(("http://", "https://")) or not base_url:
        return path
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def build_request(  # noqa: C901, PLR0912, PLR0913, PLR0917
    method: str,
    parsed: ParsedPath,
    plan: list[ParamPlan],
    values: Mapping[str, object],
    jar: Mapping[str, str] | None = None,
    base_url: str | None = None,
) -> RequestSpec:
    """Builds a request spec from the parameter plan and provided values.

    Args:
        method: HTTP method for the request.
        parsed: Bare path template and static query entries, from `split_path_template()`.
        plan: Parameter plan describing how to map values to the request.
        values: Mapping of parameter names to their values.
        jar: Snapshot of the current cookie jar.
        base_url: Base URL to prefix relative request paths with.
    """
    path_args: dict[str, object] = {}
    raw_path_args: dict[str, object] = {}
    params: dict[str, QueryValue] = dict(parsed.static_query)
    headers: dict[str, str] = {}
    cookies: dict[str, str] = {}
    fields: dict[str, object] = {}
    files: dict[str, PartValue] = {}
    body: JsonValue = None
    content: bytes | str | None = None
    for p in plan:
        value = values.get(p.py_name)
        if p.kind == "path":
            (raw_path_args if p.raw_path else path_args)[p.wire_name] = value
        elif value is None:
            continue  # None values are omitted for queries, fields, headers, and cookies
        elif p.kind == "query":
            params[p.wire_name] = value  # type: ignore[assignment]
        elif p.kind == "field":
            fields[p.wire_name] = value
        elif p.kind == "part":
            files[p.wire_name] = value  # type: ignore[assignment]
        elif p.kind == "header":
            headers[p.wire_name] = scalar_str(value)
        elif p.kind == "cookie":
            jar_value = None if p.cookie_override else (jar or {}).get(p.wire_name)
            cookies[p.wire_name] = jar_value if jar_value is not None else scalar_str(value)
        elif p.raw_body:
            if not isinstance(value, (bytes, str)):  # pragma: no cover
                raise TypeError(f"RawBody argument {p.py_name!r} must be bytes | str")
            content = value
            if p.raw_body_content_type is not None:
                headers["Content-Type"] = p.raw_body_content_type
        else:
            body = msgspec.to_builtins(value)
    if cookies:
        headers["cookie"] = "; ".join(
            f"{k}={urllib.parse.quote(v, safe='')}" for k, v in cookies.items()
        )
    formatted_path = parsed.path_only.format(
        **{k: urllib.parse.quote(scalar_str(v), safe="") for k, v in path_args.items()},
        **{k: scalar_str(v) for k, v in raw_path_args.items()},
    )
    return RequestSpec(
        method=method,
        url=join_url(base_url, formatted_path),
        params=params or None,
        headers=headers or None,
        data=fields or None,
        files=files or None,
        json=body,
        content=content,
    )
