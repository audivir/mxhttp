"""Parses a path template into its path portion and any inline query bindings."""

from __future__ import annotations

import string
import urllib.parse
import uuid
from typing import NamedTuple


class ParsedPath(NamedTuple):
    """Stores the components of a path template split by `split_path_template()`."""

    path_only: str
    static_query: dict[str, str]
    inline_query_names: dict[str, str]


def reescape(literal_text: str) -> str:
    """Re-escapes braces that `str.Formatter().parse()` unescaped."""
    return literal_text.replace("{", "{{").replace("}", "}}")


def unescape(text: str) -> str:
    """Reverses the escaping applied by `reescape()`."""
    return text.replace("{{", "{").replace("}}", "}")


def split_path_template(path: str) -> ParsedPath:  # noqa: C901, PLR0912
    """Splits a path template into its path portion and query string, if any.

    Returns:
        `ParsedPath` of the bare path, static `key: value` query entries, and `py_name:
        wire_name` pairs for inline query placeholders.

    Raises:
        TypeError: If a `{name}` field has a format spec or conversion, is anonymous or
            positional, mixes a placeholder with literal text in a query value, uses a bare
            placeholder as its own query key, reuses a query key or field name, or reuses a
            field name of a path segment as an inline query placeholder.
    """
    marker = uuid.uuid4().hex
    names: list[str] = []
    clean_parts: list[str] = []
    for literal_text, field_name, format_spec, conversion in string.Formatter().parse(path):
        clean_parts.append(reescape(literal_text))
        if field_name is None:
            continue
        if format_spec or conversion:
            raise TypeError(f"Field {field_name!r} must not have a format spec or conversion")
        if not field_name or field_name.isdigit():
            raise TypeError("Anonymous ('{}') or positional ('{0}') fields are not supported")
        names.append(field_name)
        clean_parts.append(f"{marker}{len(names) - 1}_")
    split = urllib.parse.urlsplit("".join(clean_parts))

    path_field_count = split.path.count(marker)
    path_only = split.path
    path_field_names = set(names[:path_field_count])
    for index, name in enumerate(names[:path_field_count]):
        path_only = path_only.replace(f"{marker}{index}_", f"{{{name}}}")

    static_query: dict[str, str] = {}
    inline_query_names: dict[str, str] = {}
    used_keys: set[str] = set()
    for key, value in urllib.parse.parse_qsl(split.query, keep_blank_values=True):
        if marker in key:
            raise TypeError(
                "Inline query parameters must supply an explicit key (e.g. 'key={name}')"
            )
        if key in used_keys:
            raise TypeError(f"Query key {key!r} is used more than once")
        used_keys.add(key)

        token = value.removeprefix(marker)
        index_text, sep, rest = token.partition("_")
        if value.startswith(marker) and sep and not rest and index_text.isdigit():
            field = names[int(index_text)]
            if field in inline_query_names:
                raise TypeError(f"Field {field!r} must not be bound multiple times")
            if field in path_field_names:
                raise TypeError(
                    f"Field {field!r} is used for both a path segment and an inline query "
                    "parameter; give one of them a different placeholder name"
                )
            inline_query_names[field] = key
        elif marker in value:
            raise TypeError(
                f"Inline query parameter {key!r} must be a literal value or a bare placeholder"
            )
        else:
            static_query[unescape(key)] = unescape(value)
    return ParsedPath(path_only, static_query, inline_query_names)
