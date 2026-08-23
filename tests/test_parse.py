"""Tests for path template parsing."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from mxhttp.parse import split_path_template

if TYPE_CHECKING:
    from collections.abc import Mapping


@pytest.mark.parametrize(
    ("path", "expected_path_only", "expected_static_query", "expected_inline_query_names"),
    [
        pytest.param(
            "/users/{user_id}/posts/{post_id}",
            "/users/{user_id}/posts/{post_id}",
            {},
            {},
            id="path-fields-only",
        ),
        pytest.param("/things", "/things", {}, {}, id="no-fields-no-query"),
        pytest.param(
            "/things/info?permanentId={thing_id}",
            "/things/info",
            {},
            {"thing_id": "permanentId"},
            id="inline-query-field",
        ),
        pytest.param(
            "/things?active=true", "/things", {"active": "true"}, {}, id="static-query-only"
        ),
        pytest.param(
            "/things/info?active=true&permanentId={thing_id}",
            "/things/info",
            {"active": "true"},
            {"thing_id": "permanentId"},
            id="static-and-inline-query",
        ),
        pytest.param(
            "/things?a={x}&b={y}",
            "/things",
            {},
            {"x": "a", "y": "b"},
            id="multiple-inline-query-fields",
        ),
        pytest.param(
            "/things/{?}", "/things/{?}", {}, {}, id="question-mark-field-name-isnt-a-separator"
        ),
        pytest.param(
            "/things/{{literal}}/{id}",
            "/things/{{literal}}/{id}",
            {},
            {},
            id="escaped-braces-in-path",
        ),
        pytest.param(
            "/things/info?raw={{}}&x={name}",
            "/things/info",
            {"raw": "{}"},
            {"name": "x"},
            id="escaped-braces-in-query-value",
        ),
        pytest.param(
            "/users/{user_id}/posts?sort={sort_by}",
            "/users/{user_id}/posts",
            {},
            {"sort_by": "sort"},
            id="path-field-and-inline-query-field-together",
        ),
        pytest.param(
            "/{py_name}?py_name={other_name}",
            "/{py_name}",
            {},
            {"other_name": "py_name"},
            id="query-key-matches-path-field-name-but-placeholder-doesnt",
        ),
    ],
)
def test_split_path_template(
    path: str,
    expected_path_only: str,
    expected_static_query: Mapping[str, str],
    expected_inline_query_names: Mapping[str, str],
) -> None:
    path_only, static_query, inline_query_names = split_path_template(path)

    assert path_only == expected_path_only
    assert static_query == expected_static_query
    assert inline_query_names == expected_inline_query_names


@pytest.mark.parametrize(
    ("path", "match"),
    [
        pytest.param(
            "/things/info?permanentId={kind:>10}",
            r"must not have a format spec or conversion",
            id="format-spec",
        ),
        pytest.param(
            "/things/{kind!r}", r"must not have a format spec or conversion", id="conversion"
        ),
        pytest.param("/things/{}", r"Anonymous \('\{\}'\)", id="anonymous-field"),
        pytest.param("/things/{0}", r"positional \('\{0\}'\)", id="positional-field"),
        pytest.param(
            "/things/info?{kind}", r"must supply an explicit key", id="bare-placeholder-key"
        ),
        pytest.param(
            "/things/info?filter=prefix-{kind}",
            r"must be a literal value or a bare placeholder",
            id="mixed-literal-and-placeholder-value",
        ),
        pytest.param(
            "/things?a={x}&b={x}",
            r"Field 'x' must not be bound multiple times",
            id="reused-field-name-in-second-position",
        ),
        pytest.param(
            "/things?a=1&a=2",
            r"Query key 'a' is used more than once",
            id="reused-query-key-static",
        ),
        pytest.param(
            "/things?a={x}&a={y}",
            r"Query key 'a' is used more than once",
            id="reused-query-key-inline",
        ),
        pytest.param(
            "/{py_name}?other={py_name}",
            r"Field 'py_name' is used for both a path segment and an inline query parameter",
            id="path-field-name-reused-as-inline-query-placeholder",
        ),
        pytest.param(
            "/{py_name}?py_name={py_name}",
            r"Field 'py_name' is used for both a path segment and an inline query parameter",
            id="path-field-name-reused-as-inline-query-placeholder-and-key",
        ),
    ],
)
def test_split_path_template_raises_type_error(path: str, match: str) -> None:
    with pytest.raises(TypeError, match=match):
        split_path_template(path)


def test_split_path_template_escaped_path_braces_round_trip_through_format() -> None:
    path_only, _, _ = split_path_template("/things/{{literal}}/{id}")

    assert path_only.format(id="42") == "/things/{literal}/42"


def test_split_path_template_many_fields_avoids_index_prefix_collision() -> None:
    path = "/x/" + "/".join(f"{{f{i}}}" for i in range(12))

    path_only, static_query, inline_query_names = split_path_template(path)

    assert path_only == path
    assert static_query == {}
    assert inline_query_names == {}
