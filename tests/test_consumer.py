"""Tests for the consumer closing module."""

from __future__ import annotations

import httpx
import pytest
from conftest import make_consumer
from models import ITEM, AsyncCrudApi, CrudApi, Item

from mxhttp import SyncConsumer, base_url, get

pytestmark = pytest.mark.anyio


def test_consumer_context_manager_closes_session() -> None:
    consumer = make_consumer(CrudApi, ITEM)

    with consumer as entered:
        assert entered is consumer
        assert consumer.get_item(item_id=1) == ITEM

    assert consumer.session.is_closed


async def test_consumer_asnyc_context_manager_closes_session() -> None:
    consumer = make_consumer(AsyncCrudApi, ITEM)

    async with consumer as entered:
        assert entered is consumer
        assert await consumer.get_item(item_id=1) == ITEM

    assert consumer.session.is_closed


def test_consumer_timeout() -> None:
    consumer = SyncConsumer()
    assert consumer.session.timeout == httpx.Timeout(5.0)

    consumer = SyncConsumer(timeout=42)
    assert consumer.session.timeout == httpx.Timeout(42)

    consumer = SyncConsumer(timeout=httpx.Timeout(10, read=30))
    assert consumer.session.timeout == httpx.Timeout(10, read=30)


def test_consumer_default_base_url_is_none() -> None:
    consumer = SyncConsumer()
    assert consumer._base_url is None  # noqa: SLF001


@base_url("https://api.example.com")
class DefaultConsumer(SyncConsumer):
    @get("/items")
    def get_items(self) -> list[Item]: ...  # type: ignore[empty-body]

    @get("/metrics", base_url="https://metrics.example.com")
    def get_metrics(self) -> list[Item]: ...  # type: ignore[empty-body]

    @get("https://cdn.example.com/assets")
    def get_assets(self) -> list[Item]: ...  # type: ignore[empty-body]

    @get("https://raw.example.com/data")
    def get_raw(self) -> list[Item]: ...  # type: ignore[empty-body]


def test_base_url_decorator_and_endpoint_resolution() -> None:
    consumer, requests = make_consumer(DefaultConsumer, [ITEM], track_requests=True)
    assert consumer._base_url == "https://api.example.com"  # noqa: SLF001

    consumer.get_items()
    consumer.get_metrics()
    consumer.get_assets()
    consumer.get_raw()

    assert requests[0].url == httpx.URL("https://api.example.com/items")
    assert requests[1].url == httpx.URL("https://metrics.example.com/metrics")
    assert requests[2].url == httpx.URL("https://cdn.example.com/assets")
    assert requests[3].url == httpx.URL("https://raw.example.com/data")


class UndecoratedConsumer(SyncConsumer):
    @get("https://cdn.example.com/assets")
    def get_assets(self) -> list[Item]: ...  # type: ignore[empty-body]


def test_consumer_without_base_url_calls_full_urls() -> None:
    consumer, requests = make_consumer(UndecoratedConsumer, [ITEM], track_requests=True)
    consumer.get_assets()
    assert requests[0].url == httpx.URL("https://cdn.example.com/assets")


@pytest.mark.parametrize(
    "invalid_url",
    ["api.example.com", "ftp://api.example.com", "://api.example.com"],
    ids=["no_scheme", "ftp_scheme", "missing_scheme"],
)
def test_invalid_scheme_raises_value_error(invalid_url: str) -> None:
    with pytest.raises(ValueError, match="base_url must start with 'http://' or 'https://'"):
        base_url(invalid_url)


def test_endpoint_invalid_base_url_raises_value_error() -> None:
    with pytest.raises(ValueError, match="base_url must start with 'http://' or 'https://'"):

        class InvalidEndpointConsumer(SyncConsumer):
            @get("/items", base_url="invalid.url")
            def items(self) -> list[Item]: ...  # type: ignore[empty-body]


def test_endpoint_full_url_with_base_url_raises_value_error() -> None:
    with pytest.raises(
        ValueError, match="Cannot specify base_url when path is already an absolute URL"
    ):

        class ConflictingEndpointConsumer(SyncConsumer):
            @get("https://cdn.example.com/asset", base_url="https://other.example.com")
            def asset(self) -> list[Item]: ...  # type: ignore[empty-body]


def test_relative_endpoint_without_base_url_raises_value_error() -> None:
    class RelativeConsumer(SyncConsumer):
        @get("/items")
        def items(self) -> list[Item]: ...  # type: ignore[empty-body]

    consumer = RelativeConsumer()
    with pytest.raises(
        ValueError, match="Cannot call relative endpoint '/items' without a base_url"
    ):
        consumer.items()


@base_url("https://api.example.com/v1")
class SubpathConsumer(SyncConsumer):
    @get("/items")
    def get_items(self) -> list[Item]: ...  # type: ignore[empty-body]

    @get("items")
    def get_items_no_slash(self) -> list[Item]: ...  # type: ignore[empty-body]


def test_constructor_base_url_sets_instance_default() -> None:
    consumer = SyncConsumer(base_url="https://api.example.com")

    assert consumer._base_url == "https://api.example.com"  # noqa: SLF001


def test_constructor_base_url_overrides_class_decorator() -> None:
    consumer = DefaultConsumer(base_url="https://override.example.com")

    assert consumer._base_url == "https://override.example.com"  # noqa: SLF001


def test_constructor_base_url_invalid_scheme_raises_value_error() -> None:
    with pytest.raises(ValueError, match="base_url must start with 'http://' or 'https://'"):
        SyncConsumer(base_url="api.example.com")


def test_base_url_preserves_subpath_with_slashes() -> None:
    consumer, requests = make_consumer(SubpathConsumer, [ITEM], track_requests=True)
    consumer.get_items()
    consumer.get_items_no_slash()

    assert requests[0].url == httpx.URL("https://api.example.com/v1/items")
    assert requests[1].url == httpx.URL("https://api.example.com/v1/items")
