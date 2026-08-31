"""Demo client exercising every mxhttp feature against `examples/server.py`.

Run `python examples/server.py` in one terminal, then `python examples/client.py` in another.
"""

# ruff: noqa: INP001, T201, D103

from __future__ import annotations

import concurrent.futures
import hashlib
import hmac
import tempfile
import urllib.parse
from collections.abc import Iterator  # noqa: TC003
from pathlib import Path
from typing import Annotated

import httpx
import msgspec
from data import DOWNLOAD_SHA256
from server import AUTH_PASSWORD, AUTH_USER, SIGNING_SECRET

from mxhttp import (
    Body,
    ChecksumMismatchError,
    Concurrency,
    ConcurrencyExceededError,
    Downloader,
    Event,
    Header,
    Parts,
    Query,
    RateLimit,
    RateLimitExceededError,
    RawBody,
    RequestSpec,
    Retry,
    SyncConsumer,
    TqdmProgress,
    cookies,
    get,
    headers,
    post,
    put,
)


class Item(msgspec.Struct):
    """Stores a catalog item, matching the wire shape of `server.Item`."""

    id: int
    name: str
    price: float


class NewItem(msgspec.Struct):
    """Stores the fields needed to create or replace an item."""

    name: str
    price: float


class Order(msgspec.Struct):
    """Stores a placed order."""

    id: str
    item: str


class NewOrder(msgspec.Struct):
    """Stores the fields needed to place an order."""

    item: str


class WhoAmI(msgspec.Struct):
    """Stores the headers, cookies, and query params `/whoami` echoed back."""

    headers: dict[str, str]
    cookies: dict[str, str]
    query: dict[str, str]


class RawEcho(msgspec.Struct):
    """Stores the content type and byte length `/xml-import` echoed back."""

    content_type: str | None
    length: int


class ItemNotFoundError(Exception):
    """Raised by `response_handler=strict_not_found` instead of `httpx.HTTPStatusError`."""


def strict_not_found(response: httpx.Response) -> httpx.Response:
    """Raises `ItemNotFoundError` on a 404, otherwise falls back to `raise_for_status`."""
    if response.status_code == httpx.codes.NOT_FOUND:
        raise ItemNotFoundError(response.request.url.path)
    return response.raise_for_status()


def sign_request(spec: RequestSpec) -> RequestSpec:
    """Signs the request path with an HMAC-SHA256, matching what `/signed` expects."""
    path = urllib.parse.urlsplit(spec.url).path
    signature = hmac.new(SIGNING_SECRET.encode(), path.encode(), hashlib.sha256).hexdigest()
    spec.headers = {**(spec.headers or {}), "X-Signature": signature}
    return spec


@headers({"X-Api-Version": "2"})
@cookies({"session": "demo-session"})
class Api(SyncConsumer):
    """Declarative client covering every mxhttp feature, sharing one Litestar server."""

    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item:  # type: ignore[empty-body]
        """Plain CRUD; the default `@response_handler` raises `HTTPStatusError` on a 404."""

    @get("/items/{item_id}", response_handler=strict_not_found)
    def get_item_strict(self, item_id: int) -> Item:  # type: ignore[empty-body]
        """Per-endpoint `response_handler=` override, raising `ItemNotFoundError` on a 404."""

    @get("/items")
    def list_items(self, q: Annotated[str | None, Query] = None) -> list[Item]:  # type: ignore[empty-body]
        """Inline `Query` parameter."""

    @post("/items")
    def create_item(self, item: Annotated[NewItem, Body]) -> Item:  # type: ignore[empty-body]
        """JSON request body via `Body`."""

    @put("/items/{item_id}")
    def replace_item(self, item_id: int, item: Annotated[NewItem, Body]) -> Item:  # type: ignore[empty-body]
        """Path parameter plus JSON body."""

    @get("/whoami")
    def whoami(  # type: ignore[empty-body]
        self,
        extra_headers: Annotated[dict[str, str] | None, Header] = None,
        extra_query: Annotated[dict[str, str] | None, Query] = None,
    ) -> WhoAmI:
        """Dynamic header/query bags, layered on top of the class-level `@headers`/`@cookies`."""

    @post("/xml-import")
    def import_xml(self, payload: Annotated[bytes, RawBody("application/xml")]) -> RawEcho:  # type: ignore[empty-body]
        """Raw, non-JSON request body via `RawBody`."""

    @get("/events")
    def events(self) -> Iterator[Event]:  # type: ignore[empty-body]
        """Server-Sent Events."""

    @get("/stream")
    def stream_chunks(self) -> Iterator[bytes]:  # type: ignore[empty-body]
        """Raw byte streaming."""

    @get("/flaky-files/{file_id}", resumable=Retry(attempts=3, backoff=0.05))
    def stream_flaky_file(self, file_id: int) -> Iterator[bytes]:  # type: ignore[empty-body]
        """`resumable=Retry(...)` reconnects with `Range` after a dropped connection."""

    @get("/files/{file_id}")
    def download_file(self, file_id: int) -> Downloader:  # type: ignore[empty-body]
        """Resumable download to disk; binds without any network activity yet."""

    @get("/files/{file_id}", parts=Parts(count=4, min_part_size=256 * 1024))
    def download_file_parts(self, file_id: int) -> Downloader:  # type: ignore[empty-body]
        """Same download, segmented into 4 parts downloaded in parallel."""

    @get("/flaky", retry=Retry(attempts=5, on={503}, backoff=0.05, jitter=False))
    def flaky(self) -> Item:  # type: ignore[empty-body]
        """Per-endpoint `retry=` override, retrying past the first 2 server failures."""

    @post("/orders", idempotent=True)
    def create_order(self, order: Annotated[NewOrder, Body]) -> Order:  # type: ignore[empty-body]
        """`idempotent=True`: a fresh `Idempotency-Key` is generated on every call."""

    @post("/orders", idempotent=lambda: "demo-fixed-key")
    def create_order_same_key(self, order: Annotated[NewOrder, Body]) -> Order:  # type: ignore[empty-body]
        """A callable `idempotent=` always returning the same key, so calls deduplicate."""

    @get("/limited", ratelimit=RateLimit(calls=2, period=1.0, block=False))
    def limited_by_rate(self) -> Item:  # type: ignore[empty-body]
        """Per-endpoint `ratelimit=` override; the 3rd call in one second raises."""

    @get("/limited", concurrency=Concurrency(limit=1, block=False))
    def limited_by_concurrency(self) -> Item:  # type: ignore[empty-body]
        """Per-endpoint `concurrency=` override; a 2nd simultaneous call raises."""

    @get("/secure")
    def secure(self) -> Item:  # type: ignore[empty-body]
        """Requires the constructor-level `auth=` on the consumer."""

    @get("/signed", request_handler=sign_request)
    def signed(self) -> Item:  # type: ignore[empty-body]
        """Per-endpoint `request_handler=` override, signing the request before it is sent."""


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def demo_crud(api: Api) -> None:
    section("CRUD + inline query")
    item = api.get_item(item_id=1)
    print("get_item:", item)
    print("list_items(q='Gadget'):", api.list_items(q="Gadget"))
    created = api.create_item(item=NewItem(name="Doohickey", price=3.5))
    print("create_item:", created)
    replaced = api.replace_item(item_id=created.id, item=NewItem(name="Renamed", price=4.0))
    print("replace_item:", replaced)


def demo_response_handler_override(api: Api) -> None:
    section("Per-endpoint response_handler override")
    try:
        api.get_item(item_id=999)
    except httpx.HTTPStatusError as e:
        print("default @response_handler raises:", type(e).__name__)
    try:
        api.get_item_strict(item_id=999)
    except ItemNotFoundError as e:
        print("response_handler=strict_not_found raises:", type(e).__name__, str(e))


def demo_headers_cookies_bags(api: Api) -> None:
    section("Default headers/cookies + dynamic bags")
    who = api.whoami(extra_headers={"X-Trace-Id": "demo-trace"}, extra_query={"debug": "1"})
    print("class default header present:", who.headers.get("x-api-version") == "2")
    print("bag header present:", who.headers.get("x-trace-id") == "demo-trace")
    print("class default cookie present:", who.cookies.get("session") == "demo-session")
    print("bag query present:", who.query.get("debug") == "1")


def demo_raw_body(api: Api) -> None:
    section("Raw request body")
    print("import_xml:", api.import_xml(payload=b"<order/>"))


def demo_sse(api: Api) -> None:
    section("Server-Sent Events")
    for event in api.events():
        print("event:", event.event, event.data)


def demo_streaming(api: Api) -> None:
    section("Raw byte streaming")
    chunks = b"".join(api.stream_chunks())
    print("received", len(chunks), "bytes via Iterator[bytes]")


def demo_resumable_stream(api: Api) -> None:
    section("Resumable streaming (server drops the connection mid-response)")
    chunks = b"".join(api.stream_flaky_file(file_id=1))
    digest = hashlib.sha256(chunks).hexdigest()
    matches = digest == DOWNLOAD_SHA256
    print("resumable=Retry(...) reconnected after the drop, sha256 matches:", matches)


def demo_downloader(api: Api, tmp_dir: Path) -> None:
    section("Resumable download to disk + checksum verification")
    path = tmp_dir / "report.bin"
    downloader = api.download_file(file_id=1)
    result = downloader(
        str(path), on_progress=TqdmProgress(desc="download"), checksum=DOWNLOAD_SHA256
    )
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    print("downloaded to", result, "sha256 matches:", digest == DOWNLOAD_SHA256)


def demo_multi_part(api: Api, tmp_dir: Path) -> None:
    section("Multi-part parallel download + checksum verification")
    path = tmp_dir / "dataset.bin"
    downloader = api.download_file_parts(file_id=1)
    downloader(
        str(path),
        on_progress=TqdmProgress(desc="multi-part", per_part=True),
        checksum=DOWNLOAD_SHA256,
    )
    print("sha256 matches:", hashlib.sha256(path.read_bytes()).hexdigest() == DOWNLOAD_SHA256)


def demo_checksum_mismatch(api: Api, tmp_dir: Path) -> None:
    section("Checksum mismatch raises ChecksumMismatchError")
    unverified_path = tmp_dir / "unverified.bin"
    api.download_file(file_id=1)(str(unverified_path))
    print("download without checksum= succeeded:", unverified_path.exists())

    wrong_checksum = "0" * 64
    mismatched_path = tmp_dir / "mismatched.bin"
    try:
        api.download_file(file_id=1)(str(mismatched_path), checksum=wrong_checksum)
    except ChecksumMismatchError:
        print("download with a wrong checksum= raised ChecksumMismatchError")
        print("destination was not written:", not mismatched_path.exists())


def demo_retry(api: Api) -> None:
    section("Retry on transient failures")
    print("@retry recovered after 2 HTTP 503 responses:", api.flaky())


def demo_idempotency(api: Api) -> None:
    section("Idempotency-Key")
    order = NewOrder(item="Widget")
    first = api.create_order(order=order)
    second = api.create_order(order=order)
    print("idempotent=True generates a fresh key per call, distinct orders:", first.id != second.id)
    fixed_first = api.create_order_same_key(order=order)
    fixed_second = api.create_order_same_key(order=order)
    print(
        "a fixed key deduplicates server-side, same order returned:",
        fixed_first.id == fixed_second.id,
    )


def demo_ratelimit(api: Api) -> None:
    section("Rate limiting (client-side, per-endpoint override)")
    for i in range(3):
        try:
            api.limited_by_rate()
            print(f"call {i}: succeeded")
        except RateLimitExceededError:  # noqa: PERF203
            print(f"call {i}: raised RateLimitExceededError")


def demo_concurrency(api: Api) -> None:
    section("Concurrency limiting (client-side, per-endpoint override)")
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(api.limited_by_concurrency) for _ in range(2)]
        results = []
        for future in futures:
            try:
                future.result()
                results.append("succeeded")
            except ConcurrencyExceededError:  # noqa: PERF203
                results.append("raised ConcurrencyExceededError")
    print("two simultaneous calls against concurrency=1:", results)


def demo_auth(api_authed: Api, api_plain: Api) -> None:
    section("Authentication")
    print("authenticated call:", api_authed.secure())
    try:
        api_plain.secure()
    except httpx.HTTPStatusError as e:
        print("unauthenticated call raises HTTP", e.response.status_code)


def demo_request_handler(api: Api) -> None:
    section("Per-endpoint request_handler (request signing)")
    print("signed:", api.signed())


def run_demos(api: Api, api_authed: Api) -> None:
    demo_crud(api)
    demo_response_handler_override(api)
    demo_headers_cookies_bags(api)
    demo_raw_body(api)
    demo_sse(api)
    demo_streaming(api)
    demo_resumable_stream(api)
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        demo_downloader(api, tmp_dir)
        demo_multi_part(api, tmp_dir)
        demo_checksum_mismatch(api, tmp_dir)
    demo_retry(api)
    demo_idempotency(api)
    demo_ratelimit(api)
    demo_concurrency(api)
    demo_auth(api_authed, api)
    demo_request_handler(api)


def main(host: str = "127.0.0.1", port: int = 8000) -> None:
    """Runs every mxhttp feature demo against a running `examples/server.py`.

    Args:
        host: Host the server is listening on.
        port: Port the server is listening on. Must match the `--port` passed to
            `examples/server.py`.
    """
    server_url = f"http://{host}:{port}"
    api = Api(base_url=server_url)
    api_authed = Api(base_url=server_url, auth=httpx.BasicAuth(AUTH_USER, AUTH_PASSWORD))
    run_demos(api, api_authed)


if __name__ == "__main__":
    import doctyper

    cli = doctyper.DocTyper()
    cli.command()(main)
    cli()
