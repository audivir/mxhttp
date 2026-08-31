"""Demo client exercising every mxhttp feature against `examples/server.py`.

Run `python examples/server.py` in one terminal, then `python examples/client.py` in another.
"""

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
from server import SIGNING_SECRET

from mxhttp import (
    Body,
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
    base_url,
    cookies,
    get,
    headers,
    post,
    put,
)

BASE_URL = "http://127.0.0.1:8000"


class Item(msgspec.Struct):
    id: int
    name: str
    price: float


class NewItem(msgspec.Struct):
    name: str
    price: float


class Order(msgspec.Struct):
    id: str
    item: str


class NewOrder(msgspec.Struct):
    item: str


class WhoAmI(msgspec.Struct):
    headers: dict[str, str]
    cookies: dict[str, str]
    query: dict[str, str]


class RawEcho(msgspec.Struct):
    content_type: str | None
    length: int


class ItemNotFoundError(Exception):
    """Raised by `response_handler=strict_not_found` instead of `httpx.HTTPStatusError`."""


def strict_not_found(response: httpx.Response) -> httpx.Response:
    if response.status_code == httpx.codes.NOT_FOUND:
        raise ItemNotFoundError(response.request.url.path)
    return response.raise_for_status()


def sign_request(spec: RequestSpec) -> RequestSpec:
    path = urllib.parse.urlsplit(spec.url).path
    signature = hmac.new(SIGNING_SECRET.encode(), path.encode(), hashlib.sha256).hexdigest()
    spec.headers = {**(spec.headers or {}), "X-Signature": signature}
    return spec


@base_url(BASE_URL)
@headers({"X-Api-Version": "2"})
@cookies({"session": "demo-session"})
class Api(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items/{item_id}", response_handler=strict_not_found)
    def get_item_strict(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/items")
    def list_items(self, q: Annotated[str | None, Query] = None) -> list[Item]: ...  # type: ignore[empty-body]

    @post("/items")
    def create_item(self, item: Annotated[NewItem, Body]) -> Item: ...  # type: ignore[empty-body]

    @put("/items/{item_id}")
    def replace_item(self, item_id: int, item: Annotated[NewItem, Body]) -> Item: ...  # type: ignore[empty-body]

    @get("/whoami")
    def whoami(  # type: ignore[empty-body]
        self,
        extra_headers: Annotated[dict[str, str] | None, Header] = None,
        extra_query: Annotated[dict[str, str] | None, Query] = None,
    ) -> WhoAmI: ...

    @post("/xml-import")
    def import_xml(self, payload: Annotated[bytes, RawBody("application/xml")]) -> RawEcho: ...  # type: ignore[empty-body]

    @get("/events")
    def events(self) -> Iterator[Event]: ...  # type: ignore[empty-body]

    @get("/stream")
    def stream_chunks(self) -> Iterator[bytes]: ...  # type: ignore[empty-body]

    @get("/flaky-files/{file_id}", resumable=Retry(attempts=3, backoff=0.05))
    def stream_flaky_file(self, file_id: int) -> Iterator[bytes]: ...  # type: ignore[empty-body]

    @get("/files/{file_id}")
    def download_file(self, file_id: int) -> Downloader: ...  # type: ignore[empty-body]

    @get("/files/{file_id}", parts=Parts(count=4, min_part_size=256 * 1024))
    def download_file_parts(self, file_id: int) -> Downloader: ...  # type: ignore[empty-body]

    @get("/flaky", retry=Retry(attempts=5, on={503}, backoff=0.05, jitter=False))
    def flaky(self) -> Item: ...  # type: ignore[empty-body]

    @post("/orders", idempotent=True)
    def create_order(self, order: Annotated[NewOrder, Body]) -> Order: ...  # type: ignore[empty-body]

    @post("/orders", idempotent=lambda: "demo-fixed-key")
    def create_order_same_key(self, order: Annotated[NewOrder, Body]) -> Order: ...  # type: ignore[empty-body]

    @get("/limited", ratelimit=RateLimit(calls=2, period=1.0, block=False))
    def limited_by_rate(self) -> Item: ...  # type: ignore[empty-body]

    @get("/limited", concurrency=Concurrency(limit=1, block=False))
    def limited_by_concurrency(self) -> Item: ...  # type: ignore[empty-body]

    @get("/secure")
    def secure(self) -> Item: ...  # type: ignore[empty-body]

    @get("/signed", request_handler=sign_request)
    def signed(self) -> Item: ...  # type: ignore[empty-body]


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
        print("default response_handler raises:", type(e).__name__)
    try:
        api.get_item_strict(item_id=999)
    except ItemNotFoundError as e:
        print("overridden response_handler raises:", type(e).__name__, str(e))


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
    print("streamed", len(chunks), "bytes")


def demo_resumable_stream(api: Api) -> None:
    section("Resumable streaming (server drops the connection mid-response)")
    chunks = b"".join(api.stream_flaky_file(file_id=1))
    digest = hashlib.sha256(chunks).hexdigest()
    print("reconnected and recovered the full file:", digest == DOWNLOAD_SHA256)


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
    section("Multi-part parallel download")
    path = tmp_dir / "dataset.bin"
    downloader = api.download_file_parts(file_id=1)
    downloader(str(path), on_progress=TqdmProgress(desc="multi-part", per_part=True))
    print("sha256 matches:", hashlib.sha256(path.read_bytes()).hexdigest() == DOWNLOAD_SHA256)


def demo_retry(api: Api) -> None:
    section("Retry on transient failures")
    print("flaky (fails twice server-side, retried automatically):", api.flaky())


def demo_idempotency(api: Api) -> None:
    section("Idempotency-Key")
    order = NewOrder(item="Widget")
    first = api.create_order(order=order)
    second = api.create_order(order=order)
    print("idempotent=True: two calls get different keys ->", first.id != second.id)
    fixed_first = api.create_order_same_key(order=order)
    fixed_second = api.create_order_same_key(order=order)
    print("fixed key: two calls dedupe server-side ->", fixed_first.id == fixed_second.id)


def demo_ratelimit(api: Api) -> None:
    section("Rate limiting (client-side, per-endpoint override)")
    for i in range(3):
        try:
            api.limited_by_rate()
            print(f"call {i}: ok")
        except RateLimitExceededError:  # noqa: PERF203
            print(f"call {i}: blocked by @ratelimit")


def demo_concurrency(api: Api) -> None:
    section("Concurrency limiting (client-side, per-endpoint override)")
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(api.limited_by_concurrency) for _ in range(2)]
        results = []
        for future in futures:
            try:
                future.result()
                results.append("ok")
            except ConcurrencyExceededError:  # noqa: PERF203
                results.append("rejected")
    print("two simultaneous calls against concurrency=1:", results)


def demo_auth(api_authed: Api, api_plain: Api) -> None:
    section("Authentication")
    print("with auth=:", api_authed.secure())
    try:
        api_plain.secure()
    except httpx.HTTPStatusError as e:
        print("without auth=:", e.response.status_code)


def demo_request_handler(api: Api) -> None:
    section("Per-endpoint request_handler (request signing)")
    print("signed:", api.signed())


def main() -> int:
    api = Api()
    api_authed = Api(auth=httpx.BasicAuth("alice", "secret"))

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
    demo_retry(api)
    demo_idempotency(api)
    demo_ratelimit(api)
    demo_concurrency(api)
    demo_auth(api_authed, api)
    demo_request_handler(api)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
