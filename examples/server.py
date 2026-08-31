"""Local demo API server exercising every mxhttp feature, backing `examples/client.py`.

Run with: `python examples/server.py` (needs the `examples` extra: `pip install mxhttp[examples]`).
"""

# ruff: noqa: INP001

from __future__ import annotations

import asyncio
import hashlib
import hmac
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import msgspec
from data import DOWNLOAD_SHA256, download_payload
from litestar import Litestar, Request, Response, delete, get, post, put
from litestar.exceptions import HTTPException, NotAuthorizedException
from litestar.response import ServerSentEvent, Stream

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

SIGNING_SECRET = "demo-signing-secret"  # noqa: S105
AUTH_USER = "alice"
AUTH_PASSWORD = "secret"  # noqa: S105
FLAKY_FAILURES_BEFORE_SUCCESS = 2

PAYLOAD = download_payload()
PAYLOAD_ETAG = DOWNLOAD_SHA256


class Item(msgspec.Struct):
    """Stores a catalog item."""

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
    """Stores the headers, cookies, and query params `/whoami` received."""

    headers: dict[str, str]
    cookies: dict[str, str]
    query: dict[str, str]


class RawEcho(msgspec.Struct):
    """Stores the content type and byte length of a raw request body."""

    content_type: str | None
    length: int


@dataclass
class Store:
    """Stores the in-memory server state across requests."""

    items: dict[int, Item] = field(default_factory=dict)
    next_id: int = 1
    orders_by_key: dict[str, Order] = field(default_factory=dict)
    flaky_attempts: int = 0


STORE = Store()
for _seed_name, _seed_price in [("Widget", 9.99), ("Gadget", 19.99), ("Gizmo", 29.99)]:
    item = Item(id=STORE.next_id, name=_seed_name, price=_seed_price)
    STORE.items[item.id] = item
    STORE.next_id += 1


def parse_range(range_header: str, total: int) -> tuple[int, int] | None:
    """Parses a single `bytes=start-end` range header, clamped to `total`."""
    if not range_header.startswith("bytes="):
        return None
    start_str, _, end_str = range_header.removeprefix("bytes=").partition("-")
    start = int(start_str) if start_str else 0
    end = int(end_str) if end_str else total - 1
    return start, min(end, total - 1)


def basic_auth_header() -> str:
    """Builds the base64-encoded `user:password` value expected in the `Authorization` header."""
    import base64

    return base64.b64encode(f"{AUTH_USER}:{AUTH_PASSWORD}".encode()).decode()


FLAKY_FILE_STATE = {"failed_once": False}


@get("/items/{item_id:int}")
async def get_item(item_id: int) -> Item:
    """Returns one item, or `404` if `item_id` is unknown."""
    if item_id not in STORE.items:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=f"no item {item_id}")
    return STORE.items[item_id]


@get("/items")
async def list_items(q: str | None = None) -> list[Item]:
    """Lists items, optionally filtered by a case-insensitive name substring `q`."""
    items = list(STORE.items.values())
    if q:
        items = [i for i in items if q.lower() in i.name.lower()]
    return items


@post("/items")
async def create_item(data: NewItem) -> Item:
    """Creates an item with an auto-assigned id."""
    item = Item(id=STORE.next_id, name=data.name, price=data.price)
    STORE.items[item.id] = item
    STORE.next_id += 1
    return item


@put("/items/{item_id:int}")
async def replace_item(item_id: int, data: NewItem) -> Item:
    """Replaces (or creates) the item at `item_id`."""
    item = Item(id=item_id, name=data.name, price=data.price)
    STORE.items[item_id] = item
    return item


@delete("/items/{item_id:int}")
async def delete_item(item_id: int) -> None:
    """Removes an item, if present."""
    STORE.items.pop(item_id, None)


@get("/whoami")
async def whoami(request: Request[Any, Any, Any]) -> WhoAmI:
    """Reflects the headers, cookies, and query params of the request, for the bag/default demos."""
    return WhoAmI(
        headers=dict(request.headers),
        cookies=dict(request.cookies),
        query={k: v[0] for k, v in request.query_params.dict().items()},
    )


@post("/xml-import")
async def xml_import(request: Request[Any, Any, Any]) -> RawEcho:
    """Echoes the content type and byte length of a raw request body."""
    body = await request.body()
    return RawEcho(content_type=request.content_type[0], length=len(body))


@get("/events")
async def events() -> ServerSentEvent:
    """Emits a few Server-Sent Events, for the SSE demo."""

    async def generator() -> AsyncIterator[dict[str, str]]:
        for i in range(3):
            yield {"event": "tick", "id": str(i), "data": f"count={i}"}
            await asyncio.sleep(0.05)
        yield {"event": "done", "data": "bye"}

    return ServerSentEvent(generator())


@get("/stream")
async def stream() -> Stream:
    """Streams a few chunks of plain text, for the raw byte-streaming demo."""

    async def generator() -> AsyncIterator[bytes]:
        for i in range(10):
            yield f"chunk-{i}\n".encode()
            await asyncio.sleep(0.02)

    return Stream(generator(), media_type="application/octet-stream")


@get("/files/{unused_file_id:int}")
async def download_file(unused_file_id: int, request: Request[Any, Any, Any]) -> Response[bytes]:
    """Serves the deterministic demo payload, honoring `Range`/`If-Range` for resumability."""
    total = len(PAYLOAD)
    range_header = request.headers.get("range")
    if_range = request.headers.get("if-range")
    if range_header and (if_range is None or if_range == PAYLOAD_ETAG):
        parsed = parse_range(range_header, total)
        if parsed is not None:
            start, end = parsed
            return Response(
                content=PAYLOAD[start : end + 1],
                status_code=HTTPStatus.PARTIAL_CONTENT,
                media_type="application/octet-stream",
                headers={
                    "Content-Range": f"bytes {start}-{end}/{total}",
                    "Accept-Ranges": "bytes",
                    "ETag": PAYLOAD_ETAG,
                },
            )
    return Response(
        content=PAYLOAD,
        media_type="application/octet-stream",
        headers={"Accept-Ranges": "bytes", "ETag": PAYLOAD_ETAG},
    )


@get("/flaky-files/{unused_file_id:int}")
async def flaky_download_file(unused_file_id: int, request: Request[Any, Any, Any]) -> Stream:
    """Like `/files/{id}`, but drops the connection halfway through its first response.

    Lets the client demo prove `resumable=Retry(...)` actually reconnects, instead of only
    configuring it against a server that never fails.
    """
    total = len(PAYLOAD)
    range_header = request.headers.get("range")
    if_range = request.headers.get("if-range")
    start, end = 0, total - 1
    status_code = HTTPStatus.OK
    stream_headers = {"Accept-Ranges": "bytes", "ETag": PAYLOAD_ETAG}
    if range_header and (if_range is None or if_range == PAYLOAD_ETAG):
        parsed = parse_range(range_header, total)
        if parsed is not None:
            start, end = parsed
            status_code = HTTPStatus.PARTIAL_CONTENT
            stream_headers["Content-Range"] = f"bytes {start}-{end}/{total}"

    chunk = PAYLOAD[start : end + 1]
    should_fail = not FLAKY_FILE_STATE["failed_once"]

    async def generator() -> AsyncIterator[bytes]:
        half = len(chunk) // 2
        yield chunk[:half]
        if should_fail:
            FLAKY_FILE_STATE["failed_once"] = True
            raise ConnectionResetError("simulated dropped connection")
        yield chunk[half:]

    return Stream(
        generator(),
        status_code=status_code,
        media_type="application/octet-stream",
        headers=stream_headers,
    )


@get("/limited")
async def limited() -> Item:
    """Responds slowly enough to make client-side rate/concurrency limiting observable."""
    await asyncio.sleep(0.05)
    return Item(id=0, name="limited-response", price=0.0)


@get("/flaky")
async def flaky() -> Item:
    """Fails with `503` twice per process, then succeeds, for the `@retry` demo."""
    STORE.flaky_attempts += 1
    if STORE.flaky_attempts <= FLAKY_FAILURES_BEFORE_SUCCESS:
        raise HTTPException(status_code=HTTPStatus.SERVICE_UNAVAILABLE, detail="try again")
    STORE.flaky_attempts = 0
    return Item(id=0, name="finally-succeeded", price=0.0)


@post("/orders")
async def create_order(data: NewOrder, request: Request[Any, Any, Any]) -> Order:
    """Creates an order, returning the existing one if `Idempotency-Key` was already seen."""
    key = request.headers.get("idempotency-key")
    if key and key in STORE.orders_by_key:
        return STORE.orders_by_key[key]
    order = Order(id=f"order-{len(STORE.orders_by_key) + 1}", item=data.item)
    if key:
        STORE.orders_by_key[key] = order
    return order


@get("/secure")
async def secure(request: Request[Any, Any, Any]) -> Item:
    """Requires HTTP Basic auth matching `AUTH_USER`/`AUTH_PASSWORD`."""
    auth = request.headers.get("authorization")
    if auth != f"Basic {basic_auth_header()}":
        raise NotAuthorizedException(detail="bad credentials")
    return Item(id=0, name="secured-item", price=0.0)


@get("/signed")
async def signed(request: Request[Any, Any, Any]) -> Item:
    """Requires an `X-Signature` header: HMAC-SHA256 of the path, keyed by `SIGNING_SECRET`."""
    expected = hmac.new(SIGNING_SECRET.encode(), b"/signed", hashlib.sha256).hexdigest()
    if request.headers.get("x-signature") != expected:
        raise NotAuthorizedException(detail="bad signature")
    return Item(id=0, name="signed-item", price=0.0)


def main(host: str = "127.0.0.1", port: int = 8000) -> None:
    """Runs the demo API server.

    Args:
        host: Interface to bind to.
        port: Port to listen on. Must match the `--port` passed to `examples/client.py`.
    """
    import uvicorn

    app = Litestar(
        route_handlers=[
            get_item,
            list_items,
            create_item,
            replace_item,
            delete_item,
            whoami,
            xml_import,
            events,
            stream,
            download_file,
            flaky_download_file,
            limited,
            flaky,
            create_order,
            secure,
            signed,
        ]
    )
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    import doctyper

    cli = doctyper.DocTyper()
    cli.command()(main)
    cli()
