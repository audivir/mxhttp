# mxhttp

Declarative HTTP client on top of `msgspec` and `httpx`. Write an API as a class of annotated stub methods and `mxhttp` will handle the rest (request building, sending, and response decoding).

## Prerequisites

- Python 3.10 or newer

## Installation

```bash
pip install mxhttp
```

## Usage

```python
from typing import Annotated
import msgspec
from mxhttp import Body, Query, SyncConsumer, base_url, get, post


class Item(msgspec.Struct):
    id: int
    name: str
    price: float


class NewItem(msgspec.Struct):
    name: str
    price: float


@base_url("https://api.example.com")
class Shop(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/search")
    def search(self, q: Annotated[str, Query], limit: Annotated[int, Query] = 20) -> list[Item]: ...  # type: ignore[empty-body]

    @post("/items")
    def create_item(self, item: Annotated[NewItem, Body]) -> Item: ...  # type: ignore[empty-body]


shop = Shop()
item = shop.get_item(item_id=7)
new = shop.create_item(item=NewItem(name="Gadget", price=4.5))
```

The method body is never run as it is replaced by the decorator. Parameters are bound based on their `Annotated[...]` marker:

| Marker class | Request Target | Info |
|---|---|---|
| `Path` (or implicit) | Path | Matched by parameter name unless annotated explicitly (`Path["name"]`). Must be a non-nullable `str`, `int`, or `float`. |
| `RawPath` | Path | Like `Path`, but spliced into the URL without percent-encoding, bypassing any query-parameter checks (duplicate keys, structure, etc.) that normally apply. Must be a non-nullable `str`. Use only for values that are already encoded relative paths or query fragments. |
| `Query` | Query | Must be a nullable `str`, `int`, `float`, or `bool`, or a `Sequence` of those, sent as `key=a&key=b&...`. |
| `Field` | Form Field | `application/x-www-form-urlencoded`. Accepts same types as `Query`. |
| `Part` | Multipart File Part | Forces the whole request to be multipart and any `Field` params on the same call will become multipart fields as well. Accepts the same types `httpx` takes for `files=`. |
| `Header` | HTTP Header | Must be `str`, `int`, `float`, or `bool`, but no list of those. |
| `Cookie` | Cookie | Is superseded by the cookie jar of the client if it already has a same-named cookie, unless `override=True` is set. Accepts same types as `Header`. |
| `Body` | JSON Body | Whole object, serialized with `msgspec.to_builtins`. Cannot be a scalar type. |
| `RawBody` | Raw Body | `bytes` or `str`, sent unencoded. See "Raw request bodies" below. |

- Use `Path["name"]`, `Query["name"]`, `Field["name"]`, `Header["name"]`, or `Cookie["name"]` to bind under a different name than the parameter (for example reserved `from`, a header like `X-Request-Id`, or unsupported string format arguments).
- `None`-valued `Query`, `Field`, `Header`, and `Cookie` parameters are omitted from the request.
- `Path` parameters cannot be optional as a placeholder cannot be omitted from the URL.
- Mismatched marker and type combinations raise a `TypeError` as soon as the class body runs, not at call time.
- `Literal[...]` and `Enum` types are accepted where scalar types are (`Path`, `Query`, `Field`, `Header`, `Cookie`). Every literal value or enum member value must be `str`, `int`, or `float` (plus `bool` outside of `Path`). `Enum` members are serialized by their `.value`.
- `Header` rejects `Content-Type` and `Cookie` as wire names (any casing), since mxhttp manages both itself: `Content-Type` through `RawBody`, and `Cookie` through the `Cookie` marker and cookie jar.
- An endpoint can only have one body encoding: `Body` and `RawBody` cannot be combined with each other or with `Field`/`Part`. `Field` and `Part` can still be combined with each other. Any conflicting combination raises a `TypeError` as soon as the class body runs.

### Base URL

Set the default host and path prefix for all endpoints on the class with `@base_url`:

```python
from mxhttp import SyncConsumer, base_url, get


@base_url("https://api.example.com/v1")
class Api(SyncConsumer):
    @get("/items")
    def get_items(self) -> list[Item]: ...  # type: ignore[empty-body]

    @get("/metrics", base_url="https://metrics.example.com")
    def get_metrics(self) -> list[Item]: ...  # type: ignore[empty-body]

    @get("https://cdn.example.com/assets")
    def get_assets(self) -> list[Item]: ...  # type: ignore[empty-body]
```

- `@base_url` validates that the URL begins with `http://` or `https://`.
- Pass `base_url=` directly to `@get`/`@post`/etc. to override the base URL for that specific endpoint.
- Absolute URLs passed to `@get("https://...")` call the full address directly without requiring `@base_url`.

### Inline query parameters

A path template can bake query parameters directly into the string:

```python
class Shop(SyncConsumer):
    @get("/items?category={cat}")
    def by_category(self, cat: str) -> list[Item]: ...  # type: ignore[empty-body]
```

- An unmarked parameter binds implicitly to a `{name}` placeholder, following the same mechanism as `Path`.
- `Query["cat"]` binds it to a different parameter name. Unlike other markers, the brackets are not the wire name here: the wire name is whatever query key the template assigned to `{cat}`.
- A query entry with no placeholder (for example `?active=true`) is a static value sent on every call. That key also cannot be reused by a dynamic `Query` parameter.
- Each query key, and each placeholder field, can only be used once per path template (for example `/things?a={x}&a={y}` is rejected).
- A placeholder field also cannot be reused by a real path segment (`/{id}?other={id}` is rejected).
- A placeholder cannot be mixed with literal text in the same value (`key=prefix{name}`), and cannot stand in for the key itself (`{name}` with no `=`).
- Every `{name}` field must be bound by exactly one parameter (implicit, `Path["name"]`, or `Query["name"]`).
- An inline query field cannot be a `Sequence` as the placeholder reserves exactly one query spot.
- All of the above raise a `TypeError` as soon as the class body runs, not at call time.

### Decoding the response

The return type defines the response decoding:

- `httpx.Response` for the raw response.
- `str` or `bytes` for the corresponding `.text` or `.content` with no JSON round-trip.
- `pydantic.BaseModel` subclasses via their own `.model_validate_json`.
- Anything else `msgspec.json.decode` can decode: `msgspec.Struct`, dataclasses, `TypedDict`, `NamedTuple`, and `list`, `dict`, or other containers of those.
- `Response[Item]` for a struct with the decoded `Item` as `.data` and the raw `httpx.Response` in `.response`.
- Plain `attrs` classes are decoded by `msgspec`. For type hinting, `attrs` is needed as a dependency.

For an async client, subclass `AsyncConsumer` and declare the methods `async def`. Everything else stays the same.

### Raw request bodies

Annotate a parameter with `RawBody` to send `bytes` or `str` as the request body unencoded, instead of JSON:

```python
from mxhttp import RawBody, SyncConsumer, post


class BulkImport(SyncConsumer):
    @post("/items/import")
    def import_xml(self, payload: Annotated[bytes, RawBody("application/xml")]) -> Item: ...  # type: ignore[empty-body]


class Uploads(SyncConsumer):
    @post("/blobs")
    def upload(self, payload: Annotated[bytes, RawBody]) -> Item: ...  # type: ignore[empty-body]
```

- Used bare (`RawBody`), no `Content-Type` header is sent.
- `RawBody("application/xml")` or `RawBody(content_type="application/xml")` sets a fixed `Content-Type` for that endpoint.
- `Content-Type` cannot be set through a `Header` parameter, since it is reserved to `RawBody` (see the marker table above).
- `RawBody` cannot be combined with `Body`, `Field`, or `Part` on the same endpoint, since a request can only have one body encoding.

### Dynamic parameter bags

`Query`, `Field`, `Header`, and `Cookie` also accept a `dict[str, ...] | None` parameter as a "bag" of keys that are only known at call time, instead of one fixed wire name per parameter:

```python
class Shop(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(
        self,
        item_id: int,
        extra_headers: Annotated[dict[str, str] | None, Header] = None,
        extra_query: Annotated[dict[str, str | list[str]] | None, Query] = None,
    ) -> Item: ...  # type: ignore[empty-body]


shop.get_item(item_id=7, extra_headers={"X-Trace-Id": "abc123"}, extra_query={"tag": ["a", "b"]})
```

- `Query` and `Field` bag values may be a scalar or a `Sequence` (rendered as a repeated key), the same as their named-parameter form. `Header` and `Cookie` bag values are scalar-only, also matching their named-parameter form.
- `None` for the whole bag sends no extra entries. `None` for one bag key omits just that key.
- At most one bag per kind (`Header`, `Query`, `Field`, `Cookie`) per endpoint, but one of each kind can coexist on the same endpoint.
- Bracket syntax (`Header["x"]`) is rejected on a bag: a bag has no single wire slot to rebind.
- A bag key colliding with a named parameter, a static inline query value, or another already-set key raises `ValueError` at call time, regardless of whether the two values are equal, so a call is never silently ambiguous about which value wins.
- A `Header` bag entry keyed `Content-Type` or `Cookie` (any casing) raises `ValueError`, same as a named `Header` parameter.
- A `Cookie` bag respects the jar the same way a named `Cookie` parameter does: the jar wins for every key the bag contributes, unless the bag itself is declared `Cookie(override=True)`, in which case the bag wins for every key.
- There is no `Path` bag: every path segment is a fixed, required placeholder from the URL template, so there is no "extra" segment for a bag to add.

### Response handling

By default, every response is checked by `response.raise_for_status()` before decoding, so errors during the request raise `httpx.HTTPStatusError` automatically. This behavior can be overridden by `@response_handler` and `@streaming_response_handler` class decorators.

```python
import httpx
from mxhttp import response_handler, streaming_response_handler


def ignore_errors(response: httpx.Response) -> httpx.Response:
    return response


@response_handler(ignore_errors)
@streaming_response_handler(ignore_errors)
class Shop(SyncConsumer): ...
```

The `@response_handler` hook runs on buffered responses before decoding. For streaming and SSE endpoints, `@streaming_response_handler` inspects the initial status line and headers before chunks or events are yielded.

### Retries

Configure automatic retries with exponential backoff via `@retry` on the class, so individual endpoints do not need to hand-roll a retry loop:

```python
from mxhttp import Retry, SyncConsumer, get, retry


@retry(Retry(attempts=3, on={429, 500, 502, 503, 504}))
class Shop(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]
```

- The response or exception of the last attempt (still checked by the response handler) is what is ultimately returned or raised.
- Only applies to regular (non-streaming, non-SSE) endpoints.
- `on` accepts a mix of status codes, exception types, and `Callable[[httpx.Response], bool]` predicates. Any single entry matching the outcome of an attempt triggers a retry. Combine conditions with `and` inside one predicate if all of them must hold at once.
- When a matched response carries a `Retry-After` header (seconds or an HTTP date), its delay is used instead of the computed backoff if it is larger, still capped by `max_delay`. Set `respect_retry_after=False` on `Retry` to always use the computed backoff.
- Pass `retry=` directly to `@get`/`@post`/etc. to override the class `Retry` config for that one endpoint, or `retry=None` to disable retries for it:

```python
class Shop(SyncConsumer):
    @get("/items/{item_id}", retry=Retry(attempts=5))
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @post("/items", retry=None)
    def create_item(self, item: Annotated[NewItem, Body]) -> Item: ...  # type: ignore[empty-body]
```

### Rate limiting

Configure a maximum call rate with `@ratelimit` on the class, so individual endpoints do not need to hand-roll custom throttling:

```python
from mxhttp import RateLimit, SyncConsumer, get, ratelimit


@ratelimit(RateLimit(calls=5, period=1))
class Shop(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]
```

- The limit is scoped to the `(host, port)` being called, shared across consumer instances and endpoints using the same configuration.
- Unlike `@retry`, this applies to every endpoint kind, including streaming, SSE, and downloads.
- By default, a call over the limit blocks until the current window resets. Set `block=False` to raise `RateLimitExceededError` immediately instead, or `max_delay=` to raise instead of blocking past that duration in seconds.
- Pass `key="custom_pool"` to `RateLimit` to partition rate limits into dedicated pools or keep method quotas isolated from each other.
- Pass `ratelimit=` directly to `@get`/`@post`/etc. to override the class configuration for that one endpoint, or `ratelimit=None` to disable rate limiting for it.

### Concurrency control

Configure maximum simultaneous in-flight requests with `@concurrency` on the class:

```python
from mxhttp import Concurrency, SyncConsumer, concurrency, get


@concurrency(Concurrency(limit=5))
class Shop(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]
```

- `@concurrency(5)` accepts an integer shorthand for `limit`.
- Semaphores are scoped to `(host, port)` and shared across instances using the same pool key.
- By default, requests over the limit block until a slot is released. Set `block=False` to raise `ConcurrencyExceededError` immediately instead, or set `timeout=` to raise `ConcurrencyTimeoutError` if waiting exceeds that duration in seconds.
- Pass `key="custom_pool"` to `Concurrency` to isolate concurrency quotas between different services or endpoints.
- Pass `concurrency=` directly to `@get`/`@post`/etc. to override the class configuration for that one endpoint, or `concurrency=None` to disable concurrency limiting for it.

### Streaming responses

Annotate the return type as `Iterator[bytes]` (sync) or `AsyncIterator[bytes]` (async) to stream the response body in chunks.

```python
from collections.abc import AsyncIterator, Iterator


class Files(SyncConsumer):
    @get("/files/{file_id}")
    def download(self, file_id: int) -> Iterator[bytes]: ...  # type: ignore[empty-body]


for chunk in shop_files.download(file_id=7):
    ...


class AsyncFiles(AsyncConsumer):
    @get("/files/{file_id}")
    def download(self, file_id: int) -> AsyncIterator[bytes]: ...  # type: ignore[empty-body]


async for chunk in await shop_async_files.download(file_id=7):
    ...
```

`httpx` decompresses chunks before responding according to `Content-Encoding` (gzip, deflate, br, or zstd).

Streaming responses run `@streaming_response_handler` instead of `@response_handler` (defaulting to `raise_for_status`). The handler can only inspect the status line and headers.

#### Resumable downloads

Pass `resumable=Retry(...)` to `@get` on a byte-streaming endpoint to reconnect with a `Range` request instead of restarting from scratch if the connection drops mid-download:

```python
class Files(SyncConsumer):
    @get("/files/{file_id}", resumable=Retry(attempts=3, backoff=1.0))
    def download(self, file_id: int) -> Iterator[bytes]: ...  # type: ignore[empty-body]
```

- Only valid for `GET` endpoints returning `Iterator[bytes]`/`AsyncIterator[bytes]`. Raises `TypeError` at class definition time otherwise.
- The `Retry` configuration controls reconnect attempts and backoff the same way it does for regular endpoints. `on` decides which mid-stream exceptions trigger a reconnect.
- If the server sent an `ETag` or `Last-Modified` header on the first response, it is sent back as `If-Range` on reconnects.
- If a reconnect receives a full (`200`) response instead of a partial (`206`) response, meaning the server ignored `Range` or the underlying resource changed, `ResumeLostError` is raised rather than silently restarting or splicing mismatched bytes.

#### Resumable downloads to disk

Annotate the return type as `Downloader` (sync) or `AsyncDownloader` (async) instead of `Iterator[bytes]` to get a callable that downloads straight to a file, resuming an interrupted download by calling it again with the same path, even across separate process runs:

```python
from mxhttp import Downloader, TqdmProgress


class Files(SyncConsumer):
    @get("/files/{file_id}")
    def download(self, file_id: int) -> Downloader: ...  # type: ignore[empty-body]


downloader = shop_files.download(file_id=7)
path = downloader(
    "/tmp/report.pdf",
    on_progress=TqdmProgress(desc="Downloading report"),
)

# if the process is interrupted, calling it again resumes from disk:
path = shop_files.download(file_id=7)("/tmp/report.pdf")
```

- The endpoint is called once to bind it (no network activity yet). The returned `Downloader`/`AsyncDownloader` is then called with a destination path to run or resume the download.
- Reconnects the same way `resumable=` streaming does, defaulting to `Retry()` if no `resumable=` override is given.
- Downloads to `{path}.part` plus a `{path}.part.json` sidecar recording the source URL and `ETag`/`Last-Modified`. Only on a clean finish is `{path}.part` atomically renamed to `path` and the sidecar removed, so `path` itself is never observed half-written.
- Non-blocking advisory file locking protects concurrent writers from data corruption, raising `DownloadLockError` on contention and releasing cleanly on process exit or termination signals.
- Calling the `Downloader` again for the same `path` resumes from `{path}.part` if its sidecar identity matches the current request. A mismatch raises `DownloadIdentityError` rather than silently appending to or discarding data. Pass `overwrite=True` to discard existing data and start over.
- Pass `on_progress=` to receive progress updates `(received_bytes, total_bytes_or_None)` as chunks arrive, or pass `TqdmProgress(desc="Downloading file")` for built-in terminal progress bars.

#### Multi-part parallel downloads

Configure multi-part segmented downloading for `Downloader` or `AsyncDownloader` endpoints via `parts=`:

```python
from mxhttp import Downloader, Parts, SyncConsumer, TqdmProgress, get


class Files(SyncConsumer):
    @get("/large-files/{file_id}", parts=Parts(count=4, min_part_size=10 * 1024 * 1024))
    def download(self, file_id: int) -> Downloader: ...  # type: ignore[empty-body]


downloader = files.download(file_id=42)
path = downloader(
    "/tmp/dataset.tar.gz",
    on_progress=TqdmProgress(desc="Downloading dataset"),
)
```

- `@get(..., parts=4)` accepts an integer shorthand for `Parts(count=4)`.
- Multi-part probing queries the server with a range request `Range: bytes=0-0`. If the server returns partial content (`206`), parallel workers download disjoint segments simultaneously (`.part.0`, `.part.1`, ...).
- If the server does not support byte ranges (returns `200`) or if file size is below `min_part_size`, `mxhttp` falls back cleanly to single-stream downloading without failing.
- Segments are re-assembled into the destination file upon completion.
- Each segment supports resumption independently. Interrupted downloads resume remaining bytes for incomplete parts from disk.
- Pass `parts=` on the downloader call `downloader(path, parts=8)` to override endpoint defaults at runtime.
- Pass `on_part_progress=` to receive slice-level updates `(part_index, received_bytes, total_bytes)` for each worker.
- Pass `on_progress=TqdmProgress(desc="Downloading", per_part=True)` to render an overall progress bar together with individual sub-bars for each active part.

#### Checksum verification

Validate data integrity or compute cryptographic digests for `Downloader` and `AsyncDownloader` endpoints via `checksum=`:

```python
from mxhttp import Checksum, Downloader, SyncConsumer, get


class Releases(SyncConsumer):
    @get("/downloads/{version}", parts=4)
    def fetch_release(self, version: str) -> Downloader: ...  # type: ignore[empty-body]


releases = Releases()
downloader = releases.fetch_release(version="v1.0.0")

# Validate against expected SHA-256 hash (raises ChecksumMismatchError on mismatch):
path = downloader(
    "/tmp/release.tar.gz",
    checksum="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
)

# Or capture computed digest:
cs = Checksum.sha256()
path = downloader("/tmp/release.tar.gz", checksum=cs, on_checksum=print)
print(f"Calculated hash: {cs.digest}")
```

- `checksum=` accepts hex strings (64-character SHA-256, 128-character SHA-512, 32-character MD5), prefixed strings (`"sha256:<hex>"`), algorithm names (`"sha256"`), or `Checksum` objects (`Checksum.sha256()`).
- Mismatch raises `ChecksumMismatchError` without replacing the destination path.
- Hashes are computed in-stream during download and segment assembly.

### Server-Sent Events

Annotate the return type as `Iterator[Event]` (sync) or `AsyncIterator[Event]` (async) to parse the response as a Server-Sent Events stream instead of raw bytes:

```python
from collections.abc import Iterator
from mxhttp import Event


class Chat(SyncConsumer):
    @get("/stream")
    def events(self) -> Iterator[Event]: ...  # type: ignore[empty-body]


for event in chat.events():
    print(event.event, event.data)
```

`Event` provides four attributes: `data`, `event`, `id`, and `retry`:

- `data` is the raw payload, decoded manually if the server sends JSON.
- Multi-line `data` fields are joined with newline characters.
- `id` and `retry` persist across events once set and reset on reconnect only.
- An event without a trailing blank line at the end of the stream is discarded.

SSE streams use `@streaming_response_handler` matching byte streaming above.

### Authentication

Pass `auth=` to the consumer constructor, accepting anything `httpx.Client`/`httpx.AsyncClient` support: an `httpx.Auth` instance (`httpx.BasicAuth`, `httpx.DigestAuth`, or a custom multi-step flow), or a `(username, password)` tuple as Basic auth shorthand.

```python
shop = Shop(auth=httpx.BasicAuth("alice", "secret"))
```

### Further configuration

The underlying `httpx.Client` or `httpx.AsyncClient` is stored at `.session` to set default headers or other client options after construction.

## Tests

```bash
pytest
```

## Acknowledgements

`mxhttp` is inspired by [Uplink](https://github.com/prkumar/uplink) combined with modern Python typing features.

## License

MIT
