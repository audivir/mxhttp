# mxhttp

Declarative **HTTP** client on top of _**m**sgspec_ and _http**x**_. Write an API as a class of annotated stub methods and `mxhttp` will handle the rest (request building, sending, and response decoding).

## Install

```bash
pip install mxhttp
```

## Usage

```python
from typing import Annotated
import msgspec
from mxhttp import Body, Query, SyncConsumer, get, post


class Item(msgspec.Struct):
    id: int
    name: str
    price: float


class NewItem(msgspec.Struct):
    name: str
    price: float


class Shop(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @get("/search")
    def search(self, q: Annotated[str, Query], limit: Annotated[int, Query] = 20) -> list[Item]: ...  # type: ignore[empty-body]

    @post("/items")
    def create_item(self, item: Annotated[NewItem, Body]) -> Item: ...  # type: ignore[empty-body]


shop = Shop("https://api.example.com")
item = shop.get_item(item_id=7)
new = shop.create_item(item=NewItem(name="Gadget", price=4.5))
```

The method body is never run as it is replaced by the decorator. Parameters are bound based on their `Annotated[...]` marker:

| Marker class | Request Target | Info |
|----------------------|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Path` (or implicit) | Path | Matched by parameter name unless annotated explicitly (`Path["name"]`). Must be a non-nullable `str`, `int`, or `float`. |
| `RawPath` | Path | Like `Path`, but spliced into the URL without percent-encoding, bypassing any query-parameter checks (duplicate keys, structure, etc.) that normally apply. Must be a non-nullable `str`. Use only for values that are already encoded relative paths or query fragments. |
| `Query` | Query | Must be a nullable `str`, `int`, `float`, or `bool`, or a `Sequence` of those, sent as `key=a&key=b&...`. |
| `Field` | Form Field | `application/x-www-form-urlencoded`. Accepts same types as `Query`. |
| `Part` | Multipart File Part | Forces the whole request to be multipart and any `Field` params on the same call will become multipart fields as well. Accepts the same types `httpx` takes for `files=`. |
| `Header` | HTTP Header | Must be `str`, `int`, `float`, or `bool`, but no list of those. |
| `Cookie` | Cookie | Is superseded by the cookie jar of the client if it already has a same-named cookie, unless `override=True` is set. Accepts same types as `Header` |
| `Body` | JSON Body | Whole object, serialized with `msgspec.to_builtins`. Can't be a scalar type. |

- Use `Path["name"]`, `Query["name"]`, `Field["name"]`, `Header["name"]`, or `Cookie["name"]` to bind under a different name than the parameter (e.g. reserved `from`, or a header like `X-Request-Id`, unsupported string format arguments like `?`).
- `None`-valued `Query`, `Field`, `Header`, and `Cookie` parameters are omitted from the request.
- `Path` parameters cannot be optional as a placeholder cannot be ommited from the URL.
- Mismatched marker/type combinations raise a `TypeError` as soon as the class body runs, not at call time.
- `Literal[...]` and `Enum` types are accepted where scalar types are (`Path`, `Query`, `Field`, `Header`, `Cookie`). Every literal value or enum member value must be `str`, `int`, or `float` (plus `bool` outside of `Path`). `Enum` members are serialized by their `.value`.

### Inline query parameters

A path template can bake query parameters directly into the string:

```python
class Shop(SyncConsumer):
    @get("/items?category={cat}")
    def by_category(self, cat: str) -> list[Item]: ...  # type: ignore[empty-body]
```

- An unmarked parameter binds implicitly to a `{name}` placeholder, same mechanism as `Path`.
- `Query["cat"]` binds it to a different parameter name. Unlike every other `Marker`, the brackets aren't the wire name here — the wire name is whatever query key the template assigned to `{cat}`.
- A query entry with no placeholder (e.g. `?active=true`) is a static value sent on every call. That key also cannot be reused by a dynamic `Query` parameter.
- Each query key, and each placeholder field, can only be used once per path template (e.g. `/things?a={x}&a={y}` is rejected).
- A placeholder field also cannot be reused by a real path segment (`/{id}?other={id}` is rejected).
- A placeholder cannot be mixed with literal text in the same value (`key=prefix{name}`), and cannot stand in for the key itself (`{name}` with no `=`).
- Every `{name}` field must be bound by exactly one parameter (implicit, `Path["name"]`, or `Query["name"]`).
- An inline query field cannot be a `Sequence` as the placeholder reserves exactly one query spot.
- All of the above raise a `TypeError` as soon as the class body runs, not at call time.

### Decoding the response

The return type defines the reponse decoding:

- `httpx.Response` for the raw response.
- `str` or `bytes` for the corresponding `.text` or `.content` with no JSON round-trip.
- `pydantic.BaseModel` subclasses via their own `.model_validate_json`.
- Anything else `msgspec.json.decode` can decode: `msgspec.Struct`, dataclasses, `TypedDict`, `NamedTuple`, and `list`, `dict`, or other containers of those.
- `Response[Item]` for a small struct with the decoded `Item` as `.data` and the raw `httpx.Response` in `.response`.
- Plain `attrs` classes are decoded by `msgspec`, for type hinting `attrs` is needed as dependency.

For an async client, subclass `AsyncConsumer` and declare the methods `async def`, everything else stays the same.

### Response handling

By default, every response is checked by `response.raise_for_status()` before decoding, so errors during the request raise `httpx.HTTPStatusError` automatically. This behavior can be overriden by `@response_handler` decorator for the class.

```python
import httpx
from mxhttp import response_handler


def ignore_errors(response: httpx.Response) -> httpx.Response:
    return response


@response_handler(ignore_errors)
class Shop(SyncConsumer): ...
```

The hook runs on every response before decoding.

### Retries

Configure automatic retries with exponential backoff via `@retry` on the class, so individual endpoints don't need to hand-roll their own retry loop:

```python
from mxhttp import Retry, SyncConsumer, get, retry


@retry(Retry(attempts=3, on={429, 500, 502, 503, 504}))
class Shop(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]
```

- The last attempt's response (still checked by the response handler) or exception is what's ultimately raised/returned.
- Only applies to regular (non-streaming, non-SSE) endpoints.
- `on` accepts a mix of status codes, exception types, and `Callable[[httpx.Response], bool]` predicates. Any single entry matching the outcome of an attempt triggers a retry; combine conditions with `and` inside one predicate if you need all of them to hold at once.
- Pass `retry=` directly to `@get`/`@post`/etc. to override the class's `Retry` config for that one endpoint, or `retry=None` to disable retries for it:

```python
class Shop(SyncConsumer):
    @get("/items/{item_id}", retry=Retry(attempts=5))
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @post("/items", retry=None)
    def create_item(self, item: Annotated[NewItem, Body]) -> Item: ...  # type: ignore[empty-body]
```

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

`httpx` already decompresses chunks before responding according to `Content-Encoding` (gzip/deflate/br/zstd).

Streaming responses run `@streaming_response_handler` instead of `@response_handler` (defaults to `raise_for_status` as well).
The handler can only inspect status line and headers.

```python
from mxhttp import streaming_response_handler


def check_status(response: httpx.Response) -> httpx.Response:
    response.raise_for_status()
    return response


@streaming_response_handler(check_status)
class Files(SyncConsumer): ...
```

### Server-Sent Events

Annotate the return type as `Iterator[Event]` (sync) or `AsyncIterator[Event]` (async) to parse the response as a Server-Sent Events stream instead of raw bytes:

```python
from collections.abc import Iterator
from mxhttp import Event


class Chat(SyncConsumer):
    @get("/stream")
    def events(self) -> Iterator[Event]: ...  # type: ignore[empty-body]


for event in chat.events():
    print(event.event, event.data)  # event.event defaults to "message"
```

`Event` has four attributes, `data`, `event`, `id`, and `retry`:

- `data` is the raw payload, decode it manually if the server sends JSON.
- Multi-line `data` fields are joined with `\n`.
- `id` and `retry` persist across events once set and reset on reconnect only.
- An event without a trailing blank line at the end of the stream is discarded.

SSE streams use `@streaming_response_handler` matching byte streaming above.

## Further configuration

The underlying `httpx.Client` or `httpx.AsyncClient` is stored at `.session` to set default headers, auth, or timeouts.

## Typing

The package and all its generators are typed.

## Tests

```bash
pytest
```

## Acknowledgements

`mxhttp` is inspired by [Uplink](https://github.com/prkumar/uplink) but combining it with Python typing features.
