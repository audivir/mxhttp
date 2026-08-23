"""Defines models for mxhttp API tests."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator  # noqa: TC003
from dataclasses import dataclass
from typing import Annotated, TypedDict

import attrs
import httpx
import msgspec
from pydantic import BaseModel

from mxhttp import (
    AsyncConsumer,
    Body,
    Cookie,
    Event,
    Field,
    Header,
    Part,
    PartValue,
    Query,
    Response,
    SyncConsumer,
    delete,
    get,
    head,
    patch,
    post,
    put,
)
from mxhttp.response import response_handler, streaming_response_handler


class Item(msgspec.Struct):
    id: int
    name: str
    price: float


ITEM = Item(id=1, name="Widget", price=9.99)


class NewItem(msgspec.Struct):
    name: str
    price: float


class PathApi(SyncConsumer):
    @get("/users/{user_id}/posts/{post_id}")
    def get_post(self, user_id: str, post_id: int) -> Item: ...  # type: ignore[empty-body]


class AsyncPathApi(AsyncConsumer):
    @get("/users/{user_id}/posts/{post_id}")
    async def get_post(self, user_id: str, post_id: int) -> Item: ...  # type: ignore[empty-body]


class QueryApi(SyncConsumer):
    @get("/search")
    def search(  # type: ignore[empty-body]
        self,
        q: Annotated[str, Query],
        limit: Annotated[int, Query] = 20,
        active: Annotated[bool | None, Query] = None,
        category: Annotated[str | None, Query["cat"]] = None,
    ) -> Item: ...


class AsyncQueryApi(AsyncConsumer):
    @get("/search")
    async def search(  # type: ignore[empty-body]
        self,
        q: Annotated[str, Query],
        limit: Annotated[int, Query] = 20,
        active: Annotated[bool | None, Query] = None,
        category: Annotated[str | None, Query["cat"]] = None,
    ) -> Item: ...


class ListQueryApi(SyncConsumer):
    @get("/search")
    def search(self, tags: Annotated[list[str], Query]) -> Item: ...  # type: ignore[empty-body]


class ListFieldApi(SyncConsumer):
    @post("/login")
    def login(self, tags: Annotated[list[int], Field]) -> Item: ...  # type: ignore[empty-body]


class HeaderApi(SyncConsumer):
    @get("/things")
    def get_thing(  # type: ignore[empty-body]
        self,
        request_id: Annotated[str, Header["X-Request-Id"]],
        trace: Annotated[str | None, Header] = None,
    ) -> httpx.Response: ...


class AsyncHeaderApi(AsyncConsumer):
    @get("/things")
    async def get_thing(  # type: ignore[empty-body]
        self,
        request_id: Annotated[str, Header["X-Request-Id"]],
        trace: Annotated[str | None, Header] = None,
    ) -> httpx.Response: ...


class CookieApi(SyncConsumer):
    @get("/things")
    def get_thing(  # type: ignore[empty-body]
        self,
        session_id: Annotated[str, Cookie["session_id"]],
        theme: Annotated[str | None, Cookie] = None,
    ) -> httpx.Response: ...


class AsyncCookieApi(AsyncConsumer):
    @get("/things")
    async def get_thing(  # type: ignore[empty-body]
        self,
        session_id: Annotated[str, Cookie["session_id"]],
        theme: Annotated[str | None, Cookie] = None,
    ) -> httpx.Response: ...


class OverrideCookieApi(SyncConsumer):
    @get("/things")
    def get_thing(  # type: ignore[empty-body]
        self, session_id: Annotated[str, Cookie("session_id", override=True)]
    ) -> httpx.Response: ...


class FormApi(SyncConsumer):
    @post("/login")
    def login(  # type: ignore[empty-body]
        self,
        username: Annotated[str, Field],
        password: Annotated[str, Field["pass"]],
        remember: Annotated[bool | None, Field] = None,
    ) -> Item: ...


class AsyncFormApi(AsyncConsumer):
    @post("/login")
    async def login(  # type: ignore[empty-body]
        self,
        username: Annotated[str, Field],
        password: Annotated[str, Field["pass"]],
        remember: Annotated[bool | None, Field] = None,
    ) -> Item: ...


class BodyApi(SyncConsumer):
    @post("/items")
    def create_item(self, item: Annotated[NewItem, Body]) -> Item: ...  # type: ignore[empty-body]


class AsyncBodyApi(AsyncConsumer):
    @post("/items")
    async def create_item(self, item: Annotated[NewItem, Body]) -> Item: ...  # type: ignore[empty-body]


class UploadApi(SyncConsumer):
    @post("/upload")
    def upload(  # type: ignore[empty-body]
        self,
        description: Annotated[str, Field],
        file: Annotated[PartValue, Part],
    ) -> Item: ...


class AsyncUploadApi(AsyncConsumer):
    @post("/upload")
    async def upload(  # type: ignore[empty-body]
        self,
        description: Annotated[str, Field],
        file: Annotated[PartValue, Part],
    ) -> Item: ...


class BareBytesUploadApi(SyncConsumer):
    @post("/upload")
    def upload(self, file: Annotated[bytes, Part]) -> Item: ...  # type: ignore[empty-body]


class HeadersUploadApi(SyncConsumer):
    @post("/upload")
    def upload(  # type: ignore[empty-body]
        self, file: Annotated[tuple[str, bytes, str, dict[str, str]], Part]
    ) -> Item: ...


class CrudApi(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @post("/items")
    def create_item(self, item: Annotated[NewItem, Body]) -> Item: ...  # type: ignore[empty-body]

    @put("/items/{item_id}")
    def replace_item(self, item_id: int, item: Annotated[NewItem, Body]) -> Item: ...  # type: ignore[empty-body]

    @patch("/items/{item_id}")
    def update_item(self, item_id: int, item: Annotated[NewItem, Body]) -> Item: ...  # type: ignore[empty-body]

    @delete("/items/{item_id}")
    def delete_item(self, item_id: int) -> httpx.Response: ...  # type: ignore[empty-body]

    @head("/items/{item_id}")
    def head_item(self, item_id: int) -> httpx.Response: ...  # type: ignore[empty-body]


class AsyncCrudApi(AsyncConsumer):
    @get("/items/{item_id}")
    async def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]

    @post("/items")
    async def create_item(self, item: Annotated[NewItem, Body]) -> Item: ...  # type: ignore[empty-body]

    @put("/items/{item_id}")
    async def replace_item(self, item_id: int, item: Annotated[NewItem, Body]) -> Item: ...  # type: ignore[empty-body]

    @patch("/items/{item_id}")
    async def update_item(self, item_id: int, item: Annotated[NewItem, Body]) -> Item: ...  # type: ignore[empty-body]

    @delete("/items/{item_id}")
    async def delete_item(self, item_id: int) -> httpx.Response: ...  # type: ignore[empty-body]

    @head("/items/{item_id}")
    async def head_item(self, item_id: int) -> httpx.Response: ...  # type: ignore[empty-body]


class NoneApi(SyncConsumer):
    @delete("/items/{item_id}")
    def delete_item(self, item_id: int) -> None: ...


class AsyncNoneApi(AsyncConsumer):
    @delete("/items/{item_id}")
    async def delete_item(self, item_id: int) -> None: ...


def raise_on_error(response: httpx.Response) -> httpx.Response:
    if response.is_error:
        raise ApiError(response.status_code, response.content)
    return response


@response_handler(raise_on_error)
class StrictApi(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


@response_handler(raise_on_error)
class AsyncStrictApi(AsyncConsumer):
    @get("/items/{item_id}")
    async def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


class ApiError(Exception):
    def __init__(self, status_code: int, body: bytes) -> None:
        """Initializes the exception with the HTTP status code and response body."""
        super().__init__(f"HTTP {status_code}: {body.decode(errors='replace')}")
        self.status_code = status_code


def unwrap_envelope(response: httpx.Response) -> httpx.Response:
    envelope = msgspec.json.decode(response.content)
    return httpx.Response(response.status_code, json=envelope["data"], request=response.request)


@response_handler(unwrap_envelope)
class EnvelopeApi(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Item: ...  # type: ignore[empty-body]


class WrappedResponseApi(SyncConsumer):
    @get("/items/{item_id}")
    def get_item(self, item_id: int) -> Response[Item]: ...  # type: ignore[empty-body]


class AsyncWrappedResponseApi(AsyncConsumer):
    @get("/items/{item_id}")
    async def get_item(self, item_id: int) -> Response[Item]: ...  # type: ignore[empty-body]


class GetNestedUnionApi(SyncConsumer):
    @get("/values")
    def get_nested_union(self) -> Response[list[int | None]]: ...  # type: ignore[empty-body]


class RawApi(SyncConsumer):
    @get("/ping")
    def ping(self) -> httpx.Response: ...  # type: ignore[empty-body]


class AsyncRawApi(AsyncConsumer):
    @get("/ping")
    async def ping(self) -> httpx.Response: ...  # type: ignore[empty-body]


class StreamApi(SyncConsumer):
    @get("/download/{file_id}")
    def download(self, file_id: int) -> Iterator[bytes]: ...  # type: ignore[empty-body]


class AsyncStreamApi(AsyncConsumer):
    @get("/download/{file_id}")
    async def download(self, file_id: int) -> AsyncIterator[bytes]: ...  # type: ignore[empty-body]


class SseApi(SyncConsumer):
    @get("/events")
    def events(self) -> Iterator[Event]: ...  # type: ignore[empty-body]


class AsyncSseApi(AsyncConsumer):
    @get("/events")
    async def events(self) -> AsyncIterator[Event]: ...  # type: ignore[empty-body]


def raise_on_error_status_only(response: httpx.Response) -> httpx.Response:
    if response.is_error:
        raise ApiError(response.status_code, b"")
    return response


@streaming_response_handler(raise_on_error_status_only)
class StrictStreamApi(SyncConsumer):
    @get("/download/{file_id}")
    def download(self, file_id: int) -> Iterator[bytes]: ...  # type: ignore[empty-body]


@streaming_response_handler(raise_on_error_status_only)
class AsyncStrictStreamApi(AsyncConsumer):
    @get("/download/{file_id}")
    async def download(self, file_id: int) -> AsyncIterator[bytes]: ...  # type: ignore[empty-body]


class TypedDictItem(TypedDict):
    id: int


@dataclass
class DataclassItem:
    id: int


class BaseModelItem(BaseModel):
    id: int


@attrs.define
class AttrsItem:
    id: int


class MoreApi(SyncConsumer):
    @get("/str")
    def get_string(self) -> str: ...  # type: ignore[empty-body]

    @get("/bytes")
    def get_bytes(self) -> bytes: ...  # type: ignore[empty-body]

    @get("/int-list")
    def get_int_list(self) -> list[int]: ...  # type: ignore[empty-body]

    @get("/item-list")
    def get_item_list(self) -> list[Item]: ...  # type: ignore[empty-body]

    @get("/str-int-dict")
    def get_str_int_dict(self) -> dict[str, int]: ...  # type: ignore[empty-body]

    @get("/typed-dict")
    def get_typed_dict(self) -> TypedDictItem: ...  # type: ignore[empty-body]

    @get("/dataclass")
    def get_dataclass(self) -> DataclassItem: ...  # type: ignore[empty-body]

    @get("/attrs")
    def get_attrs(self) -> AttrsItem: ...  # type: ignore[empty-body]

    @get("/base-model")
    def get_base_model(self) -> BaseModelItem: ...  # type: ignore[empty-body]
