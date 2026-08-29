"""Defines models for the mxhttp API tests module."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, TypedDict

import attrs
import httpx  # noqa: TC002
import msgspec
from pydantic import BaseModel

from mxhttp import (
    AsyncConsumer,
    Body,
    RawPath,
    SyncConsumer,
    base_url,
    delete,
    get,
    head,
    patch,
    post,
    put,
)


class Item(msgspec.Struct):
    id: int
    name: str
    price: float


ITEM = Item(id=1, name="Widget", price=9.99)
ITEM_BUILTINS = msgspec.to_builtins(ITEM)


class NewItem(msgspec.Struct):
    name: str
    price: float


@base_url("https://api.example.com")
class PathApi(SyncConsumer):
    @get("/users/{user_id}/posts/{post_id}")
    def get_post(self, user_id: str, post_id: int) -> Item: ...  # type: ignore[empty-body]


@base_url("https://api.example.com")
class AsyncPathApi(AsyncConsumer):
    @get("/users/{user_id}/posts/{post_id}")
    async def get_post(self, user_id: str, post_id: int) -> Item: ...  # type: ignore[empty-body]


@base_url("https://api.example.com")
class RawPathApi(SyncConsumer):
    @get("{raw}")
    def fetch(self, raw: Annotated[str, RawPath]) -> Item: ...  # type: ignore[empty-body]


@base_url("https://api.example.com")
class AsyncRawPathApi(AsyncConsumer):
    @get("{raw}")
    async def fetch(self, raw: Annotated[str, RawPath]) -> Item: ...  # type: ignore[empty-body]


@base_url("https://api.example.com")
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


@base_url("https://api.example.com")
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


@base_url("https://api.example.com")
class RawApi(SyncConsumer):
    @get("/ping")
    def ping(self) -> httpx.Response: ...  # type: ignore[empty-body]


@base_url("https://api.example.com")
class AsyncRawApi(AsyncConsumer):
    @get("/ping")
    async def ping(self) -> httpx.Response: ...  # type: ignore[empty-body]


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


@base_url("https://api.example.com")
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
