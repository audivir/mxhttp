"""Handles Server-Sent Event streams."""

from __future__ import annotations

from typing import get_args

import msgspec

from mxhttp.types import SseField_T

SSE_FIELDS: frozenset[SseField_T] = frozenset(get_args(SseField_T))


class Event(msgspec.Struct):
    """Stores a parsed Server-Sent Event."""

    data: str = ""
    """Raw event payload."""
    event: str = "message"
    id: str | None = None
    retry: int | None = None


class SseBuilder(msgspec.Struct):
    """Builds SSE field lines into `Event` objects, one per blank-line-terminated block."""

    data_lines: list[str] = []
    event: str = "message"
    last_id: str | None = None
    retry: int | None = None

    def feed(self, line: str) -> Event | None:
        """Feeds one decoded line and returns a completed `Event` on a blank line, else `None`."""
        if not line:
            if not self.data_lines:
                return None  # a blank line with nothing accumulated dispatches nothing
            built = Event(
                data="\n".join(self.data_lines),
                event=self.event,
                id=self.last_id,
                retry=self.retry,
            )
            self.data_lines = []
            self.event = "message"
            return built
        if line.startswith(":"):
            return None  # comment line
        field, _, value = line.partition(":")
        if field not in SSE_FIELDS:
            return None  # unrecognized field, per spec
        value = value.removeprefix(" ")
        if field == "data":
            self.data_lines.append(value)
        elif field == "event":
            self.event = value
        elif field == "id":
            self.last_id = value
        elif value.isdigit():  # field == "retry"
            self.retry = int(value)
        return None
