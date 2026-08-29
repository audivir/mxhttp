"""Progress bar callbacks for mxhttp downloads."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, TextIO

import msgspec
from tqdm import tqdm

if TYPE_CHECKING:
    from types import TracebackType

    from typing_extensions import Self


class TqdmProgress(msgspec.Struct):
    """Thread-safe progress callback integrating with `tqdm`."""

    desc: str = "Downloading"
    unit: str = "B"
    unit_scale: bool = True
    unit_divisor: int = 1024
    mininterval: float = 0.1
    file: TextIO | None = None
    per_part: bool = False
    _pb: tqdm | None = None
    _part_pbs: dict[int, tqdm] = msgspec.field(default_factory=dict)
    _lock: threading.RLock = msgspec.field(default_factory=threading.RLock)

    @property
    def progress_bar(self) -> tqdm | None:
        """Returns the underlying tqdm instance if initialized."""
        return self._pb

    @property
    def part_progress_bars(self) -> dict[int, tqdm]:
        """Returns active per-part tqdm instances."""
        return self._part_pbs

    def start(
        self,
        initial: int,
        total: int | None,
        *,
        position: int = 0,
        desc: str | None = None,
        leave: bool = True,
    ) -> tqdm:
        """Initializes and returns a tqdm progress instance."""
        return tqdm(
            total=total,
            initial=initial,
            desc=desc or self.desc,
            unit=self.unit,
            unit_scale=self.unit_scale,
            unit_divisor=self.unit_divisor,
            mininterval=self.mininterval,
            file=self.file,
            position=position,
            leave=leave,
        )

    def __call__(self, current: int, total: int | None) -> None:
        """Updates progress state on each received chunk."""
        with self._lock:
            if self._pb is None:
                self._pb = self.start(initial=current, total=total)
                return

            if self._pb.total is None and total is not None:
                self._pb.total = total
                self._pb.refresh()

            delta = current - self._pb.n
            if delta > 0:
                self._pb.update(delta)

            if total is not None and current >= total:
                self.close()

    def update_part(
        self,
        part_idx: int,
        part_received: int,
        part_total: int,
        current_total: int,
        total_size: int | None,
    ) -> None:
        """Updates per-part and overall progress state."""
        with self._lock:
            if not self.per_part:
                self(current_total, total_size)
                return

            if self._pb is None:
                self._pb = self.start(
                    initial=current_total, total=total_size, position=0, desc=self.desc
                )
            elif self._pb.total is None and total_size is not None:
                self._pb.total = total_size
                self._pb.refresh()

            delta = current_total - self._pb.n
            if delta > 0:
                self._pb.update(delta)

            part_pb = self._part_pbs.get(part_idx)
            if part_pb is None:
                part_pb = self.start(
                    initial=part_received,
                    total=part_total,
                    position=part_idx + 1,
                    desc=f"{self.desc} [part {part_idx}]",
                    leave=False,
                )
                self._part_pbs[part_idx] = part_pb
            else:
                part_delta = part_received - part_pb.n
                if part_delta > 0:
                    part_pb.update(part_delta)

            if part_received >= part_total:
                part_pb.close()

            if total_size is not None and current_total >= total_size:
                self.close()

    def close(self) -> None:
        """Closes the underlying progress bars."""
        with self._lock:
            for part_pb in self._part_pbs.values():
                part_pb.close()
            self._part_pbs.clear()
            if self._pb is not None:
                self._pb.close()
                self._pb = None

    def __enter__(self) -> Self:
        """Enters context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Closes progress bar on context exit."""
        self.close()
