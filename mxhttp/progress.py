"""Progress bar callbacks for mxhttp downloads."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

import msgspec

if TYPE_CHECKING:
    from types import TracebackType

    from tqdm import tqdm
    from typing_extensions import Self


class TqdmProgress(msgspec.Struct):
    """Thread-safe progress callback integrating with `tqdm`."""

    desc: str = "Downloading"
    unit: str = "B"
    unit_scale: bool = True
    unit_divisor: int = 1024
    mininterval: float = 0.1
    file: Any = None
    _pb: Any = None
    _lock: threading.RLock = msgspec.field(default_factory=threading.RLock)

    @property
    def progress_bar(self) -> tqdm | None:
        """Returns the underlying tqdm instance if initialized."""
        return self._pb

    def start(self, initial: int, total: int | None) -> tqdm:
        """Initializes and returns the tqdm progress instance."""
        try:
            from tqdm import tqdm
        except ImportError as e:
            raise ImportError(
                "tqdm is required to use TqdmProgress. Install it via pip install tqdm."
            ) from e

        return tqdm(
            total=total,
            initial=initial,
            desc=self.desc,
            unit=self.unit,
            unit_scale=self.unit_scale,
            unit_divisor=self.unit_divisor,
            mininterval=self.mininterval,
            file=self.file,
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

    def close(self) -> None:
        """Closes the underlying progress bar."""
        with self._lock:
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
