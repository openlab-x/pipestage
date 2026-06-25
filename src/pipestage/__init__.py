from __future__ import annotations

from collections.abc import AsyncIterable, Iterable

from ._stream import Stream
from ._utils import to_async_iter

__version__ = "0.1.0"
__all__ = ["stream", "Stream"]


def stream(source: Iterable | AsyncIterable) -> Stream:
    return Stream(to_async_iter(source))
