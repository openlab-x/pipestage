from __future__ import annotations

from collections.abc import AsyncIterable, Iterable
from importlib.metadata import version

from ._stream import Stream
from ._utils import to_async_iter

__version__ = version("pipestage")
__all__ = ["stream", "Stream"]


def stream(source: Iterable | AsyncIterable) -> Stream:
    return Stream(to_async_iter(source))
