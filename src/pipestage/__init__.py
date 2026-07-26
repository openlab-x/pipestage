from __future__ import annotations

from collections.abc import AsyncIterable, Iterable
from importlib.metadata import version
from typing import TypeVar

from ._stream import Stream
from ._utils import to_async_iter

__version__ = version("pipestage")
__all__ = ["stream", "Stream"]

T = TypeVar("T")


def stream(source: Iterable[T] | AsyncIterable[T]) -> Stream[T]:
    return Stream(to_async_iter(source))
