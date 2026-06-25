from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator, Callable
from typing import Any, Generic, TypeVar

from ._ops import batch_stage, filter_stage, flat_map_stage, map_stage

T = TypeVar("T")


def _check_concurrency(n: int) -> None:
    if n < 1:
        raise ValueError(f"concurrency must be >= 1, got {n}")


class Stream(Generic[T]):

    __slots__ = ("_source",)

    def __init__(self, source: AsyncIterable[T]) -> None:
        self._source = source

    def map(
        self,
        fn: Callable[..., Any],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Stream[Any]:
        _check_concurrency(concurrency)
        return Stream(map_stage(self._source, fn, concurrency, ordered))

    def filter(
        self,
        pred: Callable[..., Any],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Stream[T]:
        _check_concurrency(concurrency)
        return Stream(filter_stage(self._source, pred, concurrency, ordered))

    def flat_map(
        self,
        fn: Callable[..., Any],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> Stream[Any]:
        _check_concurrency(concurrency)
        return Stream(flat_map_stage(self._source, fn, concurrency, ordered))

    def batch(self, size: int) -> Stream[list[T]]:
        if size < 1:
            raise ValueError(f"batch size must be >= 1, got {size}")
        return Stream(batch_stage(self._source, size))

    async def collect(self) -> list[T]:
        return [item async for item in self._source]

    async def for_each(
        self,
        fn: Callable[..., Any],
        *,
        concurrency: int = 1,
        ordered: bool = True,
    ) -> None:
        _check_concurrency(concurrency)
        async for _ in map_stage(self._source, fn, concurrency, ordered):
            pass

    def __aiter__(self) -> AsyncIterator[T]:
        return self._source.__aiter__()  # type: ignore[return-value]
