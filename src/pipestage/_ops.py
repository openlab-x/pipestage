from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator, Callable
from typing import Any

from ._utils import _coerce, to_async_iter

_SKIP: Any = object()


def _cancel_all(tasks: list[asyncio.Task[Any]]) -> None:
    # synchronous — safe to call from inside an async generator's except/finally block
    # (awaiting inside those blocks is unreliable when the frame is closed externally)
    for t in tasks:
        if not t.done():
            t.cancel()


async def _map_serial(
    source: AsyncIterable[Any], fn: Callable[..., Any]
) -> AsyncIterator[Any]:
    async for item in source:
        yield await _coerce(fn, item)


async def _map_ordered(
    source: AsyncIterable[Any], fn: Callable[..., Any], concurrency: int
) -> AsyncIterator[Any]:
    # Tasks are created eagerly; semaphore limits active execution, not creation.
    sem = asyncio.Semaphore(concurrency)

    async def bounded(item: Any) -> Any:
        async with sem:
            return await _coerce(fn, item)

    tasks: list[asyncio.Task[Any]] = []

    try:
        async for item in source:
            tasks.append(asyncio.create_task(bounded(item)))
    except BaseException:
        _cancel_all(tasks)
        raise

    for i, task in enumerate(tasks):
        try:
            yield await task
        except BaseException:
            _cancel_all(tasks[i + 1 :])
            raise


async def _map_unordered(
    source: AsyncIterable[Any], fn: Callable[..., Any], concurrency: int
) -> AsyncIterator[Any]:
    # results are pushed to a queue as tasks complete, so we emit them as ready
    sem = asyncio.Semaphore(concurrency)
    result_q: asyncio.Queue[tuple[BaseException | None, Any]] = asyncio.Queue()
    tasks: list[asyncio.Task[Any]] = []
    total = 0

    async def worker(item: Any) -> None:
        try:
            async with sem:
                val = await _coerce(fn, item)
            await result_q.put((None, val))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            await result_q.put((exc, None))

    try:
        async for item in source:
            tasks.append(asyncio.create_task(worker(item)))
            total += 1
    except BaseException:
        _cancel_all(tasks)
        raise

    received = 0
    while received < total:
        exc, val = await result_q.get()
        received += 1
        if exc is not None:
            _cancel_all(tasks)
            raise exc
        yield val


def map_stage(
    source: AsyncIterable[Any],
    fn: Callable[..., Any],
    concurrency: int,
    ordered: bool,
) -> AsyncIterator[Any]:
    if concurrency == 1:
        return _map_serial(source, fn)
    if ordered:
        return _map_ordered(source, fn, concurrency)
    return _map_unordered(source, fn, concurrency)


async def filter_stage(
    source: AsyncIterable[Any],
    pred: Callable[..., Any],
    concurrency: int,
    ordered: bool,
) -> AsyncIterator[Any]:
    if concurrency == 1:
        async for item in source:
            if await _coerce(pred, item):
                yield item
        return

    # reuse map_stage for concurrency/ordering logic; sentinel drops non-matching items
    async def apply(item: Any) -> Any:
        return item if await _coerce(pred, item) else _SKIP

    async for result in map_stage(source, apply, concurrency, ordered):
        if result is not _SKIP:
            yield result


async def batch_stage(
    source: AsyncIterable[Any], size: int
) -> AsyncIterator[list[Any]]:
    buf: list[Any] = []
    async for item in source:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


async def flat_map_stage(
    source: AsyncIterable[Any],
    fn: Callable[..., Any],
    concurrency: int,
    ordered: bool,
) -> AsyncIterator[Any]:
    async for sub in map_stage(source, fn, concurrency, ordered):
        async for item in to_async_iter(sub):
            yield item
