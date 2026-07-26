from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable
from typing import Any

from ._utils import _coerce, to_async_iter

_SKIP: Any = object()


def _cancel_all(tasks: Iterable[asyncio.Task[Any]]) -> None:
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
    # Sliding window: at most concurrency * 2 tasks alive at once, refilled
    # from the source as completed ones are consumed from the front.
    window = concurrency * 2
    sem = asyncio.Semaphore(concurrency)

    async def bounded(item: Any) -> Any:
        async with sem:
            return await _coerce(fn, item)

    it = source.__aiter__()
    tasks: deque[asyncio.Task[Any]] = deque()
    exhausted = False

    async def fill() -> None:
        nonlocal exhausted
        while not exhausted and len(tasks) < window:
            try:
                item = await it.__anext__()
            except StopAsyncIteration:
                exhausted = True
                return
            tasks.append(asyncio.create_task(bounded(item)))

    try:
        await fill()
        while tasks:
            result = await tasks.popleft()
            yield result
            await fill()
    except BaseException:
        _cancel_all(tasks)
        raise


async def _map_unordered(
    source: AsyncIterable[Any], fn: Callable[..., Any], concurrency: int
) -> AsyncIterator[Any]:
    # Sliding window, same as _map_ordered, but results are pushed to a queue
    # as tasks complete so they can be emitted out of order as they're ready.
    window = concurrency * 2
    sem = asyncio.Semaphore(concurrency)
    result_q: asyncio.Queue[tuple[BaseException | None, Any]] = asyncio.Queue()
    it = source.__aiter__()
    tasks: set[asyncio.Task[Any]] = set()
    exhausted = False
    pending = 0

    async def worker(item: Any) -> None:
        try:
            async with sem:
                val = await _coerce(fn, item)
            await result_q.put((None, val))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            await result_q.put((exc, None))
        finally:
            task = asyncio.current_task()
            if task is not None:
                tasks.discard(task)

    async def fill() -> None:
        nonlocal exhausted, pending
        while not exhausted and len(tasks) < window:
            try:
                item = await it.__anext__()
            except StopAsyncIteration:
                exhausted = True
                return
            tasks.add(asyncio.create_task(worker(item)))
            pending += 1

    try:
        await fill()
        while pending > 0:
            exc, val = await result_q.get()
            pending -= 1
            if exc is not None:
                _cancel_all(tasks)
                raise exc
            yield val
            await fill()
    except BaseException:
        _cancel_all(tasks)
        raise


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
