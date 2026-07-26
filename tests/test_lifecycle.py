"""Windowed task creation and Stream.aclose() — v0.2.0 memory-safety behavior."""

import asyncio

from pipestage import stream


async def test_windowed_pull_ordered():
    """Only `concurrency * 2` items should be pulled from the source before
    the first result is yielded — proves tasks aren't created for the whole
    source upfront."""
    concurrency = 3
    window = concurrency * 2
    pulled = 0

    async def source():
        nonlocal pulled
        for i in range(1000):
            pulled += 1
            yield i

    async def slow(x: int) -> int:
        await asyncio.sleep(0.05)
        return x

    s = stream(source()).map(slow, concurrency=concurrency, ordered=True)
    ait = s.__aiter__()
    first = await ait.__anext__()

    assert first == 0
    assert pulled == window
    await s.aclose()


async def test_windowed_pull_unordered():
    concurrency = 4
    window = concurrency * 2
    pulled = 0

    async def source():
        nonlocal pulled
        for i in range(1000):
            pulled += 1
            yield i

    async def slow(x: int) -> int:
        await asyncio.sleep(0.05)
        return x

    s = stream(source()).map(slow, concurrency=concurrency, ordered=False)
    ait = s.__aiter__()
    await ait.__anext__()

    assert pulled == window
    await s.aclose()


async def test_aclose_cancels_inflight_tasks():
    cancelled: list[int] = []

    async def worker(x: int) -> int:
        try:
            await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            cancelled.append(x)
            raise
        return x

    s = stream(range(10)).map(worker, concurrency=3)
    ait = s.__aiter__()
    first = await ait.__anext__()
    await s.aclose()
    await asyncio.sleep(0.05)

    assert first == 0
    assert len(cancelled) > 0


async def test_aclose_on_plain_stream_is_noop():
    s = stream([1, 2, 3])
    ait = s.__aiter__()
    assert await ait.__anext__() == 1
    await s.aclose()  # must not raise even though nothing is in flight
