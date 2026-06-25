"""Concurrency, ordering, and backpressure tests."""

import asyncio
import random

from pipestage import stream


async def test_ordered_preserves_order():
    """With ordered=True, output order must match input order despite random delays."""
    async def slow(x: int) -> int:
        await asyncio.sleep(random.uniform(0, 0.04))
        return x

    items = list(range(40))
    result = await stream(items).map(slow, concurrency=10, ordered=True).collect()
    assert result == items


async def test_unordered_has_all_results():
    """With ordered=False, all items are present but order may differ."""
    async def slow(x: int) -> int:
        await asyncio.sleep(random.uniform(0, 0.04))
        return x

    items = list(range(40))
    result = await stream(items).map(slow, concurrency=10, ordered=False).collect()
    assert sorted(result) == items


async def test_concurrency_limit_respected():
    """At no point should more than `limit` tasks run simultaneously."""
    limit = 5
    running = 0
    peak = 0

    async def tracked(x: int) -> int:
        nonlocal running, peak
        running += 1
        peak = max(peak, running)
        await asyncio.sleep(0.01)
        running -= 1
        return x

    await stream(range(30)).map(tracked, concurrency=limit).collect()
    assert peak <= limit


async def test_concurrency_1_is_serial():
    """concurrency=1 must process items one at a time, no interleaving."""
    events: list[tuple[str, int]] = []

    async def tracked(x: int) -> int:
        events.append(("start", x))
        await asyncio.sleep(0.001)
        events.append(("end", x))
        return x

    await stream(range(4)).map(tracked, concurrency=1).collect()

    for i in range(4):
        start_idx = events.index(("start", i))
        end_idx = events.index(("end", i))
        assert end_idx == start_idx + 1, f"item {i} interleaved"


async def test_ordered_false_is_faster():
    import time

    delays = [0.1, 0.01, 0.01, 0.01, 0.01]  # first item is slow

    async def slow_first(x: int) -> int:
        await asyncio.sleep(delays[x])
        return x

    # ordered=False emits the first fast result immediately (~0.01s)
    start = time.monotonic()
    async for _ in stream(range(5)).map(slow_first, concurrency=5, ordered=False):
        break
    unordered_first = time.monotonic() - start

    # ordered=True must wait for item 0 before yielding anything (~0.1s)
    start = time.monotonic()
    async for _ in stream(range(5)).map(slow_first, concurrency=5, ordered=True):
        break
    ordered_first = time.monotonic() - start

    assert unordered_first < ordered_first * 0.5


async def test_filter_concurrent():
    """Concurrent filter: result must contain only matching items in order."""
    async def slow_even(x: int) -> bool:
        await asyncio.sleep(0.01)
        return x % 2 == 0

    result = await stream(range(10)).filter(slow_even, concurrency=5).collect()
    assert result == [0, 2, 4, 6, 8]


async def test_filter_concurrent_unordered():
    """Concurrent unordered filter: all matching items present, order may differ."""
    async def slow_even(x: int) -> bool:
        await asyncio.sleep(random.uniform(0, 0.03))
        return x % 2 == 0

    result = await stream(range(10)).filter(slow_even, concurrency=5, ordered=False).collect()
    assert sorted(result) == [0, 2, 4, 6, 8]


async def test_for_each_concurrent():
    """for_each with concurrency should process all items."""
    results: list[int] = []

    async def worker(x: int) -> None:
        await asyncio.sleep(0.005)
        results.append(x)

    await stream(range(10)).for_each(worker, concurrency=4)
    assert sorted(results) == list(range(10))


async def test_map_then_batch_concurrent():
    """Concurrent map followed by batch should produce correct batches."""
    async def double(x: int) -> int:
        await asyncio.sleep(0.005)
        return x * 2

    result = await stream(range(9)).map(double, concurrency=4).batch(3).collect()
    assert result == [[0, 2, 4], [6, 8, 10], [12, 14, 16]]


async def test_large_input_ordered():
    """Ordered concurrent map should handle a large number of items correctly."""
    async def identity(x: int) -> int:
        await asyncio.sleep(0)  # yield to event loop
        return x

    items = list(range(500))
    result = await stream(items).map(identity, concurrency=20, ordered=True).collect()
    assert result == items
