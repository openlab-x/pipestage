"""Basic correctness tests — serial paths, no concurrency."""

import pytest
from pipestage import stream


async def test_collect_list():
    assert await stream([1, 2, 3]).collect() == [1, 2, 3]


async def test_collect_empty():
    assert await stream([]).collect() == []


async def test_collect_range():
    assert await stream(range(5)).collect() == [0, 1, 2, 3, 4]


async def test_async_iterable_source():
    async def gen():
        for i in range(4):
            yield i

    assert await stream(gen()).collect() == [0, 1, 2, 3]


async def test_map_sync():
    result = await stream([1, 2, 3]).map(lambda x: x * 2).collect()
    assert result == [2, 4, 6]


async def test_map_async():
    async def double(x: int) -> int:
        return x * 2

    result = await stream([1, 2, 3]).map(double).collect()
    assert result == [2, 4, 6]


async def test_filter_sync():
    result = await stream(range(10)).filter(lambda x: x % 2 == 0).collect()
    assert result == [0, 2, 4, 6, 8]


async def test_filter_async():
    async def is_even(x: int) -> bool:
        return x % 2 == 0

    result = await stream(range(10)).filter(is_even).collect()
    assert result == [0, 2, 4, 6, 8]


async def test_filter_keeps_all():
    result = await stream([1, 2, 3]).filter(lambda _: True).collect()
    assert result == [1, 2, 3]


async def test_filter_drops_all():
    result = await stream([1, 2, 3]).filter(lambda _: False).collect()
    assert result == []


async def test_batch_standard():
    result = await stream(range(7)).batch(3).collect()
    assert result == [[0, 1, 2], [3, 4, 5], [6]]


async def test_batch_exact_multiple():
    result = await stream(range(6)).batch(3).collect()
    assert result == [[0, 1, 2], [3, 4, 5]]


async def test_batch_size_1():
    result = await stream(range(3)).batch(1).collect()
    assert result == [[0], [1], [2]]


async def test_batch_larger_than_source():
    result = await stream([1, 2]).batch(100).collect()
    assert result == [[1, 2]]


async def test_flat_map_list():
    result = await stream([1, 2, 3]).flat_map(lambda x: [x, x * 10]).collect()
    assert result == [1, 10, 2, 20, 3, 30]


async def test_flat_map_async_gen():
    async def expand(x: int):
        yield x
        yield x * 10

    result = await stream([1, 2]).flat_map(expand).collect()
    assert result == [1, 10, 2, 20]


async def test_flat_map_empty_inner():
    result = await stream([1, 2, 3]).flat_map(lambda _: []).collect()
    assert result == []


async def test_chain_map_filter():
    result = (
        await stream(range(10))
        .map(lambda x: x * 3)
        .filter(lambda x: x % 2 == 0)
        .collect()
    )
    assert result == [0, 6, 12, 18, 24]


async def test_chain_map_batch():
    result = (
        await stream(range(10))
        .filter(lambda x: x % 2 == 0)
        .map(lambda x: x * 10)
        .batch(3)
        .collect()
    )
    assert result == [[0, 20, 40], [60, 80]]


async def test_for_each_side_effects():
    received: list[int] = []
    await stream([1, 2, 3]).for_each(received.append)
    assert received == [1, 2, 3]


async def test_for_each_async():
    received: list[int] = []

    async def collect(x: int) -> None:
        received.append(x)

    await stream([1, 2, 3]).for_each(collect)
    assert received == [1, 2, 3]


async def test_stream_as_async_iterable():
    result = []
    async for item in stream([1, 2, 3]).map(lambda x: x + 1):
        result.append(item)
    assert result == [2, 3, 4]


async def test_validation_map_concurrency():
    with pytest.raises(ValueError, match="concurrency"):
        stream([1]).map(lambda x: x, concurrency=0)


async def test_validation_batch_size():
    with pytest.raises(ValueError, match="batch size"):
        stream([1]).batch(0)
