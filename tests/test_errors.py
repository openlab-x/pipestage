"""Error propagation tests — fail-fast behavior, no silent swallowing."""

import asyncio
import pytest
from pipestage import stream


async def test_map_serial_error():
    async def fail_on_3(x: int) -> int:
        if x == 3:
            raise ValueError("bad item")
        return x

    with pytest.raises(ValueError, match="bad item"):
        await stream(range(10)).map(fail_on_3).collect()


async def test_map_sync_error():
    def divide(x: int) -> float:
        return 1 / x  # ZeroDivisionError when x == 0

    with pytest.raises(ZeroDivisionError):
        await stream([2, 1, 0, 3]).map(divide).collect()


async def test_filter_serial_error():
    async def bad_pred(x: int) -> bool:
        if x == 3:
            raise RuntimeError("pred failed")
        return True

    with pytest.raises(RuntimeError, match="pred failed"):
        await stream(range(10)).filter(bad_pred).collect()


async def test_map_ordered_concurrent_error():
    async def maybe_fail(x: int) -> int:
        await asyncio.sleep(0.01)
        if x == 5:
            raise ValueError(f"fail:{x}")
        return x

    with pytest.raises(ValueError, match="fail:5"):
        await stream(range(20)).map(maybe_fail, concurrency=4, ordered=True).collect()


async def test_map_unordered_concurrent_error():
    async def maybe_fail(x: int) -> int:
        await asyncio.sleep(0.01)
        if x == 5:
            raise ValueError(f"fail:{x}")
        return x

    with pytest.raises(ValueError, match="fail:5"):
        await stream(range(20)).map(maybe_fail, concurrency=4, ordered=False).collect()


async def test_filter_concurrent_error():
    async def bad_pred(x: int) -> bool:
        await asyncio.sleep(0.005)
        if x == 4:
            raise RuntimeError("filter exploded")
        return True

    with pytest.raises(RuntimeError, match="filter exploded"):
        await stream(range(10)).filter(bad_pred, concurrency=5).collect()


async def test_error_type_is_original():
    """The exception raised must be the exact original type, not wrapped."""

    class CustomError(Exception):
        pass

    async def fn(x: int) -> int:
        raise CustomError("original")

    with pytest.raises(CustomError):
        await stream([1]).map(fn).collect()


async def test_no_results_after_error():
    """collect() should raise, not return partial results."""
    called: list[int] = []

    async def fn(x: int) -> int:
        called.append(x)
        if x == 2:
            raise ValueError("stop")
        return x

    with pytest.raises(ValueError):
        await stream(range(5)).map(fn).collect()

    # Some items may have been processed before the error, but the result is not returned
    assert 2 in called


async def test_flat_map_error_propagates():
    async def expand(x: int):
        if x == 2:
            raise ValueError("expand failed")
        yield x
        yield x * 10

    with pytest.raises(ValueError, match="expand failed"):
        await stream([1, 2, 3]).flat_map(expand).collect()


async def test_batch_does_not_swallow_upstream_error():
    async def fn(x: int) -> int:
        if x == 3:
            raise RuntimeError("upstream")
        return x

    with pytest.raises(RuntimeError, match="upstream"):
        await stream(range(10)).map(fn).batch(5).collect()
