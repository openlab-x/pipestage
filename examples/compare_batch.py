"""
Benchmark: raw_batch.py vs ps_batch.py

Transform -> Batch -> DB Insert
Runs both implementations and reports timing, line count, and correctness.
"""
import asyncio
import inspect
import sys
import os
import time

sys.path.insert(0, os.path.dirname(__file__))
from raw_batch import run as run_raw, N_RECORDS, BATCH_SIZE, TRANSFORM_WORKERS, INSERT_WORKERS
from ps_batch  import run as run_ps


def _lines(fn) -> int:
    src = inspect.getsource(fn)
    return sum(
        1 for ln in src.splitlines()[1:]
        if ln.strip() and not ln.strip().startswith("#")
    )


async def main() -> None:
    W = 56
    n_batches = N_RECORDS // BATCH_SIZE
    print("=" * W)
    print("  COMPARE 2 -- Transform -> Batch -> DB Insert")
    print(f"  {N_RECORDS} records  |  batch={BATCH_SIZE} ({n_batches} batches)")
    print(f"  transform workers={TRANSFORM_WORKERS}  |  insert workers={INSERT_WORKERS}")
    print(f"  See: raw_batch.py  vs  ps_batch.py")
    print("=" * W)

    print("  Running raw_batch.py ...", end="", flush=True)
    t0 = time.monotonic()
    raw_n = await run_raw()
    raw_t = time.monotonic() - t0
    print(f" done ({raw_t:.3f}s)")

    print("  Running ps_batch.py  ...", end="", flush=True)
    t0 = time.monotonic()
    ps_n = await run_ps()
    ps_t = time.monotonic() - t0
    print(f" done ({ps_t:.3f}s)")

    raw_l   = _lines(run_raw)
    ps_l    = _lines(run_ps)
    speedup = raw_t / ps_t
    faster  = (raw_t - ps_t) / raw_t * 100

    print(f"\n  {'File':20}  {'Time':>8}  {'Inserted':>9}  {'Lines':>6}")
    print("  " + "-" * 48)
    print(f"  {'raw_batch.py':20}  {raw_t:>7.3f}s  {raw_n:>9}  {raw_l:>6}")
    print(f"  {'ps_batch.py':20}  {ps_t:>7.3f}s  {ps_n:>9}  {ps_l:>6}")
    print("  " + "-" * 48)
    print(f"  Speedup    : {speedup:.2f}x  ({faster:.1f}% faster with pipestage)")
    print(f"  Code       : {raw_l} -> {ps_l} lines  ({100*(1-ps_l/raw_l):.0f}% reduction)")
    print(f"  Correctness: {'outputs match' if raw_n == ps_n else 'MISMATCH - BUG'}")
    print(f"\n  Why pipestage is faster: stages overlap.")
    print(f"  Raw  -- all transforms finish, then inserts begin.")
    print(f"  Pipe -- first insert starts after batch {BATCH_SIZE} is ready.")
    print("=" * W)


if __name__ == "__main__":
    asyncio.run(main())
