"""
Benchmark: raw_fetch.py vs ps_fetch.py

Two-Stage Fetch -> Parse -> Filter
Runs both implementations and reports timing, line count, and correctness.
"""
import asyncio
import inspect
import sys
import os
import time

sys.path.insert(0, os.path.dirname(__file__))
from raw_fetch import run as run_raw, N_URLS, FETCH_CONC, PARSE_CONC
from ps_fetch  import run as run_ps


def _lines(fn) -> int:
    src = inspect.getsource(fn)
    return sum(
        1 for ln in src.splitlines()[1:]   # skip the 'async def run():' line
        if ln.strip() and not ln.strip().startswith("#")
    )


async def main() -> None:
    W = 56
    print("=" * W)
    print("  COMPARE 1 -- Two-Stage Fetch -> Parse -> Filter")
    print(f"  {N_URLS} URLs  |  fetch conc={FETCH_CONC}  |  parse conc={PARSE_CONC}")
    print(f"  See: raw_fetch.py  vs  ps_fetch.py")
    print("=" * W)

    print("  Running raw_fetch.py ...", end="", flush=True)
    t0 = time.monotonic()
    raw_res = await run_raw()
    raw_t = time.monotonic() - t0
    print(f" done ({raw_t:.3f}s)")

    print("  Running ps_fetch.py  ...", end="", flush=True)
    t0 = time.monotonic()
    ps_res = await run_ps()
    ps_t = time.monotonic() - t0
    print(f" done ({ps_t:.3f}s)")

    raw_l   = _lines(run_raw)
    ps_l    = _lines(run_ps)
    speedup = raw_t / ps_t
    faster  = (raw_t - ps_t) / raw_t * 100
    same    = sorted(r["url"] for r in raw_res) == sorted(r["url"] for r in ps_res)

    print(f"\n  {'File':20}  {'Time':>8}  {'Results':>9}  {'Lines':>6}")
    print("  " + "-" * 48)
    print(f"  {'raw_fetch.py':20}  {raw_t:>7.3f}s  {len(raw_res):>9}  {raw_l:>6}")
    print(f"  {'ps_fetch.py':20}  {ps_t:>7.3f}s  {len(ps_res):>9}  {ps_l:>6}")
    print("  " + "-" * 48)
    print(f"  Speedup    : {speedup:.2f}x  ({faster:.1f}% faster with pipestage)")
    print(f"  Code       : {raw_l} -> {ps_l} lines  ({100*(1-ps_l/raw_l):.0f}% reduction)")
    print(f"  Correctness: {'outputs match' if same else 'MISMATCH - BUG'}")
    print(f"\n  Why pipestage is faster: stages overlap.")
    print(f"  Raw  -- all {N_URLS} fetches finish, then parsing begins.")
    print(f"  Pipe -- parsing starts on the first valid fetch.")
    print("=" * W)


if __name__ == "__main__":
    asyncio.run(main())
