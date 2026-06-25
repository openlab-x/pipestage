"""
Benchmark: raw_paginated.py vs ps_paginated.py
Feature: flat_map -- expanding one input into multiple outputs.
"""
import asyncio, inspect, sys, os, time
sys.path.insert(0, os.path.dirname(__file__))
from raw_paginated import run as run_raw, N_QUERIES, CONCURRENCY
from ps_paginated  import run as run_ps


def _lines(fn) -> int:
    src = inspect.getsource(fn)
    return sum(1 for ln in src.splitlines()[1:]
               if ln.strip() and not ln.strip().startswith("#"))


async def main() -> None:
    W = 56
    print("=" * W)
    print("  COMPARE 4 -- Paginated Search (flat_map)")
    print(f"  {N_QUERIES} queries  |  variable items per query  |  conc={CONCURRENCY}")
    print(f"  See: raw_paginated.py  vs  ps_paginated.py")
    print("=" * W)

    print("  Running raw_paginated.py ...", end="", flush=True)
    t0 = time.monotonic(); raw_res = await run_raw(); raw_t = time.monotonic() - t0
    print(f" done ({raw_t:.3f}s)")

    print("  Running ps_paginated.py  ...", end="", flush=True)
    t0 = time.monotonic(); ps_res = await run_ps();  ps_t = time.monotonic() - t0
    print(f" done ({ps_t:.3f}s)")

    raw_l = _lines(run_raw); ps_l = _lines(run_ps)
    same  = sorted(str(r) for r in raw_res) == sorted(str(r) for r in ps_res)

    print(f"\n  {'File':22}  {'Time':>8}  {'Items':>7}  {'Lines':>6}")
    print("  " + "-" * 48)
    print(f"  {'raw_paginated.py':22}  {raw_t:>7.3f}s  {len(raw_res):>7}  {raw_l:>6}")
    print(f"  {'ps_paginated.py':22}  {ps_t:>7.3f}s  {len(ps_res):>7}  {ps_l:>6}")
    print("  " + "-" * 48)
    faster = (raw_t - ps_t) / raw_t * 100
    print(f"  Perf diff  : {faster:+.1f}%")
    print(f"  Code       : {raw_l} -> {ps_l} lines  ({100*(1-ps_l/raw_l):.0f}% reduction)")
    print(f"  Correctness: {'outputs match' if same else 'MISMATCH - BUG'}")
    print(f"\n  flat_map replaces: gather + nested loop + extend + filter.")
    print("=" * W)


if __name__ == "__main__":
    asyncio.run(main())
