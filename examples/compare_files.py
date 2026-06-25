"""
Benchmark: raw_files.py vs ps_files.py
Feature: async generator as source + multi-stage pipeline with filter.
"""
import asyncio, inspect, sys, os, time
sys.path.insert(0, os.path.dirname(__file__))
from raw_files import run as run_raw, N_FILES, READ_CONC, EXTRACT_CONC, WORD_THRESHOLD
from ps_files  import run as run_ps


def _lines(fn) -> int:
    src = inspect.getsource(fn)
    return sum(1 for ln in src.splitlines()[1:]
               if ln.strip() and not ln.strip().startswith("#"))


async def main() -> None:
    W = 56
    print("=" * W)
    print("  COMPARE 7 -- File Processing Pipeline")
    print(f"  {N_FILES} files  |  read conc={READ_CONC}  |  extract conc={EXTRACT_CONC}")
    print(f"  Filter: keep files with >= {WORD_THRESHOLD} words")
    print(f"  See: raw_files.py  vs  ps_files.py")
    print("=" * W)

    print("  Running raw_files.py ...", end="", flush=True)
    t0 = time.monotonic(); raw_res = await run_raw(); raw_t = time.monotonic() - t0
    print(f" done ({raw_t:.3f}s)")

    print("  Running ps_files.py  ...", end="", flush=True)
    t0 = time.monotonic(); ps_res  = await run_ps();  ps_t = time.monotonic() - t0
    print(f" done ({ps_t:.3f}s)")

    raw_l = _lines(run_raw); ps_l = _lines(run_ps)
    same  = (sorted(r["path"] for r in raw_res) == sorted(r["path"] for r in ps_res))

    print(f"\n  {'File':20}  {'Time':>8}  {'Files out':>10}  {'Lines':>6}")
    print("  " + "-" * 50)
    print(f"  {'raw_files.py':20}  {raw_t:>7.3f}s  {len(raw_res):>10}  {raw_l:>6}")
    print(f"  {'ps_files.py':20}  {ps_t:>7.3f}s  {len(ps_res):>10}  {ps_l:>6}")
    print("  " + "-" * 50)
    speedup = raw_t / ps_t
    faster  = (raw_t - ps_t) / raw_t * 100
    print(f"  Speedup    : {speedup:.2f}x  ({faster:.1f}% faster with pipestage)")
    print(f"  Code       : {raw_l} -> {ps_l} lines  ({100*(1-ps_l/raw_l):.0f}% reduction)")
    print(f"  Correctness: {'outputs match' if same else 'MISMATCH - BUG'}")
    print(f"\n  Raw must await all reads before filtering or extracting.")
    print(f"  pipestage streams the async generator directly -- no list needed.")
    print("=" * W)


if __name__ == "__main__":
    asyncio.run(main())
