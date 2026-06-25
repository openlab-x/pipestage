"""
Benchmark: raw_resilient.py vs ps_resilient.py
Feature: error handling pattern -- graceful degradation without stopping the pipeline.
"""
import asyncio, inspect, sys, os, time
sys.path.insert(0, os.path.dirname(__file__))
from raw_resilient import run as run_raw, N_ENDPOINTS, CONCURRENCY
from ps_resilient  import run as run_ps


def _lines(fn) -> int:
    src = inspect.getsource(fn)
    return sum(1 for ln in src.splitlines()[1:]
               if ln.strip() and not ln.strip().startswith("#"))


async def main() -> None:
    W = 56
    print("=" * W)
    print("  COMPARE 5 -- Resilient API Calls (partial failures)")
    print(f"  {N_ENDPOINTS} endpoints  |  ~25% fail  |  conc={CONCURRENCY}")
    print(f"  See: raw_resilient.py  vs  ps_resilient.py")
    print("=" * W)

    print("  Running raw_resilient.py ...", end="", flush=True)
    t0 = time.monotonic(); raw_res, raw_err = await run_raw(); raw_t = time.monotonic() - t0
    print(f" done ({raw_t:.3f}s)")

    print("  Running ps_resilient.py  ...", end="", flush=True)
    t0 = time.monotonic(); ps_res, ps_err = await run_ps();   ps_t = time.monotonic() - t0
    print(f" done ({ps_t:.3f}s)")

    raw_l = _lines(run_raw); ps_l = _lines(run_ps)
    same  = (len(raw_res) == len(ps_res) and raw_err == ps_err)

    print(f"\n  {'File':22}  {'Time':>8}  {'OK':>5}  {'Err':>5}  {'Lines':>6}")
    print("  " + "-" * 52)
    print(f"  {'raw_resilient.py':22}  {raw_t:>7.3f}s  {len(raw_res):>5}  {raw_err:>5}  {raw_l:>6}")
    print(f"  {'ps_resilient.py':22}  {ps_t:>7.3f}s  {len(ps_res):>5}  {ps_err:>5}  {ps_l:>6}")
    print("  " + "-" * 52)
    faster = (raw_t - ps_t) / raw_t * 100
    print(f"  Perf diff  : {faster:+.1f}%")
    print(f"  Code       : {raw_l} -> {ps_l} lines  ({100*(1-ps_l/raw_l):.0f}% reduction)")
    print(f"  Correctness: {'outputs match' if same else 'MISMATCH - BUG'}")
    print(f"\n  Raw uses return_exceptions=True + isinstance() post-processing.")
    print(f"  pipestage: function owns error logic, pipeline owns concurrency.")
    print("=" * W)


if __name__ == "__main__":
    asyncio.run(main())
