"""
Benchmark: raw_fanout.py vs ps_fanout.py

Fan-out: Ordered vs Unordered -- four variants compared.
Key metric: time-to-first-result, which reveals the real cost of ordering.
"""
import asyncio
import inspect
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from raw_fanout import (run_ordered as raw_ordered, run_unordered as raw_unordered,
                        N_PROMPTS, CONCURRENCY, _DELAYS)
from ps_fanout  import run_ordered as ps_ordered, run_unordered as ps_unordered


def _lines(fn) -> int:
    src = inspect.getsource(fn)
    return sum(
        1 for ln in src.splitlines()[1:]
        if ln.strip() and not ln.strip().startswith("#")
    )


async def main() -> None:
    W = 60
    fast_avg = sum(_DELAYS[1:]) / (N_PROMPTS - 1)
    print("=" * W)
    print("  COMPARE 3 -- Fan-out: Ordered vs Unordered")
    print(f"  {N_PROMPTS} prompts  |  concurrency={CONCURRENCY}")
    print(f"  Prompt 0: {_DELAYS[0]:.2f}s (slow)  |  Prompts 1-{N_PROMPTS-1}: ~{fast_avg:.2f}s avg")
    print(f"  See: raw_fanout.py  vs  ps_fanout.py")
    print("=" * W)

    print("  Running raw ordered   ...", end="", flush=True)
    ro_res, ro_first, ro_total = await raw_ordered()
    print(f" done")

    print("  Running raw unordered ...", end="", flush=True)
    ru_res, ru_first, ru_total = await raw_unordered()
    print(f" done")

    print("  Running ps  ordered   ...", end="", flush=True)
    po_res, po_first, po_total = await ps_ordered()
    print(f" done")

    print("  Running ps  unordered ...", end="", flush=True)
    pu_res, pu_first, pu_total = await ps_unordered()
    print(f" done")

    def ids(r: list[dict]) -> set:
        return {x["index"] for x in r}

    all_correct = (ids(ro_res) == ids(ru_res) == ids(po_res)
                   == ids(pu_res) == set(range(N_PROMPTS)))

    raw_ord_l = _lines(raw_ordered)
    raw_uno_l = _lines(raw_unordered)
    ps_ord_l  = _lines(ps_ordered)
    ps_uno_l  = _lines(ps_unordered)

    latency_gain = po_first / pu_first if pu_first > 0 else 0

    print(f"\n  {'Variant':30}  {'Total':>7}  {'1st result':>11}  {'Lines':>6}")
    print("  " + "-" * 58)
    print(f"  {'raw_fanout -- ordered':30}  {ro_total:>6.3f}s"
          f"  {ro_first:>10.3f}s  {raw_ord_l:>6}")
    print(f"  {'raw_fanout -- unordered':30}  {ru_total:>6.3f}s"
          f"  {ru_first:>10.3f}s  {raw_uno_l:>6}")
    print(f"  {'ps_fanout  -- ordered=True':30}  {po_total:>6.3f}s"
          f"  {po_first:>10.3f}s  {ps_ord_l:>6}")
    print(f"  {'ps_fanout  -- ordered=False':30}  {pu_total:>6.3f}s"
          f"  {pu_first:>10.3f}s  {ps_uno_l:>6}")
    print("  " + "-" * 58)
    print(f"  Correctness     : {'all outputs match' if all_correct else 'MISMATCH - BUG'}")
    print(f"  Unordered code  : {raw_uno_l} -> {ps_uno_l} lines  "
          f"({100*(1-ps_uno_l/raw_uno_l):.0f}% reduction)")
    print(f"  1st-result gain : {latency_gain:.1f}x lower latency with ordered=False")
    print(f"\n  ordered=True  blocks on prompt 0 ({_DELAYS[0]:.2f}s) before yielding.")
    print(f"  ordered=False yields the fastest item first (~{min(_DELAYS[1:]):.2f}s).")
    print("=" * W)


if __name__ == "__main__":
    asyncio.run(main())
