"""
Benchmark: raw_notify.py vs ps_notify.py
Feature: for_each as primary terminal -- side-effect pipeline with no collected list.
"""
import asyncio, inspect, sys, os, time
sys.path.insert(0, os.path.dirname(__file__))
from raw_notify import run as run_raw, N_NOTIFS, CONCURRENCY
from ps_notify  import run as run_ps


def _lines(fn) -> int:
    src = inspect.getsource(fn)
    return sum(1 for ln in src.splitlines()[1:]
               if ln.strip() and not ln.strip().startswith("#"))


async def main() -> None:
    W = 56
    print("=" * W)
    print("  COMPARE 6 -- Rate-Limited Notification Sender")
    print(f"  {N_NOTIFS} notifications (email/sms/push)  |  conc={CONCURRENCY}")
    print(f"  See: raw_notify.py  vs  ps_notify.py")
    print("=" * W)

    print("  Running raw_notify.py ...", end="", flush=True)
    t0 = time.monotonic(); raw_counts = await run_raw(); raw_t = time.monotonic() - t0
    print(f" done ({raw_t:.3f}s)")

    print("  Running ps_notify.py  ...", end="", flush=True)
    t0 = time.monotonic(); ps_counts  = await run_ps();  ps_t = time.monotonic() - t0
    print(f" done ({ps_t:.3f}s)")

    raw_l = _lines(run_raw); ps_l = _lines(run_ps)
    same  = raw_counts == ps_counts

    def fmt(c: dict) -> str:
        return f"email={c['email']} sms={c['sms']} push={c['push']}"

    print(f"\n  {'File':20}  {'Time':>8}  {'Sent':>6}  {'Lines':>6}")
    print("  " + "-" * 46)
    print(f"  {'raw_notify.py':20}  {raw_t:>7.3f}s  "
          f"{sum(raw_counts.values()):>6}  {raw_l:>6}   {fmt(raw_counts)}")
    print(f"  {'ps_notify.py':20}  {ps_t:>7.3f}s  "
          f"{sum(ps_counts.values()):>6}  {ps_l:>6}   {fmt(ps_counts)}")
    print("  " + "-" * 46)
    faster = (raw_t - ps_t) / raw_t * 100
    print(f"  Perf diff  : {faster:+.1f}%")
    print(f"  Code       : {raw_l} -> {ps_l} lines  ({100*(1-ps_l/raw_l):.0f}% reduction)")
    print(f"  Correctness: {'counts match' if same else 'MISMATCH - BUG'}")
    print(f"\n  Raw needs a Lock to safely update shared counts from tasks.")
    print(f"  pipestage for_each: no lock needed (sequential counter update).")
    print("=" * W)


if __name__ == "__main__":
    asyncio.run(main())
