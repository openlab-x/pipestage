"""
Benchmark: raw_embed.py vs ps_embed.py
Feature: flat_map + map + batch + for_each -- the full pipeline.
This is the most complex example and shows the largest gains.
"""
import asyncio, inspect, sys, os, time
sys.path.insert(0, os.path.dirname(__file__))
from raw_embed import (run as run_raw, N_DOCS, CHUNK_WORKERS,
                       EMBED_WORKERS, UPSERT_WORKERS, BATCH_SIZE)
from ps_embed  import run as run_ps


def _lines(fn) -> int:
    src = inspect.getsource(fn)
    return sum(1 for ln in src.splitlines()[1:]
               if ln.strip() and not ln.strip().startswith("#"))


async def main() -> None:
    W = 60
    print("=" * W)
    print("  COMPARE 8 -- RAG Embedding Pipeline (most complex)")
    print(f"  {N_DOCS} documents  |  chunk workers={CHUNK_WORKERS}")
    print(f"  embed workers={EMBED_WORKERS}  |  upsert workers={UPSERT_WORKERS}"
          f"  |  batch={BATCH_SIZE}")
    print(f"  Pipeline: flat_map -> map -> batch -> for_each")
    print(f"  See: raw_embed.py  vs  ps_embed.py")
    print("=" * W)

    print("  Running raw_embed.py ...", end="", flush=True)
    t0 = time.monotonic(); raw_n = await run_raw(); raw_t = time.monotonic() - t0
    print(f" done ({raw_t:.3f}s)")

    print("  Running ps_embed.py  ...", end="", flush=True)
    t0 = time.monotonic(); ps_n  = await run_ps();  ps_t = time.monotonic() - t0
    print(f" done ({ps_t:.3f}s)")

    raw_l   = _lines(run_raw); ps_l = _lines(run_ps)
    speedup = raw_t / ps_t
    faster  = (raw_t - ps_t) / raw_t * 100

    print(f"\n  {'File':20}  {'Time':>8}  {'Upserted':>9}  {'Lines':>6}")
    print("  " + "-" * 50)
    print(f"  {'raw_embed.py':20}  {raw_t:>7.3f}s  {raw_n:>9}  {raw_l:>6}")
    print(f"  {'ps_embed.py':20}  {ps_t:>7.3f}s  {ps_n:>9}  {ps_l:>6}")
    print("  " + "-" * 50)
    print(f"  Speedup    : {speedup:.2f}x  ({faster:.1f}% faster with pipestage)")
    print(f"  Code       : {raw_l} -> {ps_l} lines  ({100*(1-ps_l/raw_l):.0f}% reduction)")
    print(f"  Correctness: {'outputs match' if raw_n == ps_n else 'MISMATCH - BUG'}")
    print(f"\n  Raw asyncio: 3 sequential gather() barriers (chunk, embed, upsert).")
    print(f"  pipestage : all 4 stages run simultaneously -- embedding starts as")
    print(f"  soon as first chunk is ready, upserting as soon as first batch fills.")
    print("=" * W)


if __name__ == "__main__":
    asyncio.run(main())
