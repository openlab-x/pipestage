"""
pipestage -- Fan-out: Ordered and Unordered

ordered=True  : results in input order (blocked by slow items)
ordered=False : results emitted as they complete (lowest latency)

Returns (results, time_to_first_result, total_time) for each variant.
Run standalone:  python ps_fanout.py
"""
import asyncio
import random
import time
from pipestage import stream

# -- Simulation (seed=99, matches raw_fanout.py) -------------------------------

N_PROMPTS   = 30
CONCURRENCY = 8
PROMPTS     = [f"Explain concept {i}" for i in range(N_PROMPTS)]

_rng    = random.Random(99)
_DELAYS = [0.28 if i == 0 else _rng.uniform(0.02, 0.10) for i in range(N_PROMPTS)]


async def _call_llm(i: int, prompt: str) -> dict:
    await asyncio.sleep(_DELAYS[i])
    return {"index": i, "prompt": prompt, "answer": f"Answer to: {prompt}"}


# -- Helper --------------------------------------------------------------------

async def _timed(gen) -> tuple[list[dict], float, float]:
    results = []
    first_t: float | None = None
    t0 = time.monotonic()
    async for item in gen:
        if first_t is None:
            first_t = time.monotonic() - t0
        results.append(item)
    total = time.monotonic() - t0
    return results, first_t or total, total


# -- Ordered -------------------------------------------------------------------

async def run_ordered() -> tuple[list[dict], float, float]:
    return await _timed(
        stream(enumerate(PROMPTS))
        .map(lambda t: _call_llm(*t), concurrency=CONCURRENCY, ordered=True)
    )


# -- Unordered -----------------------------------------------------------------

async def run_unordered() -> tuple[list[dict], float, float]:
    return await _timed(
        stream(enumerate(PROMPTS))
        .map(lambda t: _call_llm(*t), concurrency=CONCURRENCY, ordered=False)
    )


if __name__ == "__main__":
    async def main() -> None:
        _, f, t = await run_ordered()
        print(f"[ps ordered=True]   first={f:.3f}s  total={t:.3f}s")
        _, f, t = await run_unordered()
        print(f"[ps ordered=False]  first={f:.3f}s  total={t:.3f}s")
    asyncio.run(main())
