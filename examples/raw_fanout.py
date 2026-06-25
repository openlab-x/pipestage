"""
Raw asyncio -- Fan-out: Ordered and Unordered

ordered   : asyncio.gather -- blocks until ALL tasks finish, no streaming
unordered : Queue-based -- emits results as tasks complete

Returns (results, time_to_first_result, total_time) for each variant.
Run standalone:  python raw_fanout.py
"""
import asyncio
import random
import time

# -- Simulation (seed=99, matches ps_fanout.py) --------------------------------

N_PROMPTS   = 30
CONCURRENCY = 8
PROMPTS     = [f"Explain concept {i}" for i in range(N_PROMPTS)]

_rng    = random.Random(99)
_DELAYS = [0.28 if i == 0 else _rng.uniform(0.02, 0.10) for i in range(N_PROMPTS)]


async def _call_llm(i: int, prompt: str) -> dict:
    await asyncio.sleep(_DELAYS[i])
    return {"index": i, "prompt": prompt, "answer": f"Answer to: {prompt}"}


# -- Ordered -------------------------------------------------------------------

async def run_ordered() -> tuple[list[dict], float, float]:
    sem = asyncio.Semaphore(CONCURRENCY)

    async def call(i: int, prompt: str) -> dict:
        async with sem:
            return await _call_llm(i, prompt)

    t0 = time.monotonic()
    tasks = [asyncio.create_task(call(i, p)) for i, p in enumerate(PROMPTS)]
    results = list(await asyncio.gather(*tasks))
    total = time.monotonic() - t0
    return results, total, total  # gather returns all at once: first == total


# -- Unordered -----------------------------------------------------------------

async def run_unordered() -> tuple[list[dict], float, float]:
    sem = asyncio.Semaphore(CONCURRENCY)
    queue: asyncio.Queue = asyncio.Queue()
    first_t: float | None = None

    async def worker(i: int, prompt: str) -> None:
        try:
            async with sem:
                result = await _call_llm(i, prompt)
            await queue.put((None, result))
        except Exception as exc:
            await queue.put((exc, None))

    t0 = time.monotonic()
    tasks = [asyncio.create_task(worker(i, p)) for i, p in enumerate(PROMPTS)]

    results = []
    for _ in range(len(PROMPTS)):
        err, result = await queue.get()
        if first_t is None:
            first_t = time.monotonic() - t0
        if err:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise err
        results.append(result)

    total = time.monotonic() - t0
    return results, first_t or total, total


if __name__ == "__main__":
    async def main() -> None:
        _, f, t = await run_ordered()
        print(f"[raw ordered]    first={f:.3f}s  total={t:.3f}s")
        _, f, t = await run_unordered()
        print(f"[raw unordered]  first={f:.3f}s  total={t:.3f}s")
    asyncio.run(main())
