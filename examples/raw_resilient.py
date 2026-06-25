"""
Raw asyncio -- Resilient API Calls (partial failures)

25% of endpoints fail randomly. Raw asyncio approach:
use return_exceptions=True, then manually separate results from exceptions.

Feature demonstrated: error handling pattern in raw asyncio (the footgun).
Run standalone:  python raw_resilient.py
"""
import asyncio
import random
import time

# -- Simulation (seed=55, matches ps_resilient.py) -----------------------------

N_ENDPOINTS = 60
CONCURRENCY = 12
ENDPOINTS   = [f"https://api.service.io/data/{i}" for i in range(N_ENDPOINTS)]

_rng       = random.Random(55)
_WILL_FAIL = {ep: _rng.random() < 0.25 for ep in ENDPOINTS}
_DELAYS    = {ep: _rng.uniform(0.02, 0.07) for ep in ENDPOINTS}


async def _fetch(endpoint: str) -> dict:
    await asyncio.sleep(_DELAYS[endpoint])
    if _WILL_FAIL[endpoint]:
        raise ConnectionError(f"endpoint unreachable: {endpoint}")
    return {"url": endpoint, "data": f"payload_{endpoint[-2:]}"}


# -- Implementation ------------------------------------------------------------

async def run() -> tuple[list[dict], int]:
    sem = asyncio.Semaphore(CONCURRENCY)

    async def fetch(ep: str) -> dict:
        async with sem:
            return await _fetch(ep)

    tasks = [asyncio.create_task(fetch(ep)) for ep in ENDPOINTS]
    raw = await asyncio.gather(*tasks, return_exceptions=True)

    results: list[dict] = []
    errors = 0
    for r in raw:
        if isinstance(r, Exception):
            errors += 1
        else:
            results.append(r)

    return results, errors


if __name__ == "__main__":
    t0 = time.monotonic()
    results, errors = asyncio.run(run())
    print(f"[raw asyncio]  {len(results)} ok  {errors} failed  "
          f"in {time.monotonic()-t0:.3f}s")
