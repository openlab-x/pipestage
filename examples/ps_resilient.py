"""
pipestage -- Resilient API Calls (partial failures)

25% of endpoints fail randomly. pipestage approach:
handle errors inside the function, return None on failure, filter in pipeline.

Feature demonstrated: clean error handling — function owns error logic,
pipeline owns concurrency. No mixing of concerns.
Run standalone:  python ps_resilient.py
"""
import asyncio
import random
import time
from pipestage import stream

# -- Simulation (seed=55, matches raw_resilient.py) ----------------------------

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
    errors = 0

    async def safe_fetch(ep: str) -> dict | None:
        nonlocal errors
        try:
            return await _fetch(ep)
        except Exception:
            errors += 1
            return None

    results = await (
        stream(ENDPOINTS)
        .map(safe_fetch, concurrency=CONCURRENCY)
        .filter(lambda r: r is not None)
        .collect()
    )
    return results, errors


if __name__ == "__main__":
    t0 = time.monotonic()
    results, errors = asyncio.run(run())
    print(f"[pipestage]    {len(results)} ok  {errors} failed  "
          f"in {time.monotonic()-t0:.3f}s")
