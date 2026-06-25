"""
Raw asyncio -- Paginated Search API

Each query returns a variable number of result items.
Raw asyncio: gather all queries, manually flatten results, then filter.

Feature demonstrated: equivalent of flat_map done by hand.
Run standalone:  python raw_paginated.py
"""
import asyncio
import random
import time

# -- Simulation (seed=11, matches ps_paginated.py) -----------------------------

N_QUERIES   = 12
CONCURRENCY = 4
QUERIES     = [f"keyword_{i}" for i in range(N_QUERIES)]

_rng = random.Random(11)
_DATA = {
    q: {
        "delay": _rng.uniform(0.04, 0.12),
        "items": [
            {"query": q, "id": j, "score": round(_rng.random(), 3),
             "valid": _rng.random() > 0.18}
            for j in range(_rng.randint(3, 8))
        ],
    }
    for q in QUERIES
}


async def _search(query: str) -> list[dict]:
    await asyncio.sleep(_DATA[query]["delay"])
    return _DATA[query]["items"]


# -- Implementation ------------------------------------------------------------

async def run() -> list[dict]:
    sem = asyncio.Semaphore(CONCURRENCY)

    async def search(query: str) -> list[dict]:
        async with sem:
            return await _search(query)

    tasks = [asyncio.create_task(search(q)) for q in QUERIES]
    all_pages = await asyncio.gather(*tasks)

    all_items: list[dict] = []
    for page_items in all_pages:
        all_items.extend(page_items)

    return [item for item in all_items if item["valid"]]


if __name__ == "__main__":
    t0 = time.monotonic()
    results = asyncio.run(run())
    print(f"[raw asyncio]  {len(results)} valid items in {time.monotonic()-t0:.3f}s")
