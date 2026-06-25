"""
pipestage -- Paginated Search API

Each query returns a variable number of result items.
flat_map naturally expands each query's results into the stream.

Feature demonstrated: flat_map
Run standalone:  python ps_paginated.py
"""
import asyncio
import random
import time
from pipestage import stream

# -- Simulation (seed=11, matches raw_paginated.py) ----------------------------

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
    return await (
        stream(QUERIES)
        .flat_map(_search, concurrency=CONCURRENCY)
        .filter(lambda item: item["valid"])
        .collect()
    )


if __name__ == "__main__":
    t0 = time.monotonic()
    results = asyncio.run(run())
    print(f"[pipestage]    {len(results)} valid items in {time.monotonic()-t0:.3f}s")
