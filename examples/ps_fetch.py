"""
pipestage -- Two-Stage Fetch -> Parse -> Filter

Stage 1: Fetch 60 URLs concurrently
Stage 2: Parse only valid responses

Stages overlap: parsing begins as soon as the first valid fetch arrives.
Run standalone:  python ps_fetch.py
"""
import asyncio
import random
import time
from pipestage import stream

# -- Simulation (seed=42, matches raw_fetch.py) --------------------------------

N_URLS     = 60
FETCH_CONC = 10
PARSE_CONC = 5
URLS       = [f"https://api.example.com/item/{i}" for i in range(N_URLS)]

_rng   = random.Random(42)
_FETCH = {url: {"status": _rng.choice([200,200,200,404,200,200,200,500,200,200]),
                "delay":  _rng.uniform(0.05, 0.15)} for url in URLS}
_PARSE = {url: {"valid": _rng.random() > 0.1,
                "delay": _rng.uniform(0.03, 0.08)} for url in URLS}


async def _fetch(url: str) -> dict:
    await asyncio.sleep(_FETCH[url]["delay"])
    return {"url": url, "status": _FETCH[url]["status"]}


async def _parse(response: dict) -> dict | None:
    url = response["url"]
    await asyncio.sleep(_PARSE[url]["delay"])
    return {"url": url, "words": 1024} if _PARSE[url]["valid"] else None


# -- Implementation ------------------------------------------------------------

async def run() -> list[dict]:
    return await (
        stream(URLS)
        .map(_fetch, concurrency=FETCH_CONC)
        .filter(lambda r: r["status"] == 200)
        .map(_parse, concurrency=PARSE_CONC)
        .filter(lambda r: r is not None)
        .collect()
    )


if __name__ == "__main__":
    t0 = time.monotonic()
    results = asyncio.run(run())
    print(f"[pipestage]    {len(results)} results in {time.monotonic()-t0:.3f}s")
