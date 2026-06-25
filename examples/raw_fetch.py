"""
Raw asyncio -- Two-Stage Fetch -> Parse -> Filter

Stage 1: Fetch 60 URLs concurrently (bound by semaphore)
Stage 2: Parse only valid responses (second semaphore)

All fetches must complete before any parsing begins.
Run standalone:  python raw_fetch.py
"""
import asyncio
import random
import time

# -- Simulation (seed=42, matches ps_fetch.py) ---------------------------------

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
    sem1 = asyncio.Semaphore(FETCH_CONC)

    async def fetch(url: str) -> dict:
        async with sem1:
            return await _fetch(url)

    tasks1 = [asyncio.create_task(fetch(url)) for url in URLS]
    all_responses = await asyncio.gather(*tasks1)

    valid = [r for r in all_responses if r["status"] == 200]
    sem2 = asyncio.Semaphore(PARSE_CONC)

    async def parse(r: dict) -> dict | None:
        async with sem2:
            return await _parse(r)

    tasks2 = [asyncio.create_task(parse(r)) for r in valid]
    parsed = await asyncio.gather(*tasks2)
    return [p for p in parsed if p is not None]


if __name__ == "__main__":
    t0 = time.monotonic()
    results = asyncio.run(run())
    print(f"[raw asyncio]  {len(results)} results in {time.monotonic()-t0:.3f}s")
