"""
Raw asyncio -- File Processing Pipeline

Source is an async generator (simulates reading from a directory cursor).
Stage 1: Read each file (I/O bound)
Stage 2: Extract word count and keywords
Stage 3: Filter files with fewer than 600 words

Raw asyncio: must materialize the generator into a list first, then two
separate gather passes for read and extract.

Feature demonstrated: async generator as source + two-stage pipeline.
Run standalone:  python raw_files.py
"""
import asyncio
import random
import time

# -- Simulation (seed=77, matches ps_files.py) ---------------------------------

N_FILES     = 40
READ_CONC   = 8
EXTRACT_CONC = 5
WORD_THRESHOLD = 600

_rng = random.Random(77)
_FILES = [
    {"path": f"/data/docs/file_{i:03d}.txt",
     "words": _rng.randint(100, 2000),
     "read_delay": _rng.uniform(0.01, 0.05),
     "extract_delay": _rng.uniform(0.01, 0.03)}
    for i in range(N_FILES)
]


async def _file_source():
    """Async generator simulating a directory scan."""
    for f in _FILES:
        await asyncio.sleep(0)   # yield to event loop between items
        yield f


async def _read_file(meta: dict) -> dict:
    await asyncio.sleep(meta["read_delay"])
    return {**meta, "content": f"content of {meta['path']} ({meta['words']} words)"}


async def _extract(file: dict) -> dict:
    await asyncio.sleep(file["extract_delay"])
    keywords = [f"kw{i}" for i in range(min(5, file["words"] // 100))]
    return {**file, "keywords": keywords}


# -- Implementation ------------------------------------------------------------

async def run() -> list[dict]:
    # Must collect async generator into list before using gather
    all_meta = [f async for f in _file_source()]

    sem1 = asyncio.Semaphore(READ_CONC)

    async def read(meta: dict) -> dict:
        async with sem1:
            return await _read_file(meta)

    tasks1 = [asyncio.create_task(read(m)) for m in all_meta]
    read_results = await asyncio.gather(*tasks1)

    large_files = [f for f in read_results if f["words"] >= WORD_THRESHOLD]

    sem2 = asyncio.Semaphore(EXTRACT_CONC)

    async def extract(file: dict) -> dict:
        async with sem2:
            return await _extract(file)

    tasks2 = [asyncio.create_task(extract(f)) for f in large_files]
    return list(await asyncio.gather(*tasks2))


if __name__ == "__main__":
    t0 = time.monotonic()
    results = asyncio.run(run())
    print(f"[raw asyncio]  {len(results)}/{N_FILES} files passed filter "
          f"in {time.monotonic()-t0:.3f}s")
