"""
pipestage -- File Processing Pipeline

Source is an async generator (simulates reading from a directory cursor).
stream() accepts it directly -- no materialisation needed.
Stages overlap: extraction starts as the first large file is read.

Feature demonstrated: async generator source + multi-stage with filter.
Run standalone:  python ps_files.py
"""
import asyncio
import random
import time
from pipestage import stream

# -- Simulation (seed=77, matches raw_files.py) --------------------------------

N_FILES        = 40
READ_CONC      = 8
EXTRACT_CONC   = 5
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
        await asyncio.sleep(0)
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
    return await (
        stream(_file_source())
        .map(_read_file, concurrency=READ_CONC)
        .filter(lambda f: f["words"] >= WORD_THRESHOLD)
        .map(_extract, concurrency=EXTRACT_CONC)
        .collect()
    )


if __name__ == "__main__":
    t0 = time.monotonic()
    results = asyncio.run(run())
    print(f"[pipestage]    {len(results)}/{N_FILES} files passed filter "
          f"in {time.monotonic()-t0:.3f}s")
