"""
pipestage -- Transform -> Batch -> DB Insert

Stage 1: Transform 150 records concurrently
Stage 2: Batch into groups of 15
Stage 3: Insert batches concurrently

Stages overlap: inserts begin as soon as the first full batch is ready.
Run standalone:  python ps_batch.py
"""
import asyncio
import time
from pipestage import stream

# -- Simulation (matches raw_batch.py) -----------------------------------------

N_RECORDS         = 150
BATCH_SIZE        = 15
TRANSFORM_WORKERS = 10
INSERT_WORKERS    = 3
RECORDS           = [{"id": i, "value": f"raw_{i}"} for i in range(N_RECORDS)]


async def _transform(record: dict) -> dict:
    await asyncio.sleep(0.01)
    return {"id": record["id"], "value": record["value"].upper()}


async def _db_insert(batch: list[dict]) -> None:
    await asyncio.sleep(0.06)


# -- Implementation ------------------------------------------------------------

async def run() -> int:
    inserted = 0

    async def insert(batch: list[dict]) -> None:
        nonlocal inserted
        await _db_insert(batch)
        inserted += len(batch)

    await (
        stream(RECORDS)
        .map(_transform, concurrency=TRANSFORM_WORKERS)
        .batch(BATCH_SIZE)
        .for_each(insert, concurrency=INSERT_WORKERS)
    )
    return inserted


if __name__ == "__main__":
    t0 = time.monotonic()
    n = asyncio.run(run())
    print(f"[pipestage]    {n} records inserted in {time.monotonic()-t0:.3f}s")
