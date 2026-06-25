"""
Raw asyncio -- Transform -> Batch -> DB Insert

Stage 1: Transform 150 records concurrently
Stage 2: Manual batching into groups of 15
Stage 3: Insert batches concurrently

All transforms must complete before any insert begins.
Run standalone:  python raw_batch.py
"""
import asyncio
import random
import time

# -- Simulation (seed=7, matches ps_batch.py) ----------------------------------

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
    sem1 = asyncio.Semaphore(TRANSFORM_WORKERS)

    async def transform(r: dict) -> dict:
        async with sem1:
            return await _transform(r)

    tasks = [asyncio.create_task(transform(r)) for r in RECORDS]
    transformed = list(await asyncio.gather(*tasks))

    batches = [
        transformed[i : i + BATCH_SIZE]
        for i in range(0, len(transformed), BATCH_SIZE)
    ]

    sem2 = asyncio.Semaphore(INSERT_WORKERS)
    inserted = 0
    lock = asyncio.Lock()

    async def insert(batch: list[dict]) -> None:
        nonlocal inserted
        async with sem2:
            await _db_insert(batch)
        async with lock:
            inserted += len(batch)

    await asyncio.gather(*[asyncio.create_task(insert(b)) for b in batches])
    return inserted


if __name__ == "__main__":
    t0 = time.monotonic()
    n = asyncio.run(run())
    print(f"[raw asyncio]  {n} records inserted in {time.monotonic()-t0:.3f}s")
