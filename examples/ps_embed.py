"""
pipestage -- RAG Embedding Pipeline

Full pipeline for building a vector search index:
  Stage 1: Chunk each document into smaller pieces  (flat_map)
  Stage 2: Embed each chunk via API                 (.map)
  Stage 3: Batch chunks and upsert to vector DB     (.batch + .for_each)

All four stages run simultaneously -- embedding starts as soon as the first
chunk is ready, upserting starts as soon as the first batch fills.

Feature demonstrated: flat_map + map + batch + for_each combined.
This is the most complex pipeline and shows the biggest gains over raw asyncio.
Run standalone:  python ps_embed.py
"""
import asyncio
import random
import time
from pipestage import stream

# -- Simulation (seed=88, matches raw_embed.py) --------------------------------

N_DOCS         = 10
CHUNK_WORKERS  = 4
EMBED_WORKERS  = 6
UPSERT_WORKERS = 2
BATCH_SIZE     = 5

_rng = random.Random(88)
DOCUMENTS = [
    {"id": i, "text": f"Document {i}: " + "word " * _rng.randint(200, 500)}
    for i in range(N_DOCS)
]
_CHUNK_COUNTS = {d["id"]: _rng.randint(3, 6) for d in DOCUMENTS}


async def _chunk_document(doc: dict) -> list[dict]:
    await asyncio.sleep(0.015)
    return [
        {"doc_id": doc["id"], "chunk_id": j, "text": f"chunk {j} of doc {doc['id']}"}
        for j in range(_CHUNK_COUNTS[doc["id"]])
    ]


async def _embed_chunk(chunk: dict) -> dict:
    await asyncio.sleep(0.05)
    return {**chunk, "vector": [round(_rng.random(), 4) for _ in range(8)]}


async def _upsert_batch(batch: list[dict]) -> None:
    await asyncio.sleep(0.06)


# -- Implementation ------------------------------------------------------------

async def run() -> int:
    upserted = 0

    async def upsert(batch: list[dict]) -> None:
        nonlocal upserted
        await _upsert_batch(batch)
        upserted += len(batch)

    await (
        stream(DOCUMENTS)
        .flat_map(_chunk_document, concurrency=CHUNK_WORKERS)
        .map(_embed_chunk, concurrency=EMBED_WORKERS)
        .batch(BATCH_SIZE)
        .for_each(upsert, concurrency=UPSERT_WORKERS)
    )
    return upserted


if __name__ == "__main__":
    t0 = time.monotonic()
    n = asyncio.run(run())
    print(f"[pipestage]    {n} chunks embedded and upserted "
          f"in {time.monotonic()-t0:.3f}s")
