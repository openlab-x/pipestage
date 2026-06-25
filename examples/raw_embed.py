"""
Raw asyncio -- RAG Embedding Pipeline

Full pipeline for building a vector search index:
  Stage 1: Chunk each document into smaller pieces  (flat_map equivalent)
  Stage 2: Embed each chunk via API                 (concurrent map)
  Stage 3: Batch chunks and upsert to vector DB     (batch + concurrent inserts)

Raw asyncio: three separate gather passes with a full barrier between each.
All chunking must finish before embedding starts.
All embedding must finish before upserting starts.

Feature demonstrated: all raw asyncio pain points combined.
Run standalone:  python raw_embed.py
"""
import asyncio
import random
import time

# -- Simulation (seed=88, matches ps_embed.py) ---------------------------------

N_DOCS        = 10
CHUNK_WORKERS = 4
EMBED_WORKERS = 6
UPSERT_WORKERS = 2
BATCH_SIZE    = 5

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
    # Stage 1: chunk all documents
    sem1 = asyncio.Semaphore(CHUNK_WORKERS)

    async def chunk(doc: dict) -> list[dict]:
        async with sem1:
            return await _chunk_document(doc)

    chunk_tasks = [asyncio.create_task(chunk(doc)) for doc in DOCUMENTS]
    all_nested = await asyncio.gather(*chunk_tasks)
    all_chunks = [c for chunks in all_nested for c in chunks]

    # Stage 2: embed all chunks
    sem2 = asyncio.Semaphore(EMBED_WORKERS)

    async def embed(chunk: dict) -> dict:
        async with sem2:
            return await _embed_chunk(chunk)

    embed_tasks = [asyncio.create_task(embed(c)) for c in all_chunks]
    embeddings = list(await asyncio.gather(*embed_tasks))

    # Stage 3: batch and upsert
    batches = [
        embeddings[i : i + BATCH_SIZE]
        for i in range(0, len(embeddings), BATCH_SIZE)
    ]
    sem3 = asyncio.Semaphore(UPSERT_WORKERS)
    upserted = 0
    lock = asyncio.Lock()

    async def upsert(batch: list[dict]) -> None:
        nonlocal upserted
        async with sem3:
            await _upsert_batch(batch)
        async with lock:
            upserted += len(batch)

    upsert_tasks = [asyncio.create_task(upsert(b)) for b in batches]
    await asyncio.gather(*upsert_tasks)
    return upserted


if __name__ == "__main__":
    t0 = time.monotonic()
    n = asyncio.run(run())
    print(f"[raw asyncio]  {n} chunks embedded and upserted "
          f"in {time.monotonic()-t0:.3f}s")
