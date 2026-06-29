# pipestage

<div align="center">
  <img src="https://raw.githubusercontent.com/openlab-x/pipestage/main/assets/logo.png" width="400"/>

![Python](https://img.shields.io/badge/Python-3.11%2B-blue)
![License](https://img.shields.io/badge/License-MIT-green)
![Version](https://img.shields.io/badge/Version-0.1.1-gray)
![Dependencies](https://img.shields.io/badge/Dependencies-Zero-brightgreen)

</div>

```python
from pipestage import stream

await (
    stream(documents)
    .flat_map(chunk,  concurrency=8)
    .map(embed,       concurrency=16)
    .batch(100)
    .for_each(upsert, concurrency=4)
)
```

## About

pipestage is an open-source Python library for building async data pipelines with safe staged processing and bounded concurrency. It is developed by OpenLabX and built entirely on the Python standard library with zero runtime dependencies.

The key difference from raw `asyncio.gather()`: gather is a barrier - stage 2 cannot start until every item from stage 1 is done. pipestage stages are lazy async generators that overlap in time, so stage 2 starts consuming stage 1's output as soon as the first item is ready.

pipestage is aimed at crawlers, ingestion jobs, API fan-out, file processing, and LLM data pipelines.

## Table of Contents

- [About](#about)
- [Features](#features)
- [Install](#install)
- [Quick Start](#quick-start)
- [API](#api)
  - [stream()](#stream)
  - [.map()](#map)
  - [.filter()](#filter)
  - [.flat_map()](#flat_map)
  - [.batch()](#batch)
  - [.collect()](#collect)
  - [.for_each()](#for_each)
  - [Async iteration](#async-iteration)
- [Error Handling](#error-handling)
- [Examples](#examples)
- [Project Structure](#project-structure)
  - [Architecture](#architecture)
  - [Concurrency Model](#concurrency-model)
- [Dependencies](#dependencies)
- [Python Versions Tested](#python-versions-tested)
- [Source Code Version 0.1.1](#source-code-version-011)
- [Known Issues at v0.1.1](#known-issues-at-v011)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

## Features

- **Stage Overlap**: All stages run simultaneously. No gather barriers between steps.
- **Bounded Concurrency**: Per-stage semaphore. Set concurrency once, the rest is handled.
- **Ordered or Unordered**: Preserve input order, or emit results as ready. One flag.
- **Sync and Async**: Pass sync or async functions anywhere. No wrapping required.
- **Async Generator Source**: `stream()` accepts any async iterable directly.
- **Fail-Fast Error Handling**: First exception cancels in-flight tasks and propagates unchanged.
- **Zero Dependencies**: Pure Python standard library. Nothing to install except pipestage itself.

## Install

**From PyPI:**
```bash
pip install pipestage
```

**From source:**
```bash
git clone https://github.com/openlab-x/pipestage.git
cd pipestage
pip install -e .
```

Requires Python 3.11 or later. No runtime dependencies.

## Quick Start

**Concurrent fetch and parse:**
```python
from pipestage import stream

results = await (
    stream(urls)
    .map(fetch, concurrency=20)
    .filter(lambda r: r["status"] == 200)
    .map(parse, concurrency=8)
    .collect()
)
```

**Batch DB inserts:**
```python
await (
    stream(records)
    .map(transform, concurrency=16)
    .batch(100)
    .for_each(insert_batch, concurrency=4)
)
```

**LLM fan-out - emit as ready:**
```python
results = await (
    stream(prompts)
    .map(call_llm, concurrency=8, ordered=False)
    .collect()
)
```

**Async generator source:**
```python
results = await (
    stream(scan_directory())
    .map(read_file, concurrency=10)
    .filter(lambda f: f["word_count"] >= 500)
    .collect()
)
```

## API

### stream()

Create a pipeline from any sync or async iterable.

```python
stream([1, 2, 3])
stream(range(1000))
stream(async_generator())
```

### .map()

```python
.map(fn, *, concurrency=1, ordered=True)
```

Apply `fn` to every item. `fn` may be sync or async. `ordered=False` emits results as they finish instead of preserving input order.

### .filter()

```python
.filter(pred, *, concurrency=1, ordered=True)
```

Keep only items for which `pred` returns truthy.

### .flat_map()

```python
.flat_map(fn, *, concurrency=1, ordered=True)
```

Map `fn` over each item then flatten one level. `fn` should return a list, generator, or async iterable.

### .batch()

```python
.batch(size)
```

Group items into lists of at most `size` elements. The final batch may be smaller.

### .collect()

Terminal. Consume the pipeline and return all results as a list.

```python
results = await stream(items).map(fn).collect()
```

### .for_each()

```python
.for_each(fn, *, concurrency=1, ordered=True)
```

Terminal. Consume the pipeline calling `fn` on each item for side effects. Return values are discarded.

### Async iteration

`Stream` is an async iterable. Use it directly without `collect()`.

```python
async for item in stream(records).map(transform, concurrency=8):
    print(item)
```

## Error Handling

The pipeline fails fast by default. The first exception stops the pipeline, cancels in-flight tasks, and propagates the original exception unchanged to the caller.

```python
try:
    await stream(urls).map(fetch, concurrency=10).collect()
except RuntimeError as e:
    print(e)
```

To continue past individual failures, handle exceptions inside `fn`:

```python
async def safe_fetch(url):
    try:
        return await fetch(url)
    except Exception:
        return None

results = await (
    stream(urls)
    .map(safe_fetch, concurrency=10)
    .filter(lambda r: r is not None)
    .collect()
)
```

## Examples

Each feature in `examples/` has three files: a raw asyncio implementation, a pipestage implementation, and a compare script that runs both and prints timing and line-count metrics.

```bash
python examples/compare_embed.py   # single comparison
python examples/run_all.py         # all eight
```

| # | Example | Features | Key result |
|---|---------|----------|------------|
| 1 | Fetch and Parse | map, filter | Stage overlap: ~1.7x speedup |
| 2 | Batch Inserts | map, batch, for_each | ~1.6x speedup |
| 3 | Fan-out | ordered=False | 6x lower time-to-first-result |
| 4 | Paginated Search | flat_map | Replaces gather + nested loop |
| 5 | Resilient Calls | error handling | Error logic stays in fn |
| 6 | Notifications | for_each | No Lock needed for shared state |
| 7 | File Processing | async generator source | Streams without materializing |
| 8 | RAG Pipeline | flat_map, map, batch, for_each | All stages overlap simultaneously |

## Project Structure

```
src/pipestage/
    __init__.py     - public entry point: stream()
    _stream.py      - Stream class, fluent API
    _ops.py         - async generator stages
    _utils.py       - internal helpers
tests/
    test_basic.py
    test_concurrency.py
    test_errors.py
examples/
    raw_X.py        - plain asyncio implementations
    ps_X.py         - pipestage implementations
    compare_X.py    - timing comparisons
```

### Architecture

Every transformation returns a new `Stream` wrapping an async generator. Nothing executes until `collect()` or `for_each()` is awaited. Each stage pulls from the previous one - all stages run simultaneously.

### Concurrency Model

| concurrency | ordered | Behavior |
|---|---|---|
| 1 | any | Serial. No tasks created. |
| > 1 | True | Semaphore limits active tasks. Results in input order. |
| > 1 | False | Results emitted via Queue as tasks complete. |

## Dependencies

**Runtime:** none.

**Development:**
```bash
pip install -e ".[dev]"
# installs: pytest, pytest-asyncio, ruff, mypy
```

## Python Versions Tested

- [x] **Python 3.11**
- [x] **Python 3.12**
- [x] **Python 3.13**
- [x] **Python 3.14**

## Source Code Version 0.1.1

- **Core pipeline**: `stream()`, `map`, `filter`, `flat_map`, `batch`, `collect`, `for_each`
- **Concurrent execution**: ordered and unordered modes with `asyncio.Semaphore`
- **Async iteration**: `Stream` usable directly in `async for` loops
- **Full test suite**: 44 tests across correctness, concurrency, and error propagation
- **Zero runtime dependencies**: Python 3.11+ standard library only

## Known Issues at v0.1.1

- All source items are consumed upfront before any results are yielded. For very large sources this can accumulate many Task objects in memory.
- No per-item timeout. A hung `fn` call blocks its concurrency slot indefinitely.
- If a consumer breaks out of `async for` early, in-flight tasks keep running until the event loop cleans them up.

## Contributing

We welcome contributions.

1. Give the project a star.
2. Follow us on GitHub.
3. Fork the repository.
4. Create a new branch for your feature or fix.
5. Make your changes and add tests.
6. Submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).

## Contact

In pursuit of innovation,  
**OpenLabX Team**

- **Website**: [https://openlabx.com](https://openlabx.com)
- **Email**: contact@openlabx.com

**Follow Us:**

<div align="center">

| <a href="https://www.instagram.com/openlabx_official/" target="_blank"><strong>Instagram</strong></a> | <a href="https://x.com/openlabx" target="_blank"><strong>X (formerly Twitter)</strong></a> | <a href="https://www.facebook.com/openlabx/" target="_blank"><strong>Facebook</strong></a> | <a href="https://www.youtube.com/@OpenLabX" target="_blank"><strong>YouTube</strong></a> | <a href="https://github.com/openlab-x" target="_blank"><strong>GitHub</strong></a> |
|---|---|---|---|---|

</div>
