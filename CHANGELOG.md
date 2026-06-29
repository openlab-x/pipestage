# Changelog

All notable changes to pipestage will be documented here.

---

## [0.1.1] - 2026-06-30

### Changed

- `filter_stage` serial branch removed -- all paths now delegate to `map_stage` uniformly
- Validation helper unified -- `_check_concurrency` replaced with `_check_positive(name, value)` used by all methods including `batch`
- `__version__` now read dynamically from package metadata via `importlib.metadata.version`

---

## [0.1.0] - 2026-06-25

### Added

- `stream(source)` - entry point accepting any sync or async iterable
- `Stream.map(fn, *, concurrency, ordered)` - serial and concurrent map with ordered/unordered output
- `Stream.filter(pred, *, concurrency, ordered)` - serial and concurrent filter
- `Stream.flat_map(fn, *, concurrency, ordered)` - map then flatten one level
- `Stream.batch(size)` - group items into fixed-size lists
- `Stream.collect()` - terminal, returns `list[T]`
- `Stream.for_each(fn, *, concurrency, ordered)` - terminal, side-effect consumer
- Async iteration - `Stream` usable directly in `async for` loops
- Fail-fast error handling - first exception cancels in-flight tasks and propagates unchanged
- Zero runtime dependencies, Python 3.11+

### Released

- GitHub: https://github.com/openlab-x/pipestage - 2026-06-25
- PyPI: https://pypi.org/project/pipestage - 2026-06-26
