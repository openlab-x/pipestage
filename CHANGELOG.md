# Changelog

All notable changes to pipestage will be documented here.

---

## [0.1.0] - 2026-06-25

First release.

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
