"""
Run all eight comparisons back to back.
Usage:  python run_all.py
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
import compare_fetch
import compare_batch
import compare_fanout
import compare_paginated
import compare_resilient
import compare_notify
import compare_files
import compare_embed


async def main() -> None:
    await compare_fetch.main()
    print()
    await compare_batch.main()
    print()
    await compare_fanout.main()
    print()
    await compare_paginated.main()
    print()
    await compare_resilient.main()
    print()
    await compare_notify.main()
    print()
    await compare_files.main()
    print()
    await compare_embed.main()


if __name__ == "__main__":
    asyncio.run(main())
