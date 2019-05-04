import asyncio
import multiprocessing

from aiorun import run

from same_thing.sink import load_snapshot
from same_thing.source import fetch_latest_snapshot


CPU_COUNT = multiprocessing.cpu_count()


async def load_identifiers():
    loop = asyncio.get_event_loop()
    try:
        latest_snapshot = await fetch_latest_snapshot()
        await load_snapshot(latest_snapshot)
    except Exception:
        raise
    finally:
        loop.stop()

if __name__ == '__main__':
    run(load_identifiers(), use_uvloop=True)
