import asyncio
import multiprocessing

from aiorun import run

from .sink import load_snapshot
from .source import fetch_latest_snapshot, BASE_URL


CPU_COUNT = multiprocessing.cpu_count()


async def load_identifiers():
    loop = asyncio.get_event_loop()
    try:
        latest_snapshot = await fetch_latest_snapshot(BASE_URL)
        await load_snapshot(latest_snapshot)
    except Exception:
        loop.stop()
        raise

    # close the event loop
    loop.stop()

if __name__ == '__main__':
    run(load_identifiers(), use_uvloop=True)
