import multiprocessing
from concurrent.futures import ThreadPoolExecutor

from aiorun import run

from .sink import load_snapshot
from .source import fetch_latest_snapshot, BASE_URL


CPU_COUNT = multiprocessing.cpu_count()


async def load_uris():
    await fetch_latest_snapshot(BASE_URL)
    executor = ThreadPoolExecutor(max_workers=min(16, 2*CPU_COUNT))
    await load_snapshot(executor)

if __name__ == '__main__':
    run(load_uris(), use_uvloop=True)
