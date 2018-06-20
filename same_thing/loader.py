
from aiorun import run

from .source import fetch_parts, BASE_URL


async def load_uris():
    await fetch_parts(BASE_URL)


if __name__ == '__main__':
    run(load_uris(), use_uvloop=True)
