import asyncio
import os
from datetime import datetime, timezone
from operator import itemgetter

import aiofiles
import aiohttp
import async_timeout
from bs4 import BeautifulSoup


DOWNLOAD_PATH = '/downloads'
BASE_URL = 'http://downloads.dbpedia.org/databus/global/persistence-core/cluster-iri-provenance-ntriples/'


async def fetch_parts(base_url):
    existing_parts = set(os.listdir(DOWNLOAD_PATH))

    conn = aiohttp.TCPConnector(limit_per_host=10)
    async with aiohttp.ClientSession(connector=conn) as session:
        parts = await list_parts(session, base_url)
        new_parts = parts - existing_parts
        if new_parts:
            # delete parts that are no longer listed
            old_parts = existing_parts - parts
            for part_name in old_parts:
                os.unlink(os.path.join(DOWNLOAD_PATH, part_name))

            # download newly listed parts
            await asyncio.gather(
                *(
                    download_part(session, BASE_URL + part_href)
                    for part_href in new_parts
                )
            )
            now = get_timestamp()
            print(f'[{now}] All parts downloaded!', flush=True)


async def list_parts(session, base_url):
    async with session.get(base_url) as resp:
        assert resp.status == 200, 'Could not GET parts file'
        index_html = await resp.text()

    soup = BeautifulSoup(index_html, 'lxml')
    part_anchors = soup.find_all(href=only_parts)
    part_hrefs = set(map(itemgetter('href'), part_anchors))
    print(soup.title.get_text(), flush=True)
    return part_hrefs


def only_parts(href):
    return href and href.startswith('part') and href.endswith('.txt.bz2')


async def download_part(session, url):
    with async_timeout.timeout(300):
        async with session.get(url) as response:
            filename = os.path.basename(url)
            filepath = os.path.join(DOWNLOAD_PATH, filename)
            now = get_timestamp()
            print(f'[{now}] Downloading {filename}', flush=True)

            async with aiofiles.open(filepath, 'wb') as af:
                while True:
                    chunk = await response.content.read(4*1024)
                    if not chunk:
                        break
                    await af.write(chunk)

        now = get_timestamp()
        print(f'[{now}] Finished {filename}', flush=True)


def get_timestamp():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec='seconds')
