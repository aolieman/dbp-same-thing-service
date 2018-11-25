import asyncio
import os
import re
import shutil
from datetime import datetime, timezone
from operator import itemgetter

import aiofiles
import aiohttp
import async_timeout
from bs4 import BeautifulSoup

DOWNLOAD_PATH = '/downloads'
BASE_URL = 'https://downloads.dbpedia.org/repo/dev/global-id-management/global-ids/'
SNAPSHOT_NAME_FORMAT = 'global-ids-{snapshot_name}_base58.tsv.bz2'
DOWNLOAD_TIMEOUT = 20 * 60


async def fetch_latest_snapshot(base_url):
    existing_downloads = set(os.listdir(DOWNLOAD_PATH))

    conn = aiohttp.TCPConnector(limit_per_host=10)
    async with aiohttp.ClientSession(connector=conn, raise_for_status=True) as session:
        snapshots = await list_snapshots(session, base_url)
        if snapshots:
            latest_snapshot = snapshots[0]
            if latest_snapshot not in existing_downloads:
                # delete older snapshots
                for old_snapshot in existing_downloads:
                    shutil.rmtree(os.path.join(DOWNLOAD_PATH, old_snapshot))

                # download new snapshot
                await asyncio.gather(
                    download_snapshot(session, latest_snapshot)
                )
                print_with_timestamp(f'Saved new snapshot {latest_snapshot}')


async def list_snapshots(session, base_url):
    async with session.get(base_url) as resp:
        assert resp.status == 200, 'Could not GET snapshot list'
        index_html = await resp.text()

    date_re = re.compile(r'\d{4}\.\d{2}\.\d{2}')
    soup = BeautifulSoup(index_html, 'lxml')
    page_title = soup.title.get_text()
    print_with_timestamp(f'Listing {page_title}')
    snapshot_anchors = soup.find_all(href=date_re)
    snapshot_hrefs = map(itemgetter('href'), snapshot_anchors)
    snapshot_names = [
        href.strip('/')
        for href in snapshot_hrefs
    ]
    return sorted(snapshot_names, reverse=True)


async def download_snapshot(session, snapshot_name):
    snapshot_filename = SNAPSHOT_NAME_FORMAT.format(snapshot_name=snapshot_name)
    snapshot_url = f'{BASE_URL}{snapshot_name}/{snapshot_filename}'
    destination_dir = os.path.join(DOWNLOAD_PATH, snapshot_name)
    await download_file(session, snapshot_url, destination_dir)


async def download_file(session, url, destination_dir=DOWNLOAD_PATH):
    with async_timeout.timeout(DOWNLOAD_TIMEOUT):
        async with session.get(url) as response:
            filename = os.path.basename(url)
            filepath = os.path.join(destination_dir, filename)
            print_with_timestamp(f'Downloading {filename}')

            if os.path.exists(destination_dir):
                if os.path.exists(filepath):
                    raise IOError(f'The destination path already exists: {filepath}')
            else:
                os.makedirs(destination_dir)

            async with aiofiles.open(filepath, 'wb') as af:
                while True:
                    chunk = await response.content.read(4*1024)
                    if not chunk:
                        break
                    await af.write(chunk)

        print_with_timestamp(f'Finished downloading {filename}')


def get_timestamp():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec='seconds')


def print_with_timestamp(message):
    now = get_timestamp()
    print(f'[{now}] {message}', flush=True)
