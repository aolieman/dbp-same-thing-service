import asyncio
import os
import shutil
from datetime import datetime, timezone
from urllib.parse import urlencode, quote_plus

import aiofiles
import aiohttp
from tqdm import tqdm

from same_thing.sparql_queries import latest_global_ids

DOWNLOAD_PATH = '/downloads'
SNAPSHOT_FILENAME = 'global-ids_base58.tsv.bz2'
DOWNLOAD_TIMEOUT = 40 * 60


async def fetch_latest_snapshot():
    existing_downloads = set(os.listdir(DOWNLOAD_PATH))

    conn = aiohttp.TCPConnector(limit_per_host=10)
    async with aiohttp.ClientSession(connector=conn, raise_for_status=True) as session:
        latest_snapshot, snapshot_url = await find_latest_snapshot(session)
        if latest_snapshot not in existing_downloads:
            # delete older snapshots
            for old_snapshot in existing_downloads:
                shutil.rmtree(os.path.join(DOWNLOAD_PATH, old_snapshot))

            # download new snapshot
            await asyncio.gather(
                download_snapshot(session, latest_snapshot, snapshot_url)
            )
            print_with_timestamp(f'Saved new snapshot {latest_snapshot}')

    return latest_snapshot


async def find_latest_snapshot(session):
    payload = {'query': latest_global_ids}
    parameters = urlencode(payload, quote_via=quote_plus)
    sparql_request_url = f'https://databus.dbpedia.org/repo/sparql?{parameters}'
    sparql_json_mime = 'application/sparql-results+json'
    headers = {'Accept': sparql_json_mime}
    async with session.get(sparql_request_url, headers=headers) as resp:
        assert resp.status == 200, 'Could not GET latest snapshot from Databus'
        global_json = await resp.json(content_type=sparql_json_mime)

    assert (
        global_json.get('results')
        and global_json['results'].get('bindings')
        and len(global_json['results']['bindings'])
    ), f'No latest snapshot was found on the Databus with query: {latest_global_ids}'

    first_binding = global_json['results']['bindings'][0]
    snapshot_name = first_binding['latest']['value']
    snapshot_url = first_binding['file']['value']
    return snapshot_name, snapshot_url


async def download_snapshot(session, snapshot_name, snapshot_url):
    destination_dir = os.path.join(DOWNLOAD_PATH, snapshot_name)
    await download_file(session, snapshot_url, destination_dir)


def get_snapshot_path(snapshot_name):
    return f'{snapshot_name}/{SNAPSHOT_FILENAME}'


async def download_file(session, url, destination_dir=DOWNLOAD_PATH):
    async with session.get(url, timeout=DOWNLOAD_TIMEOUT) as response:
        filename = os.path.basename(url)
        filepath = os.path.join(destination_dir, filename)
        print_with_timestamp(f'Downloading {filename} to {destination_dir}')

        if os.path.exists(destination_dir):
            if os.path.exists(filepath):
                raise IOError(f'The destination path already exists: {filepath}')
        else:
            os.makedirs(destination_dir)

        async with aiofiles.open(filepath, 'wb') as af:
            with tqdm(
                total=response.content_length,
                desc=filename,
                unit='b',
                unit_scale=True,
                unit_divisor=1024
            ) as progress_bar:
                chunk_size = 4 * 1024
                while True:
                    chunk = await response.content.read(chunk_size)
                    if not chunk:
                        break
                    await af.write(chunk)
                    progress_bar.update(chunk_size)

    print_with_timestamp(f'Finished downloading {filename}')


def get_timestamp():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec='seconds')


def print_with_timestamp(message):
    now = get_timestamp()
    print(f'[{now}] {message}', flush=True)
