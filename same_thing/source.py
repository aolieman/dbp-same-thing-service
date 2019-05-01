import asyncio
import os
import re
import shutil
from datetime import datetime, timezone
from operator import itemgetter

import aiofiles
import aiohttp
from bs4 import BeautifulSoup
from tqdm import tqdm

DOWNLOAD_PATH = '/downloads'
BASE_URL = 'https://downloads.dbpedia.org/repo/dev/global-id-management/global-ids/'
SNAPSHOT_NAME_FORMAT = 'global-ids_base58.tsv.bz2'
DOWNLOAD_TIMEOUT = 40 * 60


async def fetch_latest_snapshot(base_url):
    existing_downloads = set(os.listdir(DOWNLOAD_PATH))

    conn = aiohttp.TCPConnector(limit_per_host=10)
    async with aiohttp.ClientSession(connector=conn, raise_for_status=True) as session:
        latest_snapshot, snapshot_url = await get_latest_snapshot(session)
        assert latest_snapshot, f'No snapshots found. check sparql query to databus or internet connection'
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

async def get_latest_snapshot(session):
    sparql_request = "https://databus.dbpedia.org/repo/sparql?query=PREFIX%20dataid%3A%20%3Chttp%3A%2F%2Fdataid.dbpedia.org%2Fns%2Fcore%23%3E%0APREFIX%20dct%3A%20%3Chttp%3A%2F%2Fpurl.org%2Fdc%2Fterms%2F%3E%0APREFIX%20dcat%3A%20%20%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fdcat%23%3E%0A%0A%23%20Get%20all%20files%0ASELECT%20DISTINCT%20%3Ffile%20%3Flatest%20WHERE%20%7B%0A%09%3Fdataset%20dataid%3Aartifact%20%3Chttps%3A%2F%2Fdatabus.dbpedia.org%2Fdbpedia%2Fid-management%2Fglobal-ids%3E%20.%0A%09%3Fdataset%20dcat%3Adistribution%20%3Fdistribution%20.%0A%09%3Fdistribution%20dataid%3AcontentVariant%20%27base58%27%5E%5E%3Chttp%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema%23string%3E%20.%0A%09%3Fdistribution%20dataid%3AformatExtension%20%27tsv%27%5E%5E%3Chttp%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema%23string%3E%20.%0A%09%3Fdistribution%20dataid%3Acompression%20%27bzip2%27%5E%5E%3Chttp%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema%23string%3E%20.%0A%09%3Fdistribution%20dcat%3AdownloadURL%20%3Ffile%20.%0A%20%20%20%20%7BSELECT%20%3Fdataset%20%3Flatest%20WHERE%20%7B%20%23%20join%20with%20latest%20version%20available%0A%09%09%09%3Fdataset%20dataid%3Aartifact%20%3Chttps%3A%2F%2Fdatabus.dbpedia.org%2Fdbpedia%2Fid-management%2Fglobal-ids%3E%20.%0A%20%20%20%20%20%20%09%09%3Fdataset%20dcat%3Adistribution%20%3Fdistribution%20.%0A%20%20%20%20%20%20%20%20%20%20%20%20%3Fdistribution%20dataid%3AcontentVariant%20%27base58%27%5E%5E%3Chttp%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema%23string%3E%20.%0A%20%20%20%20%20%20%20%20%20%20%20%20%3Fdistribution%20dataid%3AformatExtension%20%27tsv%27%5E%5E%3Chttp%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema%23string%3E%20.%0A%20%20%20%20%20%20%20%20%20%20%20%20%3Fdistribution%20dataid%3Acompression%20%27bzip2%27%5E%5E%3Chttp%3A%2F%2Fwww.w3.org%2F2001%2FXMLSchema%23string%3E%20.%0A%09%09%09%3Fdataset%20dct%3AhasVersion%20%3Flatest%20.%0A%09%09%7D%20ORDER%20BY%20DESC(%3Flatest)%20LIMIT%201%20%0A%20%20%7D%0A%7D"
    headers = {'Accept': 'application/sparql-results+json'}
    async with session.get(sparql_request, headers=headers) as resp:
        assert resp.status == 200, 'Could not GET latest snapshot from Databus'
        global_json = await resp.json(content_type='application/sparql-results+json')
    snapshot_name = global_json['results']['bindings'][0]['latest']['value']
    snapshot_url  = global_json['results']['bindings'][0]['file']['value']
    return snapshot_name, snapshot_url ;
    
async def download_snapshot(session, snapshot_name, snapshot_url):
    destination_dir = os.path.join(DOWNLOAD_PATH, snapshot_name)
    await download_file(session, snapshot_url, destination_dir)


def get_snapshot_path(snapshot_name):
    snapshot_filename = SNAPSHOT_NAME_FORMAT.format(snapshot_name=snapshot_name)
    return f'{snapshot_name}/{snapshot_filename}'


async def download_file(session, url, destination_dir=DOWNLOAD_PATH):
    async with session.get(url, timeout=DOWNLOAD_TIMEOUT) as response:
        filename = os.path.basename(url)
        filepath = os.path.join(destination_dir, filename)
        print_with_timestamp(f'Downloading {filename} to {filepath}')

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
