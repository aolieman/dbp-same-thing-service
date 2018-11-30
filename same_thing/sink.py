import asyncio
import bz2
import os
import traceback

import aiofiles
from aiofiles.os import stat
from tqdm import tqdm

from .db import get_connection, backupper, DATA_DB_PREFIX
from .source import DOWNLOAD_PATH, print_with_timestamp, get_snapshot_path, get_timestamp

DBP_GLOBAL_PREFIX = 'https://'
DBP_GLOBAL_MARKER = 'global.dbpedia.org/id/'
SNAPSHOT_KEY = b'snapshot:'
SINGLETON_LOCAL_SEPARATOR = b'||'
PART_TARGET_SIZE = 100 * 1024**2


async def load_snapshot(executor, snapshot_name):
    print_with_timestamp(f'Loading latest downloaded snapshot')
    admin_db = get_connection('admin', read_only=False)
    loop = asyncio.get_event_loop()
    snapshot_key = get_snapshot_key(snapshot_name)
    if admin_db.get(snapshot_key):
        print_with_timestamp(f'Snapshot {snapshot_name} has already been loaded')
        loop.stop()

    data_db = get_connection(DATA_DB_PREFIX + snapshot_name, read_only=False)
    snapshot_path = os.path.join(DOWNLOAD_PATH, get_snapshot_path(snapshot_name))

    stream_reader = StreamingBZ2File(snapshot_path)
    tsv_headers = None
    async for tsv_line in stream_reader.read_lines():
        if tsv_headers is None:
            tsv_headers = tsv_line.split(b'\t')
            assert [b'original_iri',
                    b'singleton_id_base58',
                    b'cluster_id_base58'] == tsv_headers, f'unexpected headers: {tsv_headers}'
            continue

        try:
            local_iri, singleton_id, cluster_id = tsv_line.split(b'\t')
        except ValueError:
            print(repr(tsv_line))
            continue

        singleton_and_local = singleton_id + SINGLETON_LOCAL_SEPARATOR + local_iri
        data_db.merge(cluster_id, singleton_and_local, disable_wal=True)
        data_db.put(local_iri, cluster_id, disable_wal=True)
        if not singleton_id == cluster_id:
            data_db.put(singleton_id, cluster_id, disable_wal=True)

    print_with_timestamp(f'Loading finished! Saving backup...')
    now = get_timestamp()
    admin_db.put(snapshot_key, now.encode('utf8'))

    backupper.create_backup(data_db, flush_before_backup=True)
    backupper.purge_old_backups(3)
    print_with_timestamp(f'Backup saved. All done!')

    # close the event loop
    loop.stop()


def compute_parts(file_path, size=PART_TARGET_SIZE, skip_first_line=True):
    with open(file_path, 'rb') as f:
        part_number = 0
        file_end = f.seek(0, os.SEEK_END)
        chunk_end = f.seek(0, os.SEEK_SET)
        if skip_first_line:
            header = f.readline()
            chunk_end = f.tell()
            print_with_timestamp(f'Skipped header: {header}')

        while chunk_end < file_end:
            chunk_start = f.tell()
            f.seek(size, os.SEEK_CUR)
            f.readline()
            chunk_end = f.tell()
            yield part_number, chunk_start, chunk_end
            part_number += 1


def get_snapshot_key(snapshot_name):
    return SNAPSHOT_KEY + snapshot_name.encode('utf8')


def load_part(data_db, part_number, chunk_start, chunk_end):
    part_path = os.path.join(DOWNLOAD_PATH, part)
    triple_count = 0
    print_with_timestamp(f'Loading: {part}', flush=True)

    prov_was_derived_from = 'http://www.w3.org/ns/prov#wasDerivedFrom'

    with bz2.open(part_path) as triples:
        for line in triples:
            subj, pred, obj = parse_triple(line.decode('utf8'))
            if pred == prov_was_derived_from:
                dbp_global = subj.lstrip(DBP_GLOBAL_PREFIX).encode('utf8')
                local_id = obj.encode('utf8')
                data_db.merge(dbp_global, local_id, disable_wal=True)
                data_db.put(local_id, dbp_global, disable_wal=True)
                triple_count += 1

    now = get_timestamp()
    print(f'[{now}] Finished {triple_count} triples from {part}')
    return part


class StreamingBZ2File:
    # TODO: implement multi-stream decompression

    def __init__(self, file_path, chunk_size=4*1024):
        assert chunk_size > 0, '`chunk_size` needs a non-zero number of bytes'
        self.decompressor = bz2.BZ2Decompressor()
        self.file_path = file_path
        self.chunk_size = chunk_size

    async def read_lines(self):
        incomplete_line = b''
        async with aiofiles.open(self.file_path, 'rb') as af:
            file_stats = await stat(af.fileno())
            with tqdm(
                    total=file_stats.st_size,
                    unit='b',
                    unit_scale=True,
                    unit_divisor=1024
            ) as progress_bar:

                while True:
                    raw_bytes = await af.read(self.chunk_size)
                    if not raw_bytes:
                        break

                    progress_bar.update(self.chunk_size)
                    chunk = self.decompressor.decompress(raw_bytes)
                    if not chunk:
                        # You must construct additional pylons!
                        continue

                    lines = chunk.splitlines()
                    yield incomplete_line + lines[0]
                    incomplete_line = b''

                    if chunk.endswith(b'\n'):
                        full_lines = lines[1:]
                    else:
                        full_lines = lines[1:-1]
                        incomplete_line = lines[-1]

                    for line in full_lines:
                        yield line

            if incomplete_line:
                yield incomplete_line
