import asyncio
import bz2
import os

import aiofiles
from aiofiles.os import stat
from tqdm import tqdm

from .db import get_connection, backupper, DATA_DB_PREFIX, SINGLETON_LOCAL_SEPARATOR
from .source import DOWNLOAD_PATH, print_with_timestamp, get_snapshot_path, get_timestamp

DBP_GLOBAL_PREFIX = 'https://'
DBP_GLOBAL_MARKER = 'global.dbpedia.org/id/'
SNAPSHOT_KEY = b'snapshot:'
QUEUE_SIZE = 40


def get_snapshot_key(snapshot_name):
    return SNAPSHOT_KEY + snapshot_name.encode('utf8')


async def load_snapshot(snapshot_name):
    """
    Load lines from a snapshot into its own DB, using async producer/consumer tasks.

    :param snapshot_name:
    :return:
    """
    print_with_timestamp(f'Loading latest downloaded snapshot {snapshot_name}')
    admin_db = get_connection('admin', read_only=False)
    loop = asyncio.get_event_loop()
    snapshot_key = get_snapshot_key(snapshot_name)
    if admin_db.get(snapshot_key):
        print_with_timestamp(f'Snapshot {snapshot_name} has already been loaded')
        loop.stop()

    data_db = get_connection(DATA_DB_PREFIX + snapshot_name, read_only=False)
    snapshot_path = os.path.join(DOWNLOAD_PATH, get_snapshot_path(snapshot_name))

    queue = asyncio.Queue(maxsize=QUEUE_SIZE)
    # schedule the consumer
    consumer = loop.create_task(consume_lines(queue, data_db))
    # wait for the producer to read the whole file
    await produce_lines(queue, snapshot_path)
    # wait until all lines have been processed
    await queue.join()
    # stop waiting for lines
    consumer.cancel()

    print_with_timestamp(f'Loading finished! Saving backup...')
    now = get_timestamp()
    admin_db.put(snapshot_key, now.encode('utf8'))

    backupper.create_backup(data_db, flush_before_backup=True)
    backupper.purge_old_backups(3)
    print_with_timestamp(f'Backup saved. All done!')


async def produce_lines(queue, snapshot_path):
    """
    Read lines from the snapshot file, split them, and put them in the queue.

    :param queue:
    :param snapshot_path:
    :return:
    """
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
            print_with_timestamp(
                f'Encountered bad line {stream_reader.last_line_number}: {repr(tsv_line)}\n'
                f'\twith incomplete line: {repr(stream_reader.incomplete_line)}'
            )
            continue

        split_line = (local_iri, singleton_id, cluster_id)
        await queue.put(split_line)


async def consume_lines(queue, data_db):
    """
    Take single split lines from the queue and load them into the data_db.

    :param queue:
    :param data_db:
    :return:
    """
    while True:
        local_iri, singleton_id, cluster_id = await queue.get()

        singleton_and_local = singleton_id + SINGLETON_LOCAL_SEPARATOR + local_iri
        data_db.merge(cluster_id, singleton_and_local, disable_wal=True)
        data_db.put(local_iri, cluster_id, disable_wal=True)
        if not singleton_id == cluster_id:
            data_db.put(singleton_id, cluster_id, disable_wal=True)

        queue.task_done()


class StreamingBZ2File:
    # TODO: implement multi-stream decompression

    def __init__(self, file_path, chunk_size=4*1024):
        assert chunk_size > 0, '`chunk_size` needs a non-zero number of bytes'
        self.decompressor = bz2.BZ2Decompressor()
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.last_line_number = 0
        self.incomplete_line = b''

    async def read_lines(self):
        self.last_line_number = 0
        self.incomplete_line = b''
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
                    if not chunk or b'\n' not in chunk:
                        # You must construct additional pylons!
                        # not enough bytes have been decompressed (no newline)
                        continue

                    lines = chunk.splitlines()
                    self.last_line_number += 1
                    yield self.incomplete_line + lines[0]
                    self.incomplete_line = b''

                    if chunk.endswith(b'\n'):
                        full_lines = lines[1:]
                    else:
                        full_lines = lines[1:-1]
                        self.incomplete_line = lines[-1]

                    for line in full_lines:
                        self.last_line_number += 1
                        yield line

            if self.incomplete_line:
                yield self.incomplete_line
