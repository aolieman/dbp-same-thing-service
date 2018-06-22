import asyncio
import bz2
import os
import re
import traceback

import rocksdb
from rocksdb import CompressionType, BackupEngine
from rocksdb.interfaces import AssociativeMergeOperator

from .source import DOWNLOAD_PATH

DB_PATH = '/dbdata'
DBP_GLOBAL_PREFIX = 'http://'
SEPARATOR = b'<>'


class StringAddOperator(AssociativeMergeOperator):
    def merge(self, key, existing_value, value):
        new_value = existing_value or value
        if existing_value and value not in existing_value:
            new_value = existing_value + SEPARATOR + value

        return True, new_value

    def name(self):
        return b'StringAddOperator'


rocks_options = rocksdb.Options()
rocks_options.create_if_missing = True
rocks_options.merge_operator = StringAddOperator()
rocks_options.compression = CompressionType.zstd_compression
rocks_options.max_open_files = 300000
rocks_options.write_buffer_size = 67108864
rocks_options.max_write_buffer_number = 3
rocks_options.target_file_size_base = 67108864

rocks_options.table_factory = rocksdb.BlockBasedTableFactory(
    filter_policy=rocksdb.BloomFilterPolicy(10),
    block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
    block_cache_compressed=rocksdb.LRUCache(500 * (1024 ** 2))
)
db = rocksdb.DB(DB_PATH, rocks_options)


async def load_all_parts(executor):
    print('Loading all downloaded parts', flush=True)
    parts = set(os.listdir(DOWNLOAD_PATH))

    loop = asyncio.get_event_loop()
    loading_tasks = [
        loop.run_in_executor(executor, load_part, part_name)
        for part_name in parts
    ]
    print('Waiting for loading tasks', flush=True)
    completed, pending = await asyncio.wait(loading_tasks)
    for task in completed:
        try:
            task.result()
        except Exception:
            traceback.print_exc()

    print('All parts loaded! Compacting...', flush=True)
    db.compact_range()

    print('Done compacting. Saving backup...', flush=True)
    backupper = BackupEngine(DB_PATH + '/backups')
    backupper.create_backup(db, flush_before_backup=True)
    print('Backup saved. All done!', flush=True)


def load_part(part):
    part_path = os.path.join(DOWNLOAD_PATH, part)
    triple_count = 0
    print(f'Loading: {part}', flush=True)
    with bz2.open(part_path) as triples:
        for line in triples:
            subj, _, obj = get_uris_from_line(line.decode('utf8'))
            dbp_global = subj.lstrip(DBP_GLOBAL_PREFIX).encode('utf8')
            local_id = obj.encode('utf8')
            db.merge(dbp_global, local_id, disable_wal=True)
            db.put(local_id, dbp_global, disable_wal=True)
            triple_count += 1

    print(f'Finished {triple_count} triples from {part}')


iri_pattern = r'<(?:[^\x00-\x1F<>"{}|^`\\]|\\u[0-9A-Fa-f]{4}|\\U[0-9A-Fa-f]{8})*>'
ntriple_pattern = (rf'(?P<subject>{iri_pattern})\s*'
                   rf'(?P<predicate>{iri_pattern})\s*'
                   rf'(?P<object>{iri_pattern})\s*\.')
ntriple_re = re.compile(ntriple_pattern)


def parse_triple(ntriple_line):
    return ntriple_re.search(ntriple_line).groups()


def get_uris_from_line(ntriple_line):
    return tuple(
        uri.strip('<>')
        for uri in parse_triple(ntriple_line)
    )
