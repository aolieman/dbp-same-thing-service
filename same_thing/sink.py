import asyncio
import bz2
import os
import traceback

from .db import get_connection, backupper, DATA_DB_PREFIX
from .source import DOWNLOAD_PATH, print_with_timestamp, get_snapshot_path, get_timestamp

DBP_GLOBAL_PREFIX = 'https://'
DBP_GLOBAL_MARKER = 'global.dbpedia.org/id/'
SNAPSHOT_KEY = b'snapshot:'
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

    loading_tasks = [
        loop.run_in_executor(
            executor,
            load_part,
            data_db,
            part_number,
            chunk_start,
            chunk_end
        )
        for part_number, chunk_start, chunk_end in compute_parts(snapshot_path)
    ]
    if loading_tasks:
        print_with_timestamp(f'Waiting for loading tasks')
        completed, pending = await asyncio.wait(loading_tasks)
        succeeded = []
        for task in completed:
            try:
                succeeded.append(task.result())
            except Exception:
                traceback.print_exc()

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
