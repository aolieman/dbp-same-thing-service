import asyncio
import bz2
import os
import re
import traceback

from .db import get_connection, backupper, DATA_DB_PREFIX
from .source import DOWNLOAD_PATH, get_timestamp

DBP_GLOBAL_PREFIX = 'https://'
DBP_GLOBAL_MARKER = 'global.dbpedia.org/id/'
PART_FILE_KEY = b'file:'
UUID_RE = re.compile(r'part-\d+-([a-fA-F0-9\-]+)\.(?:nt|txt)\.bz2')


async def load_snapshot(executor):
    now = get_timestamp()
    print(f'[{now}] Loading all downloaded parts', flush=True)
    parts = os.listdir(DOWNLOAD_PATH)
    dataset_uuid = UUID_RE.search(parts[0]).group(1)
    admin_db = get_connection('admin', read_only=False)
    data_db = get_connection(DATA_DB_PREFIX + dataset_uuid, read_only=False)

    loop = asyncio.get_event_loop()
    loading_tasks = [
        loop.run_in_executor(executor, load_part, data_db, part_name)
        for part_name in parts
        if not admin_db.get(get_part_key(part_name))
    ]
    if loading_tasks:
        now = get_timestamp()
        print(f'[{now}] Waiting for loading tasks', flush=True)
        completed, pending = await asyncio.wait(loading_tasks)
        succeeded = []
        for task in completed:
            try:
                succeeded.append(task.result())
            except Exception:
                traceback.print_exc()

        now = get_timestamp()
        print(f'[{now}] All parts loaded! Saving backup...', flush=True)
        for part in succeeded:
            admin_db.put(get_part_key(part), now.encode('utf8'))

        backupper.create_backup(data_db, flush_before_backup=True)
        backupper.purge_old_backups(3)
        now = get_timestamp()
        print(f'[{now}] Backup saved. All done!', flush=True)

    # close the event loop
    loop.stop()


def get_part_key(part_name):
    return PART_FILE_KEY + part_name.encode('utf8')


def load_part(data_db, part):
    part_path = os.path.join(DOWNLOAD_PATH, part)
    triple_count = 0
    now = get_timestamp()
    print(f'[{now}] Loading: {part}', flush=True)

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
