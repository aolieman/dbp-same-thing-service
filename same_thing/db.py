import os
import time

import rocksdb
from rocksdb import CompressionType, BackupEngine
from rocksdb.interfaces import AssociativeMergeOperator

DB_ROOT_PATH = '/dbdata'
BACKUP_PATH = os.path.join(DB_ROOT_PATH, 'backups')
DATA_DB_PREFIX = 'snapshot_'
SEPARATOR = b'<>'
SINGLETON_LOCAL_SEPARATOR = b'||'


backupper = BackupEngine(BACKUP_PATH)


def get_connection(db_name, db_options=None, read_only=True):
    if db_options is None:
        db_options = get_rocksdb_options()

    if db_name.startswith(DB_ROOT_PATH):
        db_path = db_name
    else:
        db_path = os.path.join(DB_ROOT_PATH, db_name)

    return rocksdb.DB(db_path, db_options, read_only)


def get_connection_to_latest(max_retries=0, retry=0, **kwargs):
    data_dbs = [
        os.path.join(DB_ROOT_PATH, sdir)
        for sdir in os.listdir(DB_ROOT_PATH)
        if looks_like_datadb(sdir)
    ]
    if data_dbs:
        latest_db = max(data_dbs, key=os.path.getmtime)
        return get_connection(latest_db, **kwargs)
    elif retry < max_retries:
        wait_seconds = 2 ** retry
        print(f'No DB found: will retry in {wait_seconds} seconds', flush=True)
        time.sleep(wait_seconds)
        return get_connection_to_latest(max_retries, 1 + retry, **kwargs)
    else:
        raise OSError(f'No DBs found in {DB_ROOT_PATH}')


def looks_like_datadb(dir_name):
    return (
        dir_name.startswith(DATA_DB_PREFIX)
        and os.path.isdir(
            os.path.join(DB_ROOT_PATH, dir_name)
        )
    )


def split_values(value_bytes):
    secondary_separator = SINGLETON_LOCAL_SEPARATOR.decode('utf8')
    return [
        val.decode('utf8').split(secondary_separator, maxsplit=1)
        for val in value_bytes.split(SEPARATOR)
    ]


def is_cluster_membership(value_bytes):
    return SINGLETON_LOCAL_SEPARATOR in value_bytes


def sorted_cluster(value_bytes):
    values = split_values(value_bytes)
    sorted_values = sorted(
        values,
        key=lambda t: (len(t[0]), t[0])
    )
    singletons, local_ids = zip(*sorted_values)
    return singletons, local_ids


class StringAddOperator(AssociativeMergeOperator):
    def merge(self, key, existing_value, value):
        new_value = existing_value or value
        if existing_value and value not in existing_value:
            new_value = existing_value + SEPARATOR + value

        return True, new_value

    def name(self):
        return b'StringAddOperator'


def get_rocksdb_options():
    rocks_options = rocksdb.Options()
    rocks_options.create_if_missing = True
    rocks_options.merge_operator = StringAddOperator()
    rocks_options.compression = CompressionType.zstd_compression
    rocks_options.max_open_files = 300000
    rocks_options.write_buffer_size = 67 * 1024**2
    rocks_options.max_write_buffer_number = 3
    rocks_options.target_file_size_base = 256 * 1024**2
    rocks_options.max_log_file_size = 4 * 1024**2
    rocks_options.keep_log_file_num = 100

    # we want to set this option, but it's not included in the python client
    # rocks_options.optimize_filters_for_hits = True

    rocks_options.table_factory = rocksdb.BlockBasedTableFactory(
        block_cache=rocksdb.LRUCache(1 * 1024**3),
        block_size=16 * 1024,
        filter_policy=rocksdb.BloomFilterPolicy(10),
    )
    return rocks_options
