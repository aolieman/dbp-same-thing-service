import os

import rocksdb
from rocksdb import CompressionType, BackupEngine
from rocksdb.interfaces import AssociativeMergeOperator

DB_ROOT_PATH = '/dbdata'
BACKUP_PATH = os.path.join(DB_ROOT_PATH, 'backups')
DATA_DB_PREFIX = 'uris_'
SEPARATOR = b'<>'


backupper = BackupEngine(BACKUP_PATH)


def get_connection(db_name, db_options=None, read_only=True):
    if db_options is None:
        db_options = get_rocksdb_options()

    db_path = os.path.join(DB_ROOT_PATH, db_name)
    return rocksdb.DB(db_path, db_options, read_only)


def split_values(value_bytes):
    return sorted([
        val.decode('utf8')
        for val in value_bytes.split(SEPARATOR)
    ])


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
    rocks_options.target_file_size_base = 67 * 1024**2
    rocks_options.max_log_file_size = 4 * 1024**2
    rocks_options.keep_log_file_num = 100

    rocks_options.table_factory = rocksdb.BlockBasedTableFactory(
        filter_policy=rocksdb.BloomFilterPolicy(10),
        block_cache=rocksdb.LRUCache(2 * (1024 ** 3)),
        block_cache_compressed=rocksdb.LRUCache(500 * (1024 ** 2))
    )
    return rocks_options
