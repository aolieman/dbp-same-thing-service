from same_thing.db import get_db_path, backupper, get_connection, get_data_db_name
from same_thing.source import print_with_timestamp


BACKUP_PREFIX = b'backup:'


class BackupNotFound(Exception):
    pass


def get_backup_key(backup_id):
    return b'%s%d' % (BACKUP_PREFIX, backup_id)


def create_backup(data_db, snapshot_name):
    backupper.create_backup(data_db, flush_before_backup=True)
    backup_id = next(reversed(backupper.get_backup_info()))['backup_id']
    admin_db = get_connection('admin', read_only=False)
    backup_key = get_backup_key(backup_id)
    admin_db.put(backup_key, snapshot_name.encode('utf8'))
    backupper.purge_old_backups(2)
    print_with_timestamp(
        f'Backup of {snapshot_name} was created with ID {backup_id}'
    )
    return backup_id


def restore_backup(backup_id, db_name):
    db_path = get_db_path(db_name)
    backupper.restore_backup(backup_id, db_path, db_path)
    print_with_timestamp(
        f'Backup {backup_id} was succesfully restored to {db_path}'
    )


def restore_latest_with_name(snapshot_name):
    available_backups = reversed(backupper.get_backup_info())
    db_name = get_data_db_name(snapshot_name)
    admin_db = get_connection('admin', read_only=True)
    available_snapshots = []
    for backup_meta in available_backups:
        backup_key = get_backup_key(backup_meta['backup_id'])
        backup_snapshot = admin_db.get(backup_key)
        available_snapshots.append((backup_key, backup_snapshot))
        if backup_snapshot == snapshot_name.encode('utf8'):
            restore_backup(backup_meta['backup_id'], db_name)
            break
    else:
        raise BackupNotFound(
            f'No backup of {snapshot_name} was found in {available_snapshots}'
        )
