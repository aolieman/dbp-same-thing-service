from datetime import datetime

from tabulate import tabulate

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


def get_available_snapshots():
    available_backups = reversed(backupper.get_backup_info())
    admin_db = get_connection('admin', read_only=True)
    available_snapshots = []
    for backup_meta in available_backups:
        backup_key = get_backup_key(backup_meta['backup_id'])
        backup_snapshot = admin_db.get(backup_key) or b'unknown'
        available_snapshots.append(
            {
                'id': backup_meta['backup_id'],
                'key': backup_key.decode('utf8'),
                'snapshot': backup_snapshot.decode('utf8'),
                'timestamp': datetime.utcfromtimestamp(
                    backup_meta['timestamp']
                ).astimezone().isoformat(timespec='seconds')
            }
        )
    return available_snapshots


def restore_latest_with_name(snapshot_name):
    db_name = get_data_db_name(snapshot_name)
    available_snapshots = get_available_snapshots()
    for snap in available_snapshots:
        if snap['snapshot'] == snapshot_name:
            restore_backup(snap['id'], db_name)
            break
    else:
        raise BackupNotFound(
            f'No backup of {snapshot_name} was found in {available_snapshots}'
        )


def restore_interactively():
    available_snapshots = get_available_snapshots()
    print(tabulate(available_snapshots, headers='keys'))

    while True:
        query = input('Which backup would you like to restore? ')
        for snap in available_snapshots:
            db_name = get_data_db_name(snap['snapshot'])
            backup_id = None

            if query in snap:
                backup_id = snap['id']
            else:
                try:
                    selected_id = int(query)
                except ValueError:
                    selected_id = -1

                if selected_id == snap['id']:
                    backup_id = selected_id

            if backup_id:
                restore_backup(backup_id, db_name)
                return
        else:
            example_id = available_snapshots[0]['id']
            example_name = available_snapshots[0]['snapshot']
            print(
                f'Tip: any of the shown attributes should work, '
                f'e.g. to restore the latest backup enter '
                f'"{example_id}" or "{example_name}", but not both.'
            )


if __name__ == '__main__':
    restore_interactively()
