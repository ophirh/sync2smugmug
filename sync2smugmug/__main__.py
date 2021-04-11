from typing import Tuple

from .config import config
from .policy import SyncType, SyncTypeAction
from .sync import scan, sync, print_summary


def main():
    print(config)

    sync_type: Tuple[SyncTypeAction] = SyncType.online_backup()
    # sync_type: Tuple[SyncTypeAction] = SyncType.online_backup_clean()
    # sync_type: Tuple[SyncTypeAction] = SyncType.local_backup()
    # sync_type: Tuple[SyncTypeAction] = SyncType.two_way_sync_no_deletes()

    on_disk, on_smugmug = scan()

    diff = sync(on_disk=on_disk, on_smugmug=on_smugmug, sync_type=sync_type)
    print_summary(on_disk, on_smugmug, diff)


main()
