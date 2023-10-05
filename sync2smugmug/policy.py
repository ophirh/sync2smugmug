from enum import Enum
from typing import Tuple


class SyncTypeAction(Enum):
    DOWNLOAD = "DOWNLOAD"
    UPLOAD = "UPLOAD"
    DELETE_ON_DISK = "DELETE_ON_DISK"
    DELETE_ON_CLOUD = "DELETE_ON_CLOUD"
    DELETE_ONLINE_DUPLICATES = "DELETE_ONLINE_DUPLICATES"
    PRE_PROCESS_DISK = "PRE_PROCESS_DISK"


class SyncType:
    @classmethod
    def local_backup(cls) -> Tuple[SyncTypeAction, ...]:
        return (SyncTypeAction.DOWNLOAD,)

    @classmethod
    def local_backup_clean(cls) -> Tuple[SyncTypeAction, ...]:
        return (
            *cls.local_backup(),
            SyncTypeAction.DELETE_ON_DISK,
        )

    @classmethod
    def online_backup(cls) -> Tuple[SyncTypeAction, ...]:
        return (
            SyncTypeAction.UPLOAD,
            # SyncTypeAction.PRE_PROCESS_DISK,
        )

    @classmethod
    def online_backup_clean(cls) -> Tuple[SyncTypeAction, ...]:
        return (
            *cls.online_backup(),
            SyncTypeAction.DELETE_ONLINE_DUPLICATES,
        )

    @classmethod
    def two_way_sync_no_deletes(cls) -> Tuple[SyncTypeAction, ...]:
        """
        Sync both disk and Smugmug - to make sure both systems have the same images (only additive, no deletions)
        """
        return (
            SyncTypeAction.UPLOAD,
            SyncTypeAction.DOWNLOAD,
        )

    @classmethod
    def test(cls) -> Tuple[SyncTypeAction, ...]:
        """
        For testing...
        """
        return (SyncTypeAction.PRE_PROCESS_DISK,)
