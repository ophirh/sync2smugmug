from enum import Enum
from typing import Tuple


class SyncTypeAction(Enum):
    DOWNLOAD = 'DOWNLOAD'
    UPLOAD = 'UPLOAD'
    DELETE_ON_DISK = 'DELETE_ON_DISK'
    DELETE_ON_CLOUD = 'DELETE_ON_CLOUD'


class SyncType:
    @classmethod
    def local_backup(cls) -> Tuple[SyncTypeAction, ...]:
        return SyncTypeAction.DOWNLOAD,

    @classmethod
    def local_backup_clean(cls) -> Tuple[SyncTypeAction, ...]:
        return SyncTypeAction.DOWNLOAD, SyncTypeAction.DELETE_ON_DISK

    @classmethod
    def online_backup(cls) -> Tuple[SyncTypeAction, ...]:
        return SyncTypeAction.UPLOAD,

    @classmethod
    def online_backup_clean(cls) -> Tuple[SyncTypeAction, ...]:
        return SyncTypeAction.UPLOAD, SyncTypeAction.DELETE_ON_CLOUD,

    @classmethod
    def two_way_sync_no_deletes(cls) -> Tuple[SyncTypeAction, ...]:
        """
        Sync both disk and smugmug - to make sure both systems have the same images (only additive, no deletions)
        """
        return SyncTypeAction.UPLOAD, SyncTypeAction.DOWNLOAD,