import dataclasses
from typing import List


@dataclasses.dataclass(frozen=True)
class SyncAction:
    """ Represents what the sync policy is guiding the synchronization to do """
    download: bool = False
    upload: bool = False
    delete_on_disk: bool = False
    delete_online: bool = False
    optimize_on_disk: bool = False
    optimize_online: bool = False


class SyncActionPresets:
    """ A set of pre-configured workflows to be set from the command-line """

    @classmethod
    def local_backup(cls) -> SyncAction:
        return SyncAction(download=True)

    @classmethod
    def local_backup_clean(cls) -> SyncAction:
        return dataclasses.replace(
            cls.local_backup(),
            delete_on_disk=True,
            optimize_on_disk=True,
        )

    @classmethod
    def online_backup(cls) -> SyncAction:
        return SyncAction(upload=True)  # optimize_on_disk=True

    @classmethod
    def online_backup_clean(cls) -> SyncAction:
        return dataclasses.replace(
            cls.online_backup(),
            delete_online=True,
            optimize_online=True
        )

    @classmethod
    def optimize(cls) -> SyncAction:
        """
        Run optimizations only
        """
        return SyncAction(optimize_on_disk=True, optimize_online=True)


def get_presets() -> List[str]:
    """
    Return a list of available presets
    """
    methods = [k for k, v in SyncActionPresets.__dict__.items()]
    return sorted(methods)
