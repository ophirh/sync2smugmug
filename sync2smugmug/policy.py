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
    def test(cls) -> SyncAction:
        """
        For testing...
        """
        return SyncAction(optimize_on_disk=True, optimize_online=True)

    @classmethod
    def get_presets(cls) -> List[str]:
        """
        Return a list of available presets
        """
        static_methods = []

        for k, v in cls.__dict__.items():
            # All methods (excluding this one) are considered presets
            if isinstance(v, classmethod) and k != cls.get_presets.__name__:
                static_methods.append(k)

        return sorted(static_methods)
