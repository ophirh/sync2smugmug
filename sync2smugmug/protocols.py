from pathlib import Path
from typing import Protocol, Dict


class DiskInfoShape(Protocol):
    @property
    def disk_path(self) -> Path:
        raise NotImplementedError


class DiskFolderInfoShape(Protocol):
    @property
    def disk_path(self) -> Path:
        raise NotImplementedError


class DiskAlbumInfoShape(Protocol):
    @property
    def disk_path(self) -> Path:
        raise NotImplementedError

    @property
    def online_time(self) -> float | None:
        raise NotImplementedError

    @property
    def disk_time(self) -> float | None:
        raise NotImplementedError

    @property
    def last_updated(self) -> float:
        raise NotImplementedError

    def remember_sync(self, online_time: float | None):
        """
        Persists current sync times to disk.

        Sync time will be taken as now(). Disk time will be taken as last update from disk and online time is provided.

        If online_time is None - this will reset the sync data (sync_data property will return None)
        """
        raise NotImplementedError


class DiskImageInfoShape(Protocol):
    @property
    def disk_path(self) -> Path:
        raise NotImplementedError

    @property
    def image_disk_path(self) -> Path:
        raise NotImplementedError

    @property
    def developed_disk_path(self) -> Path | None:
        raise NotImplementedError

    @property
    def has_developed(self) -> bool:
        raise NotImplementedError

    @property
    def size(self) -> int:
        raise NotImplementedError


class OnlineInfoShape(Protocol):
    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def uri(self) -> str:
        raise NotImplementedError


class OnlineAlbumInfoShape(Protocol):
    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def uri(self) -> str:
        raise NotImplementedError

    @property
    def images_uri(self) -> str:
        raise NotImplementedError

    @property
    def last_updated(self) -> float:
        raise NotImplementedError

    @property
    def image_count(self) -> int:
        raise NotImplementedError


class OnlineFolderInfoShape(Protocol):
    @property
    def record(self) -> Dict:
        raise NotImplementedError

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def uri(self) -> str:
        raise NotImplementedError

    @property
    def sub_folders_uri(self) -> str:
        raise NotImplementedError

    @property
    def albums_uri(self) -> str:
        raise NotImplementedError


class OnlineImageInfoShape(Protocol):
    @property
    def record(self) -> Dict:
        raise NotImplementedError

    @property
    def name(self) -> str:
        raise NotImplementedError

    @property
    def uri(self) -> str:
        raise NotImplementedError

    @property
    def size(self) -> int:
        raise NotImplementedError

    @property
    def is_video(self) -> bool:
        raise NotImplementedError
