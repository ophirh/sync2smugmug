import dataclasses
import json
import logging
import time
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path, PurePath
from typing import ClassVar, Generator, Tuple, List

from sync2smugmug import models, protocols
from sync2smugmug.utils import image_tools

logger = logging.getLogger(__name__)


@dataclass
class SyncData:
    sync_time: float
    online_time: float
    disk_time: float


@dataclass
class DiskFolderInfo:
    disk_path: Path


@dataclass
class DiskAlbumInfo:
    SYNC_DATA_FILENAME: ClassVar[str] = "smugmug_sync.json"

    disk_path: Path
    sync_data: SyncData | None = None

    def __post_init__(self):
        if self.sync_file_path.exists():
            try:
                with self.sync_file_path.open() as f:
                    d = json.load(f)
                    self.sync_data = SyncData(**d)

            except Exception:   # noqa
                # On any error reading the JSON, just reset the data
                self.remember_sync(None)

    @property
    def sync_file_path(self) -> Path:
        return self.disk_path.joinpath(self.SYNC_DATA_FILENAME)

    @property
    def online_time(self) -> float | None:
        if self.sync_data is None:
            return None

        return self.sync_data.online_time

    @property
    def disk_time(self) -> float | None:
        if self.sync_data is None:
            return None

        return self.sync_data.disk_time

    @property
    def last_updated(self) -> float:
        return self.disk_path.lstat().st_mtime

    def remember_sync(self, online_time: float | None):
        """ Update sync and disk time and persist it to disk """

        if online_time is not None:
            # Set the sync data and persist to disk
            self.sync_data = SyncData(
                sync_time=time.time(),
                online_time=online_time,
                disk_time=self.disk_path.lstat().st_mtime,  # Capture disk update time at the time of record
            )

            with self.sync_file_path.open("w") as f:
                json.dump(dataclasses.asdict(self.sync_data), f)

        else:
            # Reset the sync data and delete the file
            self.sync_data = None
            self.sync_file_path.unlink(missing_ok=True)


@dataclass
class DiskImageInfo:
    image_disk_path: Path
    developed_disk_path: Path = None

    @property
    def disk_path(self):
        return self.developed_disk_path or self.image_disk_path

    @property
    def has_developed(self) -> bool:
        return self.developed_disk_path is not None

    @cached_property
    def size(self) -> int:
        return self.disk_path.lstat().st_size


def create_album_disk_info(parent_disk_path: Path, album_name: str, dry_run: bool) -> protocols.DiskAlbumInfoShape:
    album_disk_path = parent_disk_path.joinpath(album_name)
    if not dry_run:
        album_disk_path.mkdir(exist_ok=True)

    return DiskAlbumInfo(disk_path=album_disk_path) # noqa


def load_album_images(album: models.Album):
    images: List[models.Image] = []

    for image_path, developed_path in iter_image_files(dir_path_to_scan=album.disk_info.disk_path):
        image = models.Image(
            album_relative_path=album.relative_path,
            filename=PurePath(image_path.name),
            disk_info=DiskImageInfo(image_disk_path=image_path, developed_disk_path=developed_path) # noqa
        )
        images.append(image)

    album.images = images
    album.image_count = len(album.images)


def delete_image_from_disk(image: models.Image, dry_run: bool):
    assert image.is_on_disk, "Expecting image to be on disk"

    if not dry_run:
        image.disk_info.disk_path.unlink()

    logger.info(f"Deleted image {image}")


def iter_image_files(dir_path_to_scan: Path) -> Generator[Tuple[Path, Path], None, None]:
    # Add support for 'Developed' sub-source_folder. This is a special case when working with LightRoom and developing
    # raw images. The developed version of the image was exported as a jpeg into a sub-folder called 'Developed'. In
    # this case, while the physical file is under 'Developed', the logical path is where the photo should have been.
    developed_images = {}
    developed_images_sub_folder = dir_path_to_scan.joinpath('Developed')

    if developed_images_sub_folder.exists():
        for image_path in developed_images_sub_folder.iterdir():
            if image_tools.is_image(image_path):
                developed_images[image_path.name] = image_path

    for image_path in dir_path_to_scan.iterdir():
        if image_tools.is_image(image_path):
            # If there is a Developed version of this image - use it instead
            developed_image_path = developed_images.get(image_path.name)
            yield image_path, developed_image_path


def create_folder(parent: models.Folder, folder_name: str, dry_run: bool) -> protocols.DiskFolderInfoShape:
    folder_disk_path = parent.disk_info.disk_path.joinpath(folder_name)
    if not dry_run:
        folder_disk_path.mkdir(exist_ok=True)

    return DiskFolderInfo(disk_path=folder_disk_path)   # noqa
