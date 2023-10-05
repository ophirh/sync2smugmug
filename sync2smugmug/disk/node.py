import itertools
import json
import logging
import os
import typing
from typing import List, Optional, Dict, Union, Generator

from shutil import rmtree

from ..config import config
from ..disk.image import ImageOnDisk
from ..node import Folder, Album

if typing.TYPE_CHECKING:
    # Import only for type checking
    from ..smugmug.node import AlbumOnSmugmug, FolderOnSmugmug

logger = logging.getLogger(__name__)

SYNC_DATA_FILENAME: str = "smugmug_sync.json"


class OnDisk:
    def __init__(self, relative_path: str, makedirs: bool):
        self.base_dir = config.base_dir
        self.disk_path = os.path.join(
            self.base_dir, relative_path, ""
        )  # Add trailing slash to indicate a folder

        if makedirs and not os.path.exists(self.disk_path):
            os.makedirs(self.disk_path)

        assert os.path.exists(self.disk_path) and os.path.isdir(self.disk_path)

    async def delete(self, dry_run: bool):
        if not dry_run:
            rmtree(self.disk_path)


# noinspection PyAbstractClass
class FolderOnDisk(Folder["FolderOnDisk", "AlbumOnDisk", "ImageOnDisk"], OnDisk):
    def __init__(
        self,
        parent: Optional["FolderOnDisk"],
        relative_path: str,
        makedirs: bool = False,
    ):
        Folder.__init__(self, "Disk", parent, relative_path)
        OnDisk.__init__(self, relative_path=self.relative_path, makedirs=makedirs)

    @classmethod
    def has_sub_folders(cls, disk_path: str) -> bool:
        with os.scandir(disk_path) as entries:
            return any(entry.is_dir() for entry in entries)

    async def download(
        self,
        from_smugmug_node: Union["AlbumOnSmugmug", "FolderOnSmugmug"],
        dry_run: bool,
    ) -> Union["AlbumOnDisk", "FolderOnDisk"]:
        """
        Download to disk a Smugmug node (folder / album) as a child to this object

        :return: The node representing the uploaded entity
        """

        # Case #1 - this is a folder. We need to recursively download down the tree
        if from_smugmug_node.is_folder:
            # Create the folder object
            new_node = FolderOnDisk(
                parent=self,
                relative_path=from_smugmug_node.relative_path,
                makedirs=True,
            )

            # Recursively download sub-folders and albums
            for sub_node in itertools.chain(
                from_smugmug_node.albums.values(),
                from_smugmug_node.sub_folders.values(),
            ):
                # Do this synchronously to make sure we complete one album/folder before moving to the next
                await new_node.download(from_smugmug_node=sub_node, dry_run=dry_run)

            self.sub_folders[new_node.name] = new_node

        # Case #2 - This is an album - we need to download its images
        else:
            new_node = AlbumOnDisk(
                parent=self,
                relative_path=from_smugmug_node.relative_path,
                makedirs=True,
            )

            # Upload the images for this album
            await from_smugmug_node.download_images(
                to_album_on_disk=new_node, dry_run=dry_run
            )

            self.albums[new_node.name] = new_node

        return new_node

    async def delete(self, dry_run: bool):
        # Remove the album from the virtual tree of its parent
        if self.parent:
            self.parent.remove_sub_folder(self)

        await OnDisk.delete(self, dry_run)


# noinspection PyAbstractClass
class AlbumOnDisk(Album["FolderOnDisk", "AlbumOnDisk", "ImageOnDisk"], OnDisk):
    def __init__(
        self, parent: FolderOnDisk, relative_path: str, makedirs: bool = False
    ):
        Album.__init__(self, "Disk", parent, relative_path)
        OnDisk.__init__(self, relative_path=self.relative_path, makedirs=makedirs)

        self.sync_data: Dict = self._load_sync_data()

    @property
    def parent(self) -> FolderOnDisk:
        assert isinstance(self._parent, FolderOnDisk)
        return self._parent

    @property
    def image_count(self) -> int:
        return len(self._sync_get_images())

    async def get_images(self) -> List[ImageOnDisk]:
        # Simply call the sync version
        return self._sync_get_images()

    def _sync_get_images(self) -> List[ImageOnDisk]:
        if self._images is None:
            # Lazy initialize images
            self._images = list(self.iter_images())

        return self._images

    def iter_images(self) -> Generator[ImageOnDisk, None, None]:
        with os.scandir(self.disk_path) as entries:
            for de in entries:
                if de.is_file() and ImageOnDisk.check_is_image(de.path):
                    yield ImageOnDisk(
                        album=self, relative_path=os.path.join(self.relative_path, de.name)
                    )

    def reload_images(self):
        self._images = None  # Reload images on next call

    @property
    def last_modified(self) -> float:
        """
        Get last modified time in Unix timestamp. Use the last_sync in meta-data (if available) to prevent redundant
        syncs and properly detect changes
        """

        current_disk_modify_date = os.path.getmtime(self.disk_path)
        disk_date_on_last_sync = self.sync_data.get("disk_date", 0)
        sync_date = self.sync_data.get("sync_date", 0)

        # Allow for 1/2 hour diff
        if sync_date and abs(current_disk_modify_date - disk_date_on_last_sync) < 1800:
            # Disk was not modified since the last sync. Return last sync date
            return sync_date
        else:
            # Disk was updated AFTER the last sync
            return current_disk_modify_date

    @classmethod
    def has_images(cls, disk_path: str) -> bool:
        return any(
            True
            for f in os.listdir(disk_path)
            if ImageOnDisk.check_is_image(os.path.join(disk_path, f))
        )

    def _load_sync_data(self) -> Dict:
        p = os.path.join(self.disk_path, SYNC_DATA_FILENAME)
        if os.path.exists(p):
            # noinspection PyBroadException
            try:
                with open(p) as f:
                    return json.load(f)

            except Exception:
                # On any error reading the JSON, just delete it (this will force to sync again)
                self.delete_sync_data()

        else:
            return {}

    def update_sync_date(self, sync_date: float):
        """
        :param sync_date: sync date
        """
        logger.info(f"Update sync date {self}")

        sync_data = self._load_sync_data()
        sync_data["sync_date"] = sync_date
        sync_data["disk_date"] = os.path.getmtime(self.disk_path)

        with open(os.path.join(self.disk_path, SYNC_DATA_FILENAME), "w+") as f:
            json.dump(sync_data, f)

    def delete_sync_data(self):
        p = os.path.join(self.disk_path, SYNC_DATA_FILENAME)
        if os.path.exists(p):
            os.remove(p)

    async def delete(self, dry_run: bool):
        # Remove the album from the virtual tree of its parent
        if self.parent:
            self.parent.remove_album(self)

        await OnDisk.delete(self, dry_run)
