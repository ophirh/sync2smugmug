import itertools
import json
import logging
import os
from typing import List, Optional, Dict, Union

from shutil import rmtree

from ..config import config
from ..disk import ImageOnDisk
from ..node import Folder, Album
from ..utils import TaskPool

logger = logging.getLogger(__name__)

SYNC_DATA_FILENAME: str = 'smugmug_sync.json'


class OnDisk:
    def __init__(self, relative_path: str, makedirs: bool):
        self.base_dir = config.base_dir
        self.disk_path = f'{self.base_dir}{relative_path}'

        if makedirs and not os.path.exists(self.disk_path):
            os.makedirs(self.disk_path)
        else:
            assert os.path.exists(self.disk_path) and os.path.isdir(self.disk_path)

    def delete(self, dry_run: bool):
        if not dry_run:
            rmtree(self.disk_path)


# noinspection PyAbstractClass
class FolderOnDisk(Folder, OnDisk):
    def __init__(self, parent: Optional['FolderOnDisk'], relative_path: str, makedirs: bool = False):
        Folder.__init__(self, 'Disk', parent, relative_path)
        OnDisk.__init__(self, relative_path=self.relative_path, makedirs=makedirs)

        self._sub_folders = {}
        self._albums = {}

    @property
    def parent(self) -> 'FolderOnDisk':
        assert isinstance(self._parent, FolderOnDisk)
        return self._parent

    @property
    def sub_folders(self) -> Dict[str, 'FolderOnDisk']:
        return self._sub_folders

    @property
    def albums(self) -> Dict[str, 'AlbumOnDisk']:
        return self._albums

    @property
    def last_modified(self) -> float:
        return os.path.getmtime(self.disk_path)

    def download(self,
                 from_smugmug_node: Union['AlbumOnSmugmug', 'FolderOnSmugmug'],
                 dry_run: bool) -> Union['AlbumOnDisk', 'FolderOnDisk']:
        """
        Download to disk a smugmug node (folder / album) as a child to this object

        :return: The node representing the uploaded entity
        """

        logger.info(f'Download {from_smugmug_node} to {self}')

        if from_smugmug_node.is_folder:
            # Create the folder object
            new_node = FolderOnDisk(parent=self,
                                    relative_path=from_smugmug_node.relative_path,
                                    makedirs=True)

            sub_node_on_smugmug: Union['FolderOnSmugmug', 'AlbumOnSmugmug']
            for sub_node in itertools.chain(from_smugmug_node.albums.values(), from_smugmug_node.sub_folders.values()):
                new_node.download(from_smugmug_node=sub_node, dry_run=dry_run)

            self.sub_folders[new_node.relative_path] = new_node

        else:
            new_node = AlbumOnDisk(parent=self,
                                   relative_path=from_smugmug_node.relative_path,
                                   makedirs=True)

            # Upload the images for this album
            new_node.download_images(from_album_on_smugmug=from_smugmug_node, dry_run=dry_run)

            self.albums[new_node.relative_path] = new_node

        return new_node


# noinspection PyAbstractClass
class AlbumOnDisk(Album, OnDisk):
    def __init__(self, parent: FolderOnDisk, relative_path: str, makedirs: bool = False):
        Album.__init__(self, 'Disk', parent, relative_path)
        OnDisk.__init__(self, relative_path=self.relative_path, makedirs=makedirs)

        self.sync_data: Dict = self._load_sync_data()
        self._images: Optional[List[ImageOnDisk]] = None

    @property
    def parent(self) -> FolderOnDisk:
        assert isinstance(self._parent, FolderOnDisk)
        return self._parent

    @property
    def images(self) -> List[ImageOnDisk]:
        if self._images is None:
            # Lazy initialize images
            self._images = [ImageOnDisk(album=self,
                                        relative_path=os.path.join(self.relative_path, f))
                            for f in os.listdir(self.disk_path) if ImageOnDisk.is_image(f)]

        return self._images

    def reload_images(self):
        self._images = None  # Reload images on next call

    @property
    def last_modified(self) -> float:
        """
        Get last modified time in Unix timestamp. Use the last_sync in meta-data (if available) to prevent redundant
        syncs and properly detect changes
        """
        current_disk_modify_date = os.path.getmtime(self.disk_path)
        disk_date_on_last_sync = self.sync_data.get('disk_date', 0)
        sync_date = self.sync_data.get('sync_date', 0)

        if sync_date and abs(current_disk_modify_date - disk_date_on_last_sync) < 20:  # Allow for a few secs time diff
            # Disk was not modified since the last sync. Return last sync date
            return sync_date
        else:
            # Disk was updated AFTER the last sync
            return current_disk_modify_date

    @classmethod
    def has_images(cls, disk_path: str) -> bool:
        return any(True for f in os.listdir(disk_path) if ImageOnDisk.is_image(f))

    def _load_sync_data(self) -> Dict:
        p = os.path.join(self.disk_path, SYNC_DATA_FILENAME)
        if os.path.exists(p):
            try:
                with open(p) as f:
                    return json.load(f)

            except Exception:
                # On any error reading the JSON, just delete it (we will sync again)
                self.delete_sync_data()

        else:
            return {}

    def update_sync_date(self, sync_date: float):
        """
        :param sync_date: sync date
        """
        sync_data = self._load_sync_data()
        sync_data['sync_date'] = sync_date
        sync_data['disk_date'] = os.path.getmtime(self.disk_path)

        with open(os.path.join(self.disk_path, SYNC_DATA_FILENAME), 'w+') as f:
            json.dump(sync_data, f)

    def delete_sync_data(self):
        p = os.path.join(self.disk_path, SYNC_DATA_FILENAME)
        if os.path.exists(p):
            os.remove(p)

    def download_images(self, from_album_on_smugmug: 'AlbumOnSmugmug', dry_run: bool):
        """
        Download missing images from the album on smugmug to the disk

        :param from_album_on_smugmug: Album to download from
        :param dry_run: If True, will not actually download anything
        """

        missing_images = [i for i in from_album_on_smugmug.images if i not in self]
        logger.info(f'Downloading {len(missing_images)} images from {from_album_on_smugmug} to {self}')

        if not dry_run:
            task_pool = TaskPool()

            for image in missing_images:
                from_album_on_smugmug.connection.image_download(task_pool=task_pool,
                                                                to_album=self,
                                                                image_on_smugmug=image)

            task_pool.join()
            logger.info(f'Downloaded {self}')

            self.update_sync_date(sync_date=self.last_modified)
            self.reload_images()
