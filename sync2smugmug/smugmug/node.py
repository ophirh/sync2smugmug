import itertools
import logging
from typing import Optional, Dict, List, Union

import dateutil.parser as dp

from .connection import SmugMugConnection
from .image import ImageOnSmugmug
from ..disk import FolderOnDisk, AlbumOnDisk
from ..node import Folder, Album
from ..utils import TaskPool

logger = logging.getLogger(__name__)


class FolderOnSmugmug(Folder):
    def __init__(self,
                 parent: Optional['FolderOnSmugmug'],
                 relative_path: str,
                 record: Dict,
                 smugmug_connection: SmugMugConnection = None):
        super().__init__('Smug', parent, relative_path)

        self._folder_record = record
        self._connection = smugmug_connection or parent.connection

        self._sub_folders = {}
        self._albums = {}

    @property
    def parent(self) -> 'FolderOnSmugmug':
        assert isinstance(self._parent, FolderOnSmugmug)
        return self._parent

    @property
    def node_id(self) -> str:
        return self._folder_record['NodeID']

    @property
    def uri(self) -> str:
        # A bug in Node's 'Uri' forces us to format the Uri manually
        return self._folder_record['Uri']

    @property
    def connection(self) -> SmugMugConnection:
        return self._connection

    @property
    def record(self) -> Dict:
        return self._folder_record

    @property
    def sub_folders(self) -> Dict[str, 'FolderOnSmugmug']:
        return self._sub_folders

    @property
    def albums(self) -> Dict[str, 'AlbumOnSmugmug']:
        return self._albums

    @property
    def last_modified(self) -> float:
        """
        Get last modified time in Unix timestamp
        """
        folder_modified = self.record['DateModified']  # u'2014-03-01T22:12:13+00:00'
        return dp.parse(folder_modified).timestamp()

    def upload(self,
               from_disk_node: Union['FolderOnDisk', 'AlbumOnDisk'],
               dry_run: bool) -> Union['FolderOnSmugmug', 'AlbumOnSmugmug']:
        """
        Upload a disk node (folder / album) to smugmug as a child to this object

        :return: The node representing the uploaded entity
        """

        # Create the folder/album on smugmug and return the record
        new_node_record = \
            self.connection.node_create(parent_folder=self, node_on_disk=from_disk_node) if not dry_run else {}

        if from_disk_node.is_folder:
            # Create a folder object from this record
            new_node = FolderOnSmugmug(parent=self,
                                       relative_path=from_disk_node.relative_path,
                                       record=new_node_record)
            self.sub_folders[new_node.relative_path] = new_node

            sub_node: Union['FolderOnDisk', 'AlbumOnDisk']
            for sub_node in itertools.chain(from_disk_node.albums.values(), from_disk_node.sub_folders.values()):
                new_node.upload(from_disk_node=sub_node, dry_run=dry_run)

        else:
            new_node = AlbumOnSmugmug(parent=self,
                                      relative_path=from_disk_node.relative_path,
                                      record=new_node_record)
            self.albums[new_node.relative_path] = new_node

            # Upload the images for this album
            new_node.upload_images(from_album_on_disk=from_disk_node, dry_run=dry_run)

        return new_node

    def delete(self, dry_run: bool):
        # Remove the album from the virtual tree
        if self.name in self.parent.sub_folders:
            del self.parent.sub_folders[self.name]

        if not dry_run:
            self.connection.folder_delete(self)


class AlbumOnSmugmug(Album):
    def __init__(self,
                 parent: FolderOnSmugmug,
                 relative_path: str,
                 record: Dict,
                 connection: SmugMugConnection = None):
        super().__init__('Smug', parent, relative_path)

        self._album_record = record
        self._connection = connection or parent.connection
        self._images = None

    @property
    def parent(self) -> FolderOnSmugmug:
        assert isinstance(self._parent, FolderOnSmugmug)
        return self._parent

    @property
    def record(self) -> Dict:
        return self._album_record

    @property
    def connection(self) -> SmugMugConnection:
        return self._connection

    @property
    def album_uri(self) -> str:
        return self.record['Uri']

    @property
    def last_modified(self) -> float:
        """
        Get last modified time in Unix timestamp
        """
        album_modified = self.record['LastUpdated']  # u'2014-03-01T22:12:13+00:00'
        album_date_modified = dp.parse(album_modified).timestamp()

        images_modified = self.record['ImagesLastUpdated']
        images_date_modified = dp.parse(images_modified).timestamp()

        # Parse UTC date string and return timestamp
        return max(album_date_modified, images_date_modified)

    @property
    def images(self) -> List[ImageOnSmugmug]:
        if self._images is None:
            self._images = [ImageOnSmugmug(album=self, image_record=image)
                            for image in self.connection.album_images_get(self.record)]

        return self._images

    def reload_images(self):
        self._images = None  # Force reload on next read

    @property
    def image_count(self) -> int:
        return self.record['ImageCount']

    def upload_images(self, from_album_on_disk: AlbumOnDisk, dry_run: bool):
        """
        Upload missing images for an album
        """

        missing_images = [i for i in from_album_on_disk.images if i not in self]
        logger.info(f'Uploading {len(missing_images)} images from {from_album_on_disk} to {self}')

        if not dry_run:
            task_pool = TaskPool()
            for image in missing_images:
                self.connection.image_upload(task_pool=task_pool, to_album=self, image_on_disk=image)

            task_pool.join()

            logger.debug(f'Album {from_album_on_disk} - Finished downloading')
            from_album_on_disk.update_sync_date(sync_date=from_album_on_disk.last_modified)

            self.reload_images()

    def delete(self, dry_run: bool):
        # Remove the album from the virtual tree
        if self.name in self.parent.albums:
            del self.parent.albums[self.name]

        if not dry_run:
            self.connection.album_delete(self)
