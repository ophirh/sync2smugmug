import asyncio
import itertools
import logging
from typing import Optional, Dict, List, Union

import dateutil.parser as dp

from .connection import SmugMugConnection
from .image import ImageOnSmugmug
from ..disk.node import FolderOnDisk, AlbumOnDisk
from ..node import Folder, Album

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

    async def upload(self,
                     from_disk_node: Union['FolderOnDisk', 'AlbumOnDisk'],
                     dry_run: bool) -> Union['FolderOnSmugmug', 'AlbumOnSmugmug']:
        """
        Upload a disk node (folder / album) to smugmug as a child to this object

        :return: The node representing the uploaded entity
        """

        # Create the folder/album on smugmug and return the record
        if from_disk_node.is_folder:
            new_node_record = \
                await self.connection.folder_create(parent_folder=self,
                                                    folder_on_disk=from_disk_node) if not dry_run else {}

            # Create a folder object from this record
            new_node = FolderOnSmugmug(parent=self,
                                       relative_path=from_disk_node.relative_path,
                                       record=new_node_record)

            self.sub_folders[new_node.relative_path] = new_node

            tasks = []

            sub_node: Union['FolderOnDisk', 'AlbumOnDisk']
            for sub_node in itertools.chain(from_disk_node.albums.values(), from_disk_node.sub_folders.values()):
                task = new_node.upload(from_disk_node=sub_node, dry_run=dry_run)
                tasks.append(asyncio.create_task(task))

            await asyncio.gather(*tasks)

        else:
            if not dry_run:
                new_node_record = \
                    await self.connection.album_create(parent_folder=self,
                                                       album_on_disk=from_disk_node)
            else:
                new_node_record = {}

            new_node = AlbumOnSmugmug(parent=self,
                                      relative_path=from_disk_node.relative_path,
                                      record=new_node_record)

            self.albums[new_node.relative_path] = new_node

            # Upload the images for this album
            await new_node.upload_images(from_album_on_disk=from_disk_node, dry_run=dry_run)

        return new_node

    async def delete(self, dry_run: bool):
        # Remove the album from the virtual tree
        if self.name in self.parent.sub_folders:
            del self.parent.sub_folders[self.name]

        if not dry_run:
            await self.connection.folder_delete(self)


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

    async def get_images(self) -> List[ImageOnSmugmug]:
        if self._images is None:
            # Lazy load images
            image_records = await self.connection.album_images_get(self.record)
            self._images = [
                ImageOnSmugmug(album=self, image_record=image)
                for image in image_records
            ]

        return self._images

    def reload_images(self):
        self._images = None  # Force reload on next read

    @property
    def image_count(self) -> int:
        return self.record['ImageCount']

    async def upload_images(self, from_album_on_disk: AlbumOnDisk, dry_run: bool):
        """
        Upload missing images for an album
        """

        tasks = []

        my_images = {i.relative_path: i for i in await self.get_images()}
        disk_images = await from_album_on_disk.get_images()

        missing_images = [i for i in disk_images if i.relative_path not in my_images]
        if missing_images:
            logger.info(f'Uploading {len(missing_images)} images from {from_album_on_disk} to {self}')

            if not dry_run:
                # noinspection PyTypeChecker
                tasks += [
                    asyncio.create_task(self.connection.image_upload(to_album=self, image_on_disk=image))
                    for image in missing_images
                ]

        for disk_image in disk_images:
            smugmug_image = my_images.get(disk_image.relative_path)
            if smugmug_image and disk_image.compare(smugmug_image) > 0:  # Only if disk is 'larger' than smugmug
                # Image needs replacement
                logger.info(f'Replacing image {disk_image} with {smugmug_image}')

                if not dry_run:
                    # noinspection PyTypeChecker
                    tasks.append(asyncio.create_task(self.connection.image_upload(to_album=self,
                                                                                  image_on_disk=disk_image,
                                                                                  image_to_replace=smugmug_image)))

        await asyncio.gather(*tasks)

        if tasks and not dry_run:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f'Album {from_album_on_disk} - Finished uploading')

            from_album_on_disk.update_sync_date(sync_date=from_album_on_disk.last_modified)

            self.reload_images()

    async def delete(self, dry_run: bool):
        # Remove the album from the virtual tree
        if self.name in self.parent.albums:
            del self.parent.albums[self.name]

        if not dry_run:
            await self.connection.album_delete(self)

    async def remove_duplicates(self, dry_run: bool):
        # Check images for duplicates
        images = list(await self.get_images())  # Local copy
        unique_images = set(images)

        duplicates = len(images) - len(unique_images)

        if duplicates > 0:
            logger.info(f"{self} - Deleting {duplicates} duplicate photos")

            while len(images) > 0:
                image = images.pop()

                if image in images:
                    # This is a duplicate!
                    await image.delete(dry_run=dry_run)

            self.reload_images()
