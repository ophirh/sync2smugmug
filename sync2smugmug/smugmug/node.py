import asyncio
import itertools
import logging
from typing import Optional, Dict, List, Union

import dateutil.parser as dp
from aioretry import retry
from httpx import HTTPError

from .connection import SmugMugConnection, retry_policy
from .image import ImageOnSmugmug
from ..disk.image import ImageOnDisk
from ..disk.node import FolderOnDisk, AlbumOnDisk
from ..node import Folder, Album

logger = logging.getLogger(__name__)


class OnSmugmug:
    def __init__(
        self,
        record: Dict,
        parent: Optional["FolderOnSmugmug"],
        connection: SmugMugConnection = None,
    ):
        self._folder_record = record
        self._connection = connection or parent.connection

    @property
    def connection(self) -> SmugMugConnection:
        return self._connection

    @property
    def record(self) -> Dict:
        return self._folder_record

    async def delete(self, dry_run: bool):
        raise NotImplementedError()


class FolderOnSmugmug(Folder, OnSmugmug):
    def __init__(
        self,
        parent: Optional["FolderOnSmugmug"],
        relative_path: str,
        record: Dict,
        connection: SmugMugConnection = None,
    ):
        Folder.__init__(self, "Smug", parent, relative_path)
        OnSmugmug.__init__(self, record=record, parent=parent, connection=connection)

        self._folder_record = record

        self._sub_folders = {}
        self._albums = {}

    @property
    def parent(self) -> "FolderOnSmugmug":
        assert self._parent is None or isinstance(self._parent, FolderOnSmugmug)
        return self._parent

    @property
    def node_id(self) -> str:
        return self._folder_record["NodeID"]

    @property
    def uri(self) -> str:
        # A bug in Node's 'Uri' forces us to format the Uri manually
        return self._folder_record["Uri"]

    @property
    def connection(self) -> SmugMugConnection:
        return self._connection

    @property
    def record(self) -> Dict:
        return self._folder_record

    @property
    def sub_folders(self) -> Dict[str, "FolderOnSmugmug"]:
        return self._sub_folders

    @property
    def albums(self) -> Dict[str, "AlbumOnSmugmug"]:
        return self._albums

    @property
    def last_modified(self) -> float:
        """
        Get last modified time in Unix timestamp
        """
        folder_modified = self.record["DateModified"]  # u'2014-03-01T22:12:13+00:00'
        return dp.parse(folder_modified).timestamp()

    async def upload(
        self, from_disk_node: Union[FolderOnDisk, AlbumOnDisk], dry_run: bool
    ) -> Union["FolderOnSmugmug", "AlbumOnSmugmug"]:
        """
        Upload a disk node (folder / album) to smugmug as a child to this object

        :return: The node representing the uploaded entity
        """

        # Create the folder/album on smugmug and return the record
        if from_disk_node.is_folder:
            new_node_record = (
                await self._folder_create(
                    parent_folder=self, folder_on_disk=from_disk_node
                )
                if not dry_run
                else {}
            )

            # Create a folder object from this record
            new_node = FolderOnSmugmug(
                parent=self,
                relative_path=from_disk_node.relative_path,
                record=new_node_record,
            )

            self.sub_folders[new_node.relative_path] = new_node

            tasks = []

            sub_node: Union[FolderOnDisk, AlbumOnDisk]
            for sub_node in itertools.chain(
                from_disk_node.albums.values(), from_disk_node.sub_folders.values()
            ):
                task = new_node.upload(from_disk_node=sub_node, dry_run=dry_run)
                tasks.append(asyncio.create_task(task))

            await asyncio.gather(*tasks)

        else:
            new_node_record = (
                await self._album_create(
                    parent_folder=self, album_on_disk=from_disk_node
                )
                if not dry_run
                else {}
            )

            new_node = AlbumOnSmugmug(
                parent=self,
                relative_path=from_disk_node.relative_path,
                record=new_node_record,
            )

            self.albums[new_node.relative_path] = new_node

            # Upload the images for this album
            await new_node.upload_images(
                from_album_on_disk=from_disk_node, dry_run=dry_run
            )

        return new_node

    async def delete(self, dry_run: bool):
        # Remove the album from the virtual tree
        if self.parent:
            self.parent.remove_sub_folder(self)

        if not dry_run:
            await self.connection.request_delete(self.uri)

    async def _folder_create(
        self, parent_folder: "FolderOnSmugmug", folder_on_disk: FolderOnDisk
    ) -> Dict:
        """
        Create a new sub-folder under the parent folder
        """

        assert parent_folder.is_folder and folder_on_disk.is_folder

        post_url = parent_folder.record["Uris"]["Folders"]["Uri"]

        r = await self.connection.request_post(
            post_url,
            json={
                "Name": folder_on_disk.name,
                "UrlName": self.connection.encode_uri_name(folder_on_disk.name),
                "Privacy": "Unlisted",
            },
        )

        return r["Folder"]

    async def _album_create(
        self,
        parent_folder: "FolderOnSmugmug",
        album_on_disk: Union[FolderOnDisk, AlbumOnDisk],
    ) -> Dict:
        """
        Create a new album under the parent folder
        """
        assert parent_folder.is_folder and album_on_disk.is_album

        # post_url = parent_folder.record['Uris']['FolderAlbums']['Uri']
        #
        # r = await self.request_post(post_url,
        #                             json={
        #                                 'Name': album_on_disk.name,
        #                                 'UrlName': self._encode_uri_name(album_on_disk.name),
        #                                 'Privacy': 'Unlisted',
        #                             })

        # For some unknown reason, the Smugmug API returns 400 errors when trying to POST to the 'FolderAlbums' Uri
        # The workaround is to use the 'Node' endpoint and then lookup the album record
        node_url = parent_folder.record["Uris"]["Node"]["Uri"]
        r = await self.connection.request_post(
            f"{node_url}!children",
            json={
                "Name": album_on_disk.name,
                # 'UrlName': self._encode_uri_name(album_on_disk.name),
                # 'Privacy': 'Unlisted',
                "Type": "Album",
            },
        )

        # Wait for eventual consistency to settle before we lookup the record again
        await asyncio.sleep(0.5)

        # Lookup the album record
        album_url = r["Node"]["Uris"]["Album"]["Uri"]
        r = await self.connection.request_get(album_url)

        return r["Album"]


class AlbumOnSmugmug(Album, OnSmugmug):
    def __init__(
        self,
        parent: FolderOnSmugmug,
        relative_path: str,
        record: Dict,
        connection: SmugMugConnection = None,
    ):
        Album.__init__(self, "Smug", parent, relative_path)
        OnSmugmug.__init__(self, record=record, parent=parent, connection=connection)

        self._images = None

    @property
    def parent(self) -> FolderOnSmugmug:
        assert isinstance(self._parent, FolderOnSmugmug)
        return self._parent

    @property
    def album_uri(self) -> str:
        return self.record["Uri"]

    @property
    def last_modified(self) -> float:
        """
        Get last modified time in Unix timestamp
        """
        album_modified = self.record["LastUpdated"]  # u'2014-03-01T22:12:13+00:00'
        album_date_modified = dp.parse(album_modified).timestamp()

        images_modified = self.record["ImagesLastUpdated"]
        images_date_modified = dp.parse(images_modified).timestamp()

        # Parse UTC date string and return timestamp
        return max(album_date_modified, images_date_modified)

    async def get_images(self) -> List[ImageOnSmugmug]:
        if self._images is None:
            # Lazy load images
            image_records = await self.connection.unpack_pagination(
                relative_uri=self.record["Uris"]["AlbumImages"]["Uri"],
                object_name="AlbumImage",
            )

            self._images = [
                ImageOnSmugmug(album=self, record=image) for image in image_records
            ]

        return self._images

    def reload_images(self):
        self._images = None  # Force reload on next read

    @property
    def image_count(self) -> int:
        return self.record["ImageCount"]

    async def upload_images(self, from_album_on_disk: AlbumOnDisk, dry_run: bool):
        """
        Upload missing images for an album
        """

        tasks = []

        smugmug_images = {i.relative_path: i for i in await self.get_images()}
        disk_images = await from_album_on_disk.get_images()
        disk_images_dict = {i.smugmug_relative_path: i for i in disk_images}

        # TODO: Need to convert names!!!!

        missing_images = [
            i for p, i in disk_images_dict.items() if p not in smugmug_images
        ]
        if missing_images:
            logger.info(
                f"Uploading {len(missing_images)} missing images from {from_album_on_disk} to {self}"
            )

            if not dry_run:
                # noinspection PyTypeChecker
                tasks += [
                    asyncio.create_task(self.image_upload(image_on_disk=image))
                    for image in missing_images
                ]

        for smugmug_relative_path, disk_image in disk_images_dict.items():
            smugmug_image = smugmug_images.get(smugmug_relative_path)
            if (
                smugmug_image and disk_image.compare(smugmug_image) > 0
            ):  # Only if disk is 'larger' than smugmug
                # Image needs replacement
                logger.info(f"Replacing image {disk_image} with {smugmug_image}")

                if not dry_run:
                    # noinspection PyTypeChecker
                    tasks.append(
                        asyncio.create_task(
                            self.image_upload(
                                image_on_disk=disk_image, image_to_replace=smugmug_image
                            )
                        )
                    )

        await asyncio.gather(*tasks)

        if tasks and not dry_run:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Album {from_album_on_disk} - Finished uploading")

            from_album_on_disk.update_sync_date(
                sync_date=from_album_on_disk.last_modified
            )

            self.reload_images()

    async def download_images(self, to_album_on_disk: AlbumOnDisk, dry_run: bool):
        """
        Download missing images from the album on smugmug to the disk

        :param to_album_on_disk: Album to download to
        :param dry_run: If True, will not actually download anything
        """

        disk_images = {
            i.smugmug_relative_path: i for i in await to_album_on_disk.get_images()
        }
        smugmug_images = await self.get_images()
        missing_images = {
            i for i in smugmug_images if i.relative_path not in disk_images
        }

        if missing_images:
            logger.info(
                f"Preparing to download {len(missing_images)} images from {self}..."
            )

            if not dry_run:
                # noinspection PyTypeChecker
                tasks = [
                    asyncio.create_task(
                        self.image_download(
                            to_album=to_album_on_disk, image_on_smugmug=image
                        )
                    )
                    for image in missing_images
                ]

                await asyncio.gather(*tasks)

                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Finished downloading {self}")

                to_album_on_disk.update_sync_date(sync_date=self.last_modified)
                self.reload_images()

    @retry(retry_policy)
    async def image_download(
        self, to_album: AlbumOnDisk, image_on_smugmug: ImageOnSmugmug
    ):
        """
        Download a single image from the Album on Smugmug to a folder on disk
        """

        # Create an image object to figure out local path
        image_on_disk = ImageOnDisk(
            album=to_album, relative_path=image_on_smugmug.relative_path
        )

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Downloading {image_on_smugmug} to {to_album}")

        await self.connection.request_download(
            image_uri=await self.get_download_url(image_on_smugmug),
            local_path=image_on_disk.disk_path,
        )

        logger.info(f"Downloaded {image_on_smugmug}")

    async def get_download_url(self, image_on_smugmug: ImageOnSmugmug) -> str:
        # TODO: Problem! The downloaded name has the original (.avi / .mov) extension but the stored format is mp4.
        if (
            image_on_smugmug.is_video
            and "LargestVideo" in image_on_smugmug.record["Uris"]
        ):
            # Need to fetch the largest video url - videos are NOT accessible via 'ArchivedUri'
            r = await self.connection.request_get(
                image_on_smugmug.record["Uris"]["LargestVideo"]["Uri"]
            )
            return r["LargestVideo"]["Url"]

        # The archived version holds the original copy of the photo (but not for videos)
        return image_on_smugmug.record["ArchivedUri"]

    @retry(retry_policy)
    async def image_upload(
        self, image_on_disk: ImageOnDisk, image_to_replace: ImageOnSmugmug = None
    ):
        """
        Upload an image to an album

        :param ImageOnDisk image_on_disk: Image to upload
        :param ImageOnSmugmug image_to_replace: Image to replace (optional)
        """

        try:
            action = "Upload" if image_to_replace is None else "Replace"

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"{action} {image_on_disk}")

            # Read the entire file into memory for multipart upload (and for the oauth signature to work)
            with open(image_on_disk.disk_path, "rb") as f:
                image_data = f.read()

            await self.connection.request_upload(
                album_uri=self.album_uri,
                image_data=image_data,
                image_name=image_on_disk.name,
                keywords=image_on_disk.keywords,
                image_to_replace_uri=image_to_replace.image_uri
                if image_to_replace
                else None,
            )

            logger.info(f"{action}ed {image_on_disk}")

        except HTTPError:
            logger.exception(f"Failed to upload {image_on_disk} to {self}")
            raise

    async def delete(self, dry_run: bool):
        # Remove the album from the virtual tree of its parent
        if self.parent:
            self.parent.remove_album(self)

        if not dry_run:
            await self.connection.request_delete(self.album_uri)

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
