import asyncio
import logging
from typing import Union, Tuple

from .disk.node import FolderOnDisk, AlbumOnDisk
from .node import Folder, Album, Node
from .policy import SyncTypeAction
from .smugmug.node import FolderOnSmugmug, AlbumOnSmugmug

logger = logging.getLogger(__name__)


class Action:
    async def perform(self, dry_run: bool):
        raise NotImplementedError()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} ({self.__dict__})"


class AddAction(Action):
    def __init__(
        self,
        what_to_add: Union[Folder, Album],
        parent_to_add_to: Folder,
        message: str = None,
    ):
        super().__init__()

        self.what_to_add = what_to_add
        self.parent_to_add_to = parent_to_add_to
        self.message = message or ""

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} (add {self.what_to_add} to {self.parent_to_add_to})"

    async def perform(self, dry_run: bool):
        raise NotImplementedError()


class DownloadAction(AddAction):
    def __init__(
        self,
        what_to_add: Union[FolderOnSmugmug, AlbumOnSmugmug],
        parent_to_add_to: FolderOnDisk,
        message: str = None,
    ):
        """
        :param what_to_add: Smugmug node to download (will add entire subtree)
        :param parent_to_add_to: Disk node of parent directory
        """
        super().__init__(what_to_add, parent_to_add_to, message)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__} ({self.what_to_add} to {self.parent_to_add_to})"
        )

    async def perform(self, dry_run: bool):
        """
        Download the entire subtree indicated by self.smugmug_node to the disk
        """
        assert isinstance(self.what_to_add, (FolderOnSmugmug, AlbumOnSmugmug))
        assert isinstance(self.parent_to_add_to, FolderOnDisk)

        await self.parent_to_add_to.download(
            from_smugmug_node=self.what_to_add, dry_run=dry_run
        )


class UploadAction(AddAction):
    def __init__(
        self,
        what_to_add: Union[FolderOnDisk, AlbumOnDisk],
        parent_to_add_to: FolderOnSmugmug,
        message: str = None,
    ):
        """
        Upload a disk node and its subtree to Smugmug

        :param what_to_add: Smugmug node to add (will add entire subtree)
        :param parent_to_add_to: Disk node of parent directory
        """
        super().__init__(what_to_add, parent_to_add_to, message)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__} ({self.what_to_add} to {self.parent_to_add_to})"
        )

    async def perform(self, dry_run: bool):
        assert isinstance(self.what_to_add, (FolderOnDisk, AlbumOnDisk))
        assert isinstance(self.parent_to_add_to, FolderOnSmugmug)

        logger.info(
            f"Upload {self.what_to_add} to {self.parent_to_add_to} ({self.message})"
        )
        await self.parent_to_add_to.upload(
            from_disk_node=self.what_to_add, dry_run=dry_run
        )


class RemoveAction(Action):
    def __init__(self, what_to_remove: Node):
        super().__init__()

        self.what_to_remove = what_to_remove

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} ({self.what_to_remove})"

    async def perform(self, dry_run: bool):
        # Simply delete the subtree
        logger.info(f"Delete {self.what_to_remove}")
        await self.what_to_remove.delete(dry_run=dry_run)


class RemoveFromDiskAction(RemoveAction):
    pass


class RemoveFromSmugmugAction(RemoveAction):
    pass


class SyncAlbumsAction(Action):
    def __init__(
        self,
        disk_album: AlbumOnDisk,
        smugmug_album: AlbumOnSmugmug,
        sync_type: Tuple[SyncTypeAction, ...],
    ):
        """
        Synchronizes albums. Any action will be done synchronously here to make sure completion is properly updated.

        :param disk_album: Album on disk
        :param smugmug_album: Album on Smugmug
        :param sync_type: What to do
        """
        super().__init__()

        self.disk_album = disk_album
        self.smugmug_album = smugmug_album
        self.sync_action = sync_type

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} ({self.disk_album} and {self.smugmug_album} [{self.sync_action}])"

    # noinspection DuplicatedCode
    async def perform(self, dry_run: bool):
        if SyncTypeAction.DOWNLOAD in self.sync_action:
            await self.smugmug_album.download_images(
                to_album_on_disk=self.disk_album, dry_run=dry_run
            )

        if SyncTypeAction.UPLOAD in self.sync_action:
            await self.smugmug_album.upload_images(
                from_album_on_disk=self.disk_album, dry_run=dry_run
            )

        if SyncTypeAction.DELETE_ON_DISK in self.sync_action:
            tasks = []

            for image_to_delete_on_disk in await self.disk_album.get_images():
                if not await self.smugmug_album.contains_image(image_to_delete_on_disk):
                    tasks.append(
                        asyncio.create_task(
                            image_to_delete_on_disk.delete(dry_run=dry_run)
                        )
                    )

            await asyncio.gather(*tasks)
            self.disk_album.reload_images()

        if SyncTypeAction.DELETE_ON_CLOUD in self.sync_action:
            tasks = []

            for image_to_delete_on_smugmug in await self.smugmug_album.get_images():
                if not await self.disk_album.contains_image(image_to_delete_on_smugmug):
                    tasks.append(
                        asyncio.create_task(
                            image_to_delete_on_smugmug.delete(dry_run=dry_run)
                        )
                    )

            await asyncio.gather(*tasks)
            self.smugmug_album.reload_images()  # Make sure album re-syncs

        if SyncTypeAction.DELETE_ONLINE_DUPLICATES in self.sync_action:
            # Detect and remove duplicates in the Smugmug album (images with same name)
            await self.smugmug_album.remove_duplicates(dry_run=dry_run)
            self.smugmug_album.reload_images()  # Make sure album re-syncs

        if not dry_run:
            self.disk_album.update_sync_date(sync_date=self.smugmug_album.last_modified)


class UpdateAlbumSyncDataAction(Action):
    def __init__(
        self, disk_album: Union[AlbumOnDisk], smugmug_album: Union[AlbumOnSmugmug]
    ):
        """
        Update the album's sync data (no real changes were detected)
        """
        super().__init__()

        self.disk_album: AlbumOnDisk = disk_album
        self.smugmug_album: AlbumOnSmugmug = smugmug_album

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} (for {self.disk_album})"

    async def perform(self, dry_run: bool):
        self.disk_album.update_sync_date(sync_date=self.smugmug_album.last_modified)


class RemoveOnlineDuplicatesAction(Action):
    def __init__(self, smugmug_album: Union[AlbumOnSmugmug]):
        """
        Check and remove duplicates on Smugmug
        """
        super().__init__()

        self.smugmug_album: AlbumOnSmugmug = smugmug_album

    def __repr__(self) -> str:
        return f"{self.__class__.__name__} (in {self.smugmug_album})"

    async def perform(self, dry_run: bool):
        await self.smugmug_album.remove_duplicates(dry_run=dry_run)
