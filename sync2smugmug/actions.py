import logging
from typing import Union, Tuple

from .disk import FolderOnDisk, AlbumOnDisk
from .node import Folder, Album
from .policy import SyncTypeAction
from .smugmug import FolderOnSmugmug, AlbumOnSmugmug

logger = logging.getLogger(__name__)


class Action:
    def perform(self, dry_run: bool):
        raise NotImplementedError()

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} ({self.__dict__})'


class AddAction(Action):
    def __init__(self, what_to_add: Union[Folder, Album], parent_to_add_to: Folder, message: str = None):
        super().__init__()

        self.what_to_add = what_to_add
        self.parent_to_add_to = parent_to_add_to
        self.message = message or ''

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} (add {self.what_to_add} to {self.parent_to_add_to} ({self.message})'

    def perform(self, dry_run: bool):
        raise NotImplementedError()


class RemoveAction(Action):
    def __init__(self, what_to_remove):
        super().__init__()

        self.what_to_remove = what_to_remove

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} (remove {self.what_to_remove})'

    def perform(self, dry_run: bool):
        # Simply delete the subtree
        logger.info(f'Delete {self.what_to_remove}')
        self.what_to_remove.delete(dry_run=dry_run)


class RemoveFromDiskAction(RemoveAction):
    pass


class RemoveFromSmugmugAction(RemoveAction):
    pass


class DownloadAction(AddAction):
    def __init__(self,
                 what_to_add: Union[FolderOnSmugmug, AlbumOnSmugmug],
                 parent_to_add_to: FolderOnDisk,
                 message: str = None):
        """
        :param what_to_add: Smugmug node to download (will add entire subtree)
        :param parent_to_add_to: Disk node of parent directory
        """
        super().__init__(what_to_add, parent_to_add_to, message)

    def perform(self, dry_run: bool):
        """
        Download the entire subtree indicated by self.smugmug_node to the disk
        """
        assert isinstance(self.what_to_add, (FolderOnSmugmug, AlbumOnSmugmug))
        assert isinstance(self.parent_to_add_to, FolderOnDisk)

        self.parent_to_add_to.download(from_smugmug_node=self.what_to_add, dry_run=dry_run)


class UploadAction(AddAction):
    def __init__(self,
                 what_to_add: Union[FolderOnDisk, AlbumOnDisk],
                 parent_to_add_to: Union[FolderOnSmugmug, AlbumOnSmugmug],
                 message: str = None):
        """
        Recursively scan a disk node and upload its contents to Smugmug

        :param what_to_add: Smugmug node to add (will add entire subtree)
        :param parent_to_add_to: Disk node of parent directory
        """
        super().__init__(what_to_add, parent_to_add_to, message)

    def perform(self, dry_run: bool):
        assert isinstance(self.what_to_add, (FolderOnDisk, AlbumOnDisk))
        assert isinstance(self.parent_to_add_to, FolderOnSmugmug)

        logger.info(f'Upload {self.what_to_add} to {self.parent_to_add_to} ({self.message})')

        if not dry_run:
            self.parent_to_add_to.upload(from_disk_node=self.what_to_add, dry_run=dry_run)


class SyncAlbumsAction(Action):
    def __init__(self,
                 disk_album: AlbumOnDisk,
                 smugmug_album: AlbumOnSmugmug,
                 sync_type: Tuple[SyncTypeAction, ...]):
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

    def perform(self, dry_run: bool):
        sync_complete = False
        changed = False

        if SyncTypeAction.DOWNLOAD in self.sync_action:
            missing_images = any(i for i in self.smugmug_album.images if i not in self.disk_album)
            if missing_images:
                self.disk_album.download_images(from_album_on_smugmug=self.smugmug_album, dry_run=dry_run)
                sync_complete = True
                changed = True

        if SyncTypeAction.UPLOAD in self.sync_action:
            missing_images = any(i for i in self.disk_album.images if i not in self.smugmug_album)
            if missing_images:
                self.smugmug_album.upload_images(from_album_on_disk=self.disk_album, dry_run=dry_run)
                sync_complete = True
                changed = True

        if SyncTypeAction.DELETE_ON_DISK in self.sync_action:
            for image_to_delete_on_disk in (i for i in self.disk_album.images if i not in self.smugmug_album):
                image_to_delete_on_disk.delete(dry_run=dry_run)
                changed = True

            self.disk_album.reload_images()  # Make sure album re-syncs

            sync_complete = True

        if SyncTypeAction.DELETE_ON_CLOUD in self.sync_action:
            for image_to_delete_on_smugmug in (i for i in self.smugmug_album.images if i not in self.disk_album):
                image_to_delete_on_smugmug.delete(dry_run=dry_run)
                changed = True

            self.smugmug_album.reload_images()  # Make sure album re-syncs

            sync_complete = True

        if not changed:
            # Simply update the sync date so we don't try to sync again
            logger.info(f'Update sync date {self.disk_album}')
            self.disk_album.update_sync_date(sync_date=self.smugmug_album.last_modified)

        if sync_complete and not dry_run:
            # Now that we're done, mark the date of synchronization on album
            logger.info(f'Update sync date {self.disk_album}')
            self.disk_album.update_sync_date(sync_date=self.smugmug_album.last_modified)


class UpdateAlbumSyncData(Action):
    def __init__(self,
                 disk_album: Union[AlbumOnDisk],
                 smugmug_album: Union[AlbumOnSmugmug]):
        """
        Update the album's sync data
        """
        super().__init__()

        self.disk_album: AlbumOnDisk = disk_album
        self.smugmug_album: AlbumOnSmugmug = smugmug_album

    def perform(self, dry_run: bool):
        # Simply update the sync date so we don't try to sync again
        logger.info(f'Update sync date {self.disk_album}')
        self.disk_album.update_sync_date(sync_date=self.smugmug_album.last_modified)
