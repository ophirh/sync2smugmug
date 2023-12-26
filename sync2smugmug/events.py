import logging
from abc import ABC
from dataclasses import dataclass, field
from typing import Protocol

from sync2smugmug import models, policy
from sync2smugmug.online import online

logger = logging.getLogger(__name__)


class EventGroup(Protocol):
    FOLDER_ADD: str
    ALBUM_ADD: str
    FOLDER_DELETE: str
    ALBUM_DELETE: str
    ALBUM_SYNC: str

    @classmethod
    def delete_permitted(cls, sync_action: policy.SyncAction) -> bool:
        raise NotImplementedError


class DiskEventGroup(EventGroup):
    """ The set of events that can be fired during a disk sync (download) """
    FOLDER_ADD = "download_folder"
    ALBUM_ADD = "download_album"
    FOLDER_DELETE = "delete_folder_disk"
    ALBUM_DELETE = "delete_album_disk"
    ALBUM_SYNC = "sync_album"

    @classmethod
    def delete_permitted(cls, sync_action: policy.SyncAction) -> bool:
        return sync_action.delete_on_disk


class OnlineEventGroup(EventGroup):
    """ The set of events that can be fired during an online sync (upload) """
    FOLDER_ADD = "upload_folder"
    ALBUM_ADD = "upload_album"
    FOLDER_DELETE = "delete_folder_online"
    ALBUM_DELETE = "delete_album_online"
    ALBUM_SYNC = "sync_album"

    @classmethod
    def delete_permitted(cls, sync_action: policy.SyncAction) -> bool:
        return sync_action.delete_online


@dataclass(frozen=True)
class EventData(ABC):
    """ Base class for all events"""
    message: str
    connection: online.OnlineConnection = field(default=None, kw_only=True, repr=False)


@dataclass(frozen=True)
class FolderEventData(EventData):
    source_folder: models.Folder
    target_parent: models.Folder


@dataclass(frozen=True)
class AlbumEventData(EventData):
    source_album: models.Album
    target_parent: models.Folder


@dataclass(frozen=True)
class SyncAlbumImagesEventData(EventData):
    disk_album: models.Album
    online_album: models.Album
    sync_action: policy.SyncAction = field(repr=False)


@dataclass(frozen=True)
class DeleteFolderEventData(EventData):
    target: models.Folder
    parent: models.Folder = None


@dataclass(frozen=True)
class DeleteAlbumEventData(EventData):
    target: models.Album
    parent: models.Folder = None
