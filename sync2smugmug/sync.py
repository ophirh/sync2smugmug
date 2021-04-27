import logging
from typing import Tuple, Callable, List, Type, Optional

from .actions import (
    Action,
    DownloadAction,
    RemoveFromDiskAction,
    RemoveFromSmugmugAction,
    UploadAction,
    SyncAlbumsAction,
    UpdateAlbumSyncData,
    AddAction,
    RemoveAction,
)
from .config import config
from .disk import DiskScanner, FolderOnDisk, AlbumOnDisk
from .node import Album, Folder
from .policy import SyncTypeAction
from .smugmug import SmugmugScanner, FolderOnSmugmug, AlbumOnSmugmug
from .utils import TaskPool, timeit

logger = logging.getLogger(__name__)


def scan() -> Tuple[FolderOnDisk, FolderOnSmugmug]:
    """
    Scan both disk and Smugmug to create two virtual trees of both systems
    """

    on_disk = DiskScanner(base_dir=config.base_dir).scan()
    logger.info(f'Scan results (on disk): {on_disk.stats()}')

    on_smugmug = SmugmugScanner(account=config.account,
                                consumer_key=config.consumer_key,
                                consumer_secret=config.consumer_secret,
                                access_token=config.access_token,
                                access_token_secret=config.access_token_secret,
                                use_test_folder=config.use_test_folder).scan()
    logger.info(f'Scan results (on smugmug): {on_smugmug.stats()}')

    return on_disk, on_smugmug


def sync(on_disk: FolderOnDisk,
         on_smugmug: FolderOnSmugmug,
         action_callback: Callable[[Action], None] = None) -> List[Action]:
    """
    Given scan results, generates a set of actions required to sync between the disk and smugmug
    If action_callback is provided, it will be called for each action (allowing on the fly sync)

    :param on_disk: Root for on disk scans
    :param on_smugmug: Root for on smugmug scans
    :param action_callback: An optional callback function that is called for each action added
    """

    sync_type = config.sync

    actions: List[Action] = []

    if action_callback is None:
        def default_callback(action: Action, _: int = None):
            # Run action without the wrapper
            action.perform(config.dry_run)
            actions.append(action)

        action_callback = default_callback

    generate_sync_actions(on_disk=on_disk,
                          on_smugmug=on_smugmug,
                          action_callback=action_callback,
                          sync_type=sync_type)

    print_summary(on_disk, on_smugmug, actions)

    TaskPool.wait_for_all_tasks()

    return actions


def recurse_sync_folders(from_folder: Folder,
                         to_folder: Folder,
                         parent_of_to_folder: Optional[Folder],
                         add_action_class: Type[AddAction],
                         remove_action_class: Type[RemoveAction],
                         should_delete: bool,
                         action_callback: Callable[[Action], None],
                         sync_type: Tuple[SyncTypeAction, ...]):
    """
    :param from_folder: Root node to compare (main)
    :param to_folder: Root node to compare (secondary) (can be None)
    :param parent_of_to_folder: Parent of to_node
    :param add_action_class:
    :param remove_action_class:
    :param should_delete: If True, objects (on 2) will also be deleted if not matched (on 1)
    :param action_callback: Optional call back for actions
    :param sync_type
    """

    assert from_folder is not None

    if not from_folder.is_root and from_folder.parent.is_root:
        # Show progress on top folders
        logger.info(f'Synchronizing {from_folder.relative_path}')

    if to_folder is None:
        assert parent_of_to_folder is not None

        logger.debug(f'[+{from_folder.source[0]}] {from_folder.relative_path}')
        action = add_action_class(what_to_add=from_folder,
                                  parent_to_add_to=parent_of_to_folder,
                                  message='entire folder')
        action_callback(action)

        return

    assert from_folder.relative_path == to_folder.relative_path

    # Recursively apply to sub-folders of from_folder.
    for name, from_sub_folder in from_folder.sub_folders.items():
        to_sub_folder = to_folder.sub_folders.get(name)

        recurse_sync_folders(from_folder=from_sub_folder,
                             to_folder=to_sub_folder,
                             parent_of_to_folder=to_folder,
                             add_action_class=add_action_class,
                             remove_action_class=remove_action_class,
                             should_delete=should_delete,
                             action_callback=action_callback,
                             sync_type=sync_type)

    if should_delete:
        delete = []

        # If delete is required, delete all children of 'to_node' that do not exist in 'from_node'
        for name, to_sub_folder in to_folder.sub_folders.items():
            if name not in from_folder.sub_folders:
                logger.debug(f'[-{to_sub_folder.source[0]}] {to_sub_folder.relative_path}')
                delete.append(to_sub_folder)

        for node in delete:
            action = remove_action_class(what_to_remove=node)
            action_callback(action)

    # Now go over albums in the same way
    # Recursively apply to sub-folders of from_folder.
    for name, from_album in from_folder.albums.items():
        to_album = to_folder.albums.get(name)

        sync_albums(from_album=from_album,
                    to_album=to_album,
                    parent_to_folder=to_folder,
                    add_action_class=add_action_class,
                    action_callback=action_callback,
                    sync_type=sync_type)

    if should_delete:
        delete = []

        # If delete is required, delete all children of 'to_node' that do not exist in 'from_node'
        for name, to_album in to_folder.albums.items():
            if name not in from_folder.albums:
                logger.debug(f'[-{to_album.source[0]}] {to_album.relative_path}')
                delete.append(to_album)

        for node in delete:
            action = remove_action_class(what_to_remove=node)
            action_callback(action)


def sync_albums(from_album: Album,
                to_album: Album,
                parent_to_folder: Folder,
                add_action_class: Type[AddAction],
                action_callback: Callable[[Action], None],
                sync_type: Tuple[SyncTypeAction, ...]):
    """
    Sync images from both versions of albums
    """
    assert from_album is not None

    if to_album is None:
        logger.debug(f'[+{from_album.source[0]}] {from_album.relative_path}')
        action = add_action_class(what_to_add=from_album, parent_to_add_to=parent_to_folder, message='entire album')
        action_callback(action)

        return

    assert from_album.relative_path == to_album.relative_path

    # Check if node changed (will check last update date, meta-data, etc...)
    need_sync = False
    only_update_sync_data = False

    if from_album.shallow_compare(to_album) != 0:
        # Shallow compare shows equality
        if from_album.deep_compare(to_album, shallow_compare_first=False) == 0:
            # Objects are really equal. Update sync data to make sure shallow_compare will show equality next time
            need_sync, only_update_sync_data = False, True

        else:
            need_sync, only_update_sync_data = True, False

    # Figure out which of the nodes is on disk and which is on Smugmug (can go both ways)
    if isinstance(from_album, AlbumOnDisk):
        node_on_disk, node_on_smugmug = from_album, to_album
    else:
        node_on_disk, node_on_smugmug = to_album, from_album

    assert isinstance(node_on_smugmug, AlbumOnSmugmug)

    if need_sync:
        # Now add a sync actions to synchronize the albums
        logger.debug(f'[<>] [{node_on_disk.source}] {node_on_disk.relative_path}')
        action = SyncAlbumsAction(disk_album=node_on_disk,
                                  smugmug_album=node_on_smugmug,
                                  sync_type=sync_type)

        action_callback(action)

    elif only_update_sync_data:
        # Simply update the sync data since albums are really the same
        logger.debug(f'[~~] {from_album.relative_path} - Need to update sync data')

        action = UpdateAlbumSyncData(disk_album=node_on_disk, smugmug_album=node_on_smugmug)
        action_callback(action)

    else:
        logger.debug(f'[==] {from_album.relative_path}')


@timeit
def generate_sync_actions(on_disk: FolderOnDisk,
                          on_smugmug: FolderOnSmugmug,
                          action_callback: Callable[[Action], None],
                          sync_type: Tuple[SyncTypeAction, ...]):
    """
    Given the two scanned views (on disk and on Smugmug), generate a list of actions that will sync the two

    :param on_disk: root for hierarchy on disk
    :param on_smugmug: root for hierarchy on Smugmug
    :param action_callback: Optional call back to be called each time an action is determined
    :param sync_type: What to do
    """
    logger.info('Generating diff...')

    if SyncTypeAction.UPLOAD in sync_type:
        recurse_sync_folders(from_folder=on_disk,
                             to_folder=on_smugmug,
                             parent_of_to_folder=None,
                             add_action_class=UploadAction,
                             remove_action_class=RemoveFromSmugmugAction,
                             should_delete=SyncTypeAction.DELETE_ON_CLOUD in sync_type,
                             action_callback=action_callback,
                             sync_type=sync_type)

    if SyncTypeAction.DOWNLOAD in sync_type:
        recurse_sync_folders(from_folder=on_smugmug,
                             to_folder=on_disk,
                             parent_of_to_folder=None,
                             add_action_class=DownloadAction,
                             remove_action_class=RemoveFromDiskAction,
                             should_delete=SyncTypeAction.DELETE_ON_DISK in sync_type,
                             action_callback=action_callback,
                             sync_type=sync_type)

    logger.info('Done.')


def print_summary(on_disk: FolderOnDisk, on_smugmug: FolderOnSmugmug, diff):
    """
    Prints statistics on the list of diffs found

    :param NodeOnDisk on_disk: root for hierarchy on disk
    :param NodeOnSmugmug on_smugmug: root for hierarchy on Smugmug
    :param list[Action] diff: List of actions to perform
    """

    print('')
    print('Scan Results')
    print('============')

    print(f'On disk             : {on_disk.stats()}')
    print(f'Smugmug             : {on_smugmug.stats()}')
    print(f'Actions:')
    print(f'  Total:            : {len(diff)}')
    print(f'  Downloads:        : {len([d for d in diff if isinstance(d, DownloadAction)])}')
    print(f'  Uploads:          : {len([d for d in diff if isinstance(d, UploadAction)])}')
    print(f'  Deletes (disk)    : {len([d for d in diff if isinstance(d, RemoveFromDiskAction)])}')
    print(f'  Deletes (smugmug) : {len([d for d in diff if isinstance(d, RemoveFromSmugmugAction)])}')
    print(f'  Album syncs       : {len([d for d in diff if isinstance(d, SyncAlbumsAction)])}')
    print(f'')
