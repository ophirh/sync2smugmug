import logging
from typing import Tuple, Callable, List, Type, Optional, Awaitable

from .actions import (
    Action,
    DownloadAction,
    RemoveFromDiskAction,
    RemoveFromSmugmugAction,
    UploadAction,
    SyncAlbumsAction,
    UpdateAlbumSyncDataAction,
    AddAction,
    RemoveAction,
    RemoveOnlineDuplicatesAction,
)
from .config import config
from .disk.scanner import DiskScanner, FolderOnDisk, AlbumOnDisk
from .node import Album, Folder
from .policy import SyncTypeAction
from .smugmug.scanner import SmugmugScanner, FolderOnSmugmug, AlbumOnSmugmug
from .smugmug.connection import SmugMugConnection

logger = logging.getLogger(__name__)


async def scan(connection: SmugMugConnection) -> Tuple[FolderOnDisk, FolderOnSmugmug]:
    """
    Scan both disk and Smugmug to create two virtual trees of both systems
    """

    on_disk = DiskScanner(base_dir=config.base_dir).scan()
    logger.info(f"Scan results (on disk): {on_disk.stats()}")

    on_smugmug = await SmugmugScanner(connection=connection).scan()
    logger.info(f"Scan results (on smugmug): {on_smugmug.stats()}")

    return on_disk, on_smugmug


async def sync(on_disk: FolderOnDisk, on_smugmug: FolderOnSmugmug) -> List[Action]:
    """
    Given scan results, generates a set of actions required to sync between the disk and smugmug

    :param on_disk: Root for on disk scans
    :param on_smugmug: Root for on smugmug scans
    """

    sync_type = config.sync

    actions: List[Action] = []

    async def execute_action(action: Action):
        """
        Execute the action and keep track of them
        """
        await action.perform(config.dry_run)
        actions.append(action)

    await generate_sync_actions(
        on_disk=on_disk,
        on_smugmug=on_smugmug,
        execute_action=execute_action,
        sync_type=sync_type,
    )

    return actions


# noinspection DuplicatedCode
async def recurse_sync_folders(
    from_folder: Folder,
    to_folder: Folder,
    to_folder_parent: Optional[Folder],
    add_action_class: Type[AddAction],
    remove_action_class: Type[RemoveAction],
    should_delete: bool,
    execute_action: Callable[[Action], Awaitable[None]],
    sync_type: Tuple[SyncTypeAction, ...],
):
    assert from_folder is not None, "from_folder must always be there!"

    if not from_folder.is_root and from_folder.parent.is_root:
        # Show progress on top folders (years)
        logger.info(f"Synchronizing {from_folder.relative_path}")

    # Case #1 - to_folder is missing - we need to add it whole
    if to_folder is None:
        assert to_folder_parent is not None

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"[+{from_folder.source[0]}] {from_folder.relative_path}")

        action = add_action_class(
            what_to_add=from_folder,
            parent_to_add_to=to_folder_parent,
            message="entire folder",
        )

        await execute_action(action)

        return

    # Case #2 - Both folders exist (and have same relative path) - Recursively apply to sub-folders of from_folder.
    assert from_folder.relative_path == to_folder.relative_path

    for name, from_sub_folder in sorted(
        from_folder.sub_folders.items(), key=lambda x: x[0]
    ):
        to_sub_folder = to_folder.sub_folders.get(name)

        # Intentionally limit concurrency here...
        await recurse_sync_folders(
            from_folder=from_sub_folder,
            to_folder=to_sub_folder,
            to_folder_parent=to_folder,
            add_action_class=add_action_class,
            remove_action_class=remove_action_class,
            should_delete=should_delete,
            execute_action=execute_action,
            sync_type=sync_type,
        )

    # Now go over albums in the same way
    for name, from_album in sorted(from_folder.albums.items(), key=lambda x: x[0]):
        if from_album.image_count == 0:
            continue

        to_album = to_folder.albums.get(name)

        # Intentionally limit concurrency here...
        await sync_albums(
            from_album=from_album,
            to_album=to_album,
            to_folder_parent=to_folder,
            add_action_class=add_action_class,
            execute_action=execute_action,
            sync_type=sync_type,
        )

    if should_delete:
        # If delete is required, delete all children of 'to_folder' that do not exist in 'from_folder'

        # Make a local copy of the album list (so we don't modify during iteration)
        local_copy_of_sub_folders = dict(to_folder.sub_folders)

        for name, to_sub_folder in local_copy_of_sub_folders.items():
            if name not in from_folder.sub_folders:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        f"[-{to_sub_folder.source[0]}] {to_sub_folder.relative_path}"
                    )

                # Intentionally limit concurrency here...
                await execute_action(remove_action_class(what_to_remove=to_sub_folder))

        # Make a local copy of the album list (so we don't modify during iteration)
        local_copy_of_albums = dict(to_folder.albums)

        for name, to_album in local_copy_of_albums.items():
            if name not in from_folder.albums:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"[-{to_album.source[0]}] {to_album.relative_path}")

                # Intentionally limit concurrency here...
                await execute_action(remove_action_class(what_to_remove=to_album))


async def sync_albums(
    from_album: Album,
    to_album: Album,
    to_folder_parent: Folder,
    add_action_class: Type[AddAction],
    execute_action: Callable[[Action], Awaitable[None]],
    sync_type: Tuple[SyncTypeAction, ...],
):
    """
    Sync images from both versions of albums
    """
    assert from_album is not None

    if to_album is None:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"[+{from_album.source[0]}] {from_album.relative_path}")

        action = add_action_class(
            what_to_add=from_album,
            parent_to_add_to=to_folder_parent,
            message="entire album",
        )

        await execute_action(action)
        return

    assert from_album.relative_path == to_album.relative_path

    # Check if node changed (will check last update date, meta-data, etc...)
    need_sync = False
    content_appears_the_same = False

    if from_album.shallow_compare(to_album) != 0:
        # Shallow compare shows equality
        if await from_album.deep_compare(to_album, shallow_compare_first=False) == 0:
            # Objects are really equal. Update sync data to make sure shallow_compare will show equality next time
            need_sync, content_appears_the_same = False, True

        else:
            need_sync, content_appears_the_same = True, False

    # Figure out which of the nodes is on disk and which is on Smugmug (can go both ways)
    if isinstance(from_album, AlbumOnDisk):
        node_on_disk, node_on_smugmug = from_album, to_album
    else:
        node_on_disk, node_on_smugmug = to_album, from_album

    assert isinstance(node_on_smugmug, AlbumOnSmugmug)
    assert isinstance(node_on_disk, AlbumOnDisk)

    if need_sync:
        # Now add a sync actions to synchronize the albums
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"[<>] [{node_on_disk.source}] {node_on_disk.relative_path}")

        action = SyncAlbumsAction(
            disk_album=node_on_disk, smugmug_album=node_on_smugmug, sync_type=sync_type
        )

        await execute_action(action)

    elif content_appears_the_same:
        if SyncTypeAction.DELETE_DUPLICATES in sync_type:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    f"[~~] {from_album.relative_path} - Checking for duplicates..."
                )

            action = RemoveOnlineDuplicatesAction(smugmug_album=node_on_smugmug)

            await execute_action(action)

        # Update the sync data since albums are the same - to prevent us from scanning it again
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"[~~] {from_album.relative_path} - Need to update sync data")

        action = UpdateAlbumSyncDataAction(
            disk_album=node_on_disk, smugmug_album=node_on_smugmug
        )

        await execute_action(action)

    else:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"[==] {from_album.relative_path}")


async def generate_sync_actions(
    on_disk: FolderOnDisk,
    on_smugmug: FolderOnSmugmug,
    execute_action: Callable[[Action], Awaitable[None]],
    sync_type: Tuple[SyncTypeAction, ...],
):
    """
    Given the two scanned views (on disk and on Smugmug), generate a list of actions that will sync the two

    :param on_disk: root for hierarchy on disk
    :param on_smugmug: root for hierarchy on Smugmug
    :param execute_action: Optional call back to be called each time an action is determined
    :param sync_type: What to do
    """
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Generating diff...")

    if SyncTypeAction.UPLOAD in sync_type:
        await recurse_sync_folders(
            from_folder=on_disk,
            to_folder=on_smugmug,
            to_folder_parent=None,
            add_action_class=UploadAction,
            remove_action_class=RemoveFromSmugmugAction,
            should_delete=SyncTypeAction.DELETE_ON_CLOUD in sync_type,
            execute_action=execute_action,
            sync_type=sync_type,
        )

    if SyncTypeAction.DOWNLOAD in sync_type:
        await recurse_sync_folders(
            from_folder=on_smugmug,
            to_folder=on_disk,
            to_folder_parent=None,
            add_action_class=DownloadAction,
            remove_action_class=RemoveFromDiskAction,
            should_delete=SyncTypeAction.DELETE_ON_DISK in sync_type,
            execute_action=execute_action,
            sync_type=sync_type,
        )

    logger.info("Done.")


def print_summary(on_disk: FolderOnDisk, on_smugmug: FolderOnSmugmug, diff):
    """
    Prints statistics on the list of diffs found

    :param NodeOnDisk on_disk: root for hierarchy on disk
    :param NodeOnSmugmug on_smugmug: root for hierarchy on Smugmug
    :param list[Action] diff: List of actions to perform
    """

    downloads, uploads, disk_deletions, online_deletions, syncs = 0, 0, 0, 0, 0

    for d in diff:
        if isinstance(d, DownloadAction):
            downloads += 1
        elif isinstance(d, UploadAction):
            uploads += 1
        elif isinstance(d, RemoveFromDiskAction):
            disk_deletions += 1
        elif isinstance(d, RemoveFromSmugmugAction):
            online_deletions += 1
        elif isinstance(d, SyncAlbumsAction):
            syncs += 1

    print("")
    print("Scan Results")
    print("============")

    print(f"On disk                : {on_disk.stats()}")
    print(f"Smugmug                : {on_smugmug.stats()}")
    print(f"Actions:")
    print(f"  Total:               : {len(diff)}")
    print(f"  Downloads:           : {downloads}")
    print(f"  Uploads:             : {uploads}")
    print(f"  Deletes (disk)       : {disk_deletions}")
    print(f"  Deletes (smugmug)    : {online_deletions}")
    print(f"  Album syncs          : {syncs}")
    print(f"")
