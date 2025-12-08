import logging

from rich.console import Console

from sync2smugmug import disk, event_manager, events, models, policy
from sync2smugmug.online import online
from sync2smugmug.utils import image_tools

logger = logging.getLogger(__name__)
console = Console()

DELTA = 360.0  # 360 seconds to allow between online and disk clocks


async def synchronize(
    on_disk: models.RootFolder,
    on_line: models.RootFolder,
    sync_action: policy.SyncAction,
    connection: online.OnlineConnection,
    dry_run: bool,
    force_refresh: bool = False,
):
    """
    Synchronizes the two scanned view (download and/or upload)
    """

    if sync_action.upload:
        # Use the online events and sync from disk to online
        event_group, source, target = events.OnlineEventGroup, on_disk, on_line
    elif sync_action.download:
        # Use the disk events and sync from online to disk
        event_group, source, target = events.DiskEventGroup, on_line, on_disk
    else:
        event_group, source, target = None, None, None
        logger.warning("Neither download nor upload was requested")

    if event_group is not None:
        await synchronize_folders(
            source_folder=source,
            target_folder=target,
            target_folder_parent=None,
            event_group=event_group,
            sync_action=sync_action,
            connection=connection,
            dry_run=dry_run,
            force_refresh=force_refresh,
        )

    # Wait until all events are processed, so we are sure everything is done before we return
    await event_manager.join()

    logger.info("Synchronization complete.")


async def synchronize_folders(
    source_folder: models.Folder,
    target_folder: models.Folder | None,
    target_folder_parent: models.Folder | None,
    event_group: events.EventGroup,
    sync_action: policy.SyncAction,
    connection: online.OnlineConnection,
    dry_run: bool,
    force_refresh: bool = False,
):
    """
    Recursively sync the directory structure from source_folder (and children) into target_folder (and children).
    The event group will determine what how each event is being handled (either Upload or Download).
    """

    assert source_folder is not None, "source_folder must always be there!"

    # Wait first for all other tasks to finish before we start this one. This will allow the synchronization to be
    # more orderly and show progress in a more meaningful way
    await event_manager.join()

    logger.info(f"Synchronizing {source_folder.relative_path}")

    # If target_folder is missing - we need to add it whole
    if target_folder is None:
        assert target_folder_parent is not None, "target_folder_parent should always be there!"

        logger.info(f"[++] {source_folder}")

        event_data = events.FolderEventData(
            source_folder=source_folder,
            target_parent=target_folder_parent,
            message=f"entire folder {source_folder}",
            connection=connection,
        )

        await event_manager.fire_event(event=event_group.FOLDER_ADD, event_data=event_data, dry_run=dry_run)

    else:
        # Both folders exist (and have same relative path)
        assert source_folder.relative_path == target_folder.relative_path

        # First process albums
        sorted_album_names = sorted(source_folder.albums.keys())
        for album_name in sorted_album_names:
            source_album = source_folder.albums[album_name]
            to_album = target_folder.albums.get(album_name)

            if source_album.image_count > 0:
                await synchronize_albums(
                    source_album=source_album,
                    target_album=to_album,
                    target_folder_parent=target_folder,
                    event_group=event_group,
                    sync_action=sync_action,
                    connection=connection,
                    dry_run=dry_run,
                    force_refresh=force_refresh,
                )

        # Now, recursively process sub folders
        sorted_folder_names = sorted(source_folder.sub_folders.keys())
        for sub_folder_name in sorted_folder_names:
            source_sub_folder = source_folder.sub_folders[sub_folder_name]
            target_sub_folder = target_folder.sub_folders.get(sub_folder_name)

            await synchronize_folders(
                source_folder=source_sub_folder,
                target_folder=target_sub_folder,
                target_folder_parent=target_folder,
                event_group=event_group,
                sync_action=sync_action,
                connection=connection,
                dry_run=dry_run,
                force_refresh=force_refresh,
            )

        if event_group.delete_permitted(sync_action):
            # If delete is required, delete all children of 'target_folder' that do not exist in 'source_folder'

            # Make a local copy of the source_album list (so we don't modify during iteration)
            target_sub_folders = dict(target_folder.sub_folders)
            for sub_folder_name, sub_folder in target_sub_folders.items():
                if sub_folder_name not in source_folder.sub_folders:
                    await handle_delete(
                        event=event_group.FOLDER_DELETE,
                        event_data_class=events.DeleteFolderEventData,
                        node_to_delete=sub_folder,
                        parent_folder=target_folder,
                        connection=connection,
                        dry_run=dry_run,
                    )

            # Make a local copy of the source_album list (so we don't modify during iteration)
            target_albums = dict(target_folder.albums)
            for album_name, album in target_albums.items():
                if album_name not in source_folder.albums:
                    await handle_delete(
                        event=event_group.ALBUM_DELETE,
                        event_data_class=events.DeleteAlbumEventData,
                        node_to_delete=album,
                        parent_folder=target_folder,
                        connection=connection,
                        dry_run=dry_run,
                    )


async def synchronize_albums(
    source_album: models.Album,
    target_album: models.Album | None,
    target_folder_parent: models.Folder | None,
    event_group: events.EventGroup,
    sync_action: policy.SyncAction,
    connection: online.OnlineConnection,
    dry_run: bool,
    force_refresh: bool = False,
):
    """
    Given both disk and online version of the album exist, we need to go down to the level of images.
    """
    assert source_album is not None

    if target_album is None:
        logger.info(f"[++] {source_album}")

        # Add a brand-new album
        event_data = events.AlbumEventData(
            source_album=source_album,
            target_parent=target_folder_parent,
            message="entire source_album",
            connection=connection,
        )

        await event_manager.fire_event(event=event_group.ALBUM_ADD, event_data=event_data, dry_run=dry_run)
        return

    assert source_album.relative_path == target_album.relative_path

    # Figure out which of the nodes is on disk and which is on Smugmug (can go both ways)
    if source_album.is_on_disk:
        disk_album, online_album = source_album, target_album
    else:
        disk_album, online_album = target_album, source_album

    # Check if node changed (will check last update date, meta-data, etc...)
    content_is_the_same, it_was_quick = await compare_disk_and_online_albums(
        disk_album=disk_album, online_album=online_album, connection=connection, force_refresh=force_refresh
    )

    if not content_is_the_same:
        assert online_album.is_online and disk_album.is_on_disk

        # Make sure online album is loaded with images
        if online_album.requires_image_load:
            await online.load_album_images(album=online_album, connection=connection)

        if disk_album.requires_image_load:
            disk.load_album_images(album=disk_album)

        # Now add a sync actions to synchronize the albums
        logger.info(f"[<>] {disk_album} != {online_album}")

        event_data = events.SyncAlbumImagesEventData(
            disk_album=disk_album,
            online_album=online_album,
            message="sync albums",
            sync_action=sync_action,
            connection=connection,
        )

        await event_manager.fire_event(event=event_group.ALBUM_SYNC, event_data=event_data, dry_run=dry_run)

    else:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"[==] {source_album.relative_path}")

    if disk_album.disk_info.disk_time is None or not it_was_quick:
        # Special case #1: If we don't have sync data yet, make it now
        # Special case #2: The comparison had to go through more thorough checking before concluding equality
        disk_album.disk_info.remember_sync(online_album.online_info.last_updated)


async def handle_delete(
    event: str,
    event_data_class: type[events.DeleteFolderEventData] | type[events.DeleteAlbumEventData],
    node_to_delete: models.Album | models.Folder,
    parent_folder: models.Folder,
    connection: online.OnlineConnection,
    dry_run: bool,
):
    logger.info(f"[--] {node_to_delete}")

    # Intentionally limit concurrency here...
    event_data = event_data_class(  # noqa PyCharm does not properly recognize dataclass constructors
        target=node_to_delete,
        parent=parent_folder,
        message=f"delete {node_to_delete}",
        connection=connection,
    )

    await event_manager.fire_event(event=event, event_data=event_data, dry_run=dry_run)


async def compare_disk_and_online_albums(
    disk_album: models.Album,
    online_album: models.Album,
    connection: online.OnlineConnection,
    force_refresh: bool = False,
) -> tuple[bool, bool]:
    """
    Perform a smart comparison between an online album and a disk album. This will take into account the last sync
    data that was persisted (to disk) to try and speed up the scans. If needed (as a last resort), online images will
    be downloaded queried here.

    The reason we make every effort not to go to image-by-image comparison is because querying image records is the
    slowest operation in the Smugmug API and will substantially slow down scanning.

    returns a bool to indicate if albums are the same and a second bool to indicate if this was a quick comparison
    """
    assert disk_album.is_on_disk and online_album.is_online

    # Use sync_data to see if we can shortcut the entire comparison
    if albums_already_synced(disk_album, online_album, force_refresh):
        return True, True

    if disk_album.relative_path != online_album.relative_path:
        return False, True

    if disk_album.image_count != online_album.image_count:
        return False, True

    logger.info(f"[^^] Loading images for comparison {online_album}")

    # Compare images - one by one
    if online_album.requires_image_load:
        await online.load_album_images(album=online_album, connection=connection)

    if disk_album.requires_image_load:
        disk.load_album_images(disk_album)

    disk_images = sorted(disk_album.images, key=lambda k: k.relative_path)
    online_images = sorted(online_album.images, key=lambda k: k.relative_path)

    for disk_image, online_image in zip(disk_images, online_images):
        if not image_tools.images_are_the_same(disk_image, online_image):
            return False, False

    # More compares?
    return True, False


def albums_already_synced(disk_album: models.Album, online_album: models.Album, force_refresh: bool = False) -> bool:
    disk_info = disk_album.disk_info
    online_info = online_album.online_info

    assert disk_info is not None and online_info is not None

    if force_refresh:
        # In this case, we are specifically asked to reload everything
        return False

    if disk_info.online_time is None:
        # Never synced
        return False

    if abs(disk_info.online_time - online_info.last_updated) > DELTA:
        # Online last update is different (online changed)
        return False

    if abs(disk_info.disk_time - disk_info.last_updated) > DELTA:
        # Disk last update is different (disk changed)
        return True

    return True


async def load_images_for_online_album(online_album: models.Album, connection: online.OnlineConnection):
    if not online_album.requires_image_load:
        return

    await online.load_album_images(album=online_album, connection=connection)


def print_summary(on_disk: models.RootFolder, on_smugmug: models.RootFolder):
    """
    Prints statistics on the list of diffs found
    """

    em = event_manager.the_events_tracker

    console.print()
    console.print("[bold]Scan Results[/bold]")
    console.print("=" * 50)

    console.print(f"On disk                : {on_disk.stats}")
    console.print(f"Smugmug                : {on_smugmug.stats}")
    console.print("[bold]Actions:[/bold]")

    console.print(f"  {'Total': <21}:               : {em.total_processed} / {em.total_submitted}")

    for action_type, count in em.event_count_by_type.items():
        console.print(f"  {action_type: <21}:               : {count}")

    console.print()
