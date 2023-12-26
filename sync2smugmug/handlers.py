import logging
import pathlib
import shutil
from typing import List

from sync2smugmug import models, events, disk, event_manager
from sync2smugmug.online import online

logger = logging.getLogger(__name__)


@event_manager.subscribe(events.OnlineEventGroup.FOLDER_ADD)
async def upload_folder(event_data: events.FolderEventData, dry_run: bool) -> bool:
    """
    Upload a disk node (source_folder / source_album) to smugmug as a child to this object

    :return: The node representing the uploaded entity
    """
    changed = False

    # Create a folder online and copy its attributes
    online_info = await event_data.connection.create_folder(
        parent=event_data.target_parent,
        folder_name=event_data.source_folder.name,
        dry_run=dry_run,
    )

    folder = models.Folder(
        relative_path=event_data.target_parent.relative_path.joinpath(event_data.source_folder.name),
        online_info=online_info,
    )

    # Update hierarchy with new folder
    event_data.target_parent.sub_folders[folder.name] = folder

    # Trigger an event for each sub folder
    for sub_folder in event_data.source_folder.sub_folders.values():
        event_data = events.FolderEventData(
            source_folder=sub_folder,
            target_parent=folder,
            message=f"folder {sub_folder}",
            connection=event_data.connection,
        )

        await event_manager.fire_event(
            event=events.OnlineEventGroup.FOLDER_ADD,
            event_data=event_data,
            dry_run=dry_run
        )

    # Trigger an event for each album
    for album in event_data.source_folder.albums.values():
        event_data = events.AlbumEventData(
            source_album=album,
            target_parent=folder,
            message=f"album {album}",
            connection=event_data.connection,
        )

        await event_manager.fire_event(
            event=events.OnlineEventGroup.ALBUM_ADD,
            event_data=event_data,
            dry_run=dry_run
        )

    return changed


@event_manager.subscribe(events.DiskEventGroup.FOLDER_ADD)
async def download_folder(event_data: events.FolderEventData, dry_run: bool):
    """
    Download source_folder from smugmug to disk
    """
    changed = False

    # Create the source_folder object
    disk_info = disk.create_folder(
        parent=event_data.target_parent,
        folder_name=event_data.source_folder.name,
        dry_run=dry_run,
    )

    folder = models.Folder(
        relative_path=event_data.source_folder.relative_path,
        disk_info=disk_info,
    )

    event_data.target_parent.sub_folders[folder.name] = folder

    # Trigger an event for each sub folder
    for sub_folder in event_data.source_folder.sub_folders.values():
        event_data = events.FolderEventData(
            source_folder=sub_folder,
            target_parent=folder,
            message=f"folder {sub_folder}",
            connection=event_data.connection,
        )

        await event_manager.fire_event(
            event=events.DiskEventGroup.FOLDER_ADD,
            event_data=event_data,
            dry_run=dry_run
        )

    # Trigger an event for each album
    for album in event_data.source_folder.albums.values():
        event_data = events.AlbumEventData(
            source_album=album,
            target_parent=folder,
            message=f"album {album}",
            connection=event_data.connection,
        )

        await event_manager.fire_event(
            event=events.DiskEventGroup.ALBUM_ADD,
            event_data=event_data,
            dry_run=dry_run
        )

    return changed


@event_manager.subscribe(events.DiskEventGroup.FOLDER_DELETE)
async def delete_folder_on_disk(event_data: events.DeleteFolderEventData, dry_run: bool) -> bool:
    if not dry_run:
        shutil.rmtree(event_data.target.disk_info.disk_path)

    # Update the data model
    del event_data.parent.sub_folders[event_data.target.name]
    return True


@event_manager.subscribe(events.DiskEventGroup.ALBUM_DELETE)
async def delete_album_on_disk(event_data: events.DeleteAlbumEventData, dry_run: bool) -> bool:
    if not dry_run:
        shutil.rmtree(event_data.target.disk_info.disk_path)

    # Update the data model
    del event_data.parent.albums[event_data.target.name]
    return True


@event_manager.subscribe(events.OnlineEventGroup.FOLDER_DELETE)
async def delete_folder_online(event_data: events.DeleteFolderEventData, dry_run: bool) -> bool:
    changed = await event_data.connection.delete(uri=event_data.target.online_info.uri, dry_run=dry_run)
    del event_data.parent.sub_folders[event_data.target.name]

    return changed


@event_manager.subscribe(events.OnlineEventGroup.ALBUM_DELETE)
async def delete_album_online(event_data: events.DeleteAlbumEventData, dry_run: bool) -> bool:
    changed = await event_data.connection.delete(uri=event_data.target.online_info.uri, dry_run=dry_run)
    del event_data.parent.albums[event_data.target.name]

    return changed


@event_manager.subscribe(events.OnlineEventGroup.ALBUM_ADD)
async def upload_album(event_data: events.AlbumEventData, dry_run: bool) -> bool:
    disk_album = event_data.source_album

    online_info = await event_data.connection.create_album_online_info(
        parent=event_data.target_parent,
        album_name=disk_album.name,
        dry_run=dry_run,
    )

    album = models.Album(
        relative_path=disk_album.relative_path,
        online_info=online_info,
    )

    event_data.target_parent.albums[album.name] = album

    # Upload the images for this source_album
    any_change = await upload_missing_images(
        from_disk_album=disk_album,
        to_online_album=album,
        connection=event_data.connection,
        dry_run=dry_run,
    )

    if any_change and not dry_run:
        album.reset_images()
        disk_album.disk_info.remember_sync(online_info.last_updated)

    return True


@event_manager.subscribe(events.DiskEventGroup.ALBUM_ADD)
async def download_album(event_data: events.AlbumEventData, dry_run: bool) -> bool:
    online_album = event_data.source_album
    disk_parent_folder = event_data.target_parent

    disk_info = disk.create_album_disk_info(
        parent_disk_path=disk_parent_folder.disk_info.disk_path,
        album_name=online_album.name,
        dry_run=dry_run
    )

    disk_album = models.Album(relative_path=online_album.relative_path, disk_info=disk_info)
    disk_parent_folder.albums[disk_album.name] = disk_album

    changed = await download_missing_images(
        from_online_album=online_album,
        to_disk_album=disk_album,
        connection=event_data.connection,
        dry_run=dry_run
    )

    # Update the sync data for these albums
    if changed and not dry_run:
        disk_album.reset_images()
        disk_album.disk_info.remember_sync(online_album.online_info.last_updated)

    return changed


@event_manager.subscribe(events.DiskEventGroup.ALBUM_SYNC)
async def sync_albums(event_data: events.SyncAlbumImagesEventData, dry_run: bool) -> bool:
    changed = False

    if event_data.online_album.requires_image_load:
        await online.load_album_images(album=event_data.online_album, connection=event_data.connection)

    if event_data.disk_album.requires_image_load:
        disk.load_album_images(album=event_data.disk_album)

    if event_data.sync_action.download:
        changed |= await download_missing_images(
            from_online_album=event_data.online_album,
            to_disk_album=event_data.disk_album,
            connection=event_data.connection,
            dry_run=dry_run
        )

    if event_data.sync_action.upload:
        changed |= await upload_missing_images(
            from_disk_album=event_data.disk_album,
            to_online_album=event_data.online_album,
            connection=event_data.connection,
            dry_run=dry_run
        )

    if event_data.sync_action.delete_on_disk:
        # Delete on disk is quick - no need for async tasks
        for image in event_data.disk_album.images:
            # Lookup image (using relative path)
            if image not in event_data.online_album.images:
                disk.delete_image_from_disk(image, dry_run=dry_run)

    if event_data.sync_action.delete_online:
        for image in event_data.online_album.images:
            # Lookup image (using relative path)
            if image not in event_data.disk_album.images:
                await event_data.connection.delete(uri=image.online_info.uri, dry_run=dry_run)

    if changed:
        disk.load_album_images(album=event_data.disk_album)
        await online.load_album_images(album=event_data.online_album, connection=event_data.connection)

        if not dry_run:
            # Update the sync data for these albums
            event_data.disk_album.disk_info.remember_sync(event_data.online_album.online_info.last_updated)

    return changed


async def download_missing_images(
        from_online_album: models.Album,
        to_disk_album: models.Album,
        connection: online.OnlineConnection,
        dry_run: bool
) -> bool:
    # Figure out which images to download
    missing_images: List[models.Image] = []

    if from_online_album.requires_image_load:
        await online.load_album_images(album=from_online_album, connection=connection)

    if to_disk_album.requires_image_load:
        disk.load_album_images(album=to_disk_album)

    disk_images_by_relative_path = {i.relative_path for i in to_disk_album.images}
    for online_image in from_online_album.images:
        if online_image.relative_path not in disk_images_by_relative_path:
            missing_images.append(online_image)

    if not missing_images:
        return False

    await connection.download_images(
        images=(i.online_info for i in missing_images),
        to_folder=to_disk_album.disk_info.disk_path,
        dry_run=dry_run
    )

    # Reload all images into disk album (to make sure it reflects the new situation on disk)
    disk.load_album_images(album=to_disk_album)

    logger.info(f"Finished downloading {len(missing_images)} images from {from_online_album}")
    return True


async def upload_missing_images(
        from_disk_album: models.Album,
        to_online_album: models.Album,
        connection: online.OnlineConnection,
        dry_run: bool
) -> bool:
    # Figure out which images to upload
    images_to_upload: List[pathlib.Path] = []

    online_images_by_relative_path = {i.relative_path for i in (to_online_album.images or [])}
    for disk_image in from_disk_album.images:
        if disk_image.relative_path not in online_images_by_relative_path:
            images_to_upload.append(disk_image.disk_info.disk_path)

    if not images_to_upload:
        return False

    await connection.upload_images(
        image_paths=images_to_upload,
        to_album_uri=to_online_album.online_info.uri,
        dry_run=dry_run
    )

    await online.load_album_images(album=to_online_album, connection=connection)

    logger.info(f"Finished uploading {len(images_to_upload)} images from {from_disk_album}")
    return True
