import logging
import os
from typing import List, Type, Dict, Tuple, Set

from .node import FolderOnDisk, AlbumOnDisk, SYNC_DATA_FILENAME
from .image import ImageOnDisk
from ..config import config
from ..utils import timeit

logger = logging.getLogger(__name__)


class Processor:
    """Base class for all Pre-Processor classes"""

    @classmethod
    async def perform(cls, on_disk: FolderOnDisk, dry_run: bool) -> bool:
        raise NotImplementedError()


class ImportIPhoneImages(Processor):
    """
    Import images from iPhone export folder into the appropriate albums
    """

    @classmethod
    @timeit
    async def perform(cls, on_disk: FolderOnDisk, dry_run: bool) -> bool:
        changed = False

        if config.iphone_photos_location:
            albums_by_date = {a.get_album_date(): a for a in on_disk.iter_albums()}
            folders_by_name = {f.name: f for f in on_disk.iter_folders()}

            with os.scandir(config.iphone_photos_location) as entries:
                for entry in entries:
                    entry: os.DirEntry

                    if not entry.is_file() or not ImageOnDisk.check_is_image(entry.path):
                        continue

                    time_taken = ImageOnDisk.extract_time_taken(entry.path)
                    if time_taken is None:
                        # Skip this one - as we can't identify the time it was taken
                        logger.warning(f"Skipping {entry.path} - could not identify time")
                        continue

                    date_taken = time_taken.date()
                    dest_album: AlbumOnDisk = albums_by_date.get(date_taken)

                    if dest_album is None:
                        # An album needs to be created!

                        # Find the parent node!
                        parent_node = folders_by_name.get(str(date_taken.year))
                        assert parent_node is not None

                        # Create a new album
                        dest_album = AlbumOnDisk(
                            parent=parent_node,
                            relative_path=os.path.join(
                                parent_node.relative_path, date_taken.strftime("%Y_%m_%d")
                            ),
                            makedirs=True,
                        )

                    else:
                        # The album already exists (at least the date does)
                        logger.debug(
                            f"Found album ({dest_album.name}) for date {date_taken}"
                        )

                        # Check if the image exists - if so, we can skip it!
                        if os.path.exists(os.path.join(dest_album.disk_path, entry.name)):
                            logger.warning(
                                f"Image ({entry.name}) already exists in album {dest_album.relative_path}. Deleting!"
                            )
                            os.remove(entry.path)
                            continue

                    # Move the image to the destination album!
                    os.replace(entry.path, os.path.join(dest_album.disk_path, entry.name))
                    logger.info(
                        f"Imported iphone photo {entry.name} into album ({dest_album.relative_path})"
                    )

                    changed = True

        if not changed:
            logger.info("No iPhone images imported")

        return changed


class DetectAlbumDuplicates(Processor):
    """
    Fina and remove albums that have the same name (date).
    - The album that remains is the one with the longer name (assuming it has more data).
    - All files (not just images) from the deleted album that do not already exist in the remaining one will be moved.
    """

    @classmethod
    @timeit
    async def perform(cls, on_disk: FolderOnDisk, dry_run: bool) -> bool:
        # Sort by name (it includes the date)
        albums: List[AlbumOnDisk] = sorted(on_disk.iter_albums(), key=lambda x: x.name)

        changed = False

        if len(albums) > 1:
            # After we've sorted, then let's check each consecutive albums to see if they point to the same date
            for previous, current in zip(albums, albums[1:]):
                # Previous contains the 'shorter' directory name.
                # Current contains the 'longer' directory name. We are assuming that longer is better.

                current_date = current.get_album_date()
                previous_date = previous.get_album_date()

                # Check if these albums are 'date' albums and both point to the same date
                if current_date == previous_date and current_date is not None:
                    current_files = {
                        f
                        for f in os.listdir(current.disk_path)
                        if f != SYNC_DATA_FILENAME
                    }

                    previous_files = {
                        f
                        for f in os.listdir(previous.disk_path)
                        if f != SYNC_DATA_FILENAME
                    }

                    for f in previous_files:
                        if f not in current_files:
                            # Move the file to its new home
                            logger.info(f"Moving {f} from {previous} to {current}")

                            if not dry_run:
                                os.rename(
                                    os.path.join(previous.disk_path, f),
                                    os.path.join(current.disk_path, f),
                                )
                                changed = True

        return changed


class DetectImageDuplicates(Processor):
    """
    Find images with same name and device and remove them (the oldest image stays).
    Exception are non date galleries (which are designed to contain duplicates)

    This is an attempt to find duplicates across albums (hence indexed by name). The challenge is that photos coming
    from different cameras are not always unique (neither between devices nor even within a device). This is why
    additional mata data is used to determine if images are indeed identical.
    """

    @classmethod
    @timeit
    async def perform(cls, on_disk: FolderOnDisk, dry_run: bool) -> bool:
        # To minimize the time (and memory) requires to get metadata - we will do this in two passes.
        photos_by_name: Set[str] = set()
        duplicate_candidates: List[ImageOnDisk] = []

        # Pass #1 - picks up all the duplicate candidates (based on name only - without metadata)
        for album in on_disk.iter_albums():
            if album.get_album_date() is None:
                # Skip any non-date albums (other albums like yearly collections have duplicates on purpose)
                continue

            for image in album.iter_images():
                if image.name in photos_by_name:
                    # Name already exists... we need to check for duplicates
                    duplicate_candidates.append(image)
                else:
                    photos_by_name.add(image.name)

        # Pass #2 - examine the metadata of each of the candidates (a smaller set) to identify actual duplicates
        # (use make, model & size)
        index: Dict[Tuple[str, int, str, str], ImageOnDisk] = {}

        changed = False

        for image in duplicate_candidates:
            key = (
                image.name,
                image.size,
                image.camera_model,
                image.camera_make,
            )

            if key in index:
                logger.info(
                    f"Duplicate detected for image {image} with {index[key]} "
                    f"[{image.camera_make} / {image.camera_model}]"
                )

                if not dry_run:
                    # Actually delete (the newer image)
                    await image.delete(dry_run=dry_run)
                    changed = True

            else:
                index[key] = image

        if not changed:
            logger.info("No image duplicates detected")

        return changed


class ConvertHeicToJpeg(Processor):
    @classmethod
    async def perform(cls, on_disk: FolderOnDisk, dry_run: bool) -> bool:
        changed = False

        async for image in on_disk.aiter_images():
            if image.convert_to_jpeg():
                changed = True

        if not changed:
            logger.info("No HEIC images converted")

        return changed


# class DetectSimilarImages(Processor):
#     """
#     Detect images (within albums) that are similar to each other
#     """
#
#     @classmethod
#     @timeit
#     async def perform(cls, on_disk: FolderOnDisk, dry_run: bool) -> bool:
#         changed = False
#
#         for album in on_disk.iter_albums():
#             if album.get_album_date() is None:
#                 # Skip any non-date albums (other albums like yearly collections have duplicates on purpose)
#                 continue
#
#             images = await album.get_images()
#
#             # TODO
#             pass
#
#         if not changed:
#             logger.info("No image similarities detected")
#
#         return changed


async def pre_process(on_disk: FolderOnDisk, dry_run: bool) -> bool:
    # List all the optimizations currently available (order matters)
    pre_processors: List[Type[Processor]] = [
        ImportIPhoneImages,
        ConvertHeicToJpeg,
        DetectAlbumDuplicates,
        DetectImageDuplicates,
        # DetectSimilarImages,
    ]

    logger.info("-" * 80)

    for processor in pre_processors:
        logger.info(f"--- Running pre-process {processor}")

        if await processor.perform(on_disk=on_disk, dry_run=dry_run):
            return True

    logger.info("-" * 80)

    return False
