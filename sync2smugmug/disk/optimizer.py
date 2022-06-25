import logging
import os
from typing import List, Type, Dict, Tuple, Set

from .node import FolderOnDisk, AlbumOnDisk, SYNC_DATA_FILENAME
from .image import ImageOnDisk

logger = logging.getLogger(__name__)


class Optimizer:
    """Base class for all Optimizer classes"""

    @classmethod
    async def perform(cls, on_disk: FolderOnDisk, dry_run: bool) -> bool:
        raise NotImplementedError()


class DetectAlbumDuplicates(Optimizer):
    """
    Fina and remove albums that have the same name (date).
    - The album that remains is the one with the longer name (assuming it has more data).
    - All files (not just images) from the deleted album that do not already exist in the remaining one will be moved.
    """

    @classmethod
    async def perform(cls, on_disk: FolderOnDisk, dry_run: bool) -> bool:
        # Sort by name (it includes the date)
        albums: List[AlbumOnDisk] = sorted(on_disk.iter_albums(), key=lambda x: x.name)

        changed = False

        if len(albums) > 1:
            # After we've sorted, then let's check each consecutive albums to see if they point to the same date
            for previous, current in zip(albums, albums[1:]):
                # Previous contains the 'shorter' directory name.
                # Current contains the 'longer' directory name. We are assuming that longer is better.

                common_prefix = os.path.commonprefix([current.name, previous.name])
                # Check if date is the only common prefix.  TODO: check for actual format of date prefix
                if len(common_prefix) == len("1111-11-11"):
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


class DetectImageDuplicates(Optimizer):
    """
    Find images with same name and device and remove them (the oldest image stays).
    Exception are non date galleries (which are designed to contain duplicates)

    This is an attempt to find duplicates across albums (hence indexed by name). The challenge is that photos coming
    from different cameras are not always unique (neither between devices nor even within a device). This is why
    additional mata data is used to determine if images are indeed identical.
    """

    @classmethod
    async def perform(cls, on_disk: FolderOnDisk, dry_run: bool) -> bool:
        # To minimize the time (and memory) requires to get metadata - we will do this in two passes.
        photos_by_name: Set[str] = set()
        duplicate_candidates: List[ImageOnDisk] = []

        # Pass #1 - picks up all the duplicate candidates (based on name only - without metadata)
        for album in on_disk.iter_albums():
            if not album.is_date_album:
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


class DetectSimilarImages(Optimizer):
    """
    Detect images (within albums) that are similar to each other
    """

    @classmethod
    async def perform(cls, on_disk: FolderOnDisk, dry_run: bool) -> bool:
        changed = False

        for album in on_disk.iter_albums():
            if not album.is_date_album:
                # Skip any non-date albums (other albums like yearly collections have duplicates on purpose)
                continue

            images = await album.get_images()

            # TODO
            pass

        if not changed:
            logger.info("No image similarities detected")

        return changed


async def optimize(on_disk: FolderOnDisk, dry_run: bool) -> bool:
    # List all the optimizations currently available (order matters)
    optimizations: List[Type[Optimizer]] = [
        DetectAlbumDuplicates,
        DetectImageDuplicates,
        # DetectSimilarImages,
    ]

    for optimizer in optimizations:
        logger.info(f"Running optimization {optimizer}")
        logger.info("-" * 80)
        if await optimizer.perform(on_disk=on_disk, dry_run=dry_run):
            return True

    return False
