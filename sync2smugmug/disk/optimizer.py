import logging
import os
from typing import List, Type

from .node import FolderOnDisk, AlbumOnDisk, SYNC_DATA_FILENAME

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
    Find images with same name and device and remove them (oldest image stays). Exception are non date galleries (which are
    designed to contain duplicates)
    """

    @classmethod
    async def perform(cls, on_disk: FolderOnDisk, dry_run: bool) -> bool:
        # 2. Scan the disk and index on images (per device) to identify duplicates.
        # TODO
        return False


async def optimize(on_disk: FolderOnDisk, dry_run: bool) -> bool:
    # List all the optimizations currently available (order matters)
    optimizations: List[Type[Optimizer]] = [
        DetectAlbumDuplicates,
        DetectImageDuplicates,
    ]

    for optimizer in optimizations:
        if await optimizer.perform(on_disk=on_disk, dry_run=dry_run):
            return True

    return False
