import logging
import pathlib
from collections.abc import Iterable

from sync2smugmug import models
from sync2smugmug.scan import disk_scanner

from .base import DiskOptimization
from .cleanup import DeleteEmptyDirectories
from .conversion import ConvertImagesAndMovies
from .duplicates import DeleteAlbumDuplicates, DeleteImageDuplicates
from .iphone import ImportIPhoneImages

logger = logging.getLogger(__name__)


async def run_disk_optimizations(base_dir: pathlib.Path, dry_run: bool):
    # List all the optimizations currently available (order matters)
    optimizations: Iterable[DiskOptimization] = (
        ImportIPhoneImages(base_dir),
        ConvertImagesAndMovies(base_dir),
        DeleteImageDuplicates(base_dir),
        DeleteAlbumDuplicates(base_dir),
        DeleteEmptyDirectories(base_dir),
        # TODO: Detect similar photos
        # # Add more optimizations here...
    )

    requires_reload = True
    on_disk: models.RootFolder | None = None

    for optimization in optimizations:
        logger.info(f"Running {optimization}...")

        if requires_reload or on_disk is None:
            # Rescan to have a fresh hierarchy after changes were made
            on_disk = await disk_scanner.scan(base_dir=base_dir)

        requires_reload = await optimization.perform(on_disk=on_disk, dry_run=dry_run)
