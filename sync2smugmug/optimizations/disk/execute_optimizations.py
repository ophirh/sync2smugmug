import logging
from typing import Iterable

from sync2smugmug.optimizations.disk import DiskOptimization, iphone, conversion, duplicates, cleanup
from sync2smugmug.configuration import config
from sync2smugmug import models
from sync2smugmug.scan import disk_scanner

logger = logging.getLogger(__name__)


async def run_disk_optimizations(dry_run: bool):
    # List all the optimizations currently available (order matters)
    optimizations: Iterable[DiskOptimization] = (
        # iphone.ImportIPhoneImages(config.base_dir),
        conversion.ConvertImagesAndMovies(config.base_dir),
        duplicates.DeleteImageDuplicates(config.base_dir),
        # duplicates.DeleteAlbumDuplicates(config.base_dir),
        # cleanup.DeleteEmptyDirectories(config.base_dir),
        # # TODO: Detect similar photos
        # # Add more optimizations here...
    )

    logger.info("-" * 80)

    requires_reload = True
    on_disk: models.RootFolder | None = None

    for optimization in optimizations:
        logger.info(f"--- Running {optimization}")

        if requires_reload or on_disk is None:
            # Rescan to have a fresh hierarchy after changes were made
            on_disk = await disk_scanner.scan(base_dir=config.base_dir)

        requires_reload = await optimization.perform(on_disk=on_disk, dry_run=dry_run)

    logger.info("-" * 80)
