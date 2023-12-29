import logging
from typing import Iterable

from sync2smugmug.optimizations.online import OnlineOptimization, duplicates, cleanup
from sync2smugmug.configuration import config
from sync2smugmug import models
from sync2smugmug.online import online
from sync2smugmug.scan import online_scanner

logger = logging.getLogger(__name__)


async def run_online_optimizations(connection: online.OnlineConnection, dry_run: bool):
    # List all the optimizations currently available (order matters)
    optimizations: Iterable[OnlineOptimization] = (
        cleanup.DeleteEmptyAlbums(config.base_dir),
        duplicates.RemoveOnlineImageDuplicates(config.base_dir),
        # Add more optimizations here...
        # TODO: Scan for online nodes with "Processing" = True (these are bad) - and delete
    )

    logger.info("-" * 80)

    requires_reload = True
    on_line: models.RootFolder | None = None

    for optimization in optimizations:
        logger.info(f"--- Running {optimization}")

        if requires_reload or on_line is None:
            # Rescan to have a fresh hierarchy after changes were made
            on_line = await online_scanner.scan(connection=connection)

        requires_reload = await optimization.perform(on_line=on_line, connection=connection, dry_run=dry_run)

    logger.info("-" * 80)
