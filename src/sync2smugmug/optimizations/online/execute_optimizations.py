import logging
import pathlib
from collections.abc import Iterable

from sync2smugmug import models
from sync2smugmug.online import online
from sync2smugmug.scan import online_scanner

from .base import OnlineOptimization
from .cleanup import DeleteEmptyAlbums
from .duplicates import RemoveOnlineImageDuplicates

logger = logging.getLogger(__name__)


async def run_online_optimizations(connection: online.OnlineConnection, base_dir: pathlib.Path, dry_run: bool):
    # List all the optimizations currently available (order matters)
    optimizations: Iterable[OnlineOptimization] = (
        DeleteEmptyAlbums(base_dir),
        RemoveOnlineImageDuplicates(base_dir),
        # Add more optimizations here...
        # TODO: Scan for online nodes with "Processing" = True (these are bad) - and delete
    )

    requires_reload = True
    on_line: models.RootFolder | None = None

    for optimization in optimizations:
        logger.info(f"Running {optimization}...")

        if requires_reload or on_line is None:
            # Rescan to have a fresh hierarchy after changes were made
            on_line = await online_scanner.scan(connection=connection)

        requires_reload = await optimization.perform(on_line=on_line, connection=connection, dry_run=dry_run)
