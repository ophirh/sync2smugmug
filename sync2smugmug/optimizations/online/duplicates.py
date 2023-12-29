import logging
from pathlib import PurePath
from typing import List, Set

from sync2smugmug import models
from sync2smugmug.online import online
from sync2smugmug.optimizations.online import OnlineOptimization
from sync2smugmug.utils import general_tools, node_tools

logger = logging.getLogger(__name__)


class RemoveOnlineImageDuplicates(OnlineOptimization):
    """
    Find images online with the same name and remove all but one version
    """

    @general_tools.timeit
    async def perform(self, on_line: models.RootFolder, connection: online.OnlineConnection, dry_run: bool) -> bool:
        changed = False

        # Go over all albums and for each, check if there are duplicates
        for album in node_tools.iter_albums(root_folder=on_line):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Analyzing {album} for duplicates")

            await online.load_album_images(album=album, connection=connection)

            # Find duplicates
            duplicates: List[models.Image] = []
            already_found: Set[PurePath] = set()

            for image in album.images:
                if image.filename in already_found:
                    duplicates.append(image)

                already_found.add(image.filename)

            if duplicates:
                logger.info(f"{self} - Deleting {len(duplicates)} duplicate photos")

                for duplicate_image in duplicates:
                    # This is a duplicate!
                    logger.info(f"Deleting image {duplicate_image}")
                    changed |= await connection.delete(duplicate_image.online_info.uri, dry_run=dry_run)

        return changed
