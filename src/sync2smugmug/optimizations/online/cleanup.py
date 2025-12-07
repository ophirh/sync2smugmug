import logging

from sync2smugmug import models
from sync2smugmug.online import online
from sync2smugmug.optimizations.online import OnlineOptimization
from sync2smugmug.utils import general_tools, node_tools

logger = logging.getLogger(__name__)


class DeleteEmptyAlbums(OnlineOptimization):
    """
    Find online albums that are empty and remove them
    """

    @general_tools.timeit
    async def perform(self, on_line: models.RootFolder, connection: online.OnlineConnection, dry_run: bool) -> bool:
        changed = False

        # Go over all albums and for each, check if there are duplicates
        for album in node_tools.iter_albums(root_folder=on_line):
            if album.online_info.image_count == 0:
                logger.info(f"{self} - Deleting empty album {album}")
                changed |= await connection.delete(album.online_info.uri, dry_run=dry_run)

        return changed
