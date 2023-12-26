import logging
from typing import List, Set

import sync2smugmug.protocols
from sync2smugmug import models
from sync2smugmug.online import online
from sync2smugmug.optimizations.online import OnlineOptimization
from sync2smugmug.utils import general_tools, node_tools

logger = logging.getLogger(__name__)


class RemoveOnlineImageDuplicates(OnlineOptimization):
    """
    Find images online with the same name and remove all but one version
    """

    @property
    def last_album_name_checked(self) -> int:
        return self.my_context.get("last_album_checked", 0)

    @last_album_name_checked.setter
    def last_album_name_checked(self, count: int):
        ctx = self.my_context
        ctx["last_album_checked"] = count
        self.save_context(ctx)

    @general_tools.timeit
    async def perform(self, on_line: models.RootFolder, connection: online.OnlineConnection, dry_run: bool) -> bool:
        changed = False
        count = 0

        # Go over all albums and for each, check if there are duplicates
        for online_album in node_tools.iter_albums(root_folder=on_line):
            images: List[sync2smugmug.protocols.OnlineImageInfoShape] = []

            if count < self.last_album_name_checked:
                # Already scanned this album
                count += 1
                continue

            # Load images for this album
            async for image_record in connection.iter_album_images(online_album.online_info):
                images.append(image_record)

            # Now check for duplicates
            unique_names = {i.name for i in images}
            duplicate_count = len(images) - len(unique_names)

            if duplicate_count > 0:
                logger.info(f"{self} - Deleting {duplicate_count} duplicate photos")

                image_names_encountered: Set[str] = set()

                while len(images) > 0:
                    image = images.pop()

                    if image.name in image_names_encountered:
                        # This is a duplicate!
                        logger.info(f"Deleting image {image}")
                        changed |= await connection.delete(image.uri, dry_run=dry_run)
                    else:
                        image_names_encountered.add(image.name)

            self.last_album_name_checked = count
            count += 1

        return changed
