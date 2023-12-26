import logging

from sync2smugmug.optimizations.disk import DiskOptimization
from sync2smugmug import models
from sync2smugmug.utils import node_tools, image_tools, general_tools

logger = logging.getLogger(__name__)


class ConvertImagesAndMovies(DiskOptimization):
    @general_tools.timeit
    async def perform(self, on_disk: models.RootFolder, dry_run: bool) -> bool:
        requires_reload = False

        for album in node_tools.iter_albums(on_disk):
            for image in album.images:
                if not image.image_type.requires_conversion:
                    continue

                # TODO - Convert all types (check which types need conversion...)
                if image.image_type.requires_conversion and not image.image_type.is_movie:
                    requires_reload |= image_tools.convert_to_jpeg(image.disk_info.disk_path, dry_run=dry_run)

        if not requires_reload:
            logger.info("No images converted")

        return requires_reload
