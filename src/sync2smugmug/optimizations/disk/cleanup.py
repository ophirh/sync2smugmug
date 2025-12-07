import logging
import shutil
from pathlib import Path

from sync2smugmug import models
from sync2smugmug.utils import general_tools, node_tools

from .base import DiskOptimization

logger = logging.getLogger(__name__)


class DeleteEmptyDirectories(DiskOptimization):
    @general_tools.timeit
    async def perform(self, on_disk: models.RootFolder, dry_run: bool) -> bool:
        requires_reload = self._scan(dir_path=on_disk.disk_info.disk_path, dry_run=dry_run)

        if not requires_reload:
            logger.info("No empty directories detected")

        return requires_reload

    def _scan(self, dir_path: Path, dry_run: bool) -> bool:
        if node_tools.dir_is_empty_of_pictures(dir_path):
            logger.warning(f"Deleting empty dir {dir_path}")

            if not dry_run:
                shutil.rmtree(dir_path)

            return True

        requires_reload = False

        for p in dir_path.iterdir():
            if p.is_dir():
                requires_reload |= self._scan(dir_path=p, dry_run=dry_run)

        return requires_reload
