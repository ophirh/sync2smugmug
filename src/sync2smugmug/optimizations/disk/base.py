from sync2smugmug import models

from ..base import Optimization


class DiskOptimization(Optimization):
    """Base class for all disk optimizations"""

    async def perform(self, on_disk: models.RootFolder, dry_run: bool) -> bool:
        raise NotImplementedError()

    def __str__(self) -> str:
        return f"DiskOptimization {self.__class__.__name__}"
