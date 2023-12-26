from sync2smugmug.optimizations import Optimization
from sync2smugmug import models


class DiskOptimization(Optimization):
    """Base class for all disk optimizations"""

    async def perform(self, on_disk: models.RootFolder, dry_run: bool) -> bool:
        raise NotImplementedError()

    def __str__(self) -> str:
        return f"DiskOptimization {self.__class__.__name__}"
