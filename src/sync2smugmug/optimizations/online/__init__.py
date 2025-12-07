from sync2smugmug import models
from sync2smugmug.online import online
from sync2smugmug.optimizations import Optimization


class OnlineOptimization(Optimization):
    """Base class for all online optimizations"""

    async def perform(self, on_line: models.RootFolder, connection: online.OnlineConnection, dry_run: bool) -> bool:
        raise NotImplementedError()

    def __str__(self) -> str:
        return f"OnlineOptimization {self.__class__.__name__}"
