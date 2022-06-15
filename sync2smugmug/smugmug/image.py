import logging
import os
from typing import Dict

from ..image import Image

logger = logging.getLogger(__name__)


class ImageOnSmugmug(Image):
    def __init__(self, album: 'AlbumOnSmugmug', record: Dict):
        super().__init__(album, os.path.join(album.relative_path, record['FileName']))

        self.record = record

    @property
    def album(self) -> 'AlbumOnSmugmug':
        return self._album

    @property
    def image_uri(self) -> str:
        return self.record['Uri']

    @property
    def caption(self) -> str:
        return self.record['Caption']

    @property
    def keywords(self) -> str:
        return self.record['Keywords']

    @property
    def size(self) -> int:
        return self.record['OriginalSize']

    async def delete(self, dry_run: bool):
        logger.info(f"Deleting {self}")

        if not dry_run:
            await self.album.connection.request_delete(self.image_uri)
