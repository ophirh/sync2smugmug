import os
from typing import Dict

from ..image import Image


class ImageOnSmugmug(Image):
    def __init__(self, album: 'AlbumOnSmugmug', image_record: Dict):
        super().__init__(album, os.path.join(album.relative_path, image_record['FileName']))

        self.record = image_record

    @property
    def album(self) -> 'AlbumOnSmugmug':
        return self._album

    @property
    def image_uri(self) -> str:
        return self.record['Uri']

    @property
    def image_download_uri(self) -> str:
        return self.record['ArchivedUri']

    @property
    def caption(self) -> str:
        return self.record['Caption']

    @property
    def keywords(self) -> str:
        return self.record['Keywords']

    def delete(self, dry_run: bool):
        if not dry_run:
            self.album.connection.image_delete(self)
