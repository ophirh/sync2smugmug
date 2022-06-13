import os

from ..image import Image
from ..node import Album


class ImageOnDisk(Image):
    def __init__(self, album: Album, relative_path: str):
        super().__init__(album, relative_path)

    @property
    def disk_path(self) -> str:
        return f'{self.album.base_dir}{self.relative_path}'

    @property
    def caption(self) -> str:
        # TODO: Get the Picasa / LightRoom
        return ''

    @property
    def keywords(self) -> str:
        # TODO: Get the Picasa / LightRoom
        return ''

    @property
    def size(self) -> int:
        return os.stat(self.disk_path).st_size

    async def delete(self, dry_run: bool):
        if not dry_run:
            os.remove(self.disk_path)
