import os
from contextlib import closing
from typing import Dict, Any

from PIL import Image as PILImage, UnidentifiedImageError
from PIL.ExifTags import TAGS

from ..image import Image
from ..node import Album


class ImageOnDisk(Image):
    def __init__(self, album: Album, relative_path: str):
        super().__init__(album, relative_path)
        self._metadata = None

    @property
    def disk_path(self) -> str:
        return os.path.join(self.album.base_dir, self.relative_path)

    @property
    def caption(self) -> str:
        # TODO: Get the Picasa / LightRoom
        return ""

    @property
    def keywords(self) -> str:
        # TODO: Get the Picasa / LightRoom
        return ""

    @property
    def size(self) -> int:
        return os.stat(self.disk_path).st_size

    async def delete(self, dry_run: bool):
        if not dry_run:
            os.remove(self.disk_path)

    def get_metadata(self) -> Dict[str, Any]:
        """
        Convert Image EXIF data into a dictionary
        """
        if self._metadata is None:
            self._metadata = {}

            if self.is_image:
                try:
                    # Extract vendor, model from metadata
                    with closing(PILImage.open(self.disk_path)) as pil_image:
                        exif_data = pil_image.getexif()

                        for tag_id in exif_data:
                            # get the tag name, instead of human unreadable tag id
                            tag = TAGS.get(tag_id, tag_id)

                            data = exif_data.get(tag_id)
                            if isinstance(data, bytes):
                                # data = data.decode()
                                continue

                            self._metadata[tag] = data

                except UnidentifiedImageError:
                    pass

            else:
                # TODO: Figure out if we want to support videos & raw images
                pass

        return self._metadata

    @property
    def camera_make(self) -> str:
        return self.get_metadata().get("Make")

    @property
    def camera_model(self) -> str:
        return self.get_metadata().get("Model")
