import os
from contextlib import closing
from datetime import datetime
from typing import Dict, Any, Optional

from PIL import Image as PILImage, UnidentifiedImageError
from PIL.ExifTags import TAGS
from pillow_heif import register_heif_opener

from ..image import Image
from ..node import Album

# Register the HEIF opener into PIL (to support iPhone images)
register_heif_opener()


class ImageOnDisk(Image):
    def __init__(self, album: Album, relative_path: str):
        super().__init__(album, relative_path)
        self._metadata = None

    @property
    def disk_path(self) -> str:
        return os.path.join(self.album.base_dir, self.relative_path)

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
        if self._metadata is None:
            self._metadata = self.extract_metadata(self.disk_path)

        return self._metadata

    def convert_to_jpeg(self) -> bool:
        if self.extension != ".heic":
            return False

        # TODO: Convert the underlying image file from HEIC to JPEG
        return False

    @classmethod
    def extract_metadata(cls, image_disk_path: str) -> Dict[str, Any]:
        """
        Convert Image EXIF data into a dictionary
        """
        metadata = {}

        if cls.check_is_image(image_disk_path):
            try:
                # Extract vendor, model from metadata
                with closing(PILImage.open(image_disk_path)) as pil_image:
                    exif_data = pil_image.getexif()

                    for tag_id in exif_data:
                        # get the tag name, instead of human unreadable tag id
                        tag = TAGS.get(tag_id, tag_id)

                        data = exif_data.get(tag_id)
                        if isinstance(data, bytes):
                            # data = data.decode()
                            continue

                        metadata[tag] = data

            except UnidentifiedImageError as e:
                pass

        else:
            # TODO: Figure out if we want to support videos & raw images
            pass

        return metadata

    @property
    def camera_make(self) -> str:
        return self.get_metadata().get("Make")

    @property
    def camera_model(self) -> str:
        return self.get_metadata().get("Model")

    @classmethod
    def extract_time_taken(cls, image_disk_path: str) -> Optional[datetime]:
        metadata = cls.extract_metadata(image_disk_path)
        datetime_str = metadata.get("DateTime")

        if datetime_str is None:
            return None

        # Parse the date!
        return datetime.strptime(datetime_str, "%Y:%m:%d %H:%M:%S")
