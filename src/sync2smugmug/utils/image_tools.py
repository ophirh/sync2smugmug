import logging
from contextlib import closing
from datetime import datetime
from functools import lru_cache
from pathlib import Path, PurePath
from typing import Any

import PIL.ExifTags
import PIL.Image

from sync2smugmug import models

logger = logging.getLogger(__name__)


def is_image(filename: PurePath) -> bool:
    return filename.suffix.lower() in models.supported_image_types


def images_are_the_same(image1: models.Image, image2: models.Image) -> bool:
    # Compare more (e.g. metadata?)
    return image1.relative_path == image2.relative_path


@lru_cache(maxsize=128)
def extract_metadata(disk_path: Path, image_type: models.ImageType) -> dict[str, Any]:
    """
    Convert Image EXIF data into a dictionary
    """
    metadata = {}

    if not image_type.is_movie:
        try:
            # Extract vendor, model from metadata
            with closing(PIL.Image.open(disk_path)) as pil_image:
                exif_data = pil_image.getexif()

                for tag_id in exif_data:
                    # get the tag name, instead of human unreadable tag id
                    tag = PIL.ExifTags.TAGS.get(tag_id, tag_id)

                    data = exif_data.get(tag_id)
                    if isinstance(data, bytes):
                        # data = data.decode()
                        continue

                    metadata[tag] = data

        except PIL.UnidentifiedImageError:
            pass

    else:
        # TODO: Figure out if we want to support videos & raw images
        pass

    return metadata


def extract_image_time_taken(disk_path: Path, image_type: models.ImageType) -> datetime | None:
    metadata = extract_metadata(disk_path, image_type)
    if metadata is None:
        return None

    datetime_str = metadata.get("DateTime")
    if datetime_str is None:
        return None

    try:
        # Parse the date!
        return datetime.strptime(datetime_str, "%Y:%m:%d %H:%M:%S")
    except ValueError:
        if logger.isEnabledFor(logging.DEBUG):
            logger.exception(f"Failed to parse date for {disk_path}")
        return None


def extract_image_camera_make(disk_path: Path, image_type: models.ImageType) -> str | None:
    metadata = extract_metadata(disk_path, image_type)
    if metadata is None:
        return None

    return metadata.get("Make")


def extract_image_camera_model(disk_path: Path, image_type: models.ImageType) -> str | None:
    metadata = extract_metadata(disk_path, image_type)
    if metadata is None:
        return None

    return metadata.get("Model")


def convert_to_jpeg(image_disk_path: Path, dry_run: bool) -> bool:
    # TODO: Convert the underlying image to JPEG
    logger.info(f"Converting {image_disk_path} (dry_run={dry_run})")
    return False
