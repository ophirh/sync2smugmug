import logging
from datetime import datetime
from pathlib import Path, PurePath
from unittest.mock import MagicMock, Mock, patch

import PIL.Image
import pytest

from sync2smugmug import models
from sync2smugmug.utils import image_tools


class TestIsImage:
    """Tests for is_image function"""

    @pytest.mark.parametrize(
        "filename,expected",
        [
            (PurePath("photo.jpg"), True),
            (PurePath("photo.jpeg"), True),
            (PurePath("photo.JPG"), True),
            (PurePath("photo.JPEG"), True),
            (PurePath("photo.heic"), True),
            (PurePath("video.mp4"), True),
            (PurePath("video.avi"), True),
            (PurePath("video.mov"), True),
            (PurePath("video.mts"), True),
            (PurePath("video.mv4"), True),
            (PurePath("document.txt"), False),
            (PurePath("document.pdf"), False),
            (PurePath("archive.zip"), False),
            (PurePath("no_extension"), False),
        ],
    )
    def test_is_image(self, filename, expected):
        assert image_tools.is_image(filename) == expected


class TestImagesAreTheSame:
    """Tests for images_are_the_same function"""

    def test_same_relative_path(self):
        image1 = models.Image(
            album_relative_path=PurePath("2023_01_15"),
            filename=PurePath("photo.jpg"),
        )
        image2 = models.Image(
            album_relative_path=PurePath("2023_01_15"),
            filename=PurePath("photo.jpg"),
        )
        assert image_tools.images_are_the_same(image1, image2) is True

    def test_different_relative_path(self):
        image1 = models.Image(
            album_relative_path=PurePath("2023_01_15"),
            filename=PurePath("photo.jpg"),
        )
        image2 = models.Image(
            album_relative_path=PurePath("2023_01_16"),
            filename=PurePath("photo.jpg"),
        )
        assert image_tools.images_are_the_same(image1, image2) is False

    def test_different_filename(self):
        image1 = models.Image(
            album_relative_path=PurePath("2023_01_15"),
            filename=PurePath("photo1.jpg"),
        )
        image2 = models.Image(
            album_relative_path=PurePath("2023_01_15"),
            filename=PurePath("photo2.jpg"),
        )
        assert image_tools.images_are_the_same(image1, image2) is False


class TestExtractMetadata:
    """Tests for extract_metadata function"""

    def setup_method(self):
        # Clear the LRU cache before each test
        image_tools.extract_metadata.cache_clear()

    def test_extract_metadata_from_image(self):
        disk_path = Path("/fake/path/photo.jpg")
        image_type = models.ImageType(ext=".jpg", is_movie=False)

        mock_exif = {
            271: "Canon",  # Make
            272: "Canon EOS 5D",  # Model
            306: "2023:01:15 10:30:45",  # DateTime
            274: 1,  # Orientation
        }

        mock_image = MagicMock()
        mock_image.getexif.return_value = mock_exif

        with patch("PIL.Image.open", return_value=mock_image):
            metadata = image_tools.extract_metadata(disk_path, image_type)

            assert "Make" in metadata
            assert metadata["Make"] == "Canon"
            assert "Model" in metadata
            assert metadata["Model"] == "Canon EOS 5D"
            assert "DateTime" in metadata
            assert metadata["DateTime"] == "2023:01:15 10:30:45"

    def test_extract_metadata_skips_bytes(self):
        disk_path = Path("/fake/path/photo.jpg")
        image_type = models.ImageType(ext=".jpg", is_movie=False)

        mock_exif = {
            271: "Canon",  # Make (string)
            37500: b"binary_data",  # MakerNote (bytes) - should be skipped
        }

        mock_image = MagicMock()
        mock_image.getexif.return_value = mock_exif

        with patch("PIL.Image.open", return_value=mock_image):
            metadata = image_tools.extract_metadata(disk_path, image_type)

            assert "Make" in metadata
            assert "MakerNote" not in metadata

    def test_extract_metadata_from_movie(self):
        disk_path = Path("/fake/path/video.mp4")
        image_type = models.ImageType(ext=".mp4", is_movie=True)

        metadata = image_tools.extract_metadata(disk_path, image_type)

        # Should return empty dict for movies (not yet implemented)
        assert metadata == {}

    def test_extract_metadata_handles_unidentified_image(self):
        disk_path = Path("/fake/path/corrupted.jpg")
        image_type = models.ImageType(ext=".jpg", is_movie=False)

        with patch("PIL.Image.open", side_effect=PIL.UnidentifiedImageError):
            metadata = image_tools.extract_metadata(disk_path, image_type)

            # Should return empty dict on error
            assert metadata == {}

    def test_extract_metadata_caching(self):
        disk_path = Path("/fake/path/photo.jpg")
        image_type = models.ImageType(ext=".jpg", is_movie=False)

        mock_exif = {271: "Canon"}
        mock_image = MagicMock()
        mock_image.getexif.return_value = mock_exif

        with patch("PIL.Image.open", return_value=mock_image) as mock_open:
            # Call twice
            metadata1 = image_tools.extract_metadata(disk_path, image_type)
            metadata2 = image_tools.extract_metadata(disk_path, image_type)

            # Should only open the image once due to caching
            assert mock_open.call_count == 1
            assert metadata1 == metadata2


class TestExtractImageTimeTaken:
    """Tests for extract_image_time_taken function"""

    def setup_method(self):
        # Clear the LRU cache before each test
        image_tools.extract_metadata.cache_clear()

    def test_extract_time_taken_success(self):
        disk_path = Path("/fake/path/photo.jpg")
        image_type = models.ImageType(ext=".jpg", is_movie=False)

        mock_exif = {306: "2023:01:15 10:30:45"}
        mock_image = MagicMock()
        mock_image.getexif.return_value = mock_exif

        with patch("PIL.Image.open", return_value=mock_image):
            dt = image_tools.extract_image_time_taken(disk_path, image_type)

            assert dt == datetime(2023, 1, 15, 10, 30, 45)

    def test_extract_time_taken_no_metadata(self):
        disk_path = Path("/fake/path/photo.jpg")
        image_type = models.ImageType(ext=".jpg", is_movie=False)

        with patch("sync2smugmug.utils.image_tools.extract_metadata", return_value=None):
            dt = image_tools.extract_image_time_taken(disk_path, image_type)

            assert dt is None

    def test_extract_time_taken_no_datetime_field(self):
        disk_path = Path("/fake/path/photo.jpg")
        image_type = models.ImageType(ext=".jpg", is_movie=False)

        mock_exif = {271: "Canon"}  # No DateTime field
        mock_image = MagicMock()
        mock_image.getexif.return_value = mock_exif

        with patch("PIL.Image.open", return_value=mock_image):
            dt = image_tools.extract_image_time_taken(disk_path, image_type)

            assert dt is None

    def test_extract_time_taken_invalid_format(self):
        disk_path = Path("/fake/path/photo.jpg")
        image_type = models.ImageType(ext=".jpg", is_movie=False)

        mock_exif = {306: "invalid date format"}
        mock_image = MagicMock()
        mock_image.getexif.return_value = mock_exif

        with patch("PIL.Image.open", return_value=mock_image):
            dt = image_tools.extract_image_time_taken(disk_path, image_type)

            assert dt is None

    def test_extract_time_taken_logs_debug_on_error(self, caplog):
        disk_path = Path("/fake/path/photo.jpg")
        image_type = models.ImageType(ext=".jpg", is_movie=False)

        mock_exif = {306: "invalid date format"}
        mock_image = MagicMock()
        mock_image.getexif.return_value = mock_exif

        with patch("PIL.Image.open", return_value=mock_image):
            with caplog.at_level(logging.DEBUG):
                dt = image_tools.extract_image_time_taken(disk_path, image_type)

                assert dt is None
                # Should log exception if debug is enabled
                if logging.getLogger("sync2smugmug.utils.image_tools").isEnabledFor(logging.DEBUG):
                    assert any("Failed to parse date" in record.message for record in caplog.records)


class TestExtractImageCameraMake:
    """Tests for extract_image_camera_make function"""

    def setup_method(self):
        # Clear the LRU cache before each test
        image_tools.extract_metadata.cache_clear()

    def test_extract_camera_make_success(self):
        disk_path = Path("/fake/path/photo.jpg")
        image_type = models.ImageType(ext=".jpg", is_movie=False)

        mock_exif = {271: "Canon"}
        mock_image = MagicMock()
        mock_image.getexif.return_value = mock_exif

        with patch("PIL.Image.open", return_value=mock_image):
            make = image_tools.extract_image_camera_make(disk_path, image_type)

            assert make == "Canon"

    def test_extract_camera_make_no_metadata(self):
        disk_path = Path("/fake/path/photo.jpg")
        image_type = models.ImageType(ext=".jpg", is_movie=False)

        with patch("sync2smugmug.utils.image_tools.extract_metadata", return_value=None):
            make = image_tools.extract_image_camera_make(disk_path, image_type)

            assert make is None

    def test_extract_camera_make_field_missing(self):
        disk_path = Path("/fake/path/photo.jpg")
        image_type = models.ImageType(ext=".jpg", is_movie=False)

        mock_exif = {272: "Canon EOS 5D"}  # Has Model but no Make
        mock_image = MagicMock()
        mock_image.getexif.return_value = mock_exif

        with patch("PIL.Image.open", return_value=mock_image):
            make = image_tools.extract_image_camera_make(disk_path, image_type)

            assert make is None


class TestExtractImageCameraModel:
    """Tests for extract_image_camera_model function"""

    def setup_method(self):
        # Clear the LRU cache before each test
        image_tools.extract_metadata.cache_clear()

    def test_extract_camera_model_success(self):
        disk_path = Path("/fake/path/photo.jpg")
        image_type = models.ImageType(ext=".jpg", is_movie=False)

        mock_exif = {272: "Canon EOS 5D"}
        mock_image = MagicMock()
        mock_image.getexif.return_value = mock_exif

        with patch("PIL.Image.open", return_value=mock_image):
            model = image_tools.extract_image_camera_model(disk_path, image_type)

            assert model == "Canon EOS 5D"

    def test_extract_camera_model_no_metadata(self):
        disk_path = Path("/fake/path/photo.jpg")
        image_type = models.ImageType(ext=".jpg", is_movie=False)

        with patch("sync2smugmug.utils.image_tools.extract_metadata", return_value=None):
            model = image_tools.extract_image_camera_model(disk_path, image_type)

            assert model is None

    def test_extract_camera_model_field_missing(self):
        disk_path = Path("/fake/path/photo.jpg")
        image_type = models.ImageType(ext=".jpg", is_movie=False)

        mock_exif = {271: "Canon"}  # Has Make but no Model
        mock_image = MagicMock()
        mock_image.getexif.return_value = mock_exif

        with patch("PIL.Image.open", return_value=mock_image):
            model = image_tools.extract_image_camera_model(disk_path, image_type)

            assert model is None


class TestConvertToJpeg:
    """Tests for convert_to_jpeg function"""

    def test_convert_to_jpeg_dry_run(self, caplog):
        disk_path = Path("/fake/path/photo.heic")

        with caplog.at_level(logging.INFO):
            result = image_tools.convert_to_jpeg(disk_path, dry_run=True)

            assert result is False
            assert any("Converting" in record.message and "dry_run=True" in record.message for record in caplog.records)

    def test_convert_to_jpeg_not_dry_run(self, caplog):
        disk_path = Path("/fake/path/photo.heic")

        with caplog.at_level(logging.INFO):
            result = image_tools.convert_to_jpeg(disk_path, dry_run=False)

            assert result is False
            assert any(
                "Converting" in record.message and "dry_run=False" in record.message for record in caplog.records
            )
