import os

from .utils import cmp


class DefaultImageNameConverter:
    @classmethod
    def to_smugmug_relative_path(cls, original_name: str) -> str:
        # By default - does not touch the name
        return original_name

    @classmethod
    def save_as(cls, original_name: str) -> str:
        # By default - does not touch the name
        return original_name


class DefaultMovieNameConverter(DefaultImageNameConverter):
    """
    All Smugmug movies are actually mp4 format
    """

    @classmethod
    def to_smugmug_relative_path(cls, original_name: str) -> str:
        return original_name + '.MP4' if not original_name.lower().endswith('.mp4') else original_name


class HeicImageNameConverter(DefaultImageNameConverter):
    """
    iPhone HEIC images are automatically converted (and downloaded as) JPG files by Smugmug
    """

    @classmethod
    def to_smugmug_relative_path(cls, original_name: str) -> str:
        name, ext = os.path.splitext(original_name)
        return name + '.JPG'


# All supported image formats and their assigned converters
image_name_converters = {
    '.jpg': DefaultImageNameConverter,
    '.jpeg': DefaultImageNameConverter,
    '.avi': DefaultMovieNameConverter,
    '.mv4': DefaultMovieNameConverter,
    '.mov': DefaultMovieNameConverter,
    '.mp4': DefaultMovieNameConverter,
    '.mts': DefaultMovieNameConverter,
    '.heic': HeicImageNameConverter,
}


class Image:
    def __init__(self, album: 'Album', relative_path: str):
        """
        :param Album album: Album this image belongs to
        :param str relative_path: Relative path of image
        """
        self._album = album
        self._relative_path = relative_path

        _, ext = os.path.splitext(self._relative_path)
        self._image_name_converter: DefaultImageNameConverter = image_name_converters[ext.lower()]
        self._smugmug_relative_path = self._image_name_converter.to_smugmug_relative_path(self._relative_path)

    @property
    def album(self) -> 'Album':
        return self._album

    @property
    def relative_path(self) -> str:
        return self._relative_path

    @property
    def smugmug_relative_path(self) -> str:
        """
        Name (path) of an image the way it would be represented in Smugmug (after conversion)
        """
        return self._smugmug_relative_path

    @classmethod
    def is_image(cls, path: str, f: str) -> bool:
        _, ext = os.path.splitext(f)
        # Unknown file types: '.3gp',
        if ext.lower() not in image_name_converters:
            return False

        return os.stat(os.path.join(path, f)).st_size > 0

    @classmethod
    def is_raw_image(cls, f: str) -> bool:
        _, ext = os.path.splitext(f)
        return ext.lower() in ('.orf', '.crw', '.cr2', '.nef', '.raw', '.dng')

    @property
    def is_video(self) -> bool:
        _, ext = os.path.splitext(self.relative_path)
        return ext.lower() in ('.avi', '.mv4', '.mov', '.mp4', '.mts')

    @property
    def caption(self) -> str:
        raise NotImplementedError()

    @property
    def keywords(self) -> str:
        raise NotImplementedError()

    @property
    def name(self) -> str:
        _, name = os.path.split(self.relative_path)
        return name

    @property
    def size(self) -> int:
        raise NotImplementedError()

    async def delete(self, dry_run: bool):
        raise NotImplementedError()

    def compare(self, other: 'Image') -> int:
        assert isinstance(other, Image)

        # There were several issues with comparing file sizes (especially for movies). Ignored for now
        # if i == 0:
        #     # Ignore minor differences in size (below 10% size)
        #     if abs(self.size - other.size) / self.size > 0.2:
        #         i = cmp(self.size, other.size)

        return cmp(self.relative_path, other.relative_path)

    def __eq__(self, other):
        return self.relative_path == other.relative_path

    def __hash__(self):
        return hash(self.relative_path)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} {self.relative_path}'
