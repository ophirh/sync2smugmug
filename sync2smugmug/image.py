import os

from .utils import cmp


class Image:
    def __init__(self, album: 'Album', relative_path: str):
        """
        :param Album album: Album this image belongs to
        :param str relative_path: Relative path of image
        """
        self._album = album
        self.relative_path = relative_path

    @property
    def album(self) -> 'Album':
        return self._album

    @classmethod
    def is_image(cls, path: str, f: str) -> bool:
        _, ext = os.path.splitext(f)
        # Unknown file types: '.3gp',
        if ext.lower() not in ('.jpg', '.jpeg', '.avi', '.mv4', '.mov', '.mp4', '.mts'):
            return False

        return os.stat(os.path.join(path, f)).st_size > 0

    @classmethod
    def is_raw_image(cls, f: str) -> bool:
        _, ext = os.path.splitext(f)
        # Unknown file types: '.3gp',
        return ext.lower() in ('.orf', '.crw', '.cr2', '.nef', '.raw', '.dng')

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

    def delete(self, dry_run: bool):
        raise NotImplementedError()

    def compare(self, other: 'Image') -> int:
        assert isinstance(other, Image)

        i = cmp(self.relative_path, other.relative_path)

        if i == 0:
            # TODO: Allow delta here? Or 10% difference?
            i = self.size - other.size
            if i != 0:
                pass

        return i

    def __repr__(self) -> str:
        return f'{self.__class__.__name__} {self.relative_path}'
