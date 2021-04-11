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
    def is_image(cls, f) -> bool:
        _, ext = os.path.splitext(f)
        # Unknown file types: '.3gp',
        return ext.lower() in ('.jpg', '.jpeg', '.avi', '.mv4', '.mov', '.mp4', '.mts')

    @classmethod
    def is_raw_image(cls, f) -> bool:
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

    def compare(self, other: 'Image') -> int:
        assert isinstance(other, Image)

        i = cmp(self.relative_path, other.relative_path)

        if i == 0:
            i = cmp(self.caption, other.caption)

#            if i == 0:
#                i = cmp(self.keywords, other.keywords)
#                # TODO: Compare more?
#                pass

        return i

    def __repr__(self) -> str:
        return '{} {}'.format(self.__class__.__name__, self.relative_path)

    def delete(self, dry_run: bool):
        raise NotImplementedError()
