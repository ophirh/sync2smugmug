import os
from typing import List, Dict

from .utils import cmp
from .image import Image


class Node:
    """
    Represents a node (folder, album or page) in the system - or a subtree of the hierarchy.
    The natural key to a node is its 'relative_path', starting with '/' (os.sep) as the root.
    """

    def __init__(self, source: str, parent: 'Folder', relative_path: str):
        """
        :param source: Source of this node (Disk or Smug)
        :param Node parent: Parent node (None for root)
        :param str relative_path: Relative path for node
        """
        self._source = source
        self._parent = parent
        self._relative_path = relative_path or os.sep

    @property
    def parent(self) -> 'Folder':
        return self._parent

    @property
    def name(self) -> str:
        """ Get the object name from its ID """
        return os.path.basename(self.relative_path)

    @property
    def source(self) -> str:
        return self._source

    @property
    def description(self) -> str:
        return ""

    @property
    def relative_path(self) -> str:
        return self._relative_path

    @property
    def is_root(self) -> bool:
        return self.get_is_root(self.relative_path)

    @classmethod
    def get_is_root(cls, relative_path) -> bool:
        return relative_path in ('', os.sep)

    @property
    def is_album(self) -> bool:
        raise NotImplementedError()

    @property
    def is_folder(self) -> bool:
        return not self.is_album

    @property
    def last_modified(self) -> float:
        """
        Get last modified time in Unix timestamp
        """
        raise NotImplementedError()

    def delete(self, dry_run: bool):
        raise NotImplementedError()

    def __repr__(self):
        return f'{self.__class__.__name__} {self.relative_path}'


class Folder(Node):
    def __init__(self, source: str, parent: 'Folder', relative_path: str):
        super().__init__(source, parent, relative_path)

        self.folder_count = 0
        self.album_count = 0
        self.image_count = 0

    def stats(self) -> str:
        # Show some stats on the scan
        return f'{self.folder_count} folders, {self.album_count} albums, {self.image_count} images'

    @property
    def last_modified(self) -> float:
        raise NotImplementedError()

    @property
    def is_album(self) -> bool:
        return False

    @property
    def sub_folders(self) -> Dict[str, 'Folder']:
        raise NotImplementedError()

    @property
    def albums(self) -> Dict[str, 'Album']:
        raise NotImplementedError()

    def delete(self, dry_run: bool):
        raise NotImplementedError()


class Album(Node):
    def __init__(self, source: str, parent: Folder, relative_path: str):
        super().__init__(source, parent, relative_path)

    @property
    def last_modified(self) -> float:
        raise NotImplementedError()

    @property
    def is_album(self) -> bool:
        return True

    @property
    def images(self) -> List[Image]:
        raise NotImplementedError()

    @property
    def image_count(self) -> int:
        return len(self.images)

    def __eq__(self, other):
        return self.compare(other) == 0

    def __lt__(self, other):
        return self.compare(other) < 0

    def __le__(self, other):
        return self.compare(other) <= 0

    def __gt__(self, other):
        return self.compare(other) > 0

    def __ge__(self, other):
        return self.compare(other) >= 0

    def __contains__(self, image: Image) -> bool:
        assert isinstance(image, Image)
        return any(i for i in self.images if i.relative_path == image.relative_path)

    def compare(self, other: 'Album') -> int:
        """
        Same functionality as old __cmp__ or C's strcmp
        """
        assert isinstance(other, Album)

        i = self.shallow_compare(other)
        if i == 0:
            i = self.deep_compare(other, shallow_compare_first=False)

        return i

    def shallow_compare(self, other: 'Album') -> int:
        i = cmp(self.relative_path, other.relative_path)
        if i != 0:
            return i

        i = self.last_modified - other.last_modified
        if i != 0:
            return 1 if i > 0 else -1

        i = self.image_count - other.image_count
        if i != 0:
            return i

        # TODO: Check change in description and other meta-data attributes
        return 0

    def deep_compare(self, other: 'Album', shallow_compare_first: bool = True) -> int:
        if shallow_compare_first:
            i = self.shallow_compare(other)
            if i != 0:
                return i

        # Compare images - one by one
        self_images = sorted(self.images, key=lambda k: k.relative_path)
        other_images = sorted(other.images, key=lambda k: k.relative_path)

        for si, oi in zip(self_images, other_images):
            i = si.compare(oi)
            if i != 0:
                return i

        # TODO: More compares?
        return 0

    def delete(self, dry_run: bool):
        raise NotImplementedError()
