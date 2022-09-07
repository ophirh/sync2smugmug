from datetime import datetime, date
import os
import re
from typing import Dict, List, TypeVar, Generic, Generator, Optional

from .utils import cmp
from .image import Image

FolderType = TypeVar("FolderType", covariant=True)
AlbumType = TypeVar("AlbumType", covariant=True)
ImageType = TypeVar("ImageType", covariant=True)

DATE_ALBUM_PATTERN = re.compile(r"([12][90]\d\d_[0-1]\d_[0-3]\d)( - .*)?")


class Node(Generic[FolderType, AlbumType, ImageType]):
    """
    Represents a node (folder, album or page) in the system - or a subtree of the hierarchy.
    The natural key to a node is its 'relative_path' ('' as the root)
    """

    ROOT = ""

    def __init__(self, source: str, parent: FolderType, relative_path: str):
        """
        :param source: Source of this node (Disk or Smug)
        :param Node parent: Parent node (None for root)
        :param str relative_path: Relative path for node
        """
        self._source = source
        self._parent = parent
        self._relative_path = relative_path

    @property
    def parent(self) -> FolderType:
        return self._parent

    @property
    def name(self) -> str:
        """Get the object name from its ID"""
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
        return relative_path == Node.ROOT

    @property
    def is_album(self) -> bool:
        raise NotImplementedError()

    @property
    def is_folder(self) -> bool:
        return not self.is_album

    def get_album_date(self) -> Optional[date]:
        match = re.match(DATE_ALBUM_PATTERN, self.name)
        if match is None:
            return None

        date_str = match.group(1)
        return datetime.strptime(date_str, "%Y_%m_%d").date()

    @property
    def last_modified(self) -> float:
        """
        Get last modified time in Unix timestamp
        """
        raise NotImplementedError()

    async def delete(self, dry_run: bool):
        raise NotImplementedError()

    def __repr__(self):
        return f"{self.__class__.__name__} {self.relative_path}"


class Folder(Node[FolderType, AlbumType, ImageType]):
    def __init__(self, source: str, parent: FolderType, relative_path: str):
        super().__init__(source, parent, relative_path)

        self.folder_count = 0
        self.album_count = 0
        self.image_count = 0

        self._sub_folders: Dict[str, FolderType] = {}
        self._albums: Dict[str, AlbumType] = {}

    @property
    def sub_folders(self) -> Dict[str, FolderType]:
        return self._sub_folders

    @property
    def albums(self) -> Dict[str, AlbumType]:
        return self._albums

    @property
    def last_modified(self) -> float:
        raise NotImplementedError()

    @property
    def is_album(self) -> bool:
        return False

    def stats(self) -> str:
        # Show some stats on the scan
        return f"{self.folder_count} folders, {self.album_count} albums, {self.image_count} images"

    def add_sub_folder(self, sub_folder: FolderType):
        self._sub_folders[sub_folder.name] = sub_folder

        # Update counts (go up the hierarchy)
        parent: Folder = self.parent
        while parent:
            parent.folder_count += 1
            parent.album_count += sub_folder.album_count
            parent.image_count += sub_folder.image_count
            parent = parent.parent

    def remove_sub_folder(self, sub_folder: FolderType):
        del self._sub_folders[sub_folder.name]

        # Update counts (go up the hierarchy)
        parent: Folder = self.parent
        while parent:
            parent.folder_count -= 1
            parent.album_count += sub_folder.album_count
            parent.image_count += sub_folder.image_count
            parent = parent.parent

    def add_album(self, album: AlbumType):
        self._albums[album.name] = album

        image_count = album.image_count

        # Update counts (go up the hierarchy)
        parent: Folder = self.parent
        while parent:
            parent.album_count += 1
            parent.image_count += image_count
            parent = parent.parent

    def remove_album(self, album: AlbumType):
        del self._albums[album.name]

        image_count = album.image_count

        # Update counts (go up the hierarchy)
        parent: Folder = self.parent
        while parent:
            parent.album_count -= 1
            parent.image_count -= image_count
            parent = parent.parent

    async def delete(self, dry_run: bool):
        raise NotImplementedError()

    def iter_folders(self) -> Generator[FolderType, None, None]:
        """
        Recursively iterate through all folders (including root) - DFS
        """
        yield from self._iter_folders(self)

    def iter_albums(self) -> Generator[AlbumType, None, None]:
        """
        Recursively iterate through all albums - DFS
        """
        yield from self._iter_albums(self)

    async def aiter_images(self) -> Generator[ImageType, None, None]:
        """
        Recursively iterate through all images in all albums - DFS
        """
        for album in self._iter_albums(self):
            for image in await album.get_images():
                yield image

    @classmethod
    def _iter_albums(cls, from_folder: FolderType) -> Generator[AlbumType, None, None]:
        for folder in cls._iter_folders(from_folder):
            for album in folder.albums.values():
                yield album

    @classmethod
    def _iter_folders(cls, from_folder: FolderType) -> Generator[FolderType, None, None]:
        # Yield first the root folder
        yield from_folder

        # Now recursively go through hierarchy under root
        for sub_folder in from_folder.sub_folders.values():
            yield from cls._iter_folders(sub_folder)


class Album(Node[FolderType, AlbumType, ImageType]):
    def __init__(self, source: str, parent: Folder, relative_path: str):
        super().__init__(source, parent, relative_path)

        self._images: Optional[List[ImageType]] = None

    @property
    def last_modified(self) -> float:
        raise NotImplementedError()

    @property
    def is_album(self) -> bool:
        return True

    async def get_images(self) -> List[ImageType]:
        """
        Returns album images (lazy load for performance)
        """
        raise NotImplementedError()

    @property
    def image_count(self) -> int:
        raise NotImplementedError()

    async def contains_image(self, image: ImageType) -> bool:
        assert isinstance(image, Image)
        return any(
            i for i in await self.get_images() if i.relative_path == image.relative_path
        )

    def compare(self, other: AlbumType) -> int:
        """
        Same functionality as old __cmp__ or C's strcmp
        """
        assert isinstance(other, Album)

        i = self.shallow_compare(other)
        if i == 0:
            i = self.deep_compare(other, shallow_compare_first=False)

        return i

    def shallow_compare(self, other: AlbumType) -> int:
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

    async def deep_compare(
        self, other: AlbumType, shallow_compare_first: bool = True
    ) -> int:
        if shallow_compare_first:
            i = self.shallow_compare(other)
            if i != 0:
                return i

        # Compare images - one by one
        self_images = sorted(await self.get_images(), key=lambda k: k.relative_path)
        other_images = sorted(await other.get_images(), key=lambda k: k.relative_path)

        if len(self_images) != len(other_images):
            return len(self_images) - len(other_images)

        for si, oi in zip(self_images, other_images):
            i = si.compare(oi)
            if i != 0:
                return i

        # TODO: More compares?
        return 0

    async def delete(self, dry_run: bool):
        raise NotImplementedError()
