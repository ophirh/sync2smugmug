import re
from abc import ABC
from dataclasses import dataclass, field
from datetime import date, datetime
from functools import total_ordering, cached_property
from pathlib import PurePath
from typing import Dict, ClassVar, Pattern, List

from sync2smugmug import protocols


@dataclass(frozen=True)
class ImageType:
    ext: str
    is_movie: bool = False
    requires_conversion: bool = False


# List all supported image types
supported_image_types = {
    t.ext: t for t in [
        ImageType(ext=".jpg"),
        ImageType(ext=".jpeg"),
        ImageType(ext=".heic", requires_conversion=True),
        ImageType(ext=".mp4", is_movie=True),
        ImageType(ext=".avi", is_movie=True, requires_conversion=True),
        ImageType(ext=".mv4", is_movie=True, requires_conversion=True),
        ImageType(ext=".mov", is_movie=True, requires_conversion=True),
        ImageType(ext=".mts", is_movie=True, requires_conversion=True),
    ]}


@dataclass
class Image:
    album_relative_path: PurePath
    filename: PurePath
    disk_info: protocols.DiskImageInfoShape = field(default=None, repr=False)
    online_info: protocols.OnlineImageInfoShape = field(default=None, repr=False)

    @cached_property
    def image_type(self) -> ImageType:
        ext = self.filename.suffix.lower()

        it = supported_image_types.get(ext)
        if it is None:
            raise ValueError(f"Unsupported extension '{ext}'")

        return it

    @property
    def relative_path(self) -> PurePath:
        return self.album_relative_path.joinpath(self.filename)

    @property
    def is_on_disk(self) -> bool:
        return self.disk_info is not None

    @property
    def is_on_smugmug(self) -> bool:
        return self.online_info is not None

    def __eq__(self, other):
        return self.relative_path == other.relative_path


@dataclass
class Node(ABC):
    relative_path: PurePath
    disk_info: protocols.DiskInfoShape = field(default=None, repr=False)
    online_info: protocols.OnlineInfoShape = field(default=None, repr=False)

    @property
    def is_album(self) -> bool:
        """ Returns True if this node is a source_album (with images) """
        raise NotImplementedError()

    @property
    def name(self) -> str:
        return self.relative_path.name

    @property
    def is_on_disk(self) -> bool:
        return self.disk_info is not None

    @property
    def is_online(self) -> bool:
        return self.online_info is not None

    @property
    def source(self) -> str:
        if self.is_on_disk:
            return "D"
        elif self.is_online:
            return "O"
        else:
            return "?"

    def __eq__(self, other):
        return self.relative_path == other.relative_path


@total_ordering
@dataclass
class Album(Node):
    DATE_ALBUM_PATTERN: ClassVar[Pattern[str]] = re.compile(r"([12][90]\d\d_[0-1]\d_[0-3]\d)( - .*)?")
    DATE_ALBUM_FORMAT: ClassVar[str] = "%Y_%m_%d"

    disk_info: protocols.DiskAlbumInfoShape = field(default=None, repr=False)
    online_info: protocols.OnlineAlbumInfoShape = field(default=None, repr=False)
    images: List[Image] | None = field(default=None, repr=False)
    image_count: int = 0

    @property
    def is_album(self) -> bool:
        return True

    @cached_property
    def album_date(self) -> date | None:
        """
        Most of our albums are named with a date as part of the directory name. Extract that name if applicable
        """
        match = re.match(self.DATE_ALBUM_PATTERN, self.name)
        if match is None:
            return None

        date_str = match.group(1)
        return datetime.strptime(date_str, self.DATE_ALBUM_FORMAT).date()

    @property
    def name_contains_date_only(self) -> bool | None:
        """
        Checks if the source_album's name is a date only or does it have a longer name (more information).
        For non date-albums - will return None.
        """
        match = re.match(self.DATE_ALBUM_PATTERN, self.name)
        return len(match.groups()) == 1 if match is not None else None

    @property
    def requires_image_load(self) -> bool:
        # Indicates that we didn't load all images to memory yet (this is especially needed for online images
        # where we will perform a lazy load)
        return self.images is None or self.image_count > len(self.images)

    def reset_images(self):
        """
        Reset image load so next time `requires_image_load` will return True
        """
        self.images = None

    def __lt__(self, other):
        assert isinstance(other, Album)

        # For source_album dates, use the date, then the length of the path as primary sort criteria
        if self.album_date and other.album_date:
            if self.album_date < other.album_date:
                return True

            # Secondly, compare length (longer path means more info for date albums)
            l1 = len(self.relative_path.name)
            l2 = len(other.relative_path.name)
            if l1 != l2:
                return l1 < l2

        # Fall back - compare the strings
        return self.relative_path < other.relative_path


@dataclass
class Folder(Node):
    disk_info: protocols.DiskFolderInfoShape = field(default=None, repr=False)
    online_info: protocols.OnlineFolderInfoShape = field(default=None, repr=False)

    sub_folders: Dict[str, 'Folder'] = field(default_factory=dict, repr=False)
    albums: Dict[str, Album] = field(default_factory=dict, repr=False)

    @property
    def is_album(self) -> bool:
        return False


@dataclass
class Stats:
    # Add some statistics to the root source_folder
    folder_count: int = 0
    album_count: int = 0
    image_count: int = 0


@dataclass
class RootFolder(Folder):
    relative_path: PurePath = PurePath()
    stats: Stats = field(default_factory=Stats)
