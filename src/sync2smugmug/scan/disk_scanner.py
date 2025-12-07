import logging
from collections.abc import Generator
from pathlib import Path, PurePath

from sync2smugmug import disk, models
from sync2smugmug.utils import general_tools, image_tools

logger = logging.getLogger(__name__)


@general_tools.timeit
async def scan(base_dir: Path) -> models.RootFolder:
    """
    Discover hierarchy of folders and albums on disk

    :return: The root source_folder for images on disk
    """
    logger.info(f"Scanning disk (starting from {base_dir})...")

    root = models.RootFolder(disk_info=disk.DiskFolderInfo(disk_path=base_dir))  # noqa

    # Keep a lookup table to be able to get the node (by path) for quick access during the os.walk
    # Parents will always be created before children, so we can assume that a lookup will be successful
    folders: dict[PurePath, models.Folder] = dict()
    folders[root.relative_path] = root

    for dir_path in iter_directories(base_dir):
        dir_relative_path = PurePath(dir_path.relative_to(base_dir))
        parent_relative_path = dir_relative_path.parent

        parent_folder = folders.get(parent_relative_path)

        if parent_folder is None:
            # If no target_parent source_folder, skip the entire subtree
            continue

        assert dir_relative_path is not None and parent_relative_path is not None and parent_folder

        # Figure out if this is an Album of a Folder
        if has_images(dir_path):  # A source_album has images
            album = models.Album(
                relative_path=dir_relative_path,
                disk_info=disk.DiskAlbumInfo(disk_path=dir_path),  # noqa
            )

            disk.load_album_images(album=album)

            parent_folder.albums[album.name] = album

            root.stats.album_count += 1
            root.stats.image_count += album.image_count

        elif has_sub_folders(dir_path):  # A source_folder has sub-folders
            folder = models.Folder(
                relative_path=dir_relative_path,
                disk_info=disk.DiskFolderInfo(disk_path=dir_path),  # noqa
            )
            parent_folder.sub_folders[folder.name] = folder

            root.stats.folder_count += 1
            folders[dir_relative_path] = folder

        else:
            # Skip empty dirs
            logger.info(f"Empty directory {dir_path}")

            continue

    return root


def _should_skip(entry: Path) -> bool:
    """
    Figures out which folders should be skipped (special folders that are not meant for upload)

    :param entry: The entry
    """

    if not entry.is_dir() or entry.stem.startswith("."):
        return True

    if "Picasa" in entry.parts:
        return True

    basename = entry.stem.lower()

    if any(a == basename for a in ("originals", "lightroom", "developed")):
        return True

    return False


def iter_directories(root_dir: Path) -> Generator[Path, None, None]:
    """
    Recursively yield Path objects for given directory (DFS).
    """
    for entry in root_dir.iterdir():
        if _should_skip(entry):
            continue

        # Yield entry first
        yield entry

        # Now yield children
        yield from iter_directories(entry)  # see below for Python 2.x


def has_images(dir_path: Path) -> bool:
    return any(image_tools.is_image(PurePath(e.name)) for e in dir_path.iterdir() if e.is_file())


def has_sub_folders(dir_path: Path) -> bool:
    return any(e.is_dir() for e in dir_path.iterdir())
