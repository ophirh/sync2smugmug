import collections
import collections.abc
import datetime
import pathlib

from sync2smugmug import models


def dir_is_empty_of_pictures(disk_path: pathlib.Path) -> bool:
    """Return True only if directory is completely empty"""
    has_only_metadata_files = all(
        not fp.is_dir() and fp.suffix in (".ini", ".json", ".info") for fp in disk_path.iterdir()
    )

    return has_only_metadata_files


def to_disk_path(base_dir: pathlib.Path, relative_path: pathlib.PurePath) -> pathlib.Path:
    return base_dir.joinpath(relative_path)


def index_albums_by_dates(root_folder: models.RootFolder) -> dict[datetime.date, list[models.Album]]:
    """
    Map albums by date, and sort any duplicates such that the first in the list will be the best candidate to put any
    photos in
    """
    albums_by_date: dict[datetime.date, list[models.Album]] = collections.defaultdict(list)

    for album in iter_albums(root_folder):
        if album.album_date is not None:
            albums_by_date[album.album_date].append(album)

    return albums_by_date


def get_folder(root_folder: models.RootFolder, relative_path: pathlib.PurePath) -> models.Folder | None:
    folder: models.Folder = root_folder

    for part in relative_path.parts:
        if part not in folder.sub_folders:
            return None

        folder = folder.sub_folders[part]

    return folder


def iter_folders(root_folder: models.RootFolder) -> collections.abc.Generator[models.Album]:
    yield from _iter_folders(root_folder)


def _iter_folders(folder: models.Folder) -> collections.abc.Generator[models.Album]:
    yield folder

    for sub_folder in folder.sub_folders.values():
        yield from _iter_folders(sub_folder)


def iter_albums(root_folder: models.RootFolder) -> collections.abc.Generator[models.Album]:
    yield from _iter_albums(root_folder)


def _iter_albums(folder: models.Folder) -> collections.abc.Generator[models.Album]:
    yield from (album for album in sorted(folder.albums.values(), key=lambda a: a.relative_path))

    for sub_folder in sorted(folder.sub_folders.values(), key=lambda sf: sf.relative_path):
        yield from _iter_albums(sub_folder)
