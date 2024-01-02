import logging
from datetime import datetime, date
from pathlib import Path, PurePath
from typing import Dict, List, Tuple

import osxphotos
from dateutil.tz import UTC

from sync2smugmug.optimizations.disk import DiskOptimization
from sync2smugmug import models, disk
from sync2smugmug.utils import node_tools
from sync2smugmug.utils import general_tools

logger = logging.getLogger(__name__)


class ImportIPhoneImages(DiskOptimization):
    """
    Import images from Max Photos and merge them into the appropriate albums (determined based on date taken)
    """

    def __init__(self, base_dir: Path, mac_photos_library_location: Path = None):
        super().__init__(base_dir)
        self.mac_photos_library_location = mac_photos_library_location

    @property
    def last_mac_photos_import_date(self) -> datetime:
        date_str = self.my_context.get("last_iphone_import_date")

        if date_str is None:
            # Return a date really far back so we basically rescan everything
            return datetime(year=1970, day=1, month=1, tzinfo=UTC)

        # Parse the date from the JSON
        return datetime.fromisoformat(date_str)

    @last_mac_photos_import_date.setter
    def last_mac_photos_import_date(self, new_date: datetime):
        ctx = self.my_context
        ctx["last_iphone_import_date"] = new_date.isoformat()
        self.save_context(ctx)

    @general_tools.timeit
    async def perform(self, on_disk: models.RootFolder, dry_run: bool) -> bool:
        requires_reload = False

        # Index all albums by date to make it easier to find which source_album each of the photos should belong to
        albums_by_date = node_tools.index_albums_by_dates(on_disk)

        last_import_date: datetime = self.last_mac_photos_import_date

        # Open the Mac Photos DB and start going over all pictures in it.
        photos_db = osxphotos.PhotosDB(dbfile=self.mac_photos_library_location)

        for photo in photos_db.photos(from_date=last_import_date):
            if photo.ismissing:
                continue

            parent_folder, was_created = find_or_create_parent_folder(on_disk=on_disk, photo=photo)
            if was_created:
                # Update the on_disk hierarchy to reflect the new source_folder
                parent_or_parent = node_tools.get_folder(
                    root_folder=on_disk,
                    relative_path=parent_folder.relative_path.parent
                )
                parent_or_parent.sub_folders[parent_folder.relative_path.name] = parent_folder

            album, was_created = \
                find_or_create_album(
                    photo=photo,
                    parent_folder=parent_folder,
                    albums_by_date=albums_by_date
                )

            if was_created:
                # Update the albums index with newly created source_album
                albums_by_date[album.album_date] = [album]

            # If the image already exists on the source_album, continue
            target_filename = Path(photo.original_filename)
            if photo.isphoto:
                # We are going to export to jpeg, so switch the name
                target_filename = target_filename.with_suffix(".jpeg")

            if str(target_filename).lower() != photo.original_filename.lower():
                self._cleanup_old_photo_export(dir_path=album.disk_info.disk_path, photo=photo, dry_run=dry_run)

            if album.disk_info.disk_path.joinpath(target_filename).exists():
                logger.warning(f"Image ({target_filename}) already exists in source_album {album.relative_path}")

                last_import_date = max(last_import_date, photo.date)
                continue

            # Export (and convert if needed) the image to the destination source_album!
            exporter = osxphotos.PhotoExporter(photo)
            options = osxphotos.ExportOptions(
                live_photo=False,
                edited=True if photo.hasadjustments else False,
                convert_to_jpeg=True,
                overwrite=True,
                dry_run=dry_run
            )
            export_result = exporter.export(
                dest=album.disk_info.disk_path,
                filename=target_filename,
                options=options
            )

            if export_result.exported:
                last_import_date = max(last_import_date, photo.date)

                logger.info(f"Imported iphone photo {photo.original_filename} as ({export_result.exported[0]})")
                requires_reload = True

        if not requires_reload:
            logger.info("No iPhone images imported")

        if not dry_run:
            self.last_mac_photos_import_date = last_import_date

        return requires_reload

    @staticmethod
    def _cleanup_old_photo_export(dir_path: Path, photo: osxphotos.PhotoInfo, dry_run: bool):
        """
        In case there are old remnants of an old import as the original file type, delete it. This will also
        attempt to find any possible copies / overwrites of that same file
        """
        # Double check we don't also have the original file there. If we do - delete it
        file_name = PurePath(photo.original_filename)
        for path_to_check in dir_path.glob(f"{file_name.stem}*{file_name.suffix}"):
            if path_to_check.exists():
                logger.info(f"Removing old export {path_to_check} (will export again later)")
                if not dry_run:
                    path_to_check.unlink()


def find_or_create_parent_folder(
        on_disk: models.RootFolder,
        photo: osxphotos.PhotoInfo
) -> Tuple[models.Folder, bool]:
    # The target_parent source_folder is simply the year taken
    parent_folder_relative_path = on_disk.relative_path.joinpath(str(photo.date.date().year))

    parent_folder = node_tools.get_folder(root_folder=on_disk, relative_path=parent_folder_relative_path)

    if parent_folder is None:
        disk_path = node_tools.to_disk_path(parent_folder_relative_path)

        parent_folder = models.Folder(
            relative_path=parent_folder_relative_path,
            disk_info=disk.DiskFolderInfo(disk_path=disk_path)  # noqa
        )

        disk_path.mkdir(parents=True, exist_ok=True)
        was_created = True

    else:
        was_created = False

    return parent_folder, was_created


def find_or_create_album(
        photo: osxphotos.PhotoInfo,
        parent_folder: models.Folder,
        albums_by_date: Dict[date, List[models.Album]]
) -> Tuple[models.Album, bool]:
    """
    Given a Mac Photos image, figure out where it should go and return / create the appropriate source_album that should
    contain this photo.

    If necessary, this will also create the supporting folders up the hierarchy
    """

    # Check if we have a source_album for this date already
    date_taken = photo.date.date()
    albums = albums_by_date.get(date_taken)

    if albums is not None:
        # We already have albums for this date. The 'largest' source_album is considered the best match
        return max(albums), False

    # Create a new source_album
    album_name = date_taken.strftime(models.Album.DATE_ALBUM_FORMAT)
    album_disk_path = parent_folder.disk_info.disk_path.joinpath(album_name)
    album_disk_path.mkdir(exist_ok=True)

    album = models.Album(
        relative_path=parent_folder.relative_path.joinpath(album_name),
        disk_info=disk.DiskAlbumInfo(disk_path=album_disk_path),    # noqa
    )

    return album, True
