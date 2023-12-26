import logging
from collections import defaultdict
from datetime import datetime
from typing import List, Dict, Tuple

from sync2smugmug.optimizations.disk import DiskOptimization
from sync2smugmug import models, disk
from sync2smugmug.utils import node_tools, general_tools, image_tools

logger = logging.getLogger(__name__)

# Alias defining how we determine an image identity (name, size, time taken, camera model)
ImageIdentityKey = Tuple[str, float, datetime, str]


class DeleteImageDuplicates(DiskOptimization):
    """
    Find images with same name and device and remove them (the oldest image stays).
    Exception are non date galleries (which are designed to contain duplicates)

    This is an attempt to find duplicates across albums (hence indexed by name). The challenge is that photos coming
    from different cameras may have overlapping names (neither between devices nor even within a device). This is why
    additional mata data is used to determine if images are indeed identical.
    """

    @general_tools.timeit
    async def perform(self, on_disk: models.RootFolder, dry_run: bool) -> bool:
        requires_reload = False

        # To minimize the time (and memory) requires to get metadata - we will do this in two passes.
        duplicate_candidates_l1: Dict[str, List[Tuple[models.Album, models.Image]]] = defaultdict(list)

        # Pass #1 - Index all photos by name. Stack all photos with the same name together to be inspected in phase 2
        date_albums = (a for a in node_tools.iter_albums(on_disk) if a.album_date is not None)
        for date_album in date_albums:
            for image in date_album.images:
                # Index by name (case-insensitive)
                duplicate_candidates_l1[image.filename.name.lower()].append((date_album, image))

        # Pass #2 - remove lists that don't have any duplicates
        duplicate_candidates_l1 = {name: lst for name, lst in duplicate_candidates_l1.items() if len(lst) > 1}

        # Pass #3 - Go over each of the duplicate lists and use more information (e.g. make, model, size) to determine
        # if these are indeed duplicates
        duplicate_candidates_l2: Dict[ImageIdentityKey, List[Tuple[models.Album, models.Image]]] = defaultdict(list)

        for image_name, candidates in duplicate_candidates_l1.items():
            for date_album, image in candidates:
                key = (
                    image_name,
                    image.disk_info.size,
                    image_tools.extract_image_time_taken(image.disk_info.disk_path, image.image_type),
                    image_tools.extract_image_camera_model(image.disk_info.disk_path, image.image_type),
                )

                duplicate_candidates_l2[key].append((date_album, image))

        # Pass #4 - remove lists that don't have any duplicates
        duplicate_candidates_l2 = {name: lst for name, lst in duplicate_candidates_l2.items() if len(lst) > 1}

        # Pass #5 - now take action on the duplicates
        for key, duplicates in duplicate_candidates_l2.items():
            # Sort images by date taken, then source_album date.
            # We will keep the oldest version we've got and delete everything else
            sorted_duplicates = sorted(duplicates, key=lambda t: (key[2], t[0].album_date))

            # Remove the first candidate (this is the one we will keep - as it is the oldest)
            # TODO - be smarter about picking the one to keep. Use the date of the image to match the album
            _, image_to_keep = sorted_duplicates[0]
            sorted_duplicates = sorted_duplicates[1:]

            for date_album, image in sorted_duplicates:
                logger.info(f"Deleting image {image.relative_path}. It's a duplicate of {image_to_keep.relative_path}")

                # Actually delete (the newer image)
                disk.delete_image_from_disk(image, dry_run=dry_run)
                requires_reload = True

        if not requires_reload:
            logger.info("No image duplicates detected")

        return requires_reload


class DeleteAlbumDuplicates(DiskOptimization):
    """
    In places where we have multiple albums for the same date, move pictures into the source_album that has more
    information (has a longer name and source source_album name is "date only"). This will empty out the other
    source_album to be later deleted by the DeleteEmptyDirectories processor
    """

    @general_tools.timeit
    async def perform(self, on_disk: models.RootFolder, dry_run: bool) -> bool:
        requires_reload = False

        albums_by_date = node_tools.index_albums_by_dates(root_folder=on_disk)
        candidates_checked = 0

        for album_date, albums in albums_by_date.items():
            if len(albums) > 1:
                candidates_checked += 1
                albums_sorted = sorted(albums)

                # First pick the best 'target' source_album (last item in the list - which is the largest)
                target_album = albums_sorted[-1]
                assert not target_album.name_contains_date_only, \
                    f"Expected a longer source_album name for date {album_date}"

                # Now for each of the 'shorter' names, if these are 'date only' albums, move their images to the
                # target source_album
                for album in albums_sorted[:-1]:
                    if not album.name_contains_date_only:
                        # We don't want to overwrite albums that were given additional information in their name
                        continue

                    requires_reload |= self._move_photos(from_album=album, to_album=target_album, dry_run=dry_run)

        if not requires_reload:
            logger.info("No album duplicates detected")

        return requires_reload

    @staticmethod
    def _move_photos(from_album: models.Album, to_album: models.Album, dry_run: bool):
        for image in from_album.images:
            if image.filename not in to_album.images:
                logger.info(f"Moving image {image} to source_album {to_album}...")

                if dry_run:
                    to_path = to_album.disk_info.disk_path

                    # Move the regular photo
                    image.disk_info.image_disk_path.rename(to_path.joinpath(image.relative_path.name))

                    # Move the developed version
                    if image.disk_info.developed_disk_path is not None:
                        developed_path = to_album.disk_info.disk_path.joinpath('Developed', image.relative_path.name)
                        image.disk_info.developed_disk_path.rename(developed_path)
