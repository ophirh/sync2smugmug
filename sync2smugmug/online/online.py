import asyncio
import logging
import pathlib
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Generator, List, Iterable

from sync2smugmug import protocols, models, configuration
from sync2smugmug.online import smugmug

logger = logging.getLogger(__name__)


class OnlineConnection:
    """
    Provides an abstraction over the Smugmug API connection details
    """

    def __init__(self, core_connection: smugmug.SmugmugCoreConnection):
        self._conn = core_connection

    @property
    def root_folder_uri(self) -> str:
        return self._conn.root_folder_uri

    def is_test_root_folder_uri(self, uri: str) -> bool:
        return self._conn.is_test_root_folder_uri(uri)

    async def get_folder(self, folder_relative_uri: str) -> protocols.OnlineFolderInfoShape:
        r = await self._conn.request_get(folder_relative_uri)
        # noinspection PyTypeChecker
        return smugmug.SmugmugFolder(r["Folder"])

    async def iter_album_images(
            self,
            album: protocols.OnlineAlbumInfoShape
    ) -> Generator[protocols.OnlineImageInfoShape, None, None]:
        async for record in self._conn.paginate(relative_uri=album.images_uri, object_name="AlbumImage"):
            yield smugmug.SmugmugImage(record)

    async def iter_sub_folders(
            self,
            folder: protocols.OnlineFolderInfoShape
    ) -> Generator[protocols.OnlineFolderInfoShape, None, None]:
        if folder.sub_folders_uri is None:
            return

        async for record in self._conn.paginate(relative_uri=folder.sub_folders_uri, object_name="Folder"):
            yield smugmug.SmugmugFolder(record)

    async def iter_albums(
            self,
            folder: protocols.OnlineFolderInfoShape
    ) -> Generator[protocols.OnlineAlbumInfoShape, None, None]:
        if folder.albums_uri is None:
            return

        async for record in self._conn.paginate(relative_uri=folder.albums_uri, object_name="Album"):
            yield smugmug.SmugmugAlbum(record)

    async def create_folder(
            self,
            parent: models.Folder,
            folder_name: str,
            dry_run: bool
    ) -> protocols.OnlineFolderInfoShape | None:
        """
        Create a new sub-source_folder under the target_parent source_folder
        """

        if dry_run:
            return None

        r = await self._conn.request_post(
            parent.online_info.sub_folders_uri,
            json_data={
                "Name": folder_name,
                "UrlName": self._conn.encode_uri_name(folder_name),
                "Privacy": "Unlisted",
            },
        )

        # noinspection PyTypeChecker
        return smugmug.SmugmugFolder(r["Folder"])

    async def create_album_online_info(
            self,
            parent: models.Folder,
            album_name: str,
            dry_run: bool
    ) -> protocols.OnlineAlbumInfoShape | None:
        """
        Create a new sub-source_folder under the target_parent source_folder
        """

        if dry_run:
            return None

        # Hack! For some unknown reason, the Smugmug API returns 400 errors when trying to POST to the
        # 'FolderAlbums' Uri The workaround is to use the 'Node' endpoint and then lookup the album record
        parent_node_uri = parent.online_info.record["Uris"]["Node"]["Uri"]

        r = await self._conn.request_post(
            f"{parent_node_uri}!children",
            json_data={
                "Name": album_name,
                # 'UrlName': self._conn.encode_uri_name(album_name),
                # 'Privacy': 'Unlisted',
                "Type": "Album",
            },
        )

        # Wait for eventual consistency to settle before we look up the record again (the response returned a Node
        # object - but we want the Folder object)
        await asyncio.sleep(0.5)

        # Lookup the album record
        album_url = r["Node"]["Uris"]["Album"]["Uri"]
        r = await self._conn.request_get(album_url)

        # r = await self._conn.request_post(
        #     parent.online_info.albums_uri,
        #     json_data={
        #         "Name": album_name,
        #         "UrlName": self._conn.encode_uri_name(album_name),
        #         "Privacy": "Unlisted",
        #     },
        # )

        # noinspection PyTypeChecker
        return smugmug.SmugmugAlbum(r["Album"])

    async def download_images(self, images: Iterable[protocols.OnlineImageInfoShape], to_folder: Path, dry_run: bool):
        """
        Download missing images from the album on smugmug to the disk

        :param images: Images to download
        :param to_folder: Folder on disk to place images in
        :param dry_run: If True, will not actually download anything, just log
        """

        if dry_run:
            return

        # Download one by one! Concurrency will be handled on the event level
        for image in images:
            # noinspection PyTypeChecker
            await self._conn.request_download(
                image_uri=await self._get_image_download_url(image),
                local_path=to_folder.joinpath(image.name),
            )

    async def _get_image_download_url(self, image: smugmug.SmugmugImage) -> str:
        if image.is_video:
            assert "LargestVideo" in image.record["Uris"], "Expecting 'LargestVideo' to exist in 'Uris'"

            # Need to fetch the largest video url - videos are NOT accessible via 'ArchivedUri'
            # TODO: Problem! The downloaded name has the original (.avi / .mov) extension but the stored format is mp4.
            r = await self._conn.request_get(image.record["Uris"]["LargestVideo"]["Uri"])
            return r["LargestVideo"]["Url"]

        else:
            # The archived version holds the original copy of the photo (but not for videos)
            return image.record["ArchivedUri"]

    async def upload_images(self, image_paths: List[Path], to_album_uri: str, dry_run: bool):
        """
        Download missing images from the album on smugmug to the disk

        :param image_paths: Images to download
        :param to_album_uri: Online folder to place images in
        :param dry_run: If True, will not actually download anything, just log
        """

        if dry_run:
            return

        # Upload one by one! Concurrency will be handled on the event level
        for image_path in image_paths:
            await self._conn.request_upload(
                image_path=image_path,
                album_uri=to_album_uri,
                image_name=image_path.name,
                dry_run=dry_run
            )

    async def delete(self, uri: str, dry_run: bool) -> bool:
        if dry_run:
            return False

        await self._conn.request_delete(uri)
        return True


@asynccontextmanager
async def open_smugmug_connection(
        connection_params: configuration.ConnectionParams
) -> Generator[OnlineConnection, None, None]:
    """ Context manager for creating a smugmug connection """

    core_connection = smugmug.SmugmugCoreConnection(connection_params)

    await core_connection.connect()

    # Yield a high-level wrapper of the connection to expose only the methods we really need, without the details
    # of the smugmug internals
    yield OnlineConnection(core_connection)

    await core_connection.disconnect()


async def load_album_images(album: models.Album, connection: OnlineConnection):
    """
    Loads images into the album
    """
    images: List[models.Image] = []

    async for image_record in connection.iter_album_images(album.online_info):
        # Check if "Processing". If so, skip this image for now
        if image_record.record["Processing"]:
            continue

        image = models.Image(
            album_relative_path=album.relative_path,
            filename=pathlib.PurePath(image_record.name),
            online_info=image_record,
        )

        images.append(image)

    # Replace the album's images in one shot
    album.images = images
