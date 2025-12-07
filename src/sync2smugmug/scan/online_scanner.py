import logging

from sync2smugmug import models
from sync2smugmug.online import online
from sync2smugmug.utils import general_tools

logger = logging.getLogger(__name__)


@general_tools.timeit
async def scan(connection: online.OnlineConnection) -> models.RootFolder:
    """
    Discover hierarchy of folders and albums on Smugmug
    :return: A root source_folder object populated with the entire smugmug hierarchy
    """

    logger.info(f"Scanning SmugMug (starting from {connection.root_folder_uri})...")

    # Create the root node, and fetch initial online info
    root = models.RootFolder()
    root.online_info = await connection.get_folder(folder_relative_uri=connection.root_folder_uri)

    # Start the recursive scan from the root
    await _scan_recursive(
        root_folder=root,
        folder=root,
        connection=connection,
    )

    return root


async def _scan_recursive(
    root_folder: models.RootFolder,
    folder: models.Folder,
    connection: online.OnlineConnection,
):
    """
    Recursively scan folders called to dig into Smugmug
    """

    # Pick up the source_folder's albums (these are leaves in the tree - and do not have children)
    async for album_record in connection.iter_albums(folder.online_info):
        album_name = album_record.name
        album_relative_path = folder.relative_path.joinpath(album_name)

        album = models.Album(
            relative_path=album_relative_path,
            online_info=album_record,
            image_count=album_record.image_count,
        )

        # Associate the source_album with our source_folder
        folder.albums[album_name] = album

        # Update target_parent counts
        root_folder.stats.album_count += 1
        root_folder.stats.image_count += album.image_count

    # Recursively scan source_folder's children (either sub-folders or albums)
    async for sub_folder_record in connection.iter_sub_folders(folder.online_info):
        sub_folder_name = sub_folder_record.name

        sub_folder = models.Folder(
            relative_path=folder.relative_path.joinpath(sub_folder_name), online_info=sub_folder_record
        )

        if connection.is_test_root_folder_uri(sub_folder.online_info.uri):
            # Skip over the test source_folder (this will be only scratch, visible only to me)
            continue

        folder.sub_folders[sub_folder_name] = sub_folder
        root_folder.stats.folder_count += 1

        # Recursively call on this source_folder to discover the subtree
        await _scan_recursive(
            root_folder=root_folder,
            folder=sub_folder,
            connection=connection,
        )

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"{folder} - scanned ({len(folder.albums)} albums)")

    return folder
