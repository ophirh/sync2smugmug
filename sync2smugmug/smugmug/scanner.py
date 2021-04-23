import logging
import os
from typing import Optional, Union

from .connection import SmugMugConnection
from .node import FolderOnSmugmug, AlbumOnSmugmug
from ..utils import timeit

logger = logging.getLogger(__name__)


class SmugmugScanner:
    def __init__(self,
                 account: str,
                 consumer_key: str,
                 consumer_secret: str,
                 access_token: str,
                 access_token_secret: str,
                 use_test_folder: bool):

        self._connection = SmugMugConnection(account=account,
                                             consumer_key=consumer_key,
                                             consumer_secret=consumer_secret,
                                             access_token=access_token,
                                             access_token_secret=access_token_secret,
                                             use_test_folder=use_test_folder)

    @timeit
    def scan(self) -> FolderOnSmugmug:
        """
        Discover hierarchy of folders and albums on Smugmug

        :return: The root folder for images on smugmug
        """
        logger.info(f'Scanning SmugMug (starting from {self._connection.root_folder_uri})...')

        return self._scan(node_uri=None, path=os.sep, parent=None)

    def _scan(self,
              node_uri: Optional[str],
              path: str,
              parent: Union[FolderOnSmugmug, AlbumOnSmugmug, None]) -> FolderOnSmugmug:
        """
        Recursively on folders called to dig into Smugmug
        """

        folder_record, sub_folders, albums = self._connection.folder_get(folder_uri=node_uri)

        folder = FolderOnSmugmug(parent=parent,
                                 relative_path=path,
                                 record=folder_record,
                                 smugmug_connection=self._connection)

        if sub_folders:
            # Recursively scan folder's children
            for sub_folder_record in sub_folders:
                sub_folder_name = sub_folder_record['Name']
                sub_folder_uri = sub_folder_record['Uri']

                if sub_folder_uri == f'{self._connection.root_folder_uri}/Test':
                    # Skip over the test folder (this will be only scratch, visible only to me)
                    continue

                # Recursively call on this folder to discover the sub-tree
                sub_folder = self._scan(node_uri=sub_folder_uri,
                                        path=os.path.join(path, sub_folder_name),
                                        parent=folder)

                # Associate the sub_folder with its parent
                folder.sub_folders[sub_folder_name] = sub_folder
                logger.debug(f'{sub_folder.relative_path} - scanned')

                # Update parent counts
                folder.folder_count += sub_folder.folder_count + 1
                folder.album_count += sub_folder.album_count
                folder.image_count += sub_folder.image_count

        if albums:
            # Recursively scan folder's children
            for album_record in albums:
                album_name = album_record['Name']

                album = AlbumOnSmugmug(parent=folder,
                                       relative_path=os.path.join(path, album_name),
                                       record=album_record,
                                       connection=self._connection)

                # Associate the album with its parent
                folder.albums[album_name] = album
                logger.debug(f'{album.relative_path} - scanned')

                # Update parent counts
                folder.album_count += 1
                folder.image_count += album.image_count

        return folder
