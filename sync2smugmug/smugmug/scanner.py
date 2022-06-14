import asyncio
import logging
import os
from typing import Optional, Union, Tuple, Dict, List

from .connection import SmugMugConnection
from .node import FolderOnSmugmug, AlbumOnSmugmug
from ..node import Node

logger = logging.getLogger(__name__)


class SmugmugScanner:
    def __init__(self, connection: SmugMugConnection):
        self._connection = connection

    async def scan(self) -> FolderOnSmugmug:
        """
        Discover hierarchy of folders and albums on Smugmug

        :return: The root folder for images on smugmug
        """

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Scanning SmugMug (starting from {self._connection.root_folder_uri})...')

        return await self._scan(node_uri=None, path=Node.ROOT, parent=None, connection=self._connection)

    async def _scan(self,
                    node_uri: Optional[str],
                    path: str,
                    parent: Union[FolderOnSmugmug, AlbumOnSmugmug, None],
                    connection: SmugMugConnection) -> FolderOnSmugmug:
        """
        Recursively on folders called to dig into Smugmug
        """

        folder_record, sub_folder_records, album_records = \
            await self._folder_get(folder_uri=node_uri, with_children=True)

        folder = FolderOnSmugmug(parent=parent,
                                 relative_path=path,
                                 record=folder_record,
                                 connection=connection)

        if sub_folder_records:
            tasks = []

            # Recursively scan folder's children (either sub-folders or albums)
            for sub_folder_record in sub_folder_records:
                sub_folder_name: str = sub_folder_record['Name']
                sub_folder_uri: str = sub_folder_record['Uri']

                if sub_folder_uri == f'{connection.root_folder_uri}/Test':
                    # Skip over the test folder (this will be only scratch, visible only to me)
                    continue

                # Recursively call on this folder to discover the sub-tree
                tasks.append(asyncio.create_task(self._scan(node_uri=sub_folder_uri,
                                                            path=os.path.join(path, sub_folder_name),
                                                            parent=folder,
                                                            connection=connection)))

            sub_folders = await asyncio.gather(*tasks)

            for sub_folder in sub_folders:
                # Associate the sub_folder with its parent
                folder.sub_folders[sub_folder.name] = sub_folder

                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f'{sub_folder.relative_path} - scanned')

                # Update parent counts
                folder.folder_count += sub_folder.folder_count + 1
                folder.album_count += sub_folder.album_count
                folder.image_count += sub_folder.image_count

        if album_records:
            # Pick up the folder's albums (these are leaves in the tree - and do not have children)
            for album_record in album_records:
                album_name: str = album_record['Name']

                album = AlbumOnSmugmug(parent=folder,
                                       relative_path=os.path.join(path, album_name),
                                       record=album_record,
                                       connection=connection)

                # Associate the album with its parent
                folder.albums[album_name] = album

                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f'{album.relative_path} - scanned')

                # Update parent counts
                folder.album_count += 1
                folder.image_count += album.image_count

        return folder

    async def _folder_get(self,
                          folder_uri: str = None,
                          with_children: bool = True) -> Tuple[Dict, List[Dict], List[Dict]]:
        folder_uri = folder_uri or self._connection.root_folder_uri

        r = await self._connection.request_get(folder_uri)
        folder = r['Folder']

        # Node get the children
        sub_folders, albums = None, None

        if with_children:
            if 'Folders' in folder['Uris']:
                sub_folders = await self._connection.unpack_pagination(folder['Uris']['Folders']['Uri'],
                                                                       object_name='Folder')

            albums = await self._connection.unpack_pagination(folder['Uris']['FolderAlbums']['Uri'],
                                                              object_name='Album')

        return folder, sub_folders, albums
