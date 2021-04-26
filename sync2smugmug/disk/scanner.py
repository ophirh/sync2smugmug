import logging
import os
from typing import Dict, Union, Optional

from .node import FolderOnDisk, AlbumOnDisk
from ..node import Node
from ..utils import timeit

logger = logging.getLogger(__name__)


class DiskScanner:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir

    @timeit
    def scan(self) -> FolderOnDisk:
        """
        Discover hierarchy of folders and albums on disk

        :return: The root folder for images on disk
        """
        logger.info(f'Scanning disk (starting from {self.base_dir})...')

        # Keep a lookup table to be able to get the node (by path) for quick access during the os.walk
        nodes: Dict[str, Union[FolderOnDisk, AlbumOnDisk]] = {}

        root = FolderOnDisk(parent=None, relative_path='')
        nodes[os.sep] = root

        for full_dir_path, dirs, files in os.walk(self.base_dir):
            relative_path = full_dir_path[len(self.base_dir):]

            if self.should_skip(full_path=full_dir_path, relative_path=full_dir_path):
                continue

            if Node.get_is_root(relative_path):
                # Root was already added at the top
                continue

            # Get parent node (if available)
            parent_path, node_name = os.path.split(relative_path)
            parent_node: Optional[FolderOnDisk] = nodes.get(parent_path)

            if parent_node is None:
                # If no parent, skip the entire sub-tree
                continue

            assert relative_path and parent_path and parent_node

            if parent_node.is_album:
                # Ignore sub-folders of albums
                continue

            # Figure out if this is an Album of a Folder
            if AlbumOnDisk.has_images(full_dir_path):
                node = AlbumOnDisk(parent=parent_node, relative_path=relative_path)

                parent_node.albums[node_name] = node

                # Update counts
                root.album_count += 1
                root.image_count += node.image_count

            else:
                node = FolderOnDisk(parent=parent_node, relative_path=relative_path)

                parent_node.sub_folders[node_name] = node

                # Update counts
                root.folder_count += 1 + node.folder_count
                root.album_count += node.album_count
                root.image_count += node.image_count

            nodes[relative_path] = node

        return root

    @staticmethod
    def should_skip(full_path: str, relative_path: str) -> bool:
        """
        Figures out which folders should be skipped (special folders that are not meant for upload)

        :param str full_path: Full path to folder
        :param str relative_path: Relative path to folder
        """

        basename = os.path.basename(full_path)

        if basename.startswith('.'):
            return True

        basename = basename.lower()

        if any(a in basename for a in ('originals', 'lightroom', 'developed')):
            return True

        if 'Picasa' in relative_path:
            return True

        return False
