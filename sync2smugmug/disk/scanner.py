import logging
import os
from typing import Dict, Union, Optional

from .node import FolderOnDisk, AlbumOnDisk
from ..node import Node
from ..utils import timeit, scan_tree

logger = logging.getLogger(__name__)


class DiskScanner:
    def __init__(self, base_dir: str):
        if not base_dir.endswith(os.sep):
            base_dir = os.path.join(base_dir, '')
        self.base_dir = base_dir

    @timeit
    def scan(self) -> FolderOnDisk:
        """
        Discover hierarchy of folders and albums on disk

        :return: The root folder for images on disk
        """
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f'Scanning disk (starting from {self.base_dir})...')

        # Keep a lookup table to be able to get the node (by path) for quick access during the os.walk
        nodes: Dict[str, Union[FolderOnDisk, AlbumOnDisk]] = {}

        root = FolderOnDisk(parent=None, relative_path=Node.ROOT)
        nodes[Node.ROOT] = root

        for entry in scan_tree(self.base_dir):
            entry: os.DirEntry

            relative_path = entry.path[len(self.base_dir):]

            if self.should_skip(entry, relative_path=relative_path):
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

            assert relative_path is not None and parent_path is not None and parent_node

            if parent_node.is_album:
                # Ignore sub-folders of albums
                continue

            # Figure out if this is an Album of a Folder
            if AlbumOnDisk.has_images(entry.path):  # An album has images
                node = AlbumOnDisk(parent=parent_node, relative_path=relative_path)

                parent_node.albums[node.name] = node

                # Update counts
                root.album_count += 1
                root.image_count += node.image_count

            elif FolderOnDisk.has_sub_folders(entry.path):  # A folder has sub-folders
                node = FolderOnDisk(parent=parent_node, relative_path=relative_path)

                parent_node.sub_folders[node.name] = node

                # Update counts
                root.folder_count += 1 + node.folder_count
                root.album_count += node.album_count
                root.image_count += node.image_count

            else:
                # Skip empty dirs
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f'Empty directory {entry.path}')

                continue

            nodes[relative_path] = node

        return root

    @staticmethod
    def should_skip(entry: os.DirEntry, relative_path: str) -> bool:
        """
        Figures out which folders should be skipped (special folders that are not meant for upload)

        :param entry: The entry
        :param relative_path: Relative path to folder
        """

        if not entry.is_dir():
            return True

        if entry.name.startswith('.'):
            return True

        basename = entry.name.lower()

        if any(a == basename for a in ('originals', 'lightroom', 'developed')):
            return True

        if 'Picasa' in relative_path:
            return True

        return False
