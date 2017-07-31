import os
import logging
from multiprocessing.pool import ThreadPool

import scan
from .policy import *


logger = logging.getLogger(__name__)


def sync_image(image, policy):
    """
    :type image: scanner.image.Image
    :type policy: int
    """
    # noinspection PyBroadException
    try:
        image.sync(policy)
        return True
    except:
        # Report and continue
        logging.getLogger(__name__).exception('Failed to sync image %s' % image)
        raise


def sync_images(images, policy):
    """
    Perform an image sync in parallel (multi-threaded) since download and upload images might take time to complete
    :type images: list[scanner.image.Image]
    :type policy: int
    """
    pool = ThreadPool(processes=10)

    results = []
    for i in images:
        r = pool.apply_async(sync_image, (i, policy))
        results.append(r)

    pool.close()
    pool.join()

    for r in results:
        # This will raise an exception if any was caught during processing
        r.get()


class SyncObject(object):
    def __init__(self, scanner, obj_id, smugmug_id=None, disk_path=None):
        """ :type scanner: scan.Scanner """
        self.scanner = scanner
        self.id = obj_id
        self.smugmug_id = smugmug_id
        self.disk_path = disk_path
        self.parent = None
        self.online_last_updated = None

    @staticmethod
    def path_to_id(prefix, path):
        """ Strip the prefix path from the file name and use it as the id """
        return path[len(prefix):]

    @staticmethod
    def id_to_path(prefix, obj_id):
        """ Given an ID, convert to disk path """
        return prefix + obj_id

    def extract_name(self):
        """ Get the object name from its ID """
        return os.path.basename(self.id)

    def on_smugmug(self):
        return self.smugmug_id is not None

    def on_disk(self):
        return self.disk_path is not None

    @property
    def smugmug(self):
        return self.scanner.smugmug

    def get_parent(self):
        if self.parent is None:
            # Try to set the parent of this folder
            parent_id, _ = os.path.split(self.id)
            if parent_id in self.scanner.collections:
                self.parent = self.scanner.collections[parent_id]
            elif parent_id in self.scanner.albums:
                self.parent = self.scanner.albums[parent_id]

        return self.parent

    def get_parent_smugmug_id(self):
        return self.get_parent().smugmug_id if self.get_parent() is not None else None

    def needs_sync(self):
        # At the minimum, this object should be on both disk and online
        return not self.on_disk() or not self.on_smugmug()

    def sync(self, policy):
        # Abstract method
        pass

    def __str__(self):
        return '%s:%s (%s)' % (self.id, self.smugmug_id, self.disk_path)


class Collection(SyncObject):
    @staticmethod
    def create_from_disk(scanner, disk_path):
        """
        :type scanner: scan.Scanner
        :type disk_path: str
        """
        collection = Collection(scanner, SyncObject.path_to_id(scanner.base_dir, disk_path))
        collection.disk_path = disk_path

        return collection

    @staticmethod
    def create_from_smugmug(scanner, folder_id, category):
        """ :type scanner: scan.Scanner """
        coll = Collection(scanner, folder_id)
        coll.update_from_smugmug(category)
        return coll

    def update_from_smugmug(self, category):
        self.smugmug_id = category['NodeID']

    def is_smugmug_category(self):
        # It is a category (vs. subcategory) if it is a top level item
        return self.get_parent() is None

    def needs_sync(self):
        return super(Collection, self).needs_sync()

    def sync(self, policy):
        if policy == POLICY_SYNC:
            # Simply make the directory
            self.disk_path = SyncObject.id_to_path(self.scanner.base_dir, self.id)
            if not os.path.exists(self.disk_path):
                os.mkdir(self.disk_path)

        if not self.on_smugmug():
            # Upload: Make the folder in Smugmug (including keywords, etc...)
            if self.is_smugmug_category():
                logger.debug('--- Creating category for %s' % self)
                r = self.smugmug.categories_create(Name=self.extract_name())
                self.smugmug_id = r['Category']['id']
            else:
                logger.debug('--- Creating subcategory for %s' % self)
                r = self.smugmug.subcategories_create(Name=self.extract_name(),
                                                            CategoryID=self.get_parent_smugmug_id())
                self.smugmug_id = r['SubCategory']['id']
        else:
            # Download: Super implementation will create the directory
            pass