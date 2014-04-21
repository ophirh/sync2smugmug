import logging
import os
from scanner.album import Album
from scanner.image import Image
from .smugmug import MySmugMug
from .objects import Collection
from .policy import *

logger = logging.getLogger(__name__)


class Scanner(object):
    albums = {}
    """ :type : dict[str, Album] """
    collections = {}
    """ :type : dict[str, Collection] """
    smugmug = MySmugMug()

    def __init__(self, base_dir, nickname, reset_cache=False):
        self.base_dir = base_dir
        self.nickname = nickname

        if not self.base_dir.endswith(os.path.sep):
            self.base_dir += os.path.sep

        self._scan_disk()
        self._scan_smugmug(reset_cache)

    def sync(self, policy=POLICY_SYNC):
        logger.info('Synchronizing categories and subcategories...')
        for key in sorted(self.collections.keys()):
            collection = self.collections[key]
            if collection.needs_sync():
                collection.sync(policy)

        logger.info('Synchronizing albums...')
        for key in sorted(self.albums.keys()):
            album = self.albums[key]
            if album.needs_sync():
                album.sync(policy)

    def print_status(self):
        self._print_summary('Collections', self.collections)
        self._print_summary('Folders', self.albums)

    def _scan_disk(self):
        logger.info('Scanning disk (starting from %s)...' % self.base_dir)
        for dir_path, dirs, files in os.walk(self.base_dir):
            # Apply some base filtering...
            if self._should_skip_directory(dir_path) or (len(dirs) == 0 and len(files) == 0):
                continue

            images = [img for img in files if Image.is_image(img)]
            if len(images) > 0:
                album = Album.create_from_disk(self, dir_path, files, images)
                self.albums[album.id] = album
            elif len(dirs) > 0:
                collection = Collection.create_from_disk(self, dir_path)
                self.collections[collection.id] = collection

    def _should_skip_directory(self, p):
        # First, take off the base_dir, then take the base name
        local_path = p[len(self.base_dir):]
        basename = os.path.basename(local_path)
        return basename.startswith('.') or basename in ['Originals', 'Lightroom', '']

    def _scan_smugmug(self, reset_cache):
        logger.info('Scanning SmugMug for categories...')
        # First get the Hierarchy (categories and subcategories)
        categories = self.smugmug.categories_get(NickName=self.nickname)
        for category in categories['Categories']:
            category_id = category['Name']
            if category_id not in self.collections:
                # Doesn't exist on disk - we will download it from Smugmug later
                self.collections[category_id] = Collection.create_from_smugmug(self, category_id, category)
            else:
                self.collections[category_id].update_from_smugmug(category)

            # Continue traversing the hierarchy (currently support only one level...)
            subcategories = self.smugmug.subcategories_getAll(NickName=self.nickname)
            for subcategory in subcategories["SubCategories"]:
                subcategory_id = os.path.join(category_id, subcategory['Name'])
                if subcategory_id not in self.collections:
                    # Doesn't exist on disk - we will download it from Smugmug later
                    self.collections[subcategory_id] = \
                        Collection.create_from_smugmug(self, subcategory_id, subcategory)
                else:
                    self.collections[subcategory_id].update_from_smugmug(subcategory)

        logger.info('Scanning SmugMug for albums...')
        albums_to_cleanup = dict(self.albums)
        albums = self.smugmug.albums_get(NickName=self.nickname, Heavy=True)
        for album in albums['Albums']:
            # Lookup the collection to make up the album id
            album_id = Album.make_album_id(album)

            # Now lookup the folder and update the smugmug id
            if album_id not in self.albums:
                self.albums[album_id] = Album.create_from_smugmug(self, album_id, album)
            else:
                a = self.albums[album_id]
                a.update_from_smugmug(album)

            if album_id in albums_to_cleanup:
                del albums_to_cleanup[album_id]

            if reset_cache:
                self.albums[album_id].delete_sync_data()

        # In albums_to_cleanup there are all the albums that might have an inaccurate sync_data
        for album in albums_to_cleanup.values():
            album.delete_sync_data()
            album.smugmug_id = None

    @staticmethod
    def _print_summary(title, coll):
        """ Given a collection of SyncObjects, print the summary of what needs to be done """
        logger.info(title)
        logger.info('===========')

        total = len(coll)
        on_both = 0
        on_smugmug = 0
        on_disk = 0
        without_parent = 0

        for i in coll.values():
            on_both += 1 if i.on_disk() and i.on_smugmug() else 0
            on_smugmug += 1 if i.on_smugmug() else 0
            on_disk += 1 if i.on_disk() else 0
            without_parent += 1 if i.get_parent() is None else 0

        logger.info('  Total          : %d' % total)
        logger.info('  On disk        : %d' % on_disk)
        logger.info('  Online         : %d' % on_smugmug)
        logger.info('  On both        : %d' % on_both)
        logger.info('  Without parent : %d' % without_parent)
        logger.info('')
