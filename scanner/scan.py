import logging
import os
from scanner.album import Album
from scanner.image import Image
from .smugmug import SmugMugConnection
from .objects import Collection
from .policy import POLICY_SYNC, AVAILABLE_POLICIES

logger = logging.getLogger(__name__)


class Scanner(object):
    def __init__(self, base_dir, nickname, reset_cache=False):
        self._base_dir = base_dir
        self._nickname = nickname
        self._smugmug = SmugMugConnection(nickname)

        self.albums = {}  # :type : dict[str, Album]
        self.collections = {}
        """ :type : dict[str, Collection] """

        if not self._base_dir.endswith(os.path.sep):
            self._base_dir += os.path.sep

        self.scan_smugmug(reset_cache)
        self.scan_disk()

    @property
    def nickname(self):
        return self._nickname

    @property
    def smugmug(self):
        return self._smugmug

    @property
    def base_dir(self):
        return self._base_dir

    def sync(self, policy=POLICY_SYNC):
        assert policy in AVAILABLE_POLICIES

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

    def scan_disk(self):
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
        if basename.startswith('.'):
            return True
        if basename in ('Originals', 'Lightroom', 'Developed', ''):
            return True
        if "Picasa" in local_path:
            return True
        return False


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


class BaseScanner(object):
    def __init__(self, base_dir, reset_cache=False):
        self._base_dir = base_dir
        self._albums = {}
        self._collections = {}
        self._reset_cache = reset_cache

    @property
    def base_dir(self):
        return self._base_dir

    @property
    def albums(self):
        """
        :rtype: dict[str, Album]
        """
        return self._albums

    @property
    def collections(self):
        """
        :rtype: dict[str, Collection]
        """
        return self._collections

    def scan(self):
        raise NotImplementedError()


class SmugmugScanner(BaseScanner):
    def __init__(self, base_dir, smugmug_connection, reset_cache=False):
        super(SmugmugScanner, self).__init__(base_dir, reset_cache)
        self._smugmug_connection = smugmug_connection

    @property
    def smugmug(self):
        return self._smugmug_connection

    def scan(self):
        logger.info('Scanning SmugMug for categories...')

        albums_to_cleanup = dict(self.albums)

        # First get the Hierarchy (categories and subcategories)
        _, categories = self.smugmug.node_get()
        for category in categories:
            category_id = category['Name']
            if category_id not in self.collections:
                # Doesn't exist on disk - we will download it from Smugmug later
                self.collections[category_id] = Collection.create_from_smugmug(self, category_id, category)
            else:
                self.collections[category_id].update_from_smugmug(category)

            # Continue traversing the hierarchy (currently support only one level...)
            _, subcategories = self.smugmug.node_get(node_uri=category['Uri'])
            for subcategory in subcategories:
                # Second level folder is already suppposed to be an Album...
                if not subcategory.type == 'Album':
                    continue


                subcategory_id = os.path.join(category_id, subcategory['Name'])
                if subcategory_id not in self.collections:
                    # Doesn't exist on disk - we will download it from Smugmug later
                    self.collections[subcategory_id] = \
                        Collection.create_from_smugmug(self, subcategory_id, subcategory)
                else:
                    self.collections[subcategory_id].update_from_smugmug(subcategory)

                logger.info('Scanning SmugMug for albums...')

                _, albums = self.smugmug.node_get(node_uri=subcategory['Uri'])
                for album in albums:
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


class DiskScanner(BaseScanner):
    def scan(self):
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
        if basename.startswith('.'):
            return True
        if basename in ('Originals', 'Lightroom', 'Developed', ''):
            return True
        if "Picasa" in local_path:
            return True
        return False
