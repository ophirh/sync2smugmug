import logging
import os
from .smugmug import MySmugMug
from .objects import Album, Image, Collection, SyncContainer

logger = logging.getLogger(__name__)


class Scanner(object):
    albums = {}
    """ :type : dict[str, Album] """
    collections = {}
    """ :type : dict[str, Collection] """
    smugmug = MySmugMug()

    def __init__(self, base_dir, nickname):
        self.base_dir = base_dir
        self.nickname = nickname

        if not self.base_dir.endswith(os.path.sep):
            self.base_dir += os.path.sep

        self._scan_disk()
        self._scan_smugmug_for_categories_and_albums()

    def skip(self, p):
        # First, take off the base_dir, then take the base name
        local_path = p[len(self.base_dir):]
        basename = os.path.basename(local_path)
        return basename.startswith('.') or basename in ['Originals', 'Lightroom', '']

    @staticmethod
    def _make_album_id(smugmug_album):
        """
        Returns album id, album key, smugmug id, last update
        :type smugmug_album: dict
        :rtype str
        """
        collection_id = os.path.join(smugmug_album['Category']['Name'], smugmug_album['SubCategory']['Name']) \
            if 'SubCategory' in smugmug_album else smugmug_album['Category']['Name']

        return os.path.join(collection_id, smugmug_album['Title'])

    def _scan_disk(self):
        logger.info('Scanning disk (starting from %s)...' % self.base_dir)
        for dir_path, dirs, files in os.walk(self.base_dir):
            # Apply some base filtering...
            if self.skip(dir_path) or (len(dirs) == 0 and len(files) == 0):
                continue

            album = SyncContainer.create_from_disk(self, dir_path, files)
            if type(album) is Album:
                self.albums[album.id] = album
            else:
                self.collections[album.id] = album

            # Scan images!
            for img in [f for f in files if Image.is_image(f)]:
                image = Image.create_from_disk(self, dir_path, img)
                album.images[image.id] = image

    def _scan_smugmug_for_categories_and_albums(self):
        logger.info('Scanning SmugMug for categories... (on disk %d)' % len(self.collections))
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

        logger.info('Scanning SmugMug for albums... (on disk %d)' % len(self.albums))
        albums = self.smugmug.albums_get(NickName=self.nickname, Heavy=True)
        for album in albums['Albums']:
            # Lookup the collection to make up the album id
            album_id = self._make_album_id(album)

            # Now lookup the folder and update the smugmug id
            if album_id not in self.albums:
                self.albums[album_id] = Album.create_from_smugmug(self, album_id, album)
            else:
                a = self.albums[album_id]
                a.update_from_smugmug(album)
                a.save_sync_data()

    def upload(self):
        """
        Upload all images on disk that don't exist online
        """
        # TODO: Deal with name changes...
        def action(container):
            """ :type container: SyncContainer """
            if container.needs_upload():
                container.upload()

        self.perform_action_on_collections('Uploading', action)

    def download(self):
        """
        Download albums (and categories) that only exist online. This will NOT download specific images - only whole
        albums.
        """
        def action(container):
            """ :type container: SyncContainer """
            if container.needs_download():
                container.download()

        self.perform_action_on_collections('Downloading', action)

    def metadata(self):
        """
        Sync changes (including deletions and renaming) between disk and online - making disk the master
        """
        def action(container):
            """ :type container: SyncContainer """
            # TODO
            # if container.needs_metadata_sync():
            #     container.metadata_sync()
            pass

        self.perform_action_on_collections('Renaming', action)

    def perform_action_on_collections(self, msg, action):
        logger.info('%s collections (categories and subcategories)...' % msg)
        for key in sorted(self.collections.keys()):
            action(self.collections[key])

        logger.info('%s folders (albums)...' % msg)
        for key in sorted(self.albums.keys()):
            action(self.albums[key])

    def print_status(self):
        self._print_summary('Collections', self.collections)
        self._print_summary('Folders', self.albums)

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
