import os
import json
import logging
import requests
import ConfigParser
import scan
from datetime import datetime
from dateutil import parser
from multiprocessing.pool import ThreadPool
from .policy import *
from .utils import date_handler, date_hook


logger = logging.getLogger(__name__)


def sync_image(image, policy):
    """
    :type image: Image
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
    :type images: list[Image]
    :type policy: int
    """
    thread_pool = ThreadPool(10)

    results = []
    for i in images:
        r = thread_pool.apply_async(sync_image, (i, policy))
        results.append(r)

    thread_pool.close()
    thread_pool.join()

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

    def get_smugmug(self):
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
        self.smugmug_id = category['id']

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
                r = self.get_smugmug().categories_create(Name=self.extract_name())
                self.smugmug_id = r['Category']['id']
            else:
                logger.debug('--- Creating subcategory for %s' % self)
                r = self.get_smugmug().subcategories_create(Name=self.extract_name(),
                                                            CategoryID=self.get_parent_smugmug_id())
                self.smugmug_id = r['SubCategory']['id']
        else:
            # Download: Super implementation will create the directory
            pass


class Album(SyncObject):
    def __init__(self, scanner, obj_id, **kwargs):
        super(Album, self).__init__(scanner, obj_id, **kwargs)
        self.album_key = None
        self.sync_data = {}
        self.images = {}
        """ :type : dict[str, Image] """
        self.metadata_last_updated = None
        self.smugmug_description = None
        self.picasa_description = None
        self.picasa_location = None

    @staticmethod
    def create_from_disk(scanner, disk_path, files, images):
        """
        :type scanner: scan.Scanner
        :type disk_path: str
        :type files: list[str]
        :type images: list[str]
        """
        album = Album(scanner, SyncObject.path_to_id(scanner.base_dir, disk_path))
        album.disk_path = disk_path
        album.load_sync_data()

        if 'Picasa.ini' in files or '.picasa.ini' in files:
            picasa_ini_file = os.path.join(disk_path, 'Picasa.ini' if 'Picasa.ini' in files else '.picasa.ini')
            picasa_ini = ConfigParser.ConfigParser()
            picasa_ini.read(picasa_ini_file)
            # Get the picasa description and location of this album
            try:
                album.picasa_description = picasa_ini.get('Picasa', 'Description')
            except ConfigParser.NoOptionError:
                album.picasa_description = None

            try:
                album.picasa_location = picasa_ini.get('Picasa', 'Location')
            except ConfigParser.NoOptionError:
                album.picasa_location = None

        # Scan images on disk and associate with the album
        for img in images:
            image = Image.create_from_disk(scanner, disk_path, img)
            album.images[image.id] = image

        return album

    @staticmethod
    def create_from_smugmug(scanner, folder_id, album):
        """ :type scanner: scan.Scanner """
        a = Album(scanner, folder_id)
        a.update_from_smugmug(album)
        return a

    @staticmethod
    def make_album_id(smugmug_album):
        """
        :type smugmug_album: dict
        :rtype str
        """
        collection_id = os.path.join(smugmug_album['Category']['Name'], smugmug_album['SubCategory']['Name']) \
            if 'SubCategory' in smugmug_album else smugmug_album['Category']['Name']

        return os.path.join(collection_id, smugmug_album['Title'])

    def update_from_smugmug(self, album):
        self.smugmug_id = album['id']
        self.album_key = album['Key']
        if 'LastUpdated' in album:
            self.online_last_updated = datetime.strptime(album['LastUpdated'].partition(' ')[0], '%Y-%m-%d').date()
        self.smugmug_description = album['Description']

    def get_images(self, heavy=True):
        return self.get_smugmug().images_get(AlbumID=self.smugmug_id,
                                             AlbumKey=self.album_key,
                                             Heavy=heavy)['Album']['Images']

    def get_description(self):
        if self.picasa_description and self.picasa_location:
            return '%s\n%s' % (self.picasa_description, self.picasa_location)
        elif self.picasa_description:
            return self.picasa_description
        elif self.picasa_location:
            return self.picasa_location
        else:
            return None

    def description_needs_sync(self):
        desc = self.get_description()
        return desc and desc != self.smugmug_description

    def images_need_sync(self):
        if self.sync_data and 'images_uploaded_date' in self.sync_data:
            upload_date = parser.parse(self.sync_data['images_uploaded_date'])
            disk_last_updated = datetime.fromtimestamp(os.path.getmtime(self.disk_path))
            if disk_last_updated <= upload_date:
                return False

        return True

    def mark_finished(self):
        self.sync_data['images_uploaded_date'] = str(datetime.utcnow())
        self.save_sync_data()

    def needs_sync(self):
        """
        In case of album, we will use the sync_data to try and optimize the album creation. If the album's last
        update date has not changed, we will not even check for images.

        TODO - check changes of Picasa.ini file (description, location)
        """
        return super(Album, self).needs_sync() or self.images_need_sync() or self.description_needs_sync()

    def sync(self, policy):
        # This will make sure we have a folder on disk for this album
        if policy == POLICY_SYNC:
            # Simply make the directory
            self.disk_path = SyncObject.id_to_path(self.scanner.base_dir, self.id)
            if not os.path.exists(self.disk_path):
                os.mkdir(self.disk_path)

        if not self.on_disk():
            return

        if not self.on_smugmug():
            # Make the album in Smugmug (including keywords, etc...)
            logger.debug('--- Creating album for %s (parent: %s)' % (self, self.get_parent()))
            r = self.get_smugmug().albums_create(Title=self.extract_name(),
                                                 CategoryID=self.get_parent_smugmug_id(),
                                                 Description=self.picasa_description)
            self.update_from_smugmug(r['Album'])
            # TODO: Add keywords (and location?) from the Picasa.ini metadata

        if self.description_needs_sync():
            # Update the album's description property
            logger.debug('--- Updating album\'s description %s (parent: %s)' % (self, self.get_parent()))
            self.get_smugmug().albums_changeSettings(AlbumID=self.smugmug_id, Description=self.get_description())

        # Check for images...
        if self.images_need_sync():
            # Sync images with their online versions
            for i in self.get_images():
                image_id = os.path.join(self.id, i['FileName'])
                if image_id not in self.images:
                    # Add this image to the index
                    self.images[image_id] = Image.create_from_smugmug(self.scanner, image_id, i)
                else:
                    # Simply update the smugmug ID
                    self.images[image_id].update_from_smugmug(i)

            tasks = []
            for image in self.images.values():
                if image.needs_sync():
                    tasks.append(image)

            # Upload concurrently
            sync_images(tasks, policy)

            # Done - double check that indeed we have the same number of images online...
            if len(self.get_images(heavy=False)) == len(self.images):
                self.mark_finished()
                logger.info('Finished uploading images for %s' % self)

    def save_sync_data(self):
        self.sync_data['smugmug_id'] = self.smugmug_id
        with open(os.path.join(self.disk_path, 'smugmug_sync.json'), 'w+') as f:
            f.write(json.dumps(self.sync_data, default=date_handler))

    def load_sync_data(self):
        p = os.path.join(self.disk_path, 'smugmug_sync.json')
        if os.path.exists(p):
            with open(p) as f:
                self.sync_data = json.loads(f.read(), object_hook=date_hook)
                self.smugmug_id = self.sync_data['smugmug_id']

    def delete_sync_data(self):
        if self.disk_path is not None:
            logger.debug('--- Deleting sync data for album %s' % self.id)
            p = os.path.join(self.disk_path, 'smugmug_sync.json')
            if os.path.exists(p):
                os.remove(p)
            self.sync_data = {}


class Image(SyncObject):
    def __init__(self, scanner, obj_id, smugmug_id=None, disk_path=None):
        super(Image, self).__init__(scanner, obj_id, smugmug_id, disk_path)
        self.original_url = None
        self.duplicated_images = []

    @staticmethod
    def create_from_disk(scanner, folder, name):
        """ :type scanner: scan.Scanner """
        image_disk_path = os.path.join(folder, name)
        image_id = SyncObject.path_to_id(scanner.base_dir, image_disk_path)
        return Image(scanner, image_id, disk_path=image_disk_path)

    @staticmethod
    def create_from_smugmug(scanner, image_id, image):
        """ :type scanner: scan.Scanner """
        i = Image(scanner, image_id)
        i.update_from_smugmug(image)
        return i

    def update_from_smugmug(self, image):
        last_update = datetime.strptime(image['LastUpdated'], '%Y-%m-%d %H:%M:%S')
        if self.smugmug_id is None:
            self.smugmug_id = image['id']
            self.original_url = image['OriginalURL']
            self.online_last_updated = last_update
        else:
            # Duplicated image on SmugMug!!! Delete older version
            self.duplicated_images.append(image['id'])
            logger.info('Duplicated image on SmugMug!!! Delete older version %s' % self.id)

    def needs_sync(self):
        # Check for upload, download, delete
        return super(Image, self).needs_sync() or len(self.duplicated_images) > 0

    def sync(self, policy):
        if not self.on_smugmug():
            self.upload()

        if not self.on_disk():
            if policy == POLICY_DISK_RULES:
                # Delete the online version (as this was deleted from the disk)
                logger.debug('--- Deleting image %d' % self.smugmug_id)
                self.get_smugmug().images_delete(ImageID=self.smugmug_id)
                self.smugmug_id = None
            else:
                # Download the file to disk
                self.download()

        if len(self.duplicated_images) > 0:
            # Delete from online any duplicates of this photo (duplicates are identified by file name)
            for smugmug_id in self.duplicated_images:
                logger.debug('--- Deleting duplicated image %d' % smugmug_id)
                self.get_smugmug().images_delete(ImageID=smugmug_id)

            self.duplicated_images = []

    def upload(self):
        upload = True

        # Check if an upload is needed...
        if self.online_last_updated:
            disk_last_updated = datetime.fromtimestamp(os.path.getmtime(self.disk_path))
            upload = disk_last_updated > self.online_last_updated

        if upload:
            # Need to delete existing images that need update
            if self.on_smugmug():
                logger.debug('--- Deleting image (for replacement) %s' % self.id)
                self.get_smugmug().images_delete(ImageID=self.smugmug_id)
                self.smugmug_id = None

            logger.debug('--- Uploading image %s' % self)
            self.get_smugmug().images_upload(File=self.disk_path, AlbumID=self.get_parent_smugmug_id())

    def download(self):
        self.disk_path = SyncObject.id_to_path(self.scanner.base_dir, self.id)
        r = requests.get(self.original_url)
        with open(self.disk_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=512 * 1024):
                # filter out keep-alive new chunks
                if chunk:
                    f.write(chunk)

    @staticmethod
    def is_image(f):
        _, ext = os.path.splitext(f)
        # Unknown file types: '.3gp',
        return ext.lower() in ['.jpg', '.jpeg', '.avi', '.mv4', '.mov', '.mp4', '.mts']