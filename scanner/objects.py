import os
import json
import logging
import ConfigParser
from datetime import datetime
from dateutil import parser
from multiprocessing.pool import ThreadPool
import requests
import scan


logger = logging.getLogger(__name__)


def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj


def date_hook(json_dict):
    for (key, value) in json_dict.items():
        # noinspection PyBroadException
        try:
            json_dict[key] = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
        except:
            pass
    return json_dict


def upload_image(image):
    """ :type image: Image """
    # noinspection PyBroadException
    try:
        image.upload()
    except:
        # Report and continue
        logging.getLogger(__name__).exception('Failed to upload image %s' % image)


def upload_images(images):
    """ :type images: list[Image] """
    thread_pool = ThreadPool(10)
    thread_pool.map(upload_image, images)
    thread_pool.close()
    thread_pool.join()


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
        return os.path.basename(self.id)

    def on_smugmug(self):
        return self.smugmug_id is not None

    def on_disk(self):
        return self.disk_path is not None

    def get_smugmug(self):
        return self.scanner.smugmug

    def upload(self):
        # Abstract method
        pass

    def download(self):
        # Abstract method
        pass

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

    def __str__(self):
        return '%s:%s (%s)' % (self.id, self.smugmug_id, self.disk_path)


class SyncContainer(SyncObject):
    def __init__(self, scanner, obj_id, **kwargs):
        super(SyncContainer, self).__init__(scanner, obj_id, **kwargs)
        self.sync_data = {}
        self.picasa_ini_file = None
        self.picasa_ini = None

    @staticmethod
    def create_from_disk(scanner, disk_path, files):
        """
        :type scanner: scan.Scanner
        :type disk_path: str
        :type files: list[str]
        """
        folder_id = SyncObject.path_to_id(scanner.base_dir, disk_path)

        images = [f for f in files if Image.is_image(f)]
        if len(images) > 0:
            # This is an album (it has images)
            folder = Album(scanner, folder_id)
            if 'Picasa.ini' in files or '.picasa.ini' in files:
                folder.picasa_ini_file = os.path.join(disk_path,
                                                      'Picasa.ini' if 'Picasa.ini' in files else '.picasa.ini')
                folder.picasa_ini = ConfigParser.ConfigParser()
                folder.picasa_ini.read(folder.picasa_ini_file)
        else:
            # This is a simple folder (does not have images)
            folder = Collection(scanner, folder_id)

        folder.disk_path = disk_path

        if 'smugmug_sync.json' in files:
            with open(os.path.join(disk_path, 'smugmug_sync.json')) as f:
                folder.sync_data = json.loads(f.read(), object_hook=date_hook)
                folder.smugmug_id = folder.sync_data['smugmug_id']

        return folder

    def needs_upload(self):
        # Abstract method
        return False

    def needs_download(self):
        return self.on_smugmug() and not self.on_disk()

    def needs_rename(self):
        # Abstract method
        return False

    def download(self):
        # Simply make the directory
        self.disk_path = SyncObject.id_to_path(self.scanner.base_dir, self.id)
        if not os.path.exists(self.disk_path):
            os.mkdir(self.disk_path)

    def rename(self):
        pass

    def is_managed_by_picasa(self):
        return self.picasa_ini is not None

    def save_sync_data(self):
        self.sync_data['smugmug_id'] = self.smugmug_id
        with open(os.path.join(self.disk_path, 'smugmug_sync.json'), 'w+') as f:
            f.write(json.dumps(self.sync_data, default=date_handler))


class Collection(SyncContainer):
    @staticmethod
    def create_from_smugmug(scanner, folder_id, props):
        """ :type scanner: scan.Scanner """
        coll = Collection(scanner, folder_id)
        coll.update_from_smugmug(props)
        return coll

    def update_from_smugmug(self, props):
        self.smugmug_id = props['id']

    def is_smugmug_category(self):
        # It is a category (vs. subcategory) if it is a top level item
        return self.get_parent() is None

    def needs_upload(self):
        return self.on_disk() and not self.on_smugmug()

    def upload(self):
        # Make the folder in Smugmug (including keywords, etc...)
        if self.is_smugmug_category():
            logger.debug('--- Creating category for %s' % self)
            r = self.get_smugmug().categories_create(Name=self.extract_name())
            self.smugmug_id = r['Category']['id']
        else:
            logger.debug('--- Creating subcategory for %s' % self)
            r = self.get_smugmug().subcategories_create(Name=self.extract_name(),
                                                        CategoryID=self.get_parent_smugmug_id())
            self.smugmug_id = r['SubCategory']['id']

    def needs_rename(self):
        # online_name = self.get_smugmug().categories_get(NickName=self.nickname)
        #
        # if self.is_smugmug_category():
        #     logger.debug('--- Creating category for %s' % self)
        #     r = self.get_smugmug().categories_create(Name=self.extract_name())
        #     self.smugmug_id = r['Category']['id']
        # else:
        #     logger.debug('--- Creating subcategory for %s' % self)
        #     r = self.get_smugmug().subcategories_create(Name=self.extract_name(),
        #                                                 CategoryID=self.get_parent_smugmug_id())
        #     self.smugmug_id = r['SubCategory']['id']
        #
        #
        # online_name = self.get_smugmug().ca
        # if self.smugmug_id and
        return False

    def rename(self):
        # TODO
        pass


class Album(SyncContainer):
    def __init__(self, scanner, obj_id, **kwargs):
        super(Album, self).__init__(scanner, obj_id, **kwargs)
        self.album_key = None
        self.images = {}

    @staticmethod
    def create_from_smugmug(scanner, folder_id, album):
        """ :type scanner: scan.Scanner """
        a = Album(scanner, folder_id)
        a.update_from_smugmug(album)
        return a

    def update_from_smugmug(self, album):
        self.smugmug_id = album['id']
        self.album_key = album['Key']
        if 'LastUpdated' in album:
            self.online_last_updated = datetime.strptime(album['LastUpdated'].partition(' ')[0], '%Y-%m-%d').date()

    def get_images(self):
        return self.get_smugmug().images_get(AlbumID=self.smugmug_id,
                                             AlbumKey=self.album_key,
                                             Heavy=True)['Album']['Images']

    def mark_finished(self):
        self.sync_data['images_uploaded'] = True
        self.sync_data['images_uploaded_date'] = str(datetime.utcnow())
        self.save_sync_data()

    def needs_upload(self):
        if not self.on_disk():
            return False

        if not self.on_smugmug():
            return True

        if self.sync_data and 'images_uploaded' not in self.sync_data:
            return True

        if 'images_uploaded_date' in self.sync_data:
            upload_date = parser.parse(self.sync_data['images_uploaded_date'])
            disk_last_updated = datetime.fromtimestamp(os.path.getmtime(self.disk_path))
            # If disk is newer by at least one day than online - flag for sync
            if disk_last_updated > upload_date:
                return True

        return False

    def upload(self):
        # TODO: Need to delete existing images that need update
        if not self.on_smugmug():
            # Make the album in Smugmug (including keywords, etc...)
            logger.debug('--- Creating album for %s (parent: %s)' % (self, self.get_parent()))
            r = self.get_smugmug().albums_create(Title=self.extract_name(), CategoryID=self.get_parent_smugmug_id())
            self.update_from_smugmug(r['Album'])

        # Get images that are already uploaded
        images_online = {i['id'] : i for i in self.get_images()}

        # Make sure the image is mapped properly
        for i in images_online.values():
            image_id = os.path.join(self.id, i['FileName'])
            if image_id not in self.images:
                # Add this image to the index
                self.images[image_id] = Image.create_from_smugmug(self.scanner, image_id, i)
            else:
                # Simply update the smugmug ID
                self.images[image_id].update_from_smugmug(i)

        # Find all images on disk that we need to upload
        tasks = []
        for image in self.images.values():
            if image.smugmug_id is not None and image.smugmug_id in images_online.keys():
                continue

            tasks.append(image)

        # Upload concurrently
        upload_images(tasks)

        # Done - double check that indeed we have the same number of images online...
        if len(self.get_images()) == len(self.images):
            self.mark_finished()
            logger.info('Finished uploading images for %s' % self)

    def needs_download(self):
        if not super(Album, self).needs_download():
            return False

        # Add a check for photos to the default check
        u = len(self.get_images())
        return u > 0

    def download(self):
        # Download actual photos...
        super(Album, self).download()

        for i in self.get_images():
            image_id = os.path.join(self.id, i['FileName'])
            image = Image.create_from_smugmug(self.scanner, image_id, i)
            self.images[image_id] = image

            image.download()

        self.mark_finished()

    def needs_rename(self):
        # TODO
        return False

    def rename(self):
        # TODO
        pass


class Image(SyncObject):
    def __init__(self, scanner, obj_id, smugmug_id=None, disk_path=None):
        super(Image, self).__init__(scanner, obj_id, smugmug_id, disk_path)
        self.original_url = None

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
        self.smugmug_id = image['id']
        self.original_url = image['OriginalURL']
        if 'LastUpdated' in image:
            self.online_last_updated = datetime.strptime(image['LastUpdated'], '%Y-%m-%d %H:%M:%S')

    def upload(self):
        # TODO: Need to delete existing images that need update
        disk_last_updated = datetime.fromtimestamp(os.path.getmtime(self.disk_path))
        if not self.on_smugmug() or disk_last_updated > self.online_last_updated:
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