from HTMLParser import HTMLParser
from datetime import datetime
import json
import os
from dateutil import parser
from scanner import POLICY_SYNC
from scanner.image import Image
from scanner.objects import SyncObject, logger, sync_images
from scanner.picasa import PicasaAlbum
from scanner.utils import date_handler, date_hook


class Album(SyncObject):
    def __init__(self, scanner, obj_id, **kwargs):
        super(Album, self).__init__(scanner, obj_id, **kwargs)
        self.album_key = None
        self.sync_data = {}
        self.images = {}
        """ :type : dict[str, Image] """
        self.metadata_last_updated = None
        self.smugmug_description = None
        self.picasa = None
        """ :type : Picasa """

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
        album.picasa = PicasaAlbum(disk_path, files)

        # Scan images on disk and associate with the album
        for img in images:
            image = Image.create_from_disk(scanner, disk_path, img, developed_path=album.developed_dir_path)
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

    @property
    def developed_dir_path(self):
        """ If exists, returns the path of the 'Developed' sub-folder for this album """
        developed_path = os.path.join(self.disk_path, "Developed")
        return developed_path if os.path.exists(developed_path) else None

    def update_from_smugmug(self, album):
        self.smugmug_id = album['id']
        self.album_key = album['Key']
        if 'LastUpdated' in album:
            self.online_last_updated = datetime.strptime(album['LastUpdated'].partition(' ')[0], '%Y-%m-%d').date()
        if 'Description' in album:
            self.smugmug_description = HTMLParser().unescape(album['Description'])

    def get_images(self, heavy=True):
        return self.get_smugmug().images_get(AlbumID=self.smugmug_id,
                                             AlbumKey=self.album_key,
                                             Heavy=heavy)['Album']['Images']

    def get_description(self):
        desc = self.picasa.get_album_description()
        location = self.picasa.get_album_location()

        if desc and location:
            return '%s\n%s' % (desc, location)
        elif desc:
            return desc
        elif location:
            return location
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
                # Check the date of the developed directory date (if exists)
                developed_path = self.developed_dir_path
                if developed_path:
                    disk_last_updated = datetime.fromtimestamp(os.path.getmtime(developed_path))
                    if disk_last_updated <= upload_date:
                        return False
                else:
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
                                                 Description=self.picasa.get_album_description())
            self.update_from_smugmug(r['Album'])

        if self.description_needs_sync():
            # Update the album's description property
            logger.debug('--- Updating album\'s description %s' % self)
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