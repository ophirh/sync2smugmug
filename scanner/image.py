import os
import requests
from datetime import datetime
from scanner import POLICY_DISK_RULES
from scanner.objects import SyncObject, logger
from scanner.picasa import PicasaDB


class Image(SyncObject):
    def __init__(self, scanner, obj_id, smugmug_id=None, disk_path=None, picasa_caption=None, smugmug_caption=None):
        super(Image, self).__init__(scanner, obj_id, smugmug_id, disk_path)
        self.original_url = None
        self.picasa_caption = picasa_caption
        self.smugmug_caption = smugmug_caption
        self.duplicated_images = []

    @staticmethod
    def create_from_disk(scanner, folder, name):
        """
        :type scanner: scan.Scanner
        """
        image_disk_path = os.path.join(folder, name)
        image_id = SyncObject.path_to_id(scanner.base_dir, image_disk_path)
        picasa_caption = PicasaDB.instance().get_image_caption(folder, name)
        return Image(scanner, image_id, disk_path=image_disk_path, picasa_caption=picasa_caption)

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
            self.smugmug_caption = image['Caption']
        else:
            # Duplicated image on SmugMug!!! Delete older version
            self.duplicated_images.append(image['id'])
            logger.info('Duplicated image on SmugMug!!! Delete older version %s' % self.id)

    def needs_sync(self):
        # Check for upload, download, delete, metadata change
        return super(Image, self).needs_sync() or self._metadata_needs_sync() or len(self.duplicated_images) > 0

    def sync(self, policy):
        if not self.on_smugmug():
            self._upload()

        if not self.on_disk():
            if policy == POLICY_DISK_RULES:
                # Delete the online version (as this was deleted from the disk)
                logger.debug('--- Deleting image %s (%d)' % (self.id, self.smugmug_id))
                self.get_smugmug().images_delete(ImageID=self.smugmug_id)
                self.smugmug_id = None
            else:
                # Download the file to disk
                self._download()

        if self._metadata_needs_sync() and self.smugmug_id:
            # TODO: This is not getting called yet...
            logger.debug('--- Updating image\'s caption %s to %s' % (self, self.picasa_caption))
            self.get_smugmug().images_changeSettings(ImageID=self.smugmug_id, Caption=self.picasa_caption)

        if len(self.duplicated_images) > 0:
            # Delete from online any duplicates of this photo (duplicates are identified by file name)
            for smugmug_id in self.duplicated_images:
                logger.debug('--- Deleting duplicated image %d' % smugmug_id)
                self.get_smugmug().images_delete(ImageID=smugmug_id)

            self.duplicated_images = []

    def _upload(self):
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

    def _metadata_needs_sync(self):
        # TODO: Change to take this from image properties
        # return self.picasa_caption and self.picasa_caption != self.smugmug_caption
        return False

    def _download(self):
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