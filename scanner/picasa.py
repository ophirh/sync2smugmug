import ConfigParser
import os


class Picasa(object):
    """
    Manages the reading and understanding of the Picasa metadata (stored in INI files)
    """
    def __init__(self, dir_path, files):
        self.ini_files = []

        if '.picasa.ini' in files:
            picasa_ini1 = ConfigParser.ConfigParser()
            picasa_ini1.read(os.path.join(dir_path, '.picasa.ini'))
            self.ini_files.append(picasa_ini1)

        if 'Picasa.ini' in files:
            picasa_ini2 = ConfigParser.ConfigParser()
            picasa_ini2.read(os.path.join(dir_path, 'Picasa.ini'))
            self.ini_files.append(picasa_ini2)

    def _get_attribute(self, section, attribute):
        for ini_file in self.ini_files:
            try:
                return ini_file.get(section, attribute)
            except ConfigParser.NoOptionError:
                pass

        return None

    def get_description(self):
        return self._get_attribute('Picasa', 'Description')

    def get_location(self):
        return self._get_attribute('Picasa', 'Location')

    # noinspection PyBroadException
    def get_image_caption(self, name):
        caption = None
        try:
            caption = self._get_attribute(name, 'caption')
        except:
            pass

        if caption is None:
            try:
                caption = self._get_attribute(name, 'description')
            except:
                pass

        return caption

