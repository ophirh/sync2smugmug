import ConfigParser
import os
import struct
import time
from scanner.utils import Singleton


def read_string_field(f):
    # Read a null terminated string...
    s = ''

    b = f.read(1)
    while ord(b) != 0:
        s += b
        b = f.read(1)

    return s


def read_byte_field(f):
    return struct.unpack('<B', f.read(1))[0]


def read_2byte_field(f):
    return struct.unpack('<H', f.read(2))[0]


def read_4byte_field(f):
    return struct.unpack('<L', f.read(4))[0]


def read_8byte_field(f):
    return struct.unpack('<Q', f.read(8))[0]


def read_date_field(f):
    # Read Microsoft Variant (date as a double)
    d = struct.unpack('<d', f.read(8))[0]
    d -= 25569
    ut = round(d * 86400l * 1000l)
    return time.gmtime(ut)


class PMPReader(object):
    def __init__(self, path, table, field):
        self.path = path
        self.entries = self._read(table, field)

    def _read(self, table, field):
        with open(os.path.join(self.path, '%s_%s.pmp' % (table, field)), 'rb') as f:
            if struct.unpack('<I', f.read(4))[0] != 0x3fcccccd:
                raise IOError('Failed magic1')

            t = struct.unpack('<H', f.read(2))[0]

            if struct.unpack('<H', f.read(2))[0] != 0x1332:
                raise IOError('Failed magic2')

            if struct.unpack('<I', f.read(4))[0] != 0x2:
                raise IOError('Failed magic3')

            if t != struct.unpack('<H', f.read(2))[0]:
                raise IOError('Failed repeat type %s' % t)

            if struct.unpack('<H', f.read(2))[0] != 0x1332:
                raise IOError('Failed magic4')

            num_of_items = struct.unpack('<I', f.read(4))[0]

            values = []
            for _ in range(num_of_items):
                if t == 0x0:
                    values.append(read_string_field(f))
                elif t == 0x1:
                    values.append(read_4byte_field(f))
                elif t == 0x2:
                    values.append(read_date_field(f))
                elif t == 0x3:
                    values.append(read_byte_field(f))
                elif t == 0x4:
                    values.append(read_8byte_field(f))
                elif t == 0x5:
                    values.append(read_2byte_field(f))
                elif t == 0x6:
                    values.append(read_string_field(f))
                elif t == 0x7:
                    values.append(read_4byte_field(f))
                else:
                    raise IOError("Unknown type: %s" % t)

            return values


class ThumbIndexDBReader(object):
    def __init__(self, path):
        self.path = path
        self.dirs, self.images = self._read()

    def _read(self):
        with open(os.path.join(self.path, 'thumbindex.db'), 'rb') as f:
            if struct.unpack('<I', f.read(4))[0] != 0x40466666:
                raise IOError('Failed magic')

            num_of_items = struct.unpack('<I', f.read(4))[0]

            dirs = {}
            lookup = {}
            images = []

            # Read the objects first (without trying to link them)
            for i in range(num_of_items):
                name = read_string_field(f)
                f.read(26)  # Useless content...
                index = read_4byte_field(f)

                if index == 4294967295:
                    # This is a folder...
                    dirs[name] = []
                    lookup[i] = name
                elif len(name) == 0:
                    # Skip virtual copies (for faces)
                    continue
                else:
                    image = {'name': name, 'parent': index, 'index': i}
                    images.append(image)

            # Now link everything together
            for image in images:
                dir_name = lookup[image['parent']]
                dirs[dir_name].append(image)

                # Replace image parent index with name
                image['parent'] = dir_name

            return dirs, images


@Singleton
class PicasaDB(object):
    """
    Reads the Picasa DB (proprietary format)
    """

    def __init__(self):
        self.picasa_db_location = 'E:\PicasaDB\Google\Picasa2\db3'
        self.images = self._construct_db()

    def _construct_db(self):
        thumbs = ThumbIndexDBReader(self.picasa_db_location)

        # Pick the interesting fields that we want to collect...
        captions = PMPReader(self.picasa_db_location, 'imagedata', 'caption').entries
        texts = PMPReader(self.picasa_db_location, 'imagedata', 'text').entries

        # Now construct a single data structure that will hold all images, their captions, etc...
        images = thumbs.images

        for image in images:
            try:
                caption = captions[image['index']]
                if caption and len(caption) > 0:
                    image['caption'] = caption
            except IndexError:
                # Reaching EOF...
                pass

            try:
                description = texts[image['index']]
                if description and len(description) > 0:
                    image['text'] = description
            except IndexError:
                # Reaching EOF...
                pass

        return images

    def get_image_caption(self, folder, name):
        # TODO

        return None


class PicasaAlbum(object):
    """
    Manages the reading and understanding of the Picasa metadata (stored in INI files) for the given album
    """

    def __init__(self, album_path, files):
        self.ini_files = []

        if '.picasa.ini' in files:
            picasa_ini1 = ConfigParser.ConfigParser()
            picasa_ini1.read(os.path.join(album_path, '.picasa.ini'))
            self.ini_files.append(picasa_ini1)

        if 'Picasa.ini' in files:
            picasa_ini2 = ConfigParser.ConfigParser()
            picasa_ini2.read(os.path.join(album_path, 'Picasa.ini'))
            self.ini_files.append(picasa_ini2)

    def _get_attribute(self, section, attribute):
        for ini_file in self.ini_files:
            try:
                return ini_file.get(section, attribute)
            except ConfigParser.NoOptionError:
                pass

        return None

    def get_album_description(self):
        return self._get_attribute('Picasa', 'Description')

    def get_album_location(self):
        return self._get_attribute('Picasa', 'Location')


if __name__ == '__main__':
    # Test reading the Picasa DB (read the title of images)
    # print PMPReader(p, 'imagedata', 'caption')).entries
    print PicasaDB.instance().images
