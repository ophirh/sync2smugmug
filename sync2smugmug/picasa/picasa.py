import configparser
import os
import struct
import time
from typing import List, Dict, Tuple, Optional
from typing.io import BinaryIO


def read_string_field(f: BinaryIO) -> str:
    # Read a null terminated string...
    s = b''

    b = f.read(1)
    while ord(b) != 0:
        s += b
        b = f.read(1)

    return s.decode()


def read_byte_field(f: BinaryIO):
    return struct.unpack(b'<B', f.read(1))[0]


def read_2byte_field(f: BinaryIO):
    return struct.unpack(b'<H', f.read(2))[0]


def read_4byte_field(f: BinaryIO):
    return struct.unpack(b'<L', f.read(4))[0]


def read_8byte_field(f: BinaryIO):
    return struct.unpack(b'<Q', f.read(8))[0]


def read_date_field(f: BinaryIO):
    # Read Microsoft Variant (date as a double)
    d = struct.unpack(b'<d', f.read(8))[0]
    d -= 25569
    ut = round(d * 86400 * 1000)
    return time.gmtime(ut)


class PMPReader:
    def __init__(self, path: str, table: str, field: str):
        self.path = path
        self.entries = self._read(table, field)

    def _read(self, table: str, field: str) -> List:
        with open(os.path.join(self.path, f'{table}_{field}.pmp'), 'rb') as f:
            if struct.unpack(b'<I', f.read(4))[0] != 0x3fcccccd:
                raise IOError('Failed magic1')

            t = struct.unpack(b'<H', f.read(2))[0]

            if struct.unpack(b'<H', f.read(2))[0] != 0x1332:
                raise IOError('Failed magic2')

            if struct.unpack(b'<I', f.read(4))[0] != 0x2:
                raise IOError('Failed magic3')

            if t != struct.unpack(b'<H', f.read(2))[0]:
                raise IOError(f'Failed repeat type {t}')

            if struct.unpack(b'<H', f.read(2))[0] != 0x1332:
                raise IOError('Failed magic4')

            num_of_items = struct.unpack(b'<I', f.read(4))[0]

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
                    raise IOError(f'Unknown type: {t}')

            return values


class ThumbIndexDBReader:
    def __init__(self, path: str):
        self.path = path
        self.dirs, self.images = self._read()

    def _read(self) -> Tuple[Dict, List[Dict]]:
        with open(os.path.join(self.path, 'thumbindex.db'), 'rb') as f:
            if struct.unpack(b'<I', f.read(4))[0] != 0x40466666:
                raise IOError('Failed magic')

            num_of_items = struct.unpack('<I', f.read(4))[0]

            dirs = {}
            lookup = {}
            images: List[Dict] = []

            # Read the objects first (without trying to link them)
            for i in range(num_of_items):
                name = read_string_field(f)
                f.read(26)  # Useless content...
                index = read_4byte_field(f)

                if index == 4294967295:
                    # This is a folder...
                    dirs[name] = {}
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
                dirs[dir_name][image['name']] = image

                # Replace image parent index with name
                image['parent'] = dir_name

            return dirs, images


class PicasaDB(object):
    """
    Reads the Picasa DB (proprietary format)
    """

    def __init__(self, **kwargs):
        self.picasa_db_location = kwargs['picasa_db_location']
        self.thumbs = self._construct_db()

    def _construct_db(self) -> ThumbIndexDBReader:
        thumbs = ThumbIndexDBReader(self.picasa_db_location)

        # Pick the interesting fields that we want to collect...
        captions = PMPReader(self.picasa_db_location, 'imagedata', 'caption').entries
        texts = PMPReader(self.picasa_db_location, 'imagedata', 'text').entries

        # Merge the image data into the index
        for image in thumbs.images:
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

                # TODO: Merge additional attributes here...

        return thumbs

    def describe(self):
        print('Dirs:')
        for d in self.thumbs.dirs.keys():
            print(d)

    def get_image_caption(self, folder: str, name: str) -> Optional[str]:
        if not folder.endswith('\\'):
            folder += '\\'

        try:
            return self.thumbs.dirs[folder][name]['caption']
        except KeyError:
            return None


class PicasaAlbum:
    """
    Manages the reading and understanding of the Picasa metadata (stored in INI files) for the given album
    """

    def __init__(self, album_path: str, files: List):
        self.ini_files = []

        if '.picasa.ini' in files:
            picasa_ini1 = configparser.ConfigParser()
            picasa_ini1.read(os.path.join(album_path, '.picasa.ini'))
            self.ini_files.append(picasa_ini1)

        if 'Picasa.ini' in files:
            picasa_ini2 = configparser.ConfigParser()
            picasa_ini2.read(os.path.join(album_path, 'Picasa.ini'))
            self.ini_files.append(picasa_ini2)

    def _get_attribute(self, section, attribute):
        for ini_file in self.ini_files:
            try:
                return ini_file.get(section, attribute)
            except configparser.NoOptionError:
                pass
            except configparser.NoSectionError:
                pass

        return None

    def get_album_description(self) -> str:
        return self._get_attribute('Picasa', 'Description')

    def get_album_location(self) -> str:
        return self._get_attribute('Picasa', 'Location')


if __name__ == '__main__':
    # Test reading the Picasa DB (read the title of images)
    # print PMPReader(p, 'imagedata', 'caption')).entries
    print(PicasaDB().get_image_caption('E:\\Pictures\\2014\\2014_05_31 - New England Open Karate',
                                       'P5310600.MOV'))
