import unittest

from sync2smugmug.config import parse_config
from sync2smugmug.disk import DiskScanner
from sync2smugmug.smugmug import SmugmugScanner


class TestDisk(unittest.TestCase):
    def setUp(self):
        self.config = {
            'base_dir': 'E:\\Pictures',
        }

    def test_scan_disk(self):
        on_disk = DiskScanner(**self.config).scan()
        print(on_disk)


class TestSmugmug(unittest.TestCase):
    def setUp(self):
        self.config = parse_config()

    def test_scan_smugmug(self):
        on_smugmug = SmugmugScanner(**self.config).scan()
        print(on_smugmug)
