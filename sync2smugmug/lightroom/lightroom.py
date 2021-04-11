import os
import sqlite3


class LightRoom:
    """
    Reads the Adobe LightRoom catalog database (SQLite)
    """
    def __init__(self, catalog_path: str):
        if not os.path.exists(catalog_path):
            raise IOError('Could not find catalog %s' % catalog_path)
        self.catalog_path = catalog_path
        self.db = conn = sqlite3.connect(self.catalog_path)
        self._read_images()

    def _read_images(self):
        pass
