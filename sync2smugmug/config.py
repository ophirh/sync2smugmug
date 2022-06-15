import logging
import os
import sys
from typing import Tuple

import configargparse

from sync2smugmug.policy import SyncTypeAction, SyncType


class Config:
    def __init__(self, args):
        self._args = args

        assert os.path.exists(self.base_dir), f'Base dir {self.base_dir} does not exist!'
        # assert os.path.exists(self.picasa_db_location), f'Picasa DB {self.picasa_db_location} does not exist!'

        # Configure logger
        logging.basicConfig(stream=sys.stdout,
                            level=getattr(logging, self.log_level),
                            format='%(asctime)s - %(message)s')

    def __repr__(self):
        return f'Sync [{self._args.sync}] - Working off {self.base_dir}: dry_run={self.dry_run}'

    @property
    def sync(self) -> Tuple[SyncTypeAction, ...]:
        name = self._args.sync
        method = getattr(SyncType, name)
        return method()

    @property
    def base_dir(self) -> str:
        return self._args.base_dir

    @property
    def picasa_db_location(self) -> str:
        return self._args.picasa_db_location

    @property
    def account(self) -> str:
        return self._args.account

    @property
    def consumer_key(self) -> str:
        return self._args.consumer_key

    @property
    def consumer_secret(self) -> str:
        return self._args.consumer_secret

    @property
    def access_token(self) -> str:
        return self._args.access_token

    @property
    def access_token_secret(self) -> str:
        return self._args.access_token_secret

    @property
    def dry_run(self) -> bool:
        return self._args.dry_run

    @property
    def log_level(self) -> str:
        return self._args.log_level

    @property
    def use_test_folder(self) -> bool:
        return self._args.use_test_folder


def parse_config() -> Config:
    path_to_configs = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
    config_files = [os.path.join(path_to_configs, p) for p in ('sync2smugmug.conf', 'sync2smugmug.my.conf')]

    # Load command line arguments (and from config files)
    arg_parser = configargparse.ArgParser(default_config_files=config_files)

    arg_parser.add_argument('--sync',
                            default='online_backup',
                            help='Type of sync to perform',
                            choices=['online_backup',
                                     'online_backup_clean',
                                     'local_backup',
                                     'local_backup_clean',
                                     'two_way_sync_no_deletes'])
    arg_parser.add_argument('--base_dir', required=True, help='Full path to pictures folder')
    arg_parser.add_argument('--picasa_db_location', required=False, help='Full path to picasa DB')
    arg_parser.add_argument('--account', required=True, help='Name (nickname) of SmugMug account')
    arg_parser.add_argument('--consumer_key', required=True, help='Smugmug API key of this account')
    arg_parser.add_argument('--consumer_secret', required=True, help='Smugmug API secret of this account')
    arg_parser.add_argument('--access_token', required=True, help='Smugmug oauth token obtained for this script')
    arg_parser.add_argument('--access_token_secret',
                            required=True,
                            help='Smugmug oauth secret obtained for this script')
    arg_parser.add_argument('--dry_run', action='store_true', default=False)
    arg_parser.add_argument('--use_test_folder', action='store_true', default=False)
    arg_parser.add_argument('--log_level',
                            required=False,
                            choices=['CRITICAL', 'DEBUG', 'ERROR', 'FATAL', 'INFO'],
                            default='INFO')

    return Config(arg_parser.parse_args())


config: Config = parse_config()
