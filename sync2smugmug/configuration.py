import dataclasses
import logging
import sys
import pathlib
from typing import List

import configargparse

from sync2smugmug import policy


@dataclasses.dataclass(frozen=True)
class ConnectionParams:
    """
    Includes necessary credentials to connect to our Online Image service (e.g. Smugmug) via oauth
    """
    account: str
    consumer_key: str
    consumer_secret: str
    access_token: str
    access_token_secret: str
    test_upload: bool


@dataclasses.dataclass(frozen=True)
class Config:
    """
    Global configuration object, configuration taken from config files and CLI
    """
    sync: policy.SyncAction
    connection_params: ConnectionParams
    base_dir: pathlib.Path
    force_refresh: bool
    dry_run: bool
    mac_photos_library_location: pathlib.Path = None


def get_config_files() -> List[pathlib.Path]:
    """
    Resolve a list of config file paths to be read (in that order) into the Configuration object
    """
    config_files_dir_path = pathlib.Path(__file__).parent.parent.resolve()
    return [
        config_files_dir_path.joinpath(config_file_name)
        for config_file_name in ("sync2smugmug.conf", "sync2smugmug.my.conf")
    ]


def parse_command_line() -> configargparse.Namespace:
    """
    Define the command line parser and load configuration files into it
    """
    arg_parser = configargparse.ArgParser(default_config_files=get_config_files())

    arg_parser.add_argument(
        "--sync",
        required=True,
        help="Type of sync to perform (choose one of the available presets)",
        choices=policy.get_presets(),
    )
    arg_parser.add_argument(
        "--base_dir",
        required=True,
        help="Full path to pictures source_folder"
    )
    arg_parser.add_argument(
        "--mac_photos_library_location",
        required=False,
        help="Full path for Mac Photos library",
    )
    arg_parser.add_argument(
        "--account",
        required=True,
        help="Name (nickname) of SmugMug account"
    )
    arg_parser.add_argument(
        "--consumer_key",
        required=True,
        help="Smugmug API key of this account"
    )
    arg_parser.add_argument(
        "--consumer_secret",
        required=True,
        help="Smugmug API secret of this account"
    )
    arg_parser.add_argument(
        "--access_token",
        required=True,
        help="Smugmug oauth token obtained for this script",
    )
    arg_parser.add_argument(
        "--access_token_secret",
        required=True,
        help="Smugmug oauth secret obtained for this script",
    )
    arg_parser.add_argument(
        "--force_refresh",
        action="store_true",
        default=False
    )
    arg_parser.add_argument(
        "--dry_run",
        action="store_true",
        default=False
    )
    arg_parser.add_argument(
        "--test_upload",
        action="store_true",
        default=False
    )
    arg_parser.add_argument(
        "--log_level",
        required=False,
        choices=["CRITICAL", "DEBUG", "ERROR", "FATAL", "INFO"],
        default="INFO",
    )

    return arg_parser.parse_args()


def configure_logging(log_level: str):
    """
    Configure logging
    """
    # Work around a problem with osxphotos (it calls logging.basicConfig directly, so we want to import it first, then
    # override with our own basicConfig)
    import osxphotos    # noqa
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        stream=sys.stdout,
        level=log_level,
        format="%(asctime)s - [%(levelname)s] %(message)s",
    )

    logging.getLogger().setLevel(log_level)

    # Silence the very verbose networking libraries
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore.http11").setLevel(logging.WARNING)
    logging.getLogger("httpcore.connection").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("osxphotos").setLevel(logging.WARNING)


def make_config() -> Config:
    args = parse_command_line()

    configure_logging(args.log_level)

    base_dir = pathlib.Path(args.base_dir).expanduser()
    assert base_dir.exists(), f"Base dir {base_dir} does not exist!"

    preset_method = getattr(policy.SyncActionPresets, args.sync)
    sync_preset = preset_method()

    if args.mac_photos_library_location:
        mac_photos_library_location = pathlib.Path(args.mac_photos_library_location).expanduser()
        assert mac_photos_library_location.exists(), \
            f"Mac photos library dir {mac_photos_library_location} does not exist!"
    else:
        mac_photos_library_location = None

    return Config(
        sync=sync_preset,
        connection_params=ConnectionParams(
            account=args.account,
            consumer_key=args.consumer_key,
            consumer_secret=args.consumer_secret,
            access_token=args.access_token,
            access_token_secret=args.access_token_secret,
            test_upload=args.test_upload,
        ),
        base_dir=base_dir,
        force_refresh=args.force_refresh,
        dry_run=args.dry_run,
        mac_photos_library_location=mac_photos_library_location,
    )


config: Config = make_config()
