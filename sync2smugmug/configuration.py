import dataclasses
import logging
import sys
from pathlib import Path
from typing import List

import configargparse

from sync2smugmug import policy


@dataclasses.dataclass(frozen=True)
class ConnectionParams:
    account: str
    consumer_key: str
    consumer_secret: str
    access_token: str
    access_token_secret: str
    test_upload: bool


@dataclasses.dataclass(frozen=True)
class Config:
    sync: policy.SyncAction
    connection_params: ConnectionParams
    base_dir: Path
    dry_run: bool
    mac_photos_library_location: Path = None


def get_config_files() -> List[Path]:
    path_to_configs = Path(__file__).parent.parent.resolve()
    return [
        path_to_configs.joinpath(p)
        for p in ("sync2smugmug.conf", "sync2smugmug.my.conf")
    ]


def define_command_line_parser(config_files: List[Path]) -> configargparse.ArgParser:
    # Load command line arguments (and from config files)
    arg_parser = configargparse.ArgParser(default_config_files=config_files)

    arg_parser.add_argument(
        "--sync",
        required=True,
        help="Type of sync to perform (choose one of the available presets)",
        choices=policy.SyncActionPresets.get_presets(),
    )
    arg_parser.add_argument(
        "--base_dir", required=True, help="Full path to pictures source_folder"
    )
    arg_parser.add_argument(
        "--mac_photos_library_location",
        required=False,
        help="Full path for Mac Photos library",
    )
    arg_parser.add_argument(
        "--account", required=True, help="Name (nickname) of SmugMug account"
    )
    arg_parser.add_argument(
        "--consumer_key", required=True, help="Smugmug API key of this account"
    )
    arg_parser.add_argument(
        "--consumer_secret", required=True, help="Smugmug API secret of this account"
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
    arg_parser.add_argument("--dry_run", action="store_true", default=False)
    arg_parser.add_argument("--test_upload", action="store_true", default=False)
    arg_parser.add_argument(
        "--log_level",
        required=False,
        choices=["CRITICAL", "DEBUG", "ERROR", "FATAL", "INFO"],
        default="INFO",
    )

    return arg_parser


def configure_logging(log_level: str):
    # Configure logger
    logging.basicConfig(
        stream=sys.stdout,
        level=getattr(logging, log_level),
        format="%(asctime)s - %(message)s",
    )

    # Silence the very verbose networking libraries
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def parse_config() -> Config:
    config_files = get_config_files()
    arg_parser = define_command_line_parser(config_files)

    args = arg_parser.parse_args()

    configure_logging(args.log_level)

    base_dir = Path(args.base_dir).expanduser()
    assert base_dir.exists(), f"Base dir {base_dir} does not exist!"

    method = getattr(policy.SyncActionPresets, args.sync)
    sync = method()

    if args.mac_photos_library_location:
        mac_photos_library_location = Path(args.mac_photos_library_location).expanduser()
        assert mac_photos_library_location.exists(), \
            f"Mac photos library dir {mac_photos_library_location} does not exist!"
    else:
        mac_photos_library_location = None

    connection_params = ConnectionParams(
        account=args.account,
        consumer_key=args.consumer_key,
        consumer_secret=args.consumer_secret,
        access_token=args.access_token,
        access_token_secret=args.access_token_secret,
        test_upload=args.test_upload,
    )

    return Config(
        sync=sync,
        connection_params=connection_params,
        base_dir=base_dir,
        dry_run=args.dry_run,
        mac_photos_library_location=mac_photos_library_location,
    )


config: Config = parse_config()
