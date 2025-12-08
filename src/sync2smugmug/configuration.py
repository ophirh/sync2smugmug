import dataclasses
import logging
import pathlib
import sys

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
    mac_photos_library_location: pathlib.Path | None = None


def get_config_files() -> list[pathlib.Path]:
    """
    Resolve a list of config file paths to be read (in that order) into the Configuration object
    """
    config_files_dir_path = pathlib.Path(__file__).parent.parent.parent.resolve()
    return [
        config_files_dir_path.joinpath(config_file_name)
        for config_file_name in ("sync2smugmug.conf", "sync2smugmug.my.conf")
    ]


def load_config_from_files() -> dict[str, str]:
    """
    Load configuration from config files if they exist.
    Returns a dictionary of configuration values.

    The config files use a simple key=value format (like ConfigArgParse).
    Lines starting with # or ; are comments.
    """
    config_dict = {}
    config_files = get_config_files()

    for config_file in config_files:
        if not config_file.exists():
            continue

        with config_file.open() as f:
            for line in f:
                line = line.strip()

                # Skip empty lines and comments
                if not line or line.startswith("#") or line.startswith(";"):
                    continue

                # Parse key=value
                if "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()

                    # Only add non-empty values
                    if value:
                        config_dict[key] = value

    return config_dict


def configure_logging(log_level: str):
    """
    Configure logging
    """
    # Work around a problem with osxphotos (it calls logging.basicConfig directly, so we want to import it first, then
    # override with our own basicConfig)
    import osxphotos  # noqa

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


def make_config(
    sync: str,
    force_refresh: bool = False,
    dry_run: bool = False,
    test_upload: bool = False,
    log_level: str = "INFO",
) -> Config:
    """
    Create a Config object from the provided parameters, loading defaults from config files.
    Command line parameters override config file values.
    """
    # Load config from files
    file_config = load_config_from_files()

    # Helper function to get value from CLI or config file
    def get_value(key: str, required: bool = True) -> str:
        value = file_config.get(key)
        if required and not value:
            raise ValueError(f"Required parameter '{key}' not provided via CLI or config file")
        return value

    def get_path_value(cli_value: pathlib.Path | None, key: str, required: bool = True) -> pathlib.Path | None:
        if cli_value is not None:
            return cli_value

        file_value = file_config.get(key)
        if file_value:
            return pathlib.Path(file_value).expanduser()

        if required:
            raise ValueError(f"Required parameter '{key}' not provided via CLI or config file")

        return None

    # Get values with fallback to config files
    base_dir_value = get_path_value("base_dir")
    account_value = get_value("account")
    consumer_key_value = get_value("consumer_key")
    consumer_secret_value = get_value("consumer_secret")
    access_token_value = get_value("access_token")
    access_token_secret_value = get_value("access_token_secret")
    mac_photos_library_location_value = get_path_value("mac_photos_library_location", required=False)

    configure_logging(log_level)

    assert base_dir_value.exists(), f"Base dir {base_dir_value} does not exist!"

    preset_method = getattr(policy.SyncActionPresets, sync)
    sync_preset = preset_method()

    if mac_photos_library_location_value:
        assert mac_photos_library_location_value.exists(), (
            f"Mac photos library dir {mac_photos_library_location_value} does not exist!"
        )

    return Config(
        sync=sync_preset,
        connection_params=ConnectionParams(
            account=account_value,
            consumer_key=consumer_key_value,
            consumer_secret=consumer_secret_value,
            access_token=access_token_value,
            access_token_secret=access_token_secret_value,
            test_upload=test_upload,
        ),
        base_dir=base_dir_value,
        force_refresh=force_refresh,
        dry_run=dry_run,
        mac_photos_library_location=mac_photos_library_location_value,
    )
