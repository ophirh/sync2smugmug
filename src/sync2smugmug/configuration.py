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
    base_dir: pathlib.Path,
    account: str,
    consumer_key: str,
    consumer_secret: str,
    access_token: str,
    access_token_secret: str,
    mac_photos_library_location: pathlib.Path | None = None,
    force_refresh: bool = False,
    dry_run: bool = False,
    test_upload: bool = False,
    log_level: str = "INFO",
) -> Config:
    """
    Create a Config object from the provided parameters
    """
    configure_logging(log_level)

    assert base_dir.exists(), f"Base dir {base_dir} does not exist!"

    preset_method = getattr(policy.SyncActionPresets, sync)
    sync_preset = preset_method()

    if mac_photos_library_location:
        assert mac_photos_library_location.exists(), (
            f"Mac photos library dir {mac_photos_library_location} does not exist!"
        )

    return Config(
        sync=sync_preset,
        connection_params=ConnectionParams(
            account=account,
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            access_token=access_token,
            access_token_secret=access_token_secret,
            test_upload=test_upload,
        ),
        base_dir=base_dir,
        force_refresh=force_refresh,
        dry_run=dry_run,
        mac_photos_library_location=mac_photos_library_location,
    )
