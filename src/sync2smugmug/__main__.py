import asyncio
import logging
import pathlib
from typing import Annotated, Optional

import typer

# Import handlers module to register all handlers
from sync2smugmug import (
    handlers,  # noqa
    policy,
    sync,
)
from sync2smugmug.configuration import Config, make_config
from sync2smugmug.online import online
from sync2smugmug.optimizations.disk import execute_optimizations as disk_optimizations
from sync2smugmug.optimizations.online import execute_optimizations as online_optimizations
from sync2smugmug.scan import disk_scanner, online_scanner

logger = logging.getLogger(__name__)

app = typer.Typer(help="Synchronize an image repository on disk with an account on SmugMug")


async def run_sync(config: Config):
    """Execute the synchronization workflow"""
    print(config)

    sync_action = config.sync

    if sync_action.optimize_on_disk:
        await disk_optimizations.run_disk_optimizations(base_dir=config.base_dir, dry_run=config.dry_run)

    async with online.connect(config.connection_params) as connection:
        if sync_action.optimize_online:
            await online_optimizations.run_online_optimizations(
                connection=connection, base_dir=config.base_dir, dry_run=config.dry_run
            )

        if sync_action.upload or sync_action.download:
            on_disk = await disk_scanner.scan(base_dir=config.base_dir)
            logger.info(f"Scan results (on disk): {on_disk.stats}")

            on_smugmug = await online_scanner.scan(connection=connection)
            logger.info(f"Scan results (on smugmug): {on_smugmug.stats}")

            await sync.synchronize(
                on_disk=on_disk,
                on_line=on_smugmug,
                sync_action=sync_action,
                connection=connection,
                dry_run=config.dry_run,
                force_refresh=config.force_refresh,
            )

            sync.print_summary(on_disk, on_smugmug)


@app.command()
def main(
    sync: Annotated[
        str,
        typer.Option(help="Type of sync to perform (choose one of the available presets)"),
    ],
    base_dir: Annotated[pathlib.Path, typer.Option(help="Full path to pictures source_folder")],
    account: Annotated[str, typer.Option(help="Name (nickname) of SmugMug account")],
    consumer_key: Annotated[str, typer.Option(help="Smugmug API key of this account")],
    consumer_secret: Annotated[str, typer.Option(help="Smugmug API secret of this account")],
    access_token: Annotated[str, typer.Option(help="Smugmug oauth token obtained for this script")],
    access_token_secret: Annotated[str, typer.Option(help="Smugmug oauth secret obtained for this script")],
    mac_photos_library_location: Annotated[
        Optional[pathlib.Path], typer.Option(help="Full path for Mac Photos library")
    ] = None,
    force_refresh: Annotated[bool, typer.Option(help="Force refresh of cached data")] = False,
    dry_run: Annotated[bool, typer.Option(help="Perform a dry run without making changes")] = False,
    test_upload: Annotated[bool, typer.Option(help="Enable test upload mode")] = False,
    log_level: Annotated[
        str,
        typer.Option(help="Set the logging level"),
    ] = "INFO",
):
    """
    Synchronize an image repository on disk with an account on SmugMug
    """
    # Validate sync preset
    available_presets = policy.get_presets()
    if sync not in available_presets:
        typer.echo(f"Error: Invalid sync preset '{sync}'. Available presets: {', '.join(available_presets)}")
        raise typer.Exit(code=1)

    # Validate log level
    valid_log_levels = ["CRITICAL", "DEBUG", "ERROR", "FATAL", "INFO"]
    if log_level not in valid_log_levels:
        typer.echo(f"Error: Invalid log level '{log_level}'. Valid levels: {', '.join(valid_log_levels)}")
        raise typer.Exit(code=1)

    # Expand paths
    base_dir = base_dir.expanduser()
    if mac_photos_library_location:
        mac_photos_library_location = mac_photos_library_location.expanduser()

    # Create configuration
    config = make_config(
        sync=sync,
        base_dir=base_dir,
        account=account,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
        mac_photos_library_location=mac_photos_library_location,
        force_refresh=force_refresh,
        dry_run=dry_run,
        test_upload=test_upload,
        log_level=log_level,
    )

    # Run the async workflow
    asyncio.run(run_sync(config))


if __name__ == "__main__":
    app()
