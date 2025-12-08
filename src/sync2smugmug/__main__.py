import asyncio
import logging
from typing import Annotated

import typer
from rich.console import Console

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
console = Console()

app = typer.Typer(help="Synchronize an image repository on disk with an account on SmugMug")


async def run_sync(config: Config):
    """
    Execute the synchronization workflow
    """
    console.print(config)

    sync_action = config.sync

    if sync_action.optimize_on_disk:
        await disk_optimizations.run_disk_optimizations(base_dir=config.base_dir, dry_run=config.dry_run)

    async with online.connect(config.connection_params) as connection:
        if sync_action.optimize_online:
            await online_optimizations.run_online_optimizations(
                connection=connection,
                base_dir=config.base_dir,
                dry_run=config.dry_run,
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
    sync: Annotated[str, typer.Option(help="Type of sync to perform (choose one of the available presets)")],
    force_refresh: Annotated[bool, typer.Option(help="Force refresh of cached data")] = False,
    dry_run: Annotated[bool, typer.Option(help="Perform a dry run without making changes")] = False,
    test_upload: Annotated[bool, typer.Option(help="Enable test upload mode")] = False,
    log_level: Annotated[str, typer.Option(help="Set the logging level")] = "INFO",
):
    """
    Synchronize an image repository on disk with an account on SmugMug.

    Configuration can be provided via command line options or config files.
    Config files are loaded from: sync2smugmug.conf and sync2smugmug.my.conf
    Command line options override config file values.
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

    try:
        config = make_config(
            sync=sync,
            force_refresh=force_refresh,
            dry_run=dry_run,
            test_upload=test_upload,
            log_level=log_level,
        )

    except ValueError as e:
        typer.echo(f"Configuration error: {e}", err=True)
        raise typer.Exit(code=1)

    # Run the async workflow
    asyncio.run(run_sync(config))


if __name__ == "__main__":
    app()
