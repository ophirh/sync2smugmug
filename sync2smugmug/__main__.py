import asyncio
import logging

from sync2smugmug import sync
from sync2smugmug.online import online
from sync2smugmug.configuration import config
from sync2smugmug.optimizations.disk import execute_optimizations as disk_optimizations
from sync2smugmug.optimizations.online import execute_optimizations as online_optimizations
from sync2smugmug.scan import disk_scanner, online_scanner

# Import handlers module to register all handlers
from sync2smugmug import handlers   # noqa

logger = logging.getLogger(__name__)


async def main():
    print(config)

    sync_action = config.sync

    if sync_action.optimize_on_disk:
        await disk_optimizations.run_disk_optimizations(dry_run=config.dry_run)

    async with online.open_smugmug_connection(config.connection_params) as connection:
        if sync_action.optimize_online:
            await online_optimizations.run_online_optimizations(connection=connection, dry_run=config.dry_run)

        on_disk = await disk_scanner.scan(base_dir=config.base_dir)
        logger.info(f"Scan results (on disk): {on_disk.stats}")

        on_smugmug = await online_scanner.scan(connection=connection)
        logger.info(f"Scan results (on smugmug): {on_smugmug.stats}")

        await sync.synchronize(
            on_disk=on_disk,
            on_line=on_smugmug,
            sync_action=sync_action,
            connection=connection,
            dry_run=config.dry_run
        )

    sync.print_summary(on_disk, on_smugmug)


asyncio.run(main())
