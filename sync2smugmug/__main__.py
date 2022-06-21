import asyncio
import logging

from .config import config
from .disk.optimizer import optimize
from .disk.scanner import DiskScanner
from .policy import SyncTypeAction
from .smugmug.connection import SmugMugConnection
from .smugmug.scanner import SmugmugScanner
from .sync import sync, print_summary

logger = logging.getLogger(__name__)


async def main():
    print(config)

    connection = SmugMugConnection(
        account=config.account,
        consumer_key=config.consumer_key,
        consumer_secret=config.consumer_secret,
        access_token=config.access_token,
        access_token_secret=config.access_token_secret,
        use_test_folder=config.use_test_folder,
    )

    sync_type = config.sync

    async with connection:
        on_disk = DiskScanner(base_dir=config.base_dir).scan()

        if SyncTypeAction.OPTIMIZE_DISK in sync_type:
            # Loop through disk scanning until no optimization left
            while await optimize(on_disk, dry_run=config.dry_run):
                on_disk = DiskScanner(base_dir=config.base_dir).scan()

        logger.info(f"Scan results (on disk): {on_disk.stats()}")

        on_smugmug = await SmugmugScanner(connection=connection).scan()
        logger.info(f"Scan results (on smugmug): {on_smugmug.stats()}")

        diff = await sync(on_disk=on_disk, on_smugmug=on_smugmug, sync_type=sync_type)

    print_summary(on_disk, on_smugmug, diff)


asyncio.run(main())
