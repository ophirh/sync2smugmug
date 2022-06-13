import asyncio

from .config import config
from .smugmug.connection import SmugMugConnection
from .sync import scan, sync, print_summary


async def main():
    print(config)

    connection = SmugMugConnection(account=config.account,
                                   consumer_key=config.consumer_key,
                                   consumer_secret=config.consumer_secret,
                                   access_token=config.access_token,
                                   access_token_secret=config.access_token_secret,
                                   use_test_folder=config.use_test_folder)

    async with connection:
        on_disk, on_smugmug = await scan(connection)
        diff = await sync(on_disk=on_disk, on_smugmug=on_smugmug)

    print_summary(on_disk, on_smugmug, diff)


asyncio.run(main())
