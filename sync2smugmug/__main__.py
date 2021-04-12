from typing import Tuple

from .config import config
from .policy import SyncType, SyncTypeAction
from .sync import scan, sync, print_summary


def main():
    print(config)

    on_disk, on_smugmug = scan()

    diff = sync(on_disk=on_disk, on_smugmug=on_smugmug)
    print_summary(on_disk, on_smugmug, diff)


main()
