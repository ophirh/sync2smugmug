import sys
import logging
from argparse import ArgumentParser
from scanner import scan


def setup_logger():
    root = logging.getLogger()
    root.setLevel(level=10)  # CRITICAL(50) / ERROR(40) / WARNING(30) / INFO(20) / DEBUG(10) / NOTSET(0)

    # Reset all existing handlers
    for h in root.handlers:
        root.removeHandler(h)

    formatter = logging.Formatter('%(message)s')
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    root.addHandler(ch)


def main():
    setup_logger()

    arg_parser = ArgumentParser(version='Picasa2Smugmug 0.1')
    arg_parser.add_argument('--path', required=True, help='Directory to start scanning from')
    arg_parser.add_argument('--nickname', required=True, help='Nickname of SmugMug account')
    # arg_parser.add_argument('--action',
    #                         choices=['download', 'upload', 'sync'],
    #                         default='upload',
    #                         help='Action to perform')
    args = vars(arg_parser.parse_args())

    scan(base_dir=args['path'], nickname=args['nickname'], reset=False)


if __name__ == '__main__': main()