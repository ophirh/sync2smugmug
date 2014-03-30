from .scan import Scanner


def scan(base_dir, nickname, action):
    s = Scanner(base_dir=base_dir, nickname=nickname)
    s.print_status()

    if action == 'upload':
        s.upload()

    if action == 'download':
        s.download()

    if action == 'sync':
        s.sync()

    # TODO: Sync meta data for album