from .policy import *
from .scan import Scanner


def scan(base_dir, nickname, reset):
    s = Scanner(base_dir=base_dir, nickname=nickname, reset_cache=reset)
    s.print_status()
    s.sync(policy=POLICY_DISK_RULES)
