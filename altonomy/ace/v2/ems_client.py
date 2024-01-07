import argparse
from altonomy.ace.v2.ems.ems_ctrl import EMSCtrl
import logging
from altonomy.ace import config
import sys

logger = logging.getLogger()
logger.setLevel(config.LOGGING_LEVEL)
handler = logging.StreamHandler(sys.stdout)
logger.addHandler(handler)


def sync_balance(batch_size=50):
    ems_ctrl = EMSCtrl(True)
    ems_ctrl.sync_balance(batch_size)


def main():
    parser = argparse.ArgumentParser(description='EMS Client')
    parser.add_argument('command', type=str, default=None, help='The command to execute')
    args = parser.parse_args()
    cmd = args.command
    if cmd == "sync_balance":
        sync_balance()
    else:
        print(f"Unrecognised command: {cmd}")


if __name__ == "__main__":
    main()
