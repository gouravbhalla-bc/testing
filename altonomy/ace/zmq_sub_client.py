import argparse
import json
import logging
import time
import zmq

from altonomy.ace import config
from altonomy.ace.ctrls import SystemFeedCtrl
from altonomy.ace.db.sessions_l2 import SessionLocal
from datetime import datetime
from datetime import timedelta
from logging.handlers import RotatingFileHandler


class ZmqSubClient(object):

    def __init__(self, zep: str, app_log: any):
        context = zmq.Context()
        self.socket = context.socket(zmq.SUB)
        self.socket.connect(zep)
        self.socket.subscribe("")
        session = SessionLocal()
        self.ctrl = SystemFeedCtrl(session)
        self.app_log = app_log

    def safe_run(self):
        while True:
            try:
                message = self.socket.recv_string()
                self.app_log.debug(f"========> {message}")
                if message is not None:
                    data = json.loads(message)
                    batch_date = datetime.utcnow()
                    as_of = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
                    self.ctrl.handle_xalpha_deals([data], batch_date, True, as_of)
            except Exception as e:
                self.app_log.error(str(e))
            finally:
                time.sleep(0.001)

    def fast_run(self, batch_size: int = 20):
        tick = 0
        batch_deals = []
        while True:
            try:
                if len(batch_deals) >= batch_size or tick >= 150:
                    batch_date = datetime.utcnow()
                    as_of = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
                    self.ctrl.handle_xalpha_deals(batch_deals, batch_date, True, as_of)
                    batch_deals.clear()
                    tick = 0
                message = self.socket.recv_string(zmq.NOBLOCK)
                self.app_log.debug(f"========> {message}")
                if message is not None:
                    data = json.loads(message)
                    batch_deals.append(data)
            except Exception as e:
                if str(e) != "Resource temporarily unavailable":
                    self.app_log.error(str(e))
                time.sleep(0.1)
                tick += 1
            finally:
                time.sleep(0.001)


def main():
    parser = argparse.ArgumentParser(description='Fetch Deals from XAlpha, convert and insert into Ace feeds.')
    parser.add_argument('-cl', '--channel', type=str, dest="channel", help='Override subscribe channel')
    parser.set_defaults(channel=None)
    parser.add_argument('-fr', '--fast_run', type=int, dest="fast_run_size", help='Fast run mode with batch size')
    parser.set_defaults(fast_run_size=None)
    args = parser.parse_args()

    channel = args.channel
    zep = config.XALPHA_ZMQ_EP if channel is None else channel

    fast_run_size = args.fast_run_size

    channel_name = ""
    if channel is not None:
        channel_name = channel.replace(":", "_").replace(".", "_").replace("/", "")

    # logging.basicConfig(level=logging.DEBUG, filename=f"zmq_subscribe_{channel_name}.log", filemode="w")
    logging_handler = RotatingFileHandler(f"zmq_subscribe_{channel_name}.log", mode='a', maxBytes=5 * 1024 * 1024, backupCount=5, encoding=None, delay=0)
    logging_handler.setLevel(logging.DEBUG)
    app_log = logging.getLogger('root')
    app_log.setLevel(logging.DEBUG)
    app_log.addHandler(logging_handler)

    zsc = ZmqSubClient(zep, app_log)
    if fast_run_size is not None:
        zsc.fast_run(fast_run_size)
    else:
        zsc.safe_run()


if __name__ == "__main__":
    main()
