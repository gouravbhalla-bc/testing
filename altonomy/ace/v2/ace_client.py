import argparse

from altonomy.ace.console_scripts import pull_nitro_tickers
from altonomy.ace.db.sessions_l2 import SessionLocal
from altonomy.ace.v2.common.xalpha_utils import subscribe_xalpha_deal
from altonomy.ace.v2.feed.ace_feed_create_publisher import \
    AceFeedCreatePublisher
from altonomy.ace.v2.feed.ace_feed_delete_publisher import \
    AceFeedDeletePublisher
from altonomy.ace.v2.feed.ace_manual_feed_create_publisher import \
    AceManualFeedCreatePublisher
from altonomy.ace.v2.feed.xalpha_feed_processor import XAlphaFeedProcessor
from altonomy.ace.v2.trade.ace_trade_create_publisher import \
    AceTradeCreatePublisher
from altonomy.ace.v2.trade.ace_trade_delete_publisher import \
    AceTradeDeletePublisher
from altonomy.ace.v2.trade.xalpha_trade_processor import XAlphaTradeProcessor


def feed():
    db = SessionLocal()
    processor = XAlphaFeedProcessor(db)
    subscribe_xalpha_deal(processor.process_deal, "feed")


def feed_publisher_create():
    publisher = AceFeedCreatePublisher()
    publisher.run()


def feed_publisher_delete():
    publisher = AceFeedDeletePublisher()
    publisher.run()


def manual_feed_publisher_create():
    publisher = AceManualFeedCreatePublisher()
    publisher.run()


def trade():
    db = SessionLocal()
    processor = XAlphaTradeProcessor(db)
    subscribe_xalpha_deal(processor.process_deal, "trade")


def trade_publisher_create():
    publisher = AceTradeCreatePublisher()
    publisher.run()


def trade_publisher_delete():
    publisher = AceTradeDeletePublisher()
    publisher.run()


def main():
    parser = argparse.ArgumentParser(description='Ace V2 Client')
    parser.add_argument('command', type=str, default=None, help='The command to execute')
    args = parser.parse_args()

    cmd = args.command
    if cmd == "trade":
        trade()
    elif cmd == "feed":
        feed()
    elif cmd == "ticker":
        pull_nitro_tickers.main()
    elif cmd == "feed_publisher_create":
        feed_publisher_create()
    elif cmd == "feed_publisher_delete":
        feed_publisher_delete()
    elif cmd == "manual_feed_publisher_create":
        manual_feed_publisher_create()
    elif cmd == "trade_publisher_create":
        trade_publisher_create()
    elif cmd == "trade_publisher_delete":
        trade_publisher_delete()
    else:
        print(f"Unrecognised command: {cmd}")


if __name__ == "__main__":
    main()
