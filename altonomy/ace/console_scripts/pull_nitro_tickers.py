from altonomy.ace.v2.log_util import get_v2_logger
import time

from altonomy.ace.db.sessions_l2 import SessionLocal
from altonomy.ace.external.nitro_client import get_asset_price
from altonomy.ace.models import TickerFeed
from datetime import datetime


logger = get_v2_logger("pull_nitro_tickers")


class PullNitroTickers:
    def __init__(self):
        self.db = SessionLocal()
        self.pairs = []
        self.ticker = {}
        self.trigger_time = datetime.utcnow()

    def run(self):
        s = time.time()
        self.prepare_pairs()
        self.get_prices()
        self.store_to_db()
        logger.debug(
            f"========== {len(self.ticker)} tickers added, {time.time() - s} sec."
        )

    def prepare_pairs(self):
        results = self.db.execute(
            """
            SELECT DISTINCT asset
            FROM feed_v2;
        """
        )
        for row in results:
            asset = row[0]
            if asset:
                self.pairs.append(f"{asset}_USDT")

    def get_prices(self):
        ts = datetime.utcnow().timestamp()
        self.ticker = get_asset_price(self.pairs, ts)

    def store_to_db(self):
        ticker_feeds = []
        for pair in self.ticker:
            base_asset, quote_asset = pair.rsplit("_", 1)
            price = self.ticker[pair]
            if base_asset == quote_asset:
                price = 1
            if base_asset in ("USDT", "USD", "USDC") and quote_asset == "USDT":
                price = 1
            # print(base_asset, quote_asset, price)
            tf = TickerFeed(
                base_asset=base_asset,
                quote_asset=quote_asset,
                price=price,
                effective_date=self.trigger_time,
            )
            ticker_feeds.append(tf)
        if len(ticker_feeds):
            self.db.bulk_save_objects(ticker_feeds)
            self.db.commit()


def main():
    pnt = PullNitroTickers()
    pnt.run()


if __name__ == "__main__":
    main()
