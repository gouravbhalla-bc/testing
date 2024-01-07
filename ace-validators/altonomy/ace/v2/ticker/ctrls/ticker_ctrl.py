from datetime import datetime
import time
from typing import Dict, Tuple
from redis import Redis, StrictRedis
import json
from decimal import Decimal
from functools import lru_cache

from altonomy.ace.daos import TickerFeedDao
from altonomy.ace.models import TickerFeed
from altonomy.ace import config
from sqlalchemy.orm import Session


class TickerCtrl(object):

    def __init__(self, db: Session, trade_redis: Redis = None):
        self.dao = TickerFeedDao(db, TickerFeed)
        self.trade_redis: Redis = trade_redis
        if not trade_redis:
            self.start_trade_redis()

    def start_trade_redis(self, redis_host=None, redis_port=None, redis_password=None, decode_responses=True):
        host = redis_host if redis_host else config.REDIS_HOST_T
        port = redis_port if redis_port else config.REDIS_PORT
        password = redis_password if redis_password else config.REDIS_PASS
        self.trade_redis = self.__get_redis_client(host, port, password, decode_responses)

    def __get_redis_client(self, host, port, password, decode_responses):
        redis = StrictRedis(host=host, port=port, password=password, decode_responses=decode_responses)
        try:
            redis.ping()
            return redis
        except Exception:
            return None

    def get_ticker(
        self,
        base_asset: str,
        quote_asset: str,
        trade_date: datetime,
    ) -> Tuple[float, float]:
        ticker = self.dao.get_asset_latest_ticker_price_at_time(base_asset, quote_asset, trade_date)
        if ticker is None:
            return 0, 0
        else:
            price = ticker.price if ticker.price is not None else 0
            last_price = ticker.last_price if ticker.last_price is not None else 0
            return price, last_price

    @lru_cache(maxsize=config.TICKER_FEED_CACHE_SIZE)
    def get_ticker_by_date_cached(
            self,
            base_asset: str,
            quote_asset: str,
            trade_date: datetime,
    ) -> Tuple[float, float]:
        return self.get_ticker(base_asset, quote_asset, trade_date)[0]

    def get_tickers(
        self,
        quote_asset: str,
        trade_date: datetime,
    ) -> Dict[str, Tuple[float, float]]:
        tickers = self.dao.get_all_asset_latest_ticker_price_at_time(quote_asset, trade_date)
        return {
            ticker.base_asset.upper() if ticker.base_asset is not None else ticker.base_asset: (
                ticker.price if ticker.price is not None else 0,
                ticker.last_price if ticker.last_price is not None else 0,
            )
            for ticker in tickers
        }

    def __get_ticker_key(self, exchange_name: str):
        return f"ticker:{exchange_name}"

    def get_tickers_cached(
            self,
            quote_asset: str,
    ) -> Dict[str, Decimal]:
        pipeline = self.trade_redis.pipeline()
        for exchange in config.TICKER_FEED_SOURCE:
            pipeline.hgetall(self.__get_ticker_key(exchange))
        results = pipeline.execute()
        base_price_map = {
            "USDT": Decimal(1),
            "USD": Decimal(1)
        }
        usd_price_map = {
        }
        for result in results:
            if isinstance(result, dict):
                ts = float(result.get("timestamp", 0))
                raw = result.get("raw", "{}")
                try:
                    if time.time() - ts <= 300:
                        raw = json.loads(raw)
                        for symbol in raw:
                            if symbol.endswith(quote_asset):
                                base = symbol[:-len(quote_asset)]
                                price = raw[symbol].get("price", 0)
                                if base not in base_price_map and price > 0:
                                    base_price_map.update({base.upper(): Decimal(price)})
                            elif quote_asset == "USDT" and symbol.endswith("USD"):
                                # Add USD as backup price
                                base = symbol[:-len("USD")]
                                price = raw[symbol].get("price", 0)
                                if base not in usd_price_map and price > 0:
                                    usd_price_map.update({base.upper(): Decimal(price)})
                except BaseException:
                    pass
        for base, price in usd_price_map.items():
            if base not in base_price_map:
                base_price_map.update({base: price})
        return base_price_map
