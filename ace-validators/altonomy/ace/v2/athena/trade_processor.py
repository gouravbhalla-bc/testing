from datetime import datetime
from typing import Set, Tuple

from altonomy.ace.v2.athena.processor import (get_batch_after_date,
                                              last_batch_date)
from altonomy.ace.v2.athena.snapshot import Position
from altonomy.ace.v2.log_util import get_v2_logger
from altonomy.ace.v2.trade.daos import TradeV2Dao
from altonomy.ace.v2.trade.models import TradeV2
from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import Session

logger = get_v2_logger("athena_trade_snapshot_processor")


def create_trade_schedule_message(effective_date: datetime, trade_date: datetime) -> dict:
    return {
        "type": "create",
        "effective_date": effective_date,
        "trade_date": trade_date,
    }


def create_trade_invalidate_message(
    portfolio: str,
    base_asset: str,
    quote_asset: str,
    trade_date_start: datetime,
    trade_date_end: datetime,
) -> dict:
    return {
        "type": "update",
        "portfolio": portfolio,
        "base_asset": base_asset,
        "quote_asset": quote_asset,
        "trade_dates_start": {trade_date_start},
        "trade_date_end": trade_date_end,
    }


def is_trade_message_mergable(message: dict) -> bool:
    msg_type = message.get("type", "")
    return msg_type == "update"


def hash_trade_message(message: dict) -> Tuple[str, str]:
    msg_type = message.get("type", "")

    if msg_type == "update":
        msg_keys = (
            "portfolio",
            "base_asset",
            "quote_asset",
        )
    else:
        msg_keys = ()

    key = "-".join(str(message.get(msg_key, "")) for msg_key in msg_keys)
    return (msg_type, key)


def merge_trade_message(msg1: dict, msg2: dict) -> dict:
    msg_type = msg1.get("type", "")

    if msg_type == "update":
        return {
            "type": "update",
            "portfolio": msg1.get("portfolio", ""),
            "base_asset": msg1.get("base_asset", ""),
            "quote_asset": msg1.get("quote_asset", ""),
            "trade_dates_start": msg1.get("trade_dates_start", set()).union(msg2.get("trade_dates_start", set())),
            "trade_date_end": max(msg1.get("trade_date_end", datetime.min), msg2.get("trade_date_end", datetime.min)),
        }
    else:
        return {}


def last_trade_batch_date() -> datetime:
    return last_batch_date()


class AthenaTradeProcessor:

    def __init__(self, db: Session):
        self.db: Session = db
        self.trade_dao: TradeV2Dao = TradeV2Dao(db, TradeV2)

    def write(self, msg) -> None:
        logger.info(str(msg))
        msg_type = msg["type"]

        if msg_type == "update":
            portfolio = msg["portfolio"]
            base_asset = msg["base_asset"]
            quote_asset = msg["quote_asset"]
            trade_dates_start = msg["trade_dates_start"]
            trade_date_end = msg["trade_date_end"]
            self.update(portfolio, base_asset, quote_asset, trade_dates_start, trade_date_end)

        elif msg_type == "create":
            trade_date = msg["trade_date"]
            effective_date = msg["effective_date"]
            self.create(trade_date, effective_date)

        else:
            logger.error(f"not recognised {msg}")

    def update(
        self,
        portfolio: str,
        base_asset: str,
        quote_asset: str,
        trade_dates_start: Set[datetime],
        trade_date_end: datetime,
    ) -> None:
        effective_date = trade_date_end

        trade_dates_start = sorted(trade_dates_start)
        for trade_date_start in trade_dates_start:
            logger.info(f"UPDATE | portfolio={portfolio} | base_asset={base_asset} | quote_asset={quote_asset} | effective_date={effective_date} | trade_date_start={trade_date_start} | trade_date_end={trade_date_end}")

            date = get_batch_after_date(trade_date_start)
            while date <= trade_date_end:
                p = Position(self.db, portfolio, base_asset, quote_asset)
                p.load_2(date, effective_date)
                is_updated = p.save()
                if not is_updated:
                    logger.info(f"UPDATE | STOP | date={date}")
                    break
                date += relativedelta(days=1)

    def create(self, trade_date: datetime, effective_date: datetime) -> None:
        portfolios = self.trade_dao.get_all_portfolios()
        logger.info(f"CREATE | len(portfolios)={len(portfolios)} | trade_date={trade_date} | effective_date={effective_date}")
        for portfolio in portfolios:
            pairs = self.trade_dao.get_all_pairs(portfolio)
            logger.info(f"CREATE | portfolio={portfolio} | len(pairs)={len(pairs)} | trade_date={trade_date} | effective_date={effective_date}")
            for base_asset, quote_asset in pairs:
                p = Position(self.db, portfolio, base_asset, quote_asset)
                p.load(trade_date, effective_date)
                p.save()
