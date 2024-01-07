import traceback
from datetime import datetime
from enum import Enum
from typing import Set, Tuple

from altonomy.ace.v2.athena.snapshot import Settlement, Summary
from altonomy.ace.v2.athena.snapshot.summary_v2 import SummaryV2
from altonomy.ace.v2.feed.daos import FeedV2Dao
from altonomy.ace.v2.feed.models import FeedV2
from altonomy.ace.v2.log_util import get_v2_logger
from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import Session

logger = get_v2_logger("athena_snapshot_processor")


def create_schedule_message(effective_date: datetime, trade_date: datetime) -> dict:
    return {
        "type": "create",
        "effective_date": effective_date,
        "trade_date": trade_date,
    }


def create_invalidate_message(
    portfolio: str, asset: str, trade_date_start: datetime, trade_date_end: datetime
) -> dict:
    return {
        "type": "update",
        "portfolio": portfolio,
        "asset": asset,
        "trade_dates_start": {trade_date_start},
        "trade_date_end": trade_date_end,
    }


def create_invalidate_counterparty_message(
    portfolio: str,
    asset: str,
    counterparty_ref: str,
    counterparty_name: str,
    trade_date_start: datetime,
    trade_date_end: datetime,
) -> dict:
    return {
        "type": "update_counterparty",
        "portfolio": portfolio,
        "asset": asset,
        "counterparty_ref": counterparty_ref,
        "counterparty_name": counterparty_name,
        "trade_dates_start": {trade_date_start},
        "trade_date_end": trade_date_end,
    }


def create_invalidate_summary_v2_message(
    portfolio: str,
    asset: str,
    product: str,
    contract: str,
    trade_date_start: datetime,
    trade_date_end: datetime,
) -> dict:
    return {
        "type": "update_summary_v2",
        "portfolio": portfolio,
        "asset": asset,
        "product": product,
        "contract": contract,
        "trade_dates_start": {trade_date_start},
        "trade_date_end": trade_date_end,
    }


def is_feed_message_mergable(message: dict) -> bool:
    msg_type = message.get("type", "")
    return msg_type in ["update", "update_counterparty", "update_counterparty"]


def hash_feed_message(message: dict) -> Tuple[str, str]:
    msg_type = message.get("type", "")

    if msg_type == "update":
        msg_keys = (
            "portfolio",
            "asset",
        )
    elif msg_type == "update_counterparty":
        msg_keys = (
            "portfolio",
            "asset",
            "counterparty_ref",
            "counterparty_name",
        )
    elif msg_type == "update_summary_v2":
        msg_keys = [
            "portfolio",
            "asset",
            "product",
        ]
        if message.get("contract", "") is not None:
            msg_keys.append("contract")

    else:
        msg_keys = ()

    key = "-".join(str(message.get(msg_key, "")) for msg_key in msg_keys)
    return (msg_type, key)


def merge_feed_message(msg1: dict, msg2: dict) -> dict:
    msg_type = msg1.get("type", "")

    if msg_type == "update":
        return {
            "type": "update",
            "portfolio": msg1.get("portfolio", ""),
            "asset": msg1.get("asset", ""),
            "trade_dates_start": msg1.get("trade_dates_start", set()).union(
                msg2.get("trade_dates_start", set())
            ),
            "trade_date_end": max(
                msg1.get("trade_date_end", datetime.min),
                msg2.get("trade_date_end", datetime.min),
            ),
        }
    elif msg_type == "update_counterparty":
        return {
            "type": "update_counterparty",
            "portfolio": msg1.get("portfolio", ""),
            "asset": msg1.get("asset", ""),
            "counterparty_ref": msg1.get("counterparty_ref", ""),
            "counterparty_name": msg1.get("counterparty_name", ""),
            "trade_dates_start": msg1.get("trade_dates_start", set()).union(
                msg2.get("trade_dates_start", set())
            ),
            "trade_date_end": max(
                msg1.get("trade_date_end", datetime.min),
                msg2.get("trade_date_end", datetime.min),
            ),
        }
    elif msg_type == "update_summary_v2":
        return {
            "type": "update_summary_v2",
            "portfolio": msg1.get("portfolio", ""),
            "asset": msg1.get("asset", ""),
            "product": msg1.get("product", ""),
            "contract": msg1.get("contract", ""),
            "trade_dates_start": msg1.get("trade_dates_start", set()).union(
                msg2.get("trade_dates_start", set())
            ),
            "trade_date_end": max(
                msg1.get("trade_date_end", datetime.min),
                msg2.get("trade_date_end", datetime.min),
            ),
        }
    else:
        return {}


def last_batch_date() -> datetime:
    t = datetime.utcnow()
    return get_batch_before_date(t)


def get_batch_after_date(date: datetime) -> datetime:
    return get_batch_before_date(date) + relativedelta(days=1)


def get_batch_before_date(date: datetime) -> datetime:
    if date.hour < 1:
        date -= relativedelta(days=1)
    return date.replace(hour=1, minute=0, second=0, microsecond=0)


class AthenaSnapshotType(str, Enum):
    Summary = "summary"
    SummaryV2 = "summary-v2"
    Settlement = "settlement"


class AthenaProcessor:
    def __init__(self, db: Session):
        self.db: Session = db
        self.feed_dao: FeedV2Dao = FeedV2Dao(db, FeedV2)

    def write(self, msg) -> None:
        logger.info(str(msg))
        msg_type = msg["type"]

        if msg_type == "update":
            portfolio = msg["portfolio"]
            asset = msg["asset"]
            trade_dates_start = msg["trade_dates_start"]
            trade_date_end = msg["trade_date_end"]
            self.update(portfolio, asset, trade_dates_start, trade_date_end)

        elif msg_type == "update_summary_v2":
            portfolio = msg["portfolio"]
            asset = msg["asset"]
            product = msg["product"]
            contract = msg["contract"]
            trade_dates_start = msg["trade_dates_start"]
            trade_date_end = msg["trade_date_end"]
            self.update_summary_v2(
                portfolio, asset, contract, product, trade_dates_start, trade_date_end
            )

        elif msg_type == "update_counterparty":
            portfolio = msg["portfolio"]
            asset = msg["asset"]
            counterparty_ref = msg["counterparty_ref"]
            counterparty_name = msg["counterparty_name"]
            trade_dates_start = msg["trade_dates_start"]
            trade_date_end = msg["trade_date_end"]
            self.update_counterparty(
                portfolio,
                asset,
                counterparty_ref,
                counterparty_name,
                trade_dates_start,
                trade_date_end,
            )

        elif msg_type == "create":
            trade_date = msg["trade_date"]
            effective_date = msg["effective_date"]
            self.create(trade_date, effective_date)

        else:
            logger.error(f"not recognised {msg}")

    def update(
        self,
        portfolio: str,
        asset: str,
        trade_dates_start: Set[datetime],
        trade_date_end: datetime,
    ) -> None:
        effective_date = trade_date_end

        trade_dates_start = sorted(trade_dates_start)
        for trade_date_start in trade_dates_start:
            logger.info(
                f"UPDATE | portfolio={portfolio} | asset={asset} | effective_date={effective_date} | trade_date_start={trade_date_start} | trade_date_end={trade_date_end}"
            )

            date = get_batch_after_date(trade_date_start)
            while date <= trade_date_end:
                s = Summary(self.db, portfolio, asset)
                s.load_2(date, effective_date)
                is_updated = s.save()
                if not is_updated:
                    logger.info(f"UPDATE | STOP | date={date}")
                    break
                date += relativedelta(days=1)

    def update_summary_v2(
        self,
        portfolio: str,
        asset: str,
        contract: str,
        product: str,
        trade_dates_start: Set[datetime],
        trade_date_end: datetime,
    ) -> None:
        effective_date = trade_date_end

        trade_dates_start = sorted(trade_dates_start)
        for trade_date_start in trade_dates_start:
            logger.info(
                f"UPDATE SUMMARY_V2 | portfolio={portfolio} | asset={asset} | effective_date={effective_date} | trade_date_start={trade_date_start} | trade_date_end={trade_date_end}"
            )

            date = get_batch_after_date(trade_date_start)
            while date <= trade_date_end:
                s = SummaryV2(self.db, portfolio, asset, product, contract)
                s.load_2(date, effective_date)
                is_updated = s.save()
                if not is_updated:
                    logger.info(f"UPDATE SUMMARY_V2 | STOP | date={date}")
                    break
                date += relativedelta(days=1)

    def update_counterparty(
        self,
        portfolio: str,
        asset: str,
        counterparty_ref: str,
        counterparty_name: str,
        trade_dates_start: Set[datetime],
        trade_date_end: datetime,
    ) -> None:
        effective_date = trade_date_end

        trade_dates_start = sorted(trade_dates_start)
        for trade_date_start in trade_dates_start:
            logger.info(
                f"UPDATE COUNTERPARTY | portfolio={portfolio} | asset={asset} | counterparty_ref={counterparty_ref} | counterparty_name={counterparty_name} | effective_date={effective_date} | trade_date_start={trade_date_start} | trade_date_end={trade_date_end}"
            )

            date = get_batch_after_date(trade_date_start)
            while date <= trade_date_end:
                settlement = Settlement(
                    self.db, portfolio, asset, counterparty_ref, counterparty_name
                )
                settlement.load_2(date, effective_date)
                is_updated = settlement.save()
                if not is_updated:
                    logger.info(f"UPDATE COUNTERPARTY | STOP | date={date}")
                    break
                date += relativedelta(days=1)

    def create(
        self, trade_date: datetime, effective_date: datetime, snapshot_types=None
    ) -> None:
        if snapshot_types is None:
            snapshot_types = [
                AthenaSnapshotType.Summary,
                AthenaSnapshotType.SummaryV2,
                AthenaSnapshotType.Settlement,
            ]
        portfolios = self.feed_dao.get_all_portfolios()
        logger.info(
            f"CREATE | len(portfolios)={len(portfolios)} | trade_date={trade_date} | effective_date={effective_date}| types={snapshot_types}"
        )

        for portfolio in portfolios:
            assets = self.feed_dao.get_all_assets(portfolio)
            logger.info(
                f"CREATE | portfolio={portfolio} | len(assets)={len(assets)} | trade_date={trade_date} | effective_date={effective_date}"
            )
            for asset in assets:
                if AthenaSnapshotType.Summary in snapshot_types:
                    s = Summary(self.db, portfolio, asset)
                    s.load(trade_date, effective_date)
                    s.save()

                if AthenaSnapshotType.SummaryV2 in snapshot_types:
                    contract_product_pairs = (
                        self.feed_dao.get_all_product_contract_pair(portfolio, asset)
                    )
                    for contract, product in contract_product_pairs:
                        s = SummaryV2(self.db, portfolio, asset, product, contract)
                        s.load(trade_date, effective_date)
                        s.save()

                if AthenaSnapshotType.Settlement in snapshot_types:
                    counterparties = self.feed_dao.get_all_counterparties(
                        portfolio, asset
                    )
                    for cp_ref, cp_name in counterparties:
                        settlement = Settlement(
                            self.db, portfolio, asset, cp_ref, cp_name
                        )
                        settlement.load(trade_date, effective_date)
                        settlement.save()

    def update_all(
        self, trade_date_start: datetime, trade_date_end: datetime, snapshot_types=None
    ):
        if snapshot_types is None:
            snapshot_types = [
                AthenaSnapshotType.Summary,
                AthenaSnapshotType.SummaryV2,
                AthenaSnapshotType.Settlement,
            ]

        feed_dao = self.feed_dao
        portfolios = feed_dao.get_all_portfolios()
        for portfolio in portfolios:
            assets = feed_dao.get_all_assets(portfolio)
            for asset in assets:
                try:
                    if AthenaSnapshotType.Summary in snapshot_types:
                        logger.info(f"{trade_date_start} | {portfolio} | {asset}")
                        self.update(
                            portfolio, asset, {trade_date_start}, trade_date_end
                        )

                    if AthenaSnapshotType.SummaryV2 in snapshot_types:
                        contract_product_pairs = feed_dao.get_all_product_contract_pair(
                            portfolio, asset
                        )
                        for contract, product in contract_product_pairs:
                            self.update_summary_v2(
                                portfolio,
                                asset,
                                contract,
                                product,
                                {trade_date_start},
                                trade_date_end,
                            )

                    if AthenaSnapshotType.Settlement in snapshot_types:
                        counterparties = feed_dao.get_all_counterparties(
                            portfolio, asset
                        )
                        for cp_ref, cp_name in counterparties:
                            self.update_counterparty(
                                portfolio,
                                asset,
                                cp_ref,
                                cp_name,
                                {trade_date_start},
                                trade_date_end,
                            )
                except Exception:
                    logger.error(traceback.format_exc())
