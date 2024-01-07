from datetime import datetime

from altonomy.ace.enums import CompCode, FeedType
from altonomy.ace.v2.athena.daos import SnapshotBaseDao
from altonomy.ace.v2.athena.models import SettlementSnapshot
from altonomy.ace.v2.athena.snapshot.base import Snapshot
from altonomy.ace.v2.feed.daos import FeedV2Dao, ManualFeedV2Dao
from altonomy.ace.v2.feed.models import FeedV2
from altonomy.ace.v2.feed.models.manual_feed_v2 import ManualFeedV2
from altonomy.ace.v2.ticker.ctrls import TickerCtrl
from sqlalchemy import func


class Settlement(Snapshot[SettlementSnapshot, FeedV2]):

    def __init__(
        self,
        db,
        portfolio: str,
        asset: str,
        counterparty_ref: str,
        counterparty_name: str,
    ) -> None:
        item_dao = FeedV2Dao(db, FeedV2)
        manual_item_dao = ManualFeedV2Dao(db, ManualFeedV2)

        snapshot_dao = SnapshotBaseDao(db, SettlementSnapshot)
        super().__init__(db, item_dao, snapshot_dao, manual_item_dao=manual_item_dao)

        self.db = db
        self.portfolio = portfolio
        self.asset = asset
        self.counterparty_ref = counterparty_ref
        self.counterparty_name = counterparty_name

    def reset(self) -> None:
        self.position = 0
        self.net_exposure = 0

    def get_value(self) -> dict:
        return {
            "portfolio": self.portfolio,
            "counterparty_ref": self.counterparty_ref,
            "counterparty_name": self.counterparty_name,
            "asset": self.asset,
            "position": self.position,
            "net_exposure": self.net_exposure,
        }

    def create_snapshot(self) -> SettlementSnapshot:
        return SettlementSnapshot(
            portfolio=self.portfolio,
            asset=self.asset,
            counterparty_ref=self.counterparty_ref,
            counterparty_name=self.counterparty_name,
            position=self.position,
        )

    def is_equal_snapshot(self, snapshot: SettlementSnapshot) -> bool:
        return (
            self.portfolio == snapshot.portfolio and
            self.asset == snapshot.asset and
            self.counterparty_ref == snapshot.counterparty_ref and
            self.counterparty_name == snapshot.counterparty_name and
            self.position == snapshot.position
        )

    def read_cached_snapshot(self, cached_snapshot: SettlementSnapshot) -> None:
        self.position = cached_snapshot.position

    def process_item(self, item: FeedV2) -> None:
        if item.comp_code in (
            CompCode.EXECUTION_START,
            CompCode.EXECUTION_END,
            CompCode.EXECUTION_CASHFLOW_START,
            CompCode.EXECUTION_CASHFLOW_END
        ):
            return
        if item.feed_type != FeedType.PV:
            return

        self.position += item.amount

    def post_load(self, trade_date: datetime, effective_date: datetime) -> None:
        ticker_dao = TickerCtrl(self.db)
        price, _last_price = ticker_dao.get_ticker(self.asset, "USDT", trade_date)
        self.net_exposure = self.position * price

    def get_snapshot_filters(self):
        return (
            SettlementSnapshot.portfolio == self.portfolio,
            SettlementSnapshot.asset == func.binary(self.asset),
            SettlementSnapshot.counterparty_ref == self.counterparty_ref,
            SettlementSnapshot.counterparty_name == self.counterparty_name,
        )

    def get_item_filters(self):
        return (
            FeedV2.portfolio == self.portfolio,
            FeedV2.asset == func.binary(self.asset),
            FeedV2.counterparty_ref == self.counterparty_ref,
            FeedV2.counterparty_name == self.counterparty_name,
        )

    def get_manual_item_filters(self):
        return (
            ManualFeedV2.portfolio == self.portfolio,
            ManualFeedV2.asset == func.binary(self.asset),
            ManualFeedV2.counterparty_ref == self.counterparty_ref,
            ManualFeedV2.counterparty_name == self.counterparty_name,
        )
