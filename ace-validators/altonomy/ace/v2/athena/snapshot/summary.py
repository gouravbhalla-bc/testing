from datetime import datetime

from altonomy.ace.enums import CompCode
from altonomy.ace.v2.athena.daos import SnapshotBaseDao
from altonomy.ace.v2.athena.models import SummarySnapshot
from altonomy.ace.v2.athena.snapshot.base import Snapshot
from altonomy.ace.v2.feed.daos import FeedV2Dao, ManualFeedV2Dao
from altonomy.ace.v2.feed.models import FeedV2, ManualFeedV2
from altonomy.ace.v2.ticker.ctrls import TickerCtrl
from sqlalchemy import func


class Summary(Snapshot[SummarySnapshot, FeedV2]):

    def __init__(
        self,
        db,
        portfolio: str,
        asset: str,
    ) -> None:
        item_dao = FeedV2Dao(db, FeedV2)
        manual_item_dao = ManualFeedV2Dao(db, ManualFeedV2)

        snapshot_dao = SnapshotBaseDao(db, SummarySnapshot)
        super().__init__(db, item_dao, snapshot_dao, manual_item_dao=manual_item_dao)

        self.db = db
        self.portfolio = portfolio
        self.asset = asset

    def reset(self) -> None:
        self.position = 0
        self.last_price = 0
        self.change = 0

    def get_value(self) -> dict:
        return {
            "portfolio": self.portfolio,
            "asset": self.asset,
            "position": self.position,
            "last_price": self.last_price,
            "change": self.change,
        }

    def create_snapshot(self) -> SummarySnapshot:
        return SummarySnapshot(
            portfolio=self.portfolio,
            asset=self.asset,
            position=self.position,
        )

    def is_equal_snapshot(self, snapshot: SummarySnapshot) -> bool:
        return (
            self.portfolio == snapshot.portfolio and
            self.asset == snapshot.asset and
            self.position == snapshot.position
        )

    def read_cached_snapshot(self, cached_snapshot: SummarySnapshot) -> None:
        self.position = cached_snapshot.position

    def process_item(self, item: FeedV2) -> None:
        if item.comp_code in (
            CompCode.EXECUTION_START,
            CompCode.EXECUTION_END,
            CompCode.EXECUTION_CASHFLOW_START,
            CompCode.EXECUTION_CASHFLOW_END,
        ):
            return

        self.position += item.amount

    def post_load(self, trade_date: datetime, effective_date: datetime) -> None:
        ticker_dao = TickerCtrl(self.db)
        price, last_price = ticker_dao.get_ticker(self.asset, "USDT", trade_date)
        self.last_price = price
        self.change = (price - last_price) / price if price != 0 else 0

    def get_snapshot_filters(self):
        return (
            SummarySnapshot.portfolio == self.portfolio,
            SummarySnapshot.asset == func.binary(self.asset),
        )

    def get_item_filters(self):
        return (
            FeedV2.portfolio == self.portfolio,
            FeedV2.asset == func.binary(self.asset),
        )

    def get_manual_item_filters(self):
        return (
            ManualFeedV2.portfolio == self.portfolio,
            ManualFeedV2.asset == func.binary(self.asset),
        )
