from datetime import datetime

from altonomy.ace.enums import CompCode
from altonomy.ace.v2.athena.daos import SnapshotBaseDao
from altonomy.ace.v2.athena.models import SummaryV2Snapshot
from altonomy.ace.v2.athena.snapshot.base import Snapshot
from altonomy.ace.v2.feed.daos import FeedV2Dao, ManualFeedV2Dao
from altonomy.ace.v2.feed.models import FeedV2, ManualFeedV2
from altonomy.ace.v2.ticker.ctrls import TickerCtrl
from sqlalchemy import func


class SummaryV2(Snapshot[SummaryV2Snapshot, FeedV2]):

    def __init__(
        self,
        db,
        portfolio: str,
        asset: str,
        product: str,
        contract: str
    ) -> None:
        item_dao = FeedV2Dao(db, FeedV2)
        manual_item_dao = ManualFeedV2Dao(db, ManualFeedV2)
        snapshot_dao = SnapshotBaseDao(db, SummaryV2Snapshot)
        super().__init__(db, item_dao, snapshot_dao, manual_item_dao=manual_item_dao)

        self.db = db
        self.portfolio = portfolio
        self.asset = asset
        self.product = product
        self.contract = contract

    def reset(self) -> None:
        self.position = 0
        self.last_price = 0
        self.change = 0

    def get_value(self) -> dict:
        return {
            "portfolio": self.portfolio,
            "asset": self.asset,
            "position": self.position,
            "product": self.product,
            "contract": self.contract,
            "last_price": self.last_price,
            "change": self.change,
        }

    def create_snapshot(self) -> SummaryV2Snapshot:
        return SummaryV2Snapshot(
            portfolio=self.portfolio,
            asset=self.asset,
            product=self.product,
            contract=self.contract,
            position=self.position,
        )

    def is_equal_snapshot(self, snapshot: SummaryV2Snapshot) -> bool:
        return (
            self.portfolio == snapshot.portfolio and
            self.asset == snapshot.asset and
            self.position == snapshot.position and
            self.product == snapshot.product and
            self.contract == snapshot.contract
        )

    def read_cached_snapshot(self, cached_snapshot: SummaryV2Snapshot) -> None:
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
            SummaryV2Snapshot.portfolio == self.portfolio,
            SummaryV2Snapshot.contract == self.contract,
            SummaryV2Snapshot.product == self.product,
            SummaryV2Snapshot.asset == func.binary(self.asset),
        )

    def get_item_filters(self):
        return (
            FeedV2.portfolio == self.portfolio,
            FeedV2.product == self.product,
            FeedV2.contract == self.contract,
            FeedV2.asset == func.binary(self.asset),
        )

    def get_manual_item_filters(self):
        return (
            ManualFeedV2.portfolio == self.portfolio,
            ManualFeedV2.product == self.product,
            ManualFeedV2.contract == self.contract,
            ManualFeedV2.asset == func.binary(self.asset),
        )
