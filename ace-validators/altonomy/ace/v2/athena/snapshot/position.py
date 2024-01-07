from datetime import datetime
from decimal import Decimal

from altonomy.ace.enums import Product, TransferType
from altonomy.ace.v2.athena.daos import SnapshotBaseDao
from altonomy.ace.v2.athena.models import PositionSnapshot
from altonomy.ace.v2.athena.snapshot.base import Snapshot
from altonomy.ace.v2.ticker.ctrls import TickerCtrl
from altonomy.ace.v2.trade.daos import TradeV2Dao
from altonomy.ace.v2.trade.models import TradeV2
from sqlalchemy import func


class Position(Snapshot[PositionSnapshot, TradeV2]):
    '''
    source: https://lichgo.github.io/2015/10/29/40-lines-pnl-calculation.html
    '''

    def __init__(
        self,
        db,
        portfolio: str,
        base_asset: str,
        quote_asset: str,
    ) -> None:
        item_dao = TradeV2Dao(db, TradeV2)
        snapshot_dao = SnapshotBaseDao(db, PositionSnapshot)
        super().__init__(db, item_dao, snapshot_dao)

        self.db = db
        self.portfolio = portfolio
        self.base_asset = base_asset
        self.quote_asset = quote_asset

    def reset(self) -> None:
        self.net_position = 0
        self.net_investment = 0
        self.avg_open_price = 0
        self.realized_pnl = 0
        self.unrealized_pnl = 0
        self.total_pnl = self.realized_pnl + self.unrealized_pnl
        self.realized_pnl_usd = 0
        self.unrealized_pnl_usd = 0
        self.total_pnl_usd = self.realized_pnl_usd + self.unrealized_pnl_usd
        self.total_buy_quantity = 0
        self.total_sell_quantity = 0
        self.average_buy_price = 0
        self.average_sell_price = 0
        self.last_price = 0
        self.market_price = 0

    def get_value(self) -> dict:
        return {
            "portfolio": self.portfolio,
            "base_asset": self.base_asset,
            "quote_asset": self.quote_asset,
            "net_position": self.net_position,
            "net_investment": self.net_investment,
            "avg_open_price": self.avg_open_price,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "total_pnl": self.total_pnl,
            "realized_pnl_usd": self.realized_pnl_usd,
            "unrealized_pnl_usd": self.unrealized_pnl_usd,
            "total_pnl_usd": self.total_pnl_usd,
            "total_buy_quantity": self.total_buy_quantity,
            "total_sell_quantity": self.total_sell_quantity,
            "average_buy_price": self.average_buy_price,
            "average_sell_price": self.average_sell_price,
            "last_price": self.last_price,
            "market_price": self.market_price,
        }

    def create_snapshot(self) -> PositionSnapshot:
        return PositionSnapshot(
            portfolio=self.portfolio,
            base_asset=self.base_asset,
            quote_asset=self.quote_asset,
            net_position=self.net_position,
            net_investment=self.net_investment,
            avg_open_price=self.avg_open_price,
            realized_pnl=self.realized_pnl,
            unrealized_pnl=self.unrealized_pnl,
            total_pnl=self.total_pnl,
            realized_pnl_usd=self.realized_pnl_usd,
            unrealized_pnl_usd=self.unrealized_pnl_usd,
            total_pnl_usd=self.total_pnl_usd,
            total_buy_quantity=self.total_buy_quantity,
            total_sell_quantity=self.total_sell_quantity,
            average_buy_price=self.average_buy_price,
            average_sell_price=self.average_sell_price,
            last_price=self.last_price,
            market_price=self.market_price,
        )

    def is_equal_snapshot(self, snapshot: PositionSnapshot) -> bool:
        return (
            self.portfolio == snapshot.portfolio and
            self.base_asset == snapshot.base_asset and
            self.quote_asset == snapshot.quote_asset and
            self.net_position == snapshot.net_position and
            self.net_investment == snapshot.net_investment and
            self.avg_open_price == snapshot.avg_open_price and
            self.realized_pnl == snapshot.realized_pnl and
            self.unrealized_pnl == snapshot.unrealized_pnl and
            self.total_pnl == snapshot.total_pnl and
            self.realized_pnl_usd == snapshot.realized_pnl_usd and
            self.unrealized_pnl_usd == snapshot.unrealized_pnl_usd and
            self.total_pnl_usd == snapshot.total_pnl_usd and
            self.total_buy_quantity == snapshot.total_buy_quantity and
            self.total_sell_quantity == snapshot.total_sell_quantity and
            self.average_buy_price == snapshot.average_buy_price and
            self.average_sell_price == snapshot.average_sell_price and
            self.last_price == snapshot.last_price and
            self.market_price == snapshot.market_price
        )

    def read_cached_snapshot(self, cached_snapshot: PositionSnapshot) -> None:
        self.net_position = cached_snapshot.net_position
        self.net_investment = cached_snapshot.net_investment
        self.avg_open_price = cached_snapshot.avg_open_price
        self.realized_pnl = cached_snapshot.realized_pnl
        self.unrealized_pnl = cached_snapshot.unrealized_pnl
        self.total_pnl = cached_snapshot.total_pnl
        self.realized_pnl_usd = cached_snapshot.realized_pnl_usd
        self.unrealized_pnl_usd = cached_snapshot.unrealized_pnl_usd
        self.total_pnl_usd = cached_snapshot.total_pnl_usd
        self.total_buy_quantity = cached_snapshot.total_buy_quantity
        self.total_sell_quantity = cached_snapshot.total_sell_quantity
        self.average_buy_price = cached_snapshot.average_buy_price
        self.average_sell_price = cached_snapshot.average_sell_price
        self.last_price = cached_snapshot.last_price
        self.market_price = cached_snapshot.market_price

    def pre_load(self, trade_date: datetime, effective_date: datetime) -> None:
        ticker_dao = TickerCtrl(self.db)
        base_price, _ = ticker_dao.get_ticker(self.base_asset, "USDT", trade_date)
        quote_price, _ = ticker_dao.get_ticker(self.quote_asset, "USDT", trade_date)

        self.base_ticker_price = base_price
        self.quote_ticker_price = quote_price

    def process_item(self, item: TradeV2) -> None:
        if item.transfer_type != TransferType.TRADE:
            return

        market_price = Decimal(self.base_ticker_price / self.quote_ticker_price) if self.quote_ticker_price != 0 else Decimal(0)

        if item.product == Product.EXECUTION:
            self._update_by_tradefeed_execution(item.fee_amount, self.quote_ticker_price)
            self._update_by_marketdata(market_price, self.quote_ticker_price)

        elif item.product != Product.EXECUTION and item.base_amount != 0:
            self._update_by_tradefeed(item.base_amount, item.quote_amount, self.quote_ticker_price)
            self._update_by_marketdata(market_price, self.quote_ticker_price)

    def _cal_avg_price(self, curr_average_price, total_quantity, traded_quantity, traded_price):
        return (curr_average_price * (total_quantity - traded_quantity) + traded_price * traded_quantity) / total_quantity

    def _update_by_tradefeed_execution(
        self,
        fee_quantity: Decimal,
        quote_asset_ticker_price: Decimal,
    ):
        # realized pnl
        self.realized_pnl += fee_quantity
        self.realized_pnl_usd = quote_asset_ticker_price * self.realized_pnl

        # total pnl
        self.total_pnl = self.realized_pnl + self.unrealized_pnl
        self.total_pnl_usd = self.total_pnl * quote_asset_ticker_price

    def _update_by_tradefeed(
        self,
        base_amount: Decimal,
        quote_amount: Decimal,
        quote_asset_ticker_price: Decimal,
    ):
        buy_or_sell = 1 if base_amount > 0 else 2
        traded_price = abs(quote_amount / base_amount) if base_amount != 0 else 0
        traded_quantity = abs(base_amount)

        self.last_price = traded_price
        # buy_or_sell: 1 is buy, 2 is sell
        # buy: positive position, sell: negative position
        if buy_or_sell == 1:
            self.total_buy_quantity += traded_quantity
            if self.total_buy_quantity > 0:
                self.average_buy_price = self._cal_avg_price(self.average_buy_price, self.total_buy_quantity, traded_quantity, traded_price)
        elif buy_or_sell == 2:
            self.total_sell_quantity += traded_quantity
            if self.total_sell_quantity > 0:
                self.average_sell_price = self._cal_avg_price(self.average_sell_price, self.total_sell_quantity, traded_quantity, traded_price)

        quantity_with_direction = traded_quantity if buy_or_sell == 1 else (-1) * traded_quantity
        is_still_open = (self.net_position * quantity_with_direction) >= 0

        # net investment
        self.net_investment = max(self.net_investment, abs(self.net_position * self.avg_open_price))

        # realized pnl
        if not is_still_open:
            # Remember to keep the sign as the net position
            self.realized_pnl += (traded_price - self.avg_open_price) * min(
                abs(quantity_with_direction),
                abs(self.net_position)
            ) * (abs(self.net_position) / self.net_position)
            self.realized_pnl_usd = quote_asset_ticker_price * self.realized_pnl
        # total pnl
        self.total_pnl = self.realized_pnl + self.unrealized_pnl
        self.total_pnl_usd = self.total_pnl * quote_asset_ticker_price

        # avg open price
        if is_still_open:
            if self.net_position + quantity_with_direction != 0:
                self.avg_open_price = ((self.avg_open_price * self.net_position) + (traded_price * quantity_with_direction)) / (self.net_position + quantity_with_direction)
            else:
                self.avg_open_price = traded_price
        else:
            # Check if it is close-and-open
            if traded_quantity > abs(self.net_position):
                self.avg_open_price = traded_price

        # net position
        self.net_position += quantity_with_direction

    def _update_by_marketdata(
        self,
        market_price: Decimal,
        quote_asset_ticker_price: Decimal,
    ):
        self.unrealized_pnl = (market_price - self.avg_open_price) * self.net_position
        self.unrealized_pnl_usd = quote_asset_ticker_price * self.unrealized_pnl
        self.total_pnl = self.realized_pnl + self.unrealized_pnl
        self.total_pnl_usd = self.total_pnl * quote_asset_ticker_price
        self.market_price = market_price

    def get_snapshot_filters(self):
        return (
            PositionSnapshot.portfolio == self.portfolio,
            PositionSnapshot.base_asset == func.binary(self.base_asset),
            PositionSnapshot.quote_asset == func.binary(self.quote_asset),
        )

    def get_item_filters(self):
        return (
            TradeV2.portfolio == self.portfolio,
            TradeV2.base_asset == func.binary(self.base_asset),
            TradeV2.quote_asset == func.binary(self.quote_asset),
        )
