from decimal import Decimal
from copy import copy
from collections import defaultdict


class Position(object):
    '''
    source: https://lichgo.github.io/2015/10/29/40-lines-pnl-calculation.html
    '''
    def __init__(
        self,
        portfolio: str,
        base_asset: str,
        quote_asset: str,
        net_position: Decimal = Decimal(0),
        net_investment: Decimal = Decimal(0),
        avg_open_price: Decimal = Decimal(0),
        realized_pnl: Decimal = Decimal(0),
        unrealized_pnl: Decimal = Decimal(0),
        realized_pnl_usd: Decimal = Decimal(0),
        unrealized_pnl_usd: Decimal = Decimal(0),
        total_pnl_usd: Decimal = Decimal(0),
        total_buy_quantity: Decimal = Decimal(0),
        total_sell_quantity: Decimal = Decimal(0),
        average_buy_price: Decimal = Decimal(0),
        average_sell_price: Decimal = Decimal(0),
        last_price: Decimal = Decimal(0),
        market_price: Decimal = Decimal(0)
    ):
        self.portfolio = portfolio
        self.base_asset = base_asset
        self.quote_asset = quote_asset
        self.net_position = net_position
        self.net_investment = net_investment
        self.avg_open_price = avg_open_price
        self.realized_pnl = realized_pnl
        self.unrealized_pnl = unrealized_pnl
        self.total_pnl = realized_pnl + unrealized_pnl
        self.fee = dict()
        self.realized_pnl_usd = realized_pnl_usd
        self.unrealized_pnl_usd = unrealized_pnl_usd
        self.total_pnl_usd = total_pnl_usd
        self.total_pnl_usd = total_pnl_usd
        self.total_buy_quantity = total_buy_quantity
        self.total_sell_quantity = total_sell_quantity
        self.average_buy_price = average_buy_price
        self.average_sell_price = average_sell_price
        self.last_price = last_price
        self.market_price = market_price

    def clone(self):
        c = copy(self)
        c.fee = copy(self.fee)
        return c

    def _cal_avg_price(self, curr_average_price, total_quantity, traded_quantity, traded_price):
        return (curr_average_price * (total_quantity - traded_quantity) + traded_price * traded_quantity) / total_quantity

    def update_by_tradefeed(
        self,
        base_amount: Decimal,
        quote_amount: Decimal,
        fee_asset: str,
        fee_quantity: Decimal,
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

        # record fee
        if fee_asset is not None and fee_asset != "":
            if fee_asset not in self.fee:
                self.fee[fee_asset] = 0
            self.fee[fee_asset] += fee_quantity

    def update_by_tradefeed_execution(
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

    def update_by_marketdata(self, market_price: Decimal, quote_asset_ticker_price: Decimal):
        self.unrealized_pnl = (market_price - self.avg_open_price) * self.net_position
        self.unrealized_pnl_usd = quote_asset_ticker_price * self.unrealized_pnl
        self.total_pnl = self.realized_pnl + self.unrealized_pnl
        self.total_pnl_usd = self.total_pnl * quote_asset_ticker_price
        self.market_price = market_price


class Summary(object):

    def __init__(self, portfolio: str, asset: str):
        self.portfolio = portfolio
        self.asset = asset
        self.position = 0

    def clone(self):
        return copy(self)

    def as_dict(self):
        return {
            "Portfolio": self.portfolio,
            "Asset": self.asset,
            "Position": self.position,
        }

    def update(self, feed):
        self.position += feed["amount"]


class Settlement(object):

    def __init__(self, portfolio: str, counterparty_ref: str, counterparty_name: str):
        self.portfolio = portfolio
        self.counterparty_ref = counterparty_ref
        self.counterparty_name = counterparty_name
        self.settlements = defaultdict(int)
        self.net_exposure = 0

    def clone(self):
        c = copy(self)
        c.settlements = copy(self.settlements)
        return c

    def as_dict(self):
        d = {
            "Portfolio": self.portfolio,
            "Counterparty Ref": self.counterparty_ref,
            "Counterparty Name": self.counterparty_name,
            "Net Exposure": self.net_exposure,
        }
        d.update(self.settlements)
        return d

    def update(self, feed):
        asset = feed["asset"]
        amount = feed["amount"]

        self.settlements[asset] += amount
        self.net_exposure += amount * feed["ticker_price"]


class AccountBalance(object):

    def __init__(self, portfolio: str, account: str):
        self.portfolio = portfolio
        self.account = account
        self.balance = defaultdict(int)
        self.net_exposure = 0

    def clone(self):
        c = copy(self)
        c.balance = copy(self.balance)
        return c

    def is_empty(self):
        return len(self.balance) == 0

    def as_dict(self):
        d = {
            "Portfolio": self.portfolio,
            "Account": "" if self.account is None else self.account,
            "Net Exposure": self.net_exposure,
        }
        d.update(self.balance)
        return d

    def update(self, feed):
        asset = feed["asset"]
        amount = feed["amount"]

        self.balance[asset] += amount
        self.net_exposure += amount * feed["ticker_price"]

        if self.balance[asset] == 0:
            del self.balance[asset]
