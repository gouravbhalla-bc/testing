from decimal import Decimal, getcontext
from copy import copy


class TradeSnapshot(object):

    def __init__(self):
        self.quantity = Decimal(0)
        self.amount_usdt = Decimal(0)
        self.weighted_average_price = Decimal(0)
        self.pnl = Decimal(0)
        self.sum_pnl = Decimal(0)
        self.traded_price = Decimal(0)

    def clone(self):
        c = copy(self)
        return c

    def update(self, feed):
        getcontext().prec = 20

        base_qty = feed["amount"]
        base_price = feed["ticker_price"]

        quantity = base_qty
        traded_price = abs(base_price)

        amount_usdt = base_qty * traded_price

        is_sign_flip = self.quantity * quantity < 0

        self.quantity += quantity

        if is_sign_flip:
            self.amount_usdt = self.weighted_average_price * self.quantity
        else:
            self.amount_usdt += amount_usdt

        if self.quantity != 0:
            self.weighted_average_price = self.amount_usdt / self.quantity

        if quantity < 0:
            self.pnl = (
                (traded_price - self.weighted_average_price) * abs(quantity)
            )
        else:
            self.pnl = 0

        self.sum_pnl += self.pnl

        self.traded_price = traded_price
