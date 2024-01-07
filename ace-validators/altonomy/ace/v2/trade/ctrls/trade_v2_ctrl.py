from datetime import datetime
from typing import Dict, List, Tuple

from altonomy.ace.v2.trade.daos import TradeV2Dao
from sqlalchemy.orm import Session

from altonomy.ace.v2.trade.models.trade_v2 import TradeV2


class TradeV2Ctrl(object):

    def __init__(self, db: Session):
        self.dao = TradeV2Dao(db, TradeV2)

    def get_all_pairs_in_portfolio(self, portfolio: int) -> List[Tuple[str, str]]:
        return self.dao.get_all_pairs(portfolio)

    def get_trades_by_product_portfolio_time(
        self,
        portfolios: List[int],
        product: str,
        from_date: datetime,
        to_date: datetime,
    ) -> List[TradeV2]:
        return self.dao.get_trades_by_product_portfolio_time(
            portfolios,
            product,
            from_date,
            to_date,
        )

    def get_trades_by_portfolio_time(
        self,
        portfolios: List[int],
        from_date: datetime,
        to_date: datetime,
    ) -> List[TradeV2]:
        return self.dao.get_trades_by_portfolio_time(
            portfolios,
            from_date,
            to_date,
        )

    def get_last_trade_dict(
        self,
        last_trade: List[TradeV2]
    ) -> Dict:
        trade_dict = {}
        if last_trade is not None and len(last_trade) != 0:
            for trade in last_trade:
                trade_dict[trade.export] = trade
        return trade_dict
