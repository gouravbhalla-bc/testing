from datetime import datetime
from typing import List, Tuple

from altonomy.ace.daos.base_dao import BaseDao
from altonomy.ace.enums import TransferType
from altonomy.ace.v2.feed.models import ManualFeedV2
from sqlalchemy import func


class ManualFeedV2Dao(BaseDao[ManualFeedV2]):

    def get_filtered_at_trade_date_at_effective_date(
        self,
        filters,
        trade_date_start: datetime,
        trade_date_end: datetime,
        effective_date: datetime,
        order_by=[],
    ) -> List[ManualFeedV2]:
        return (
            self.db.query(self.model)
            .filter(
                *filters,
                self.model.trade_date >= trade_date_start,
                self.model.trade_date < trade_date_end,
                self.model.effective_date_start <= effective_date,
                ((self.model.effective_date_end > effective_date) | self.model.effective_date_end.is_(None)),
            )
            .order_by(
                self.model.effective_date_start,
                *order_by,
            )
        ).all()

    def get_N_feeds_after_record_id(
        self,
        record_id: int,
        limit: int,
    ) -> List[ManualFeedV2]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.id > record_id,
            )
            .order_by(
                self.model.id.asc(),
            )
            .limit(limit)
        ).all()

    def get_N_feeds_after_last_effective_date_end(
        self,
        effective_date_end: datetime,
        limit: int,
    ) -> List[ManualFeedV2]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.effective_date_end > effective_date_end,
            )
            .order_by(
                self.model.effective_date_end.asc(),
            )
            .limit(limit)
        ).all()

    def get_all_portfolios(self) -> List[int]:
        rows = (
            self.db.query(self.model.portfolio).distinct()
        ).all()
        return [row[0] for row in rows]

    def get_all_assets(self, portfolio: int) -> List[str]:
        rows = (
            self.db.query(func.binary(self.model.asset)).distinct()
            .filter(
                self.model.portfolio == portfolio,
            )
        ).all()
        return [row[0] for row in rows]

    def get_all_counterparties(self, portfolio: int, asset: str) -> List[Tuple[str, str]]:
        rows = (
            self.db.query(
                self.model.counterparty_ref,
                self.model.counterparty_name,
            )
            .distinct()
            .filter(
                self.model.portfolio == portfolio,
                self.model.asset == asset,
            )
        ).all()
        return rows

    def get_all_product_contract_pair(self, portfolio: int, asset: str) -> List[Tuple[str, str]]:
        rows = (
            self.db.query(
                self.model.contract,
                self.model.product,
            )
            .distinct()
            .filter(
                self.model.portfolio == portfolio,
                self.model.asset == asset,
            )
        ).all()
        return rows

    def get_feeds_transfer_by_portfolio_time(
        self,
        portfolios: List[int],
        from_date: datetime,
        to_date: datetime,
    ) -> List[ManualFeedV2]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.portfolio.in_(portfolios),
                self.model.transfer_type == TransferType.TRANSFER,
                self.model.trade_date <= to_date,
                self.model.effective_date_start > from_date,
                ((self.model.effective_date_end <= to_date) | self.model.effective_date_end.is_(None)),
            )
        ).all()
