from datetime import datetime
from typing import Iterator, List, Optional, Tuple

from altonomy.ace.config import DB_QUERY_PAGE_SIZE
from altonomy.ace.daos.base_dao import BaseDao
from altonomy.ace.enums import RecordType
from altonomy.ace.v2.trade.models import TradeV2


class TradeV2Dao(BaseDao[TradeV2]):
    def get_filtered_at_trade_date_at_effective_date(
        self,
        filters,
        trade_date_start: datetime,
        trade_date_end: datetime,
        effective_date: datetime,
        order_by=[],
    ) -> Iterator[TradeV2]:
        q = (
            self.db.query(self.model)
            .filter(
                *filters,
                self.model.trade_date >= trade_date_start,
                self.model.trade_date < trade_date_end,
                self.model.effective_date_start <= effective_date,
                (
                    (
                        self.model.effective_date_end > effective_date
                    ) | self.model.effective_date_end.is_(None)
                ),
            )
            .order_by(
                self.model.effective_date_start,
                *order_by,
            )
        )

        page_size = DB_QUERY_PAGE_SIZE
        pages = int(q.count() / page_size) + 1

        for page in range(pages):
            result = q.limit(page_size).offset(page * page_size).all()
            for item in result:
                yield item

    def get_current_feed(
        self,
        deal_id: int,
    ) -> Optional[TradeV2]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.deal_id == deal_id,
                self.model.effective_date_end.is_(None),
            )
            .order_by(
                self.model.effective_date_start.desc(),
            )
        ).first()

    def get_current_feed_by_product(
        self,
        deal_id: int,
        product: str
    ) -> Optional[TradeV2]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.deal_id == deal_id,
                self.model.product == product,
                self.model.effective_date_end.is_(None),
            )
            .order_by(
                self.model.effective_date_start.desc(),
            )
        ).first()

    def get_current_feeds_by_deal_id(
        self,
        deal_id: int,
    ) -> List[TradeV2]:
        return (
            self.db.query(self.model).filter(
                self.model.deal_id == deal_id,
                self.model.effective_date_end.is_(None),
            )
        ).all()

    def get_all_create_by_deal_id_and_effective_date_end(
        self,
        deal_id: int,
        effective_date_end: datetime,
    ) -> List[TradeV2]:
        feed = (
            self.db.query(self.model).filter(
                self.model.deal_id == deal_id,
                self.model.effective_date_end == effective_date_end,
                self.model.record_type == RecordType.CREATE,
            )
            # hotfix, take last trade_v2 ID to prevent duplicates when copying parent feed
            .order_by(self.model.id.desc())
        ).first()
        return [feed] if feed is not None else []

    def get_last_effective_date_end(self, deal_id: int) -> Optional[datetime]:
        result = (
            self.db.query(self.model.effective_date_end)
            .filter(
                self.model.deal_id == deal_id,
            )
            .order_by(
                self.model.effective_date_end.desc(),
            )
        ).first()
        return result[0] if result is not None else None

    def count_current_sibling_feeds(
        self,
        master_deal_id: int,
        exclude_deal_id: int,
    ) -> int:
        return (
            self.db.query(self.model).filter(
                self.model.master_deal_id == master_deal_id,
                self.model.deal_id != exclude_deal_id,
                self.model.effective_date_end.is_(None),
            )
        ).count()

    def get_N_feeds_after_record_id(
        self,
        record_id: int,
        limit: int,
    ) -> List[TradeV2]:
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
    ) -> List[TradeV2]:
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
        rows = (self.db.query(self.model.portfolio).distinct()).all()
        return [row[0] for row in rows]

    def get_all_pairs(self, portfolio: int) -> List[Tuple[str, str]]:
        rows = (
            self.db.query(
                self.model.base_asset,
                self.model.quote_asset,
            )
            .distinct()
            .filter(
                self.model.portfolio == portfolio,
            )
        ).all()
        return rows

    def get_trades_by_product_portfolio_time(
        self,
        portfolios: List[int],
        product: str,
        from_date: datetime,
        to_date: datetime,
    ) -> List[TradeV2]:
        return (
            self.db.query(self.model).filter(
                self.model.portfolio.in_(portfolios),
                self.model.product == product,
                self.model.trade_date <= to_date,
                self.model.effective_date_start > from_date,
                (
                    (self.model.effective_date_end <= to_date) |
                    self.model.effective_date_end.is_(None)
                ),
            )
        ).all()

    def get_trades_by_portfolio_time(
        self,
        portfolios: List[int],
        from_date: datetime,
        to_date: datetime,
    ) -> Iterator[TradeV2]:
        q = (
            self.db.query(self.model)
            .filter(
                self.model.portfolio.in_(portfolios),
                self.model.system_record_date >= from_date,
                self.model.system_record_date < to_date,
            )
            .order_by(
                self.model.system_record_date
            )
        )

        page_size = 1000
        pages = int(q.count() / page_size) + 1

        for page in range(pages):
            result = q.limit(page_size).offset(page * page_size).all()
            print("result", len(result))
            yield result
