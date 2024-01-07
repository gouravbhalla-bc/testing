from datetime import datetime
from typing import Iterator, List, Optional, Tuple

from altonomy.ace.config import DB_QUERY_PAGE_SIZE
from altonomy.ace.daos.base_dao import BaseDao
from altonomy.ace.enums import RecordType, TransferType, CompCode
from altonomy.ace.v2.feed.models import FeedV2
from sqlalchemy import func
from sqlalchemy.sql.operators import eq, gt
from sqlalchemy import and_


class FeedV2Dao(BaseDao[FeedV2]):
    def get_filtered_at_trade_date_at_effective_date(
        self,
        filters,
        trade_date_start: datetime,
        trade_date_end: datetime,
        effective_date: datetime,
        order_by=[],
    ) -> Iterator[FeedV2]:
        q = (
            self.db.query(self.model)
            .filter(
                *filters,
                self.model.comp_code.not_in([CompCode.INITIAL_MARGIN_IN, CompCode.INITIAL_MARGIN_OUT, CompCode.VARIATION_MARGIN]),
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

    def get_current_feed_by_comp_code(
        self,
        deal_id: int,
        comp_code: str,
    ) -> Optional[FeedV2]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.deal_id == deal_id,
                self.model.comp_code == comp_code,
                self.model.effective_date_end.is_(None),
            )
            .order_by(
                self.model.effective_date_start.desc(),
            )
        ).first()

    def get_current_feed(
        self,
        deal_id: int,
    ) -> Optional[FeedV2]:
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

    def get_current_feeds_by_deal_id(
        self,
        deal_id: int,
    ) -> List[FeedV2]:
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
    ) -> List[FeedV2]:
        return (
            self.db.query(self.model).filter(
                self.model.deal_id == deal_id,
                self.model.effective_date_end == effective_date_end,
                self.model.record_type == RecordType.CREATE,
            )
        ).all()

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

    def count_current_children_feeds(
        self,
        deal_id: int,
    ) -> int:
        return (
            self.db.query(self.model).filter(
                self.model.master_deal_id == deal_id,
                self.model.effective_date_end.is_(None),
            )
        ).count()

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
    ) -> List[FeedV2]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.id > record_id,
                self.model.comp_code.not_in([CompCode.INITIAL_MARGIN_IN, CompCode.INITIAL_MARGIN_OUT, CompCode.VARIATION_MARGIN]),
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
    ) -> List[FeedV2]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.effective_date_end > effective_date_end,
                self.model.comp_code.not_in([CompCode.INITIAL_MARGIN_IN, CompCode.INITIAL_MARGIN_OUT, CompCode.VARIATION_MARGIN]),
            )
            .order_by(
                self.model.effective_date_end.asc(),
            )
            .limit(limit)
        ).all()

    def get_all_portfolios(self) -> List[int]:
        rows = (self.db.query(self.model.portfolio).distinct()).all()
        return [row[0] for row in rows]

    def get_all_assets(self, portfolio: int) -> List[str]:
        rows = (
            self.db.query(func.binary(self.model.asset))
            .distinct()
            .filter(
                self.model.portfolio == portfolio,
            )
        ).all()
        return [row[0] for row in rows]

    def get_all_counterparties(
        self, portfolio: int, asset: str
    ) -> List[Tuple[str, str]]:
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

    def get_all_product_contract_pair(
        self, portfolio: int, asset: str
    ) -> List[Tuple[str, str]]:
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
    ) -> List[FeedV2]:
        return (
            self.db.query(self.model).filter(
                self.model.portfolio.in_(portfolios),
                self.model.transfer_type == TransferType.TRANSFER,
                self.model.trade_date <= to_date,
                self.model.effective_date_start > from_date,
                (
                    (
                        self.model.effective_date_end <= to_date
                    ) | self.model.effective_date_end.is_(None)
                ),
            )
        ).all()

    def get_feeds_position_by_asset_product_portfolio_time(
        self,
        asset: str,
        from_date: datetime,
        products: List[int],
        portfolios: List[int],
    ) -> List[Tuple[str, str, float]]:
        return (
            self.db.query(
                self.model.asset,
                self.model.product,
                func.sum(self.model.amount).label("amount")
            ).filter(
                self.model.asset == asset,
                self.model.trade_date >= from_date,
                self.model.product.in_(products),
                self.model.portfolio.in_(portfolios)
            ).group_by(
                self.model.product
            )
        ).all()

    def get_options_feed(
            self,
            portfolios: List[str],
            assets: List[str],
            counterparties: List[str],
    ) -> List[FeedV2]:
        filter_list = [
            self.model.portfolio.in_(portfolios),
            eq(self.model.comp_code, CompCode.OPTIONS_NOTIONAL),
            gt(self.model.value_date, datetime.utcnow()),
            eq(self.model.effective_date_end, None)
        ]
        if len(assets):
            filter_list.append(self.model.asset.in_(assets))
        if len(counterparties):
            filter_list.append(self.model.counterparty_name.in_(counterparties))

        query = self.db.query(
            self.model.counterparty_name,
            self.model.contract,
            func.sum(self.model.amount).label("amount")
        ).filter(
            and_(*filter_list)
        ).group_by(
            self.model.counterparty_name,
            self.model.contract
        ).order_by(
            self.model.counterparty_name,
            self.model.asset,
            self.model.value_date,
            self.model.contract
        )
        return query.all()

    def get_open_option_paid_premium_list(
            self,
            portfolios: List[str],
            assets: List[str],
    ) -> List[FeedV2]:
        filter_list = [
            self.model.portfolio.in_(portfolios),
            eq(self.model.comp_code, CompCode.OPTIONS_PREMIUM),
            gt(self.model.value_date, datetime.utcnow()),
            eq(self.model.effective_date_end, None)
        ]
        if len(assets):
            filter_list.append(self.model.asset.in_(assets))

        query = self.db.query(
            self.model.counterparty_name,
            self.model.contract,
            self.model.asset,
            self.model.amount,
            self.model.trade_date
        ).filter(
            and_(*filter_list)
        ).order_by(
            self.model.counterparty_name,
            self.model.contract,
            self.model.asset
        )
        return query.all()
