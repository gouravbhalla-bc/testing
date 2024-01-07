from altonomy.ace.daos.base_dao import BaseDao
from altonomy.ace.models import SystemFeed
from altonomy.ace.models import ManualFeed
from altonomy.ace.enums import FeedType
from altonomy.ace.accounting_core.comp_code import CompCode
from datetime import datetime
from typing import List, Optional
from sqlalchemy import func
from sqlalchemy.sql.operators import eq
from sqlalchemy.sql.expression import literal


class SystemFeedDao(BaseDao[SystemFeed]):

    def get_last_cash_records(self, deal_ref, comp_code: CompCode, as_of_date):
        q = (
            self.db.query(
                func.row_number().over(partition_by=[self.model.deal_ref], order_by=self.model.input_date.desc()).label("row_number"),
                self.model,
            )
            .filter(
                self.model.feed_type == "Cash",
                self.model.deal_ref == deal_ref,
                self.model.comp_code == comp_code,
                self.model.as_of_date <= as_of_date,
            )
            .order_by(
                func.IF(self.model.record_type == "CREATE", 1, 0)
            )
            .subquery('c')
        )

        q = (
            self.db.query(q)
            .select_entity_from(q)
            .filter(q.c.row_number == 1, q.c.record_type == "CREATE")
        )

        return q.all()

    def get_asof_feeds(self, as_of_date: datetime):
        return self.db.query(self.model).filter(eq(
            self.model.as_of_date, as_of_date
        )).all()

    def delete_feeds_by_as_of_date(self, as_of_date):
        self.db.execute("""
            DELETE
            FROM system_feed
            WHERE as_of_date = :y;
        """, {
            "y": as_of_date
        })

    def get_all_cash_records_including_manual(self):
        sf_cols = [
            self.model.feed_type,
            self.model.portfolio,
            self.model.asset,
            self.model.amount,
            self.model.master_deal_ref,
            self.model.deal_ref,
            self.model.coa_code,
            self.model.entity,
            self.model.product,
            self.model.comp_code,
            self.model.effective_date,
            self.model.value_date,
            self.model.input_date,
            self.model.system_source,
            self.model.record_type,
            self.model.account,
            self.model.asset_price,
            self.model.counterparty_ref,
            self.model.transfer_type,
            self.model.trade_date,
            self.model.as_of_date,
            self.model.counterparty_name,
        ]
        sf_cols = [c.label(c.key) for c in sf_cols]
        sf = self.db.query(*sf_cols).filter(
            self.model.feed_type == "Cash",
            self.model.portfolio != 8014,
            self.model.portfolio < 90000,
        )

        mf = self.db.query(
            ManualFeed.feed_type,
            ManualFeed.portfolio,
            ManualFeed.asset,
            ManualFeed.amount,
            ManualFeed.master_deal_ref,
            ManualFeed.deal_ref,
            ManualFeed.coa_code,
            ManualFeed.entity,
            ManualFeed.product,
            ManualFeed.comp_code,
            ManualFeed.effective_date,
            ManualFeed.value_date,
            ManualFeed.input_date,
            literal("MAN"),
            literal("MAN"),
            ManualFeed.account,
            ManualFeed.asset_price,
            ManualFeed.counterparty_ref,
            ManualFeed.transfer_type,
            ManualFeed.trade_date,
            ManualFeed.as_of_date,
            ManualFeed.counterparty_name,
        ).filter(
            ManualFeed.feed_type == "Cash",
            ManualFeed.portfolio != 8014,
            ManualFeed.portfolio < 90000,
        )

        return sf.union_all(mf).all()

    def get_all_by_product_and_portfolio(self, product: str, portfolios: List[str]) -> List[SystemFeed]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.product == product,
                self.model.portfolio.in_(portfolios),
            )
        ).all()

    def get_all_transfer_by_portfolio(self, portfolios: List[str]) -> List[SystemFeed]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.transfer_type == "transfer",
                self.model.portfolio.in_(portfolios),
            )
        ).all()

    def get_all_by_deal_ref_and_not_in_portfolio(self, deal_refs: List[str], portfolios: List[str]) -> List[SystemFeed]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.deal_ref.in_(deal_refs),
                self.model.portfolio.notin_(portfolios),
            )
        ).all()

    def get_all_by_portfolio_and_asset_between_date_with_comp_codes(
        self,
        portfolio,
        asset,
        comp_codes,
        from_date,
        to_date,
    ) -> List[SystemFeed]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.portfolio == portfolio,
                self.model.asset == asset,
                self.model.comp_code.in_(comp_codes),
                self.model.trade_date > from_date,
                self.model.trade_date <= to_date,
                self.model.record_type == "CREATE",
                self.model.as_of_date_end.is_(None),
            )
        ).all()

    def get_latest_by_deal_ref_and_comp_code(
        self,
        deal_ref: str,
        comp_code: int,
    ) -> Optional[SystemFeed]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.deal_ref == deal_ref,
                self.model.comp_code == comp_code,
                self.model.record_type == "CREATE",
                self.model.as_of_date_end.is_(None),
            )
        ).first()

    def get_all_open_by_deal_ref_and_comp_code_before_as_of_date(
        self,
        deal_refs: List[str],
        comp_code: str,
        before_as_of_date: datetime,
    ) -> List[SystemFeed]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.deal_ref.in_(deal_refs),
                self.model.as_of_date < before_as_of_date,
                self.model.comp_code == comp_code,
                self.model.as_of_date_end.is_(None),
            )
        ).all()

    def get_all_open_cash_by_deal_refs_and_comp_code(
        self,
        deal_refs: List[str],
        comp_code: str,
    ) -> List[SystemFeed]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.deal_ref.in_(deal_refs),
                self.model.comp_code == comp_code,
                self.model.feed_type == FeedType.Cash,
                self.model.as_of_date_end.is_(None),
            )
        ).all()

    def count_open_create_cash_feeds_by_deal_ref(
        self,
        deal_ref: str,
    ) -> int:
        return (
            self.db.query(self.model)
            .filter(
                self.model.deal_ref == deal_ref,
                self.model.record_type == "CREATE",
                self.model.feed_type == FeedType.Cash,
                self.model.as_of_date_end.is_(None),
            )
        ).count()
