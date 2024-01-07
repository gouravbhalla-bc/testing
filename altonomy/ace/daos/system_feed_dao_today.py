from altonomy.ace.daos.base_dao import BaseDao
from altonomy.ace.models import SystemFeedToday
from datetime import datetime
from typing import List, Optional
from sqlalchemy.sql.operators import eq


class SystemFeedTodayDao(BaseDao[SystemFeedToday]):

    def get_current_as_of_date(self):
        result = self.db.execute("""
            SELECT MAX(as_of_date)
            FROM system_feed_today;
        """)
        as_of_date = None
        for row in result:
            as_of_date = row[0]
            break
        return as_of_date

    def get_asof_feeds(self, as_of_date: datetime):
        return self.db.query(self.model).filter(eq(
            self.model.as_of_date, as_of_date
        )).all()

    def truncate(self):
        self.db.execute("""
            TRUNCATE TABLE system_feed_today;
        """)

    def delete_feeds_by_deal_refs(self, deal_refs, as_of_date):
        if len(deal_refs):
            self.db.execute("""
                DELETE
                FROM system_feed_today
                WHERE deal_ref in :x
                AND as_of_date = :y;
            """, {
                "x": deal_refs,
                "y": as_of_date,
            })

    def get_today_feeds_by_portfolio(self, portfolios):
        return (
            self.db.query(self.model)
            .filter(self.model.portfolio.in_(portfolios))
        ).all()

    def get_all_by_product_and_portfolio(self, product: str, portfolios: List[str]) -> List[SystemFeedToday]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.product == product,
                self.model.portfolio.in_(portfolios),
            )
        ).all()

    def get_all_transfer_by_portfolio(self, portfolios: List[str]) -> List[SystemFeedToday]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.transfer_type == "transfer",
                self.model.portfolio.in_(portfolios),
            )
        ).all()

    def get_all_by_deal_ref_and_not_in_portfolio(self, deal_refs: List[str], portfolios: List[str]) -> List[SystemFeedToday]:
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
    ) -> List[SystemFeedToday]:
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
    ) -> Optional[SystemFeedToday]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.deal_ref == deal_ref,
                self.model.comp_code == comp_code,
                self.model.record_type == "CREATE",
                self.model.as_of_date_end.is_(None),
            )
        ).first()
