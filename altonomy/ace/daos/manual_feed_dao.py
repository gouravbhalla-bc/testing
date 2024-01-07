from altonomy.ace.daos.base_dao import BaseDao
from altonomy.ace.models import ManualFeed
from datetime import datetime
from typing import List
from sqlalchemy import func
from sqlalchemy.sql.operators import eq


class ManualFeedDao(BaseDao[ManualFeed]):

    def get_last_cash_records(self, deal_ref, as_of_date):
        q = (
            self.db.query(
                func.row_number().over(partition_by=[self.model.deal_ref, self.model.comp_code], order_by=self.model.input_date.desc()).label("row_number"),
                self.model,
            )
            .filter(
                self.model.feed_type == "Cash",
                self.model.deal_ref == deal_ref,
                self.model.as_of_date <= as_of_date
            )
            .subquery('c')
        )

        q = (
            self.db.query(q)
            .select_entity_from(q)
            .filter(q.c.row_number == 1)
        )

        return q.all()

    def get_asof_feeds(self, as_of_date: datetime):
        return self.db.query(self.model).filter(eq(
            self.model.as_of_date, as_of_date
        )).all()

    def get_all_by_product_and_portfolio(self, product: str, portfolios: List[str]) -> List[ManualFeed]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.product == product,
                self.model.portfolio.in_(portfolios),
            )
        ).all()

    def get_all_transfer_by_portfolio(self, portfolios: List[str]) -> List[ManualFeed]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.transfer_type == "transfer",
                self.model.portfolio.in_(portfolios),
            )
        ).all()

    def get_all_by_deal_ref_and_not_in_portfolio(self, deal_refs: List[str], portfolios: List[str]) -> List[ManualFeed]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.deal_ref.in_(deal_refs),
                self.model.portfolio.notin_(portfolios),
            )
        ).all()
