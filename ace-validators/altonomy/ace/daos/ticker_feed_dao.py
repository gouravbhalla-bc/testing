from altonomy.ace.daos.base_dao import BaseDao
from altonomy.ace.models import TickerFeed
from datetime import datetime, timedelta
from sqlalchemy.sql.operators import eq
from sqlalchemy import desc
from sqlalchemy import func


class TickerFeedDao(BaseDao[TickerFeed]):

    def get_latest_ticker_price(self, base_asset):
        price = (
            self.db.query(self.model)
            .filter(
                eq(self.model.feed_type, base_asset)
            ).order_by(desc(self.model.input_date))
        ).first()

        if not price:
            price = 0

        return price

    def get_all_asset_latest_ticker_price(self):
        w = {
            "partition_by": [self.model.base_asset, self.model.quote_asset],
            "order_by": self.model.input_date.desc(),
        }

        q = (
            self.db.query(
                func.row_number().over(**w).label("row_number"),
                func.lead(self.model.price, 1).over(**w).label("last_price"),
                self.model,
            ).filter(
                self.model.input_date >= datetime.utcnow() - timedelta(days=1)
            ).subquery('c')
        )

        q = (
            self.db.query(q)
                .select_entity_from(q)
                .filter(q.c.row_number == 1)
        )

        return q.all()

    def get_asset_latest_ticker_price_at_time(
        self,
        base_asset: str,
        quote_asset: str,
        date: datetime,
    ):
        w = {
            "order_by": self.model.effective_date.desc(),
        }

        q = (
            self.db.query(
                func.row_number().over(**w).label("row_number"),
                func.lead(self.model.price, 1).over(**w).label("last_price"),
                self.model,
            ).filter(
                self.model.base_asset == base_asset,
                self.model.quote_asset == quote_asset,
                self.model.effective_date >= date - timedelta(days=1),
                self.model.effective_date < date,
            ).subquery('c')
        )

        q = (
            self.db.query(q)
                .select_entity_from(q)
                .filter(q.c.row_number == 1)
        )

        return q.first()

    def get_all_asset_latest_ticker_price_at_time(
        self,
        quote_asset: str,
        date: datetime,
    ):
        w = {
            "partition_by": self.model.base_asset,
            "order_by": self.model.effective_date.desc(),
        }

        q = (
            self.db.query(
                func.row_number().over(**w).label("row_number"),
                func.lead(self.model.price, 1).over(**w).label("last_price"),
                self.model,
            ).filter(
                self.model.quote_asset == quote_asset,
                self.model.effective_date >= date - timedelta(days=1),
                self.model.effective_date < date,
            ).subquery('c')
        )

        q = (
            self.db.query(q)
                .select_entity_from(q)
                .filter(q.c.row_number == 1)
        )

        return q.all()

    def get_ticker_price_by_time(self, base_asset, input_date: datetime):
        price = (
            self.db.query(self.model).filter(
                self.model.feed_type == base_asset
            ).order_by(desc(abs(self.model.input_date - input_date)))
        ).first()

        if not price:
            price = 0

        return price

    def get_asset_ticker_price(self, base_asset: str):
        return (
            self.db.query(self.model)
            .filter(
                self.model.base_asset == base_asset,
                self.model.quote_asset == "USDT",
            )
        ).all()

    def get_first_asset_ticker_price(self, base_asset: str, date: datetime):
        return (
            self.db.query(self.model)
            .filter(
                self.model.base_asset == base_asset,
                self.model.quote_asset == "USDT",
                self.model.effective_date <= date,
            )
            .order_by(
                self.model.effective_date.desc()
            )
        ).first()
