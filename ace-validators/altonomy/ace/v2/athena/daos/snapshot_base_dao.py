from datetime import datetime
from typing import List, Optional, TypeVar

from altonomy.ace.daos.base_dao import BaseDao
from altonomy.ace.v2.athena.models import SnapshotBase
from sqlalchemy import func


S = TypeVar("S", bound=SnapshotBase)


class SnapshotBaseDao(BaseDao[S]):

    def get_cached_snapshot(
        self,
        additional_filters,
        trade_date: datetime,
        effective_date: datetime,
    ) -> Optional[S]:
        return (
            self.db.query(self.model)
            .filter(
                *additional_filters,
                self.model.trade_date <= trade_date,
                self.model.effective_date_start <= effective_date,
                (self.model.effective_date_end > effective_date) | self.model.effective_date_end.is_(None),
            )
            .order_by(
                self.model.trade_date.desc(),
                self.model.effective_date_start.desc(),
                self.model.version.desc(),
            )
        ).first()

    def get_cached_snapshots_in_portfolios(
        self,
        additional_filters,
        portfolios: List[str],
        trade_date: datetime,
        effective_date: datetime,
        order_by=[],
    ) -> List[S]:
        w = {
            "partition_by": [
                self.model.portfolio,
                *additional_filters,
            ],
            "order_by": [
                self.model.trade_date.desc(),
                self.model.effective_date_start.desc(),
                self.model.version.desc(),
            ],
        }

        q = (
            self.db.query(
                func.row_number().over(**w).label("row_number"),
                self.model,
            )
            .filter(
                self.model.portfolio.in_(portfolios),
                self.model.trade_date <= trade_date,
                self.model.effective_date_start <= effective_date,
                (self.model.effective_date_end > effective_date) | self.model.effective_date_end.is_(None),
            )
        ).subquery('c')

        q = (
            self.db.query(q)
            .select_entity_from(q)
            .filter(q.c.row_number == 1)
            .order_by(*order_by)
        )

        return q.all()

    def get_previous_cached_snapshot(
        self,
        additional_filters,
        trade_date: datetime,
        effective_date: datetime,
    ) -> Optional[S]:
        return (
            self.db.query(self.model)
            .filter(
                *additional_filters,
                self.model.trade_date < trade_date,
                self.model.effective_date_start <= effective_date,
                (self.model.effective_date_end > effective_date) | self.model.effective_date_end.is_(None),
            )
            .order_by(
                self.model.trade_date.desc(),
                self.model.effective_date_start.desc(),
                self.model.version.desc(),
            )
        ).first()

    def get_previous_version_snapshot(
        self,
        additional_filters,
        trade_date: datetime,
    ) -> Optional[S]:
        return (
            self.db.query(self.model)
            .filter(
                *additional_filters,
                self.model.trade_date == trade_date,
                self.model.effective_date_end.is_(None),
            )
        ).first()
