from typing import List
from altonomy.ace.daos.base_dao import BaseDao
from altonomy.ace.models import ElwoodExport
from sqlalchemy.orm import Session


class ElwoodExportDao(BaseDao[ElwoodExport]):
    def __init__(self, db: Session):
        super().__init__(db, ElwoodExport)

    def create_many(self, input: List[ElwoodExport]) -> List[ElwoodExport]:
        return super().create_many(input)

    def create(self, input: ElwoodExport) -> ElwoodExport:
        return super().create(input)

    def update(self, input: ElwoodExport) -> ElwoodExport:
        return super().update(input)

    def get_active_trade(
        self,
        deal_id: int,
        portfolios: List[str]
    ) -> List[ElwoodExport]:
        return (
            self.db.query(self.model)
            .filter(
                self.model.deal_id == deal_id,
                self.model.portfolio.in_(portfolios),
                self.model.effective_date_end.is_(None)
            )
            .order_by(
                self.model.effective_date_start.desc(),
            )
        ).all()

    def get_trade_by_export(
        self,
        deal_id: int,
        portfolios: List[str],
        export: int
    ) -> ElwoodExport:
        return (
            self.db.query(self.model)
            .filter(
                self.model.deal_id == deal_id,
                self.model.export == export,
                self.model.portfolio.in_(portfolios),
                self.model.effective_date_end.is_(None)
            )
            .order_by(
                self.model.effective_date_start.desc(),
            )
        ).first()
