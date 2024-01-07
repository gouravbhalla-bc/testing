from typing import List
from altonomy.ace.daos.base_dao import BaseDao
from altonomy.ace.models import ElwoodSettlementExport
from sqlalchemy.orm import Session


class ElwoodSettlementExportDao(BaseDao[ElwoodSettlementExport]):
    def __init__(self, db: Session):
        super().__init__(db, ElwoodSettlementExport)

    def create_many(self, input: List[ElwoodSettlementExport]) -> List[ElwoodSettlementExport]:
        return super().create_many(input)

    def create(self, input: ElwoodSettlementExport) -> ElwoodSettlementExport:
        return super().create(input)

    def get_active_settlement(
        self,
        settlement_id: int,
    ) -> ElwoodSettlementExport:
        return (
            self.db.query(self.model)
            .filter(
                self.model.settlement_id == settlement_id,
                self.model.effective_date_end.is_(None)
            )
            .order_by(
                self.model.effective_date_start.desc(),
            )
        ).first()
