from typing import List
from altonomy.ace.daos.base_dao import BaseDao
from altonomy.ace.models import ElwoodTransferCounter
from sqlalchemy.orm import Session


class ElwoodTransferCounterDao(BaseDao[ElwoodTransferCounter]):
    def __init__(self, db: Session):
        super().__init__(db, ElwoodTransferCounter)

    def create(self, input: ElwoodTransferCounter) -> ElwoodTransferCounter:
        return super().create(input)

    def update(self, input: ElwoodTransferCounter) -> ElwoodTransferCounter:
        return super().create(input)

    def get_all(self) -> List[ElwoodTransferCounter]:
        return self.db.query(self.model).all()

    def get(self, name: str) -> ElwoodTransferCounter:
        return (
            self.db.query(self.model)
            .filter(
                self.model.name == name,
            )
            .first()
        )
