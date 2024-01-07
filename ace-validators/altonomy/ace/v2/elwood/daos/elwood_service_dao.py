from altonomy.ace.daos.base_dao import BaseDao
from altonomy.ace.models import ElwoodService
from sqlalchemy import and_
from sqlalchemy.sql.operators import eq
from typing import Optional
from sqlalchemy.orm import Session


class ElwoodServiceDao(BaseDao[ElwoodService]):

    def __init__(self, db: Session):
        super().__init__(db, ElwoodService)

    def create(self, input: ElwoodService) -> ElwoodService:
        return super().create(input)

    def get_previous_service_by_name(self, name: str) -> Optional[ElwoodService]:
        return self.db.query(self.model).filter(and_(
            eq(self.model.name, name)
        )).order_by(self.model.id.desc()).first()
