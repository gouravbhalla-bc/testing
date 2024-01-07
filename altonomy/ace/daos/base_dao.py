from altonomy.ace.common.pagination import PaginateUtil
from altonomy.ace.db.base_class import Base
from sqlalchemy.orm import Session
from sqlalchemy.sql.operators import eq
from typing import Generic
from typing import List
from typing import Optional
from typing import Type
from typing import TypeVar

ModelType = TypeVar("ModelType", bound=Base)


class BaseDao(Generic[ModelType]):

    def __init__(self, db: Session, model: Type[ModelType]):
        self.db = db
        self.model = model

    def get(self, id: int) -> Optional[ModelType]:
        return self.db.query(self.model).filter(eq(self.model.id, id)).first()

    def get_all(self) -> List[ModelType]:
        return self.db.query(self.model).all()

    def get_all_paginated(self, page: int, page_size: int) -> (List[ModelType], int, int, int, int):
        query = self.db.query(self.model)
        paginate_util = PaginateUtil(query)
        page = paginate_util.paginate(page=page, per_page=page_size)
        return page.items, page.page, page.pages, page.per_page, page.total

    # CreateSchemaType is a pydantic schema
    def create(self, db_obj: ModelType) -> ModelType:
        self.db.add(db_obj)
        self.db.commit()
        self.db.refresh(db_obj)
        return db_obj

    def create_many(self, db_objs: List[ModelType]) -> List[ModelType]:
        self.db.add_all(db_objs)
        self.db.commit()
        for obj in db_objs:
            self.db.refresh(obj)
        return db_objs

    def update(self, db_obj: ModelType) -> ModelType:
        self.db.add(db_obj)
        self.db.commit()
        self.db.refresh(db_obj)
        return db_obj

    # Remove will not be use!!!
    def remove(self, id: int) -> ModelType:
        db_obj = self.db.query(self.model).get(id)
        self.db.delete(db_obj)
        self.db.commit()
        return db_obj
