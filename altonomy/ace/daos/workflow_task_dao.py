from altonomy.ace.daos.base_dao import BaseDao
from altonomy.ace.models import WorkflowTask
from sqlalchemy import and_
from sqlalchemy.sql.operators import eq
from typing import List


class WorkflowTaskDao(BaseDao[WorkflowTask]):

    def get_maker_logic_workflow_tasks(self, maker_id: int) -> List[WorkflowTask]:
        return self.db.query(self.model).filter(and_(
            eq(self.model.maker_id, maker_id),
            eq(self.model.status, "pending"),
            eq(self.model.task_type, "logic")
        )).all()

    def get_checker_workflow_tasks(self, checker_id: int) -> List[WorkflowTask]:
        return self.db.query(self.model).filter(and_(
            eq(self.model.checker_id, checker_id),
            eq(self.model.status, "pending")
        )).all()
