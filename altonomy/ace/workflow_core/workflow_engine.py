import inspect
import time

from altonomy.ace import ctrls
from altonomy.ace import workflows
from altonomy.ace.daos import WorkflowTaskDao
from altonomy.ace.models import WorkflowTask
from datetime import datetime
from sqlalchemy.orm import Session


class WorkflowEngine(object):

    def __init__(self, db: Session):
        self.workflow_task_dao = WorkflowTaskDao(db, WorkflowTask)
        self.workflow_map = {}
        self.function_map = {}
        self.load_workflow()
        self.load_function(db)

    def workflow_task_to_json(self, workflow_task: WorkflowTask):
        return {
            "id": workflow_task.id,
            "execution_id": workflow_task.execution_id,
            "workflow_name": workflow_task.workflow_name,
            "task_name": workflow_task.task_name,
            "description": workflow_task.description,
            "maker_id": workflow_task.maker_id,
            "checker_id": workflow_task.checker_id,
            "stage": workflow_task.stage,
            "total_step": workflow_task.total_step,
            "data": workflow_task.data,
            "extra": workflow_task.extra,
            "status": workflow_task.status,
            "task_type": workflow_task.task_type,
            "create_time": workflow_task.create_time.timestamp(),
            "update_time": None if workflow_task.update_time is None else workflow_task.update_time.timestamp(),
        }

    def load_workflow(self):
        classes = inspect.getmembers(workflows, predicate=inspect.isclass)
        for _class in classes:
            _ctrl = _class[1]()
            wf_dict = _ctrl.load_json()
            wf_name = wf_dict.get("name", None)
            self.workflow_map.update({
                wf_name: wf_dict
            })

    def load_function(self, db: Session):
        classes = inspect.getmembers(ctrls, predicate=inspect.isclass)
        for _class in classes:
            functions = inspect.getmembers(_class[1], predicate=inspect.isfunction)
            _ctrl = _class[1](db)
            for _func in functions:
                if _func[0][:4] == "wft_":
                    ctrl_func = getattr(_ctrl, _func[0])
                    self.function_map.update({
                        f"{_class[0]}::{_func[0]}": ctrl_func
                    })

    def init_workflow(self, workflow_name: str, data: dict, extra: dict = {}):
        workflow = self.workflow_map.get(workflow_name, None)
        if workflow is not None:
            class_name = workflow.get("class", None)
            if class_name is not None:
                init_task_name = workflow.get("start_at", None)
                if init_task_name is not None:
                    err, _data = self.create_task(workflow_name, init_task_name, 1, data, extra=extra)
                    if err is None:
                        task_id = _data.get("id", None)
                        err, _data = self.execute_task(task_id, True, extra)
                        if err is None:
                            return None, _data
                        else:
                            self.close_error_task(task_id, err)
                            return err, None
                    else:
                        return err, None
                else:
                    return "task name not found", None
            else:
                return "class name not found", None
        else:
            return "workflow not found", None

    def create_task(self, workflow_name: str, task_name: str, stage: int, data: dict, execution_id: int = None, extra: dict = {}):
        workflow = self.workflow_map.get(workflow_name, None)
        if workflow is not None:
            workflow_states = workflow.get("states", {})
            task_meta = workflow_states.get(task_name, {})
            workflow_task = WorkflowTask(
                execution_id=int(time.time() * 1000) if execution_id is None else execution_id,
                workflow_name=workflow_name,
                task_name=task_name,
                description=task_meta.get("description", None),
                maker_id=data.get("maker_id", None),
                checker_id=data.get("checker_id", None),
                stage=stage,
                total_step=len(workflow_states.keys()),
                data=data,
                extra=extra,
                status="pending",
                task_type=task_meta.get("type", None)
            )
            self.workflow_task_dao.create(workflow_task)
            return None, self.workflow_task_to_json(workflow_task)
        else:
            return "workflow not found", None

    def execute_task(self, task_id: int, is_approval: bool, extra: dict = {}, by_maker: bool = False):
        workflow_task = self.workflow_task_dao.get(task_id)
        if workflow_task is not None:
            if workflow_task.status == "pending":
                execution_id = workflow_task.execution_id
                workflow_name = workflow_task.workflow_name
                task_name = workflow_task.task_name
                stage = workflow_task.stage
                data = workflow_task.data
                maker_id = workflow_task.maker_id
                checker_id = workflow_task.checker_id
                _workflow = self.workflow_map.get(workflow_name, None)
                if _workflow is not None:
                    _class_name = _workflow.get("class", None)
                    if _class_name is not None:
                        _states = _workflow.get("states", {})
                        _func = self.function_map.get(f"{_class_name}::{task_name}", None)
                        if _func is not None:
                            workflow_task.update_time = datetime.utcnow()
                            if is_approval:
                                workflow_task.status = "approved"
                                err, _data = _func(**data)
                                if err is None:
                                    self.workflow_task_dao.update(workflow_task)
                                    _task_meta = _states.get(task_name, {})
                                    next_task_name = _task_meta.get("next", None)
                                    if next_task_name is not None:
                                        err, _data = self.create_task(workflow_name, next_task_name, stage + 1, _data, execution_id=execution_id, extra=extra)
                                        if err is None:
                                            return None, _data
                                        else:
                                            return err, None
                                    else:
                                        err, _data = self.create_info_task(True, execution_id, workflow_name, checker_id, maker_id, _data, extra)
                                        if err is None:
                                            return None, _data
                                        else:
                                            return err, None
                                else:
                                    return err, None
                            else:
                                workflow_task.status = "rejected"
                                self.workflow_task_dao.update(workflow_task)
                                if by_maker:
                                    err, _data = self.create_info_task(False, execution_id, workflow_name, maker_id, maker_id, data, extra)
                                else:
                                    err, _data = self.create_info_task(False, execution_id, workflow_name, checker_id, maker_id, data, extra)
                                if err is None:
                                    return None, _data
                                else:
                                    return err, None
                        else:
                            return "task function not found", None
                    else:
                        return "class name not found", None
                else:
                    return "workflow not found", None
            else:
                return "workflow task has executed", None
        else:
            return "task not found", None

    def create_info_task(self, is_success: bool, execution_id: int, workflow_name: str, maker_id: int, checker_id: int, data: dict, extra: dict = {}):
        workflow_task = WorkflowTask(
            execution_id=execution_id,
            workflow_name=workflow_name,
            task_name="success_info" if is_success else "rejected_info",
            description=f"{workflow_name} has completed" if is_success else f"{workflow_name} has been rejected",
            maker_id=maker_id,
            checker_id=checker_id,
            stage=0,
            total_step=0,
            data=data,
            extra=extra,
            status="pending",
            task_type="info"
        )
        self.workflow_task_dao.create(workflow_task)
        return None, self.workflow_task_to_json(workflow_task)

    def close_error_task(self, task_id: int, error_message: str):
        workflow_task = self.workflow_task_dao.get(task_id)
        if workflow_task is not None:
            workflow_task.update_time = datetime.utcnow()
            workflow_task.status = "failed"
            extra = {
                "error_message": error_message
            }
            extra.update(workflow_task.extra)
            workflow_task.extra = extra
            self.workflow_task_dao.update(workflow_task)
            return None, self.workflow_task_to_json(workflow_task)
        else:
            return "task not found", None

    def close_info_task(self, task_id: int):
        workflow_task = self.workflow_task_dao.get(task_id)
        if workflow_task is not None:
            if workflow_task.status == "pending":
                if workflow_task.task_type == "info":
                    workflow_task.update_time = datetime.utcnow()
                    workflow_task.status = "checked"
                    self.workflow_task_dao.update(workflow_task)
                    return None, self.workflow_task_to_json(workflow_task)
                else:
                    return "this is not an information task", None
            else:
                return "information task has checked", None
        else:
            return "task not found", None

    def check_execute_authorization(self, task_id: int, is_approval: bool, executor_id: int):
        workflow_task = self.workflow_task_dao.get(task_id)
        if workflow_task is not None:
            if workflow_task.status == "pending":
                if is_approval:
                    if workflow_task.checker_id == executor_id:
                        return None, False  # by checker
                else:
                    if workflow_task.maker_id == executor_id:
                        return None, True   # by maker
                    elif workflow_task.checker_id == executor_id:
                        return None, False  # by checker
                return "not authorized to execute", True
            else:
                return "workflow task has executed", None
        else:
            return "task not found", None

    def get_maker_tasks(self, maker_id: int):
        workflow_tasks = self.workflow_task_dao.get_maker_logic_workflow_tasks(maker_id)
        data_list = []
        for wt in workflow_tasks:
            data_list.append(self.workflow_task_to_json(wt))
        return None, data_list

    def get_checker_tasks(self, checker_id: int):
        workflow_tasks = self.workflow_task_dao.get_checker_workflow_tasks(checker_id)
        data_list = []
        for wt in workflow_tasks:
            data_list.append(self.workflow_task_to_json(wt))
        return None, data_list
