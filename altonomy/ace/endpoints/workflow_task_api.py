from altonomy.ace.common import api_utils
from altonomy.ace.db import deps
from altonomy.ace.endpoints.schemas import WorkflowTaskReturnData
from altonomy.ace.workflow_core.workflow_engine import WorkflowEngine
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Header
from sqlalchemy.orm import Session
from typing import Any
from typing import List

router = APIRouter()


@router.get("/maker_list", response_model=List[WorkflowTaskReturnData])
def workflow_maker_task_list(
    alt_auth_token: str = Header(None),
    db: Session = Depends(deps.get_db)
) -> Any:
    err, result = api_utils.get_jwt_payload(alt_auth_token, ["ace_admin_read"])
    if err is None:
        workflow_engine = WorkflowEngine(db)
        err, result = workflow_engine.get_maker_tasks(result["i"])
    if err is not None:
        raise HTTPException(status_code=api_utils.get_status_code(err), detail=err)
    return result


@router.get("/list", response_model=List[WorkflowTaskReturnData])
def workflow_task_list(
    alt_auth_token: str = Header(None),
    db: Session = Depends(deps.get_db)
) -> Any:
    err, result = api_utils.get_jwt_payload(alt_auth_token, ["ace_admin_read"])
    if err is None:
        workflow_engine = WorkflowEngine(db)
        err, result = workflow_engine.get_checker_tasks(result["i"])
    if err is not None:
        raise HTTPException(status_code=api_utils.get_status_code(err), detail=err)
    return result


@router.put("/approve/{id}", response_model=WorkflowTaskReturnData)
def approve_workflow_task(
    id: int,
    alt_auth_token: str = Header(None),
    db: Session = Depends(deps.get_db)
) -> Any:
    err, result = api_utils.get_jwt_payload(alt_auth_token, ["ace_admin_create"])
    if err is None:
        workflow_engine = WorkflowEngine(db)
        err, result = workflow_engine.check_execute_authorization(id, True, result["i"])
        if err is None:
            err, result = workflow_engine.execute_task(id, True, {})
    if err is not None:
        raise HTTPException(status_code=api_utils.get_status_code(err), detail=err)
    return result


@router.put("/reject/{id}", response_model=WorkflowTaskReturnData)
def reject_workflow_task(
    id: int,
    alt_auth_token: str = Header(None),
    db: Session = Depends(deps.get_db)
) -> Any:
    err, result = api_utils.get_jwt_payload(alt_auth_token, ["ace_admin_create"])
    if err is None:
        workflow_engine = WorkflowEngine(db)
        err, result = workflow_engine.check_execute_authorization(id, False, result["i"])
        if err is None:
            err, result = workflow_engine.execute_task(id, False, {}, result)
    if err is not None:
        raise HTTPException(status_code=api_utils.get_status_code(err), detail=err)
    return result


@router.put("/check_info/{id}", response_model=WorkflowTaskReturnData)
def check_info_task(
    id: int,
    alt_auth_token: str = Header(None),
    db: Session = Depends(deps.get_db)
) -> Any:
    err, result = api_utils.get_jwt_payload(alt_auth_token, ["ace_admin_create"])
    if err is None:
        workflow_engine = WorkflowEngine(db)
        err, result = workflow_engine.close_info_task(id)
    if err is not None:
        raise HTTPException(status_code=api_utils.get_status_code(err), detail=err)
    return result
