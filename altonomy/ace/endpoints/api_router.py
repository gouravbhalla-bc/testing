from altonomy.ace.endpoints import athena_api
from altonomy.ace.endpoints import athena_v2_api
from altonomy.ace.endpoints import workflow_task_api
from fastapi import APIRouter


api_router = APIRouter()
api_router.include_router(workflow_task_api.router, prefix="/workflow_task", tags=["workflow_task"])
api_router.include_router(athena_api.router, prefix="/athena", tags=["athena"])
api_router.include_router(athena_v2_api.router, prefix="/athena/v2", tags=["athena_v2"])


@api_router.get("/ping")
def ping() -> str:
    return "pong"
