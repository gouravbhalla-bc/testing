from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_amis_admin.admin.settings import Settings
from fastapi_amis_admin.admin.site import AdminSite
from fastapi_scheduler import SchedulerAdmin
from altonomy.ace.endpoints import api_router
from altonomy.ace.endpoints.athena_v2_api import oc


app = FastAPI()

site = AdminSite(settings=Settings(database_url_async='sqlite+aiosqlite:///amisadmin.db'))
scheduler = SchedulerAdmin.bind(site)
site.mount_app(app)


@scheduler.scheduled_job('interval', seconds=24 * 3600)
def sync_credential_to_mem():
    print('authenticate optimus...')
    oc._OptimusClient__authenticate()


origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    try:
        scheduler.start()
    except BaseException:
        pass

app.include_router(api_router, prefix="/ace_api")
