from altonomy.ace.v2.elwood.daos.elwood_service_dao import ElwoodServiceDao

from altonomy.ace.models import ElwoodService
from sqlalchemy.orm import Session


class ElwoodServiceCtrl(object):

    def __init__(self, db: Session):
        self.dao = ElwoodServiceDao(db)

    def create(self, elwood_service: ElwoodService):
        self.dao.create(elwood_service)
