from altonomy.ace.db.base_class import Base as BaseClass
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String


class Base(BaseClass):
    id = Column(Integer, primary_key=True, index=True)
    message = Column(String(255), index=True)
