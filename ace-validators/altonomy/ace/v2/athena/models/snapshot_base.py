import datetime

from altonomy.ace.db.base_class import Base
from sqlalchemy import Column, Integer
from sqlalchemy.dialects.mysql import DATETIME


class SnapshotBase(Base):
    __abstract__ = True

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    ref_snapshot = Column(Integer, nullable=True, index=True)
    version = Column(Integer, index=True)
    system_load_start_date = Column(DATETIME(fsp=6), index=True)
    system_record_date = Column(DATETIME(fsp=6), default=datetime.datetime.utcnow, index=True)

    trade_date = Column(DATETIME(fsp=6), index=True)
    effective_date_start = Column(DATETIME(fsp=6), index=True)
    effective_date_end = Column(DATETIME(fsp=6), nullable=True, index=True)
