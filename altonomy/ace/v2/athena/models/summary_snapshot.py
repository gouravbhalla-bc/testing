from altonomy.ace.v2.athena.models import SnapshotBase
from sqlalchemy import Column, Numeric, String


class SummarySnapshot(SnapshotBase):
    portfolio = Column(String(255), index=True)
    asset = Column(String(255), index=True)
    position = Column(Numeric(40, 20), index=True)
