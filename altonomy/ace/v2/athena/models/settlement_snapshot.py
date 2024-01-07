from altonomy.ace.v2.athena.models import SnapshotBase
from sqlalchemy import Column, Numeric, String


class SettlementSnapshot(SnapshotBase):
    portfolio = Column(String(255), index=True)
    counterparty_ref = Column(String(255), index=True)
    counterparty_name = Column(String(255), index=True)
    asset = Column(String(255), index=True)
    position = Column(Numeric(40, 20), index=True)
