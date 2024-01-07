from altonomy.ace.v2.athena.models import SnapshotBase
from sqlalchemy import Column, Numeric, String


class PositionSnapshot(SnapshotBase):
    portfolio = Column(String(255), index=True)
    base_asset = Column(String(255), index=True)
    quote_asset = Column(String(255), index=True)
    net_position = Column(Numeric(40, 20), index=True)
    net_investment = Column(Numeric(40, 20), index=True)
    avg_open_price = Column(Numeric(40, 20), index=True)
    realized_pnl = Column(Numeric(40, 20), index=True)
    unrealized_pnl = Column(Numeric(40, 20), index=True)
    total_pnl = Column(Numeric(40, 20), index=True)
    realized_pnl_usd = Column(Numeric(40, 20), index=True)
    unrealized_pnl_usd = Column(Numeric(40, 20), index=True)
    total_pnl_usd = Column(Numeric(40, 20), index=True)
    total_buy_quantity = Column(Numeric(40, 20), index=True)
    total_sell_quantity = Column(Numeric(40, 20), index=True)
    average_buy_price = Column(Numeric(40, 20), index=True)
    average_sell_price = Column(Numeric(40, 20), index=True)
    last_price = Column(Numeric(40, 20), index=True)
    market_price = Column(Numeric(40, 20), index=True)
