import datetime

from altonomy.ace.db.base_class import Base
from sqlalchemy import Column, Index, Integer, Numeric, String
from sqlalchemy.dialects.mysql import DATETIME


def _compare_float(f1, f2):
    if f1 is None and f2 is None:
        return True

    if f1 is None or f2 is None:
        return False

    return round(float(f1), 8) == round(float(f2), 8)


class FeedV2(Base):
    #  metadata
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    system_source = Column(String(32), index=True)
    system_record_date = Column(DATETIME(fsp=6), default=datetime.datetime.utcnow, index=True)
    version = Column(Integer, index=True)
    ref_id = Column(Integer, index=True)
    record_type = Column(String(32), index=True)

    deal_id = Column(Integer, index=True)
    master_deal_id = Column(Integer, index=True)

    #  data
    feed_type = Column(String(255), index=True)
    portfolio = Column(String(255), index=True)
    transfer_type = Column(String(255), index=True)
    contract = Column(String(255), index=True, nullable=True)

    deal_ref = Column(String(255), index=True)
    master_deal_ref = Column(String(255), index=True)
    product = Column(String(255), index=True)
    coa_code = Column(String(255), index=True)
    comp_code = Column(String(255), index=True)

    asset = Column(String(255), index=True)
    amount = Column(Numeric(40, 20), index=True)
    asset_price = Column(Numeric(40, 20), default=0, index=True)

    counterparty_ref = Column(String(255), index=True)
    counterparty_name = Column(String(255), nullable=True)
    account = Column(String(255), index=True)
    entity = Column(String(255), index=True)

    value_date = Column(DATETIME(fsp=6), nullable=True, index=True)
    trade_date = Column(DATETIME(fsp=6), nullable=True, index=True)
    effective_date_start = Column(DATETIME(fsp=6), index=True)
    effective_date_end = Column(DATETIME(fsp=6), nullable=True, index=True)

    __table_args__ = (
        Index('ix_feed_v2_portfolio_asset', portfolio, asset),
    )

    def unsafe_equal_values(self, another) -> bool:
        return (
            (self.portfolio == another.portfolio) and
            (self.counterparty_ref == another.counterparty_ref) and
            (self.counterparty_name == another.counterparty_name) and
            (self.account == another.account) and
            (self.entity == another.entity) and
            (self.feed_type == another.feed_type) and
            (self.transfer_type == another.transfer_type) and
            (self.trade_date == another.trade_date) and
            (self.value_date == another.value_date) and
            (self.asset == another.asset) and
            _compare_float(self.amount, another.amount) and
            (self.deal_id == another.deal_id) and
            (self.master_deal_id == another.master_deal_id) and
            (self.deal_ref == another.deal_ref) and
            (self.product == another.product) and
            (self.comp_code == another.comp_code) and
            (self.coa_code == another.coa_code) and
            (self.contract == another.contract)
        )
