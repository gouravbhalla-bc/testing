import datetime
from sqlalchemy import Boolean

from altonomy.ace.db.base_class import Base
from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import Numeric
from sqlalchemy.dialects.mysql import DATETIME


def _compare_float(f1, f2):
    if f1 is None and f2 is None:
        return True

    if f1 is None or f2 is None:
        return False

    return round(float(f1), 8) == round(float(f2), 8)


class ElwoodDeal(Base):
    id = Column(BigInteger, primary_key=True, autoincrement=True, index=True)
    name = Column(String(255), nullable=True, index=True)
    record_date = Column(DATETIME(fsp=6), default=datetime.datetime.utcnow, index=True)

    # Contain Primary key of deal table
    parent_id = Column(Integer, index=True)
    deal_id = Column(Integer, index=True)
    deal_ref = Column(String(255), index=True)
    deal_type = Column(String(255), index=True)

    trade_date = Column(DATETIME(fsp=6), default=datetime.datetime.utcnow, index=True)
    value_date = Column(DATETIME(fsp=6), nullable=True, index=True)

    counterparty_ref = Column(String(255), nullable=True, index=True)
    counterparty_name = Column(String(255), nullable=True, index=True)

    portfolio_number = Column(String(255), nullable=True, index=True)
    portfolio_name = Column(String(255), nullable=True, index=True)
    portfolio_entity = Column(String(255), nullable=True, index=True)

    deal_status = Column(String(255), nullable=True, index=True)
    deal_processing_status = Column(String(255), nullable=True, index=True)
    external_id = Column(String(255), nullable=True, index=True)
    account = Column(String(255), nullable=True, index=True)

    # deal_type = Column(String(255), index=True)
    master_deal_id = Column(Integer, nullable=True, index=True)
    comment = Column(Text, nullable=True)

    # deal_type_data = Column(JSON, default={})
    reference_price = Column(Numeric(40, 20), nullable=True)
    start_asset = Column(String(255), nullable=True)
    start_asset_amount = Column(Numeric(40, 20), nullable=True)
    end_asset = Column(String(255), nullable=True)
    end_asset_amount = Column(Numeric(40, 20), nullable=True)
    fee_asset = Column(String(255), nullable=True)
    fee_proportion = Column(Numeric(40, 20), nullable=True)
    fee_amount = Column(Numeric(40, 20), nullable=True)
    fee_adjustment_amount = Column(Numeric(40, 20), nullable=True)
    is_complete = Column(Boolean, nullable=True)
    incoming_settled = Column(Boolean, nullable=True)
    client_settled = Column(Boolean, nullable=True)
    fee_settled = Column(Boolean, nullable=True)
    incoming_settled_date = Column(DATETIME(fsp=6), nullable=True)
    client_settled_date = Column(DATETIME(fsp=6), nullable=True)
    fee_settled_date = Column(DATETIME(fsp=6), nullable=True)

    extra = Column(JSON, nullable=True, default={})
    version = Column(Integer, index=True)
    valid_from = Column(DATETIME(fsp=6), index=True)
    valid_to = Column(DATETIME(fsp=6), nullable=True, index=True)
    maker_id = Column(Integer, index=True)
    checker_id = Column(Integer, index=True)

    system_remark = Column(String(255), nullable=True, index=True)

    def unsafe_equal(self, _deal):
        return (
            (self.counterparty_ref == _deal.counterparty_ref) and
            (self.counterparty_name == _deal.counterparty_name) and
            (self.portfolio_number == _deal.portfolio_number) and
            (self.value_date == _deal.value_date) and
            (self.start_asset == _deal.start_asset) and
            _compare_float(self.start_asset_amount, _deal.start_asset_amount) and
            (self.end_asset == _deal.end_asset) and
            _compare_float(self.end_asset_amount, _deal.end_asset_amount) and
            (self.fee_asset == _deal.fee_asset) and
            _compare_float(self.fee_proportion, _deal.fee_proportion) and
            _compare_float(self.fee_amount, _deal.fee_amount)
        )
