import datetime
from typing import List

from altonomy.ace.db.base_class import Base
from sqlalchemy import Column, BigInteger, Integer, String, Text, JSON
from sqlalchemy.dialects.mysql import DATETIME


class Deal(Base):
    id = Column(BigInteger, primary_key=True, autoincrement=True, index=True)

    deal_id = Column(Integer, index=True)
    deal_ref = Column(String(255), index=True)

    master_deal_id = Column(Integer, nullable=True, index=True)

    counterparty_ref = Column(String(255), nullable=True, index=True)
    counterparty_name = Column(String(255), nullable=True, index=True)

    portfolio_number = Column(String(255), nullable=True, index=True)
    portfolio_name = Column(String(255), nullable=True, index=True)
    portfolio_entity = Column(String(255), nullable=True, index=True)

    deal_type = Column(String(255), index=True)
    trade_date = Column(DATETIME(fsp=6), default=datetime.datetime.utcnow, index=True)
    value_date = Column(DATETIME(fsp=6), nullable=True, index=True)
    external_id = Column(String(255), nullable=True, index=True)
    account = Column(String(255), nullable=True, index=True)
    deal_processing_status = Column(String(255), default="pending", nullable=True, index=True)
    deal_status = Column(String(255), default="live", nullable=True, index=True)
    comment = Column(Text, nullable=True)

    deal_type_data = Column(JSON, default={})

    extra = Column(JSON, nullable=True, default={})
    version = Column(Integer, index=True)
    valid_from = Column(DATETIME(fsp=6), default=datetime.datetime.utcnow, index=True)
    valid_to = Column(DATETIME(fsp=6), nullable=True, index=True)
    maker_id = Column(Integer, index=True)
    checker_id = Column(Integer, index=True)

    def unsafe_equal(self, _deal):
        deal_type_data_equal = True
        for k in self.deal_type_data:
            if k in self.deal_type_status_column():
                if _deal.deal_type_data.get(k, None) != self.deal_type_data.get(k, None):
                    deal_type_data_equal = False
                    break
        for k in _deal.deal_type_data:
            if k in self.deal_type_status_column():
                if _deal.deal_type_data.get(k, None) != self.deal_type_data.get(k, None):
                    deal_type_data_equal = False
                    break
        return (
            self.master_deal_id == _deal.master_deal_id and
            self.counterparty_ref == _deal.counterparty_ref and
            self.counterparty_name == _deal.counterparty_name and
            self.portfolio_number == _deal.portfolio_number and
            self.portfolio_name == _deal.portfolio_name and
            self.portfolio_entity == _deal.portfolio_entity and
            self.deal_type == _deal.deal_type and
            self.trade_date == _deal.trade_date and
            self.value_date == _deal.value_date and
            deal_type_data_equal
        )

    def deal_type_status_column(self) -> List:
        return ['start_asset', 'start_asset_amount', 'end_asset', 'end_asset_amount', 'fee_asset', 'fee_proportion', 'fee_amount', 'fee_adjustment_amount']
