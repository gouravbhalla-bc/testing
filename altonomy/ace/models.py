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

from altonomy.ace.v2.feed.models import FeedV2, ManualFeedV2  # noqa
from altonomy.ace.v2.trade.models import TradeV2  # noqa
from altonomy.ace.v2.athena.models import SummarySnapshot, SummaryV2Snapshot, SettlementSnapshot, PositionSnapshot  # noqa


class WorkflowTask(Base):
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    execution_id = Column(BigInteger, index=True)
    workflow_name = Column(String(255), index=True)
    task_name = Column(String(255), index=True)
    description = Column(Text)
    maker_id = Column(Integer, index=True)
    checker_id = Column(Integer, index=True)
    stage = Column(Integer)
    total_step = Column(Integer)
    data = Column(JSON, default={})
    extra = Column(JSON, default={})
    status = Column(String(255), index=True)                                        # pending/approved/rejected/checked
    task_type = Column(String(255), index=True)
    create_time = Column(DATETIME(fsp=6), default=datetime.datetime.utcnow, index=True)
    update_time = Column(DATETIME(fsp=6), nullable=True, index=True)


class BaseFeed(Base):

    __abstract__ = True

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    feed_type = Column(String(255), index=True)
    transfer_type = Column(String(255), index=True)
    portfolio = Column(String(255), index=True)
    asset = Column(String(255), index=True)
    amount = Column(Numeric(40, 20), index=True)
    asset_price = Column(Numeric(40, 20), default=0, index=True)
    master_deal_ref = Column(String(255), index=True)
    deal_ref = Column(String(255), index=True)
    counterparty_ref = Column(String(255), index=True)
    counterparty_name = Column(String(255), nullable=True)
    account = Column(String(255), index=True)
    coa_code = Column(String(255), index=True)
    entity = Column(String(255), index=True)
    product = Column(String(255), index=True)
    comp_code = Column(String(255), index=True)
    effective_date = Column(DATETIME(fsp=6), nullable=True, index=True)
    value_date = Column(DATETIME(fsp=6), nullable=True, index=True)
    trade_date = Column(DATETIME(fsp=6), primary_key=True, nullable=True, index=True)
    input_date = Column(DATETIME(fsp=6), default=datetime.datetime.utcnow, index=True)
    as_of_date = Column(DATETIME(fsp=6), nullable=True, index=True)
    as_of_date_end = Column(DATETIME(fsp=6), nullable=True, index=True)
    record_type = Column(String(255), index=True)

    def unsafe_equal(self, another):
        # print("============")
        # print(self.feed_type, another.feed_type, self.feed_type == another.feed_type)
        # print(self.transfer_type, another.transfer_type, self.transfer_type == another.transfer_type)
        # print(self.asset, another.asset, self.asset == another.asset)
        # print(round(float(self.amount), 8), round(float(another.amount), 8), round(float(self.amount), 8) == round(float(another.amount), 8))
        # print(self.deal_ref, another.deal_ref, self.deal_ref == another.deal_ref)
        # print(self.comp_code, another.comp_code, str(self.comp_code) == str(another.comp_code))
        # print(self.as_of_date.timestamp(), another.as_of_date.timestamp(), self.as_of_date.timestamp() == another.as_of_date.timestamp())
        return (
            (self.unsafe_equal_values(another)) and
            (self.as_of_date.timestamp() == another.as_of_date.timestamp()) and
            (self.as_of_date_end.timestamp() == another.as_of_date_end.timestamp()) and
            (self.record_type == another.record_type)
        )

    def unsafe_equal_values(self, another):
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
            (round(float(self.amount), 8) == round(float(another.amount), 8)) and
            (self.deal_ref == another.deal_ref) and
            (self.master_deal_ref == another.master_deal_ref) and
            (self.product == another.product) and
            (self.comp_code == another.comp_code) and
            (self.coa_code == another.coa_code)
        )


class SystemFeed(BaseFeed):
    system_source = Column(String(255), index=True)


class SystemFeedToday(BaseFeed):
    system_source = Column(String(255), index=True)


class SystemFeedError(Base):
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    input_date = Column(DATETIME(fsp=6), default=datetime.datetime.utcnow, index=True)
    version = Column(String(255), index=True)
    system_source = Column(String(255), index=True)
    product = Column(String(255), index=True)
    error_type = Column(String(255), index=True)
    reason = Column(Text)
    data = Column(JSON)


class ManualFeed(BaseFeed):
    extra = Column(JSON, nullable=True, default={})
    version = Column(Integer, index=True)
    valid_from = Column(DATETIME(fsp=6), default=datetime.datetime.utcnow, index=True)
    valid_to = Column(DATETIME(fsp=6), nullable=True, index=True)
    maker_id = Column(Integer, index=True)
    checker_id = Column(Integer, index=True)


class TickerFeed(Base):
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    base_asset = Column(String(255), index=True)
    quote_asset = Column(String(255), index=True)
    price = Column(Numeric(40, 20), default=0, index=True)
    effective_date = Column(DATETIME(fsp=6), nullable=True, index=True)
    input_date = Column(DATETIME(fsp=6), default=datetime.datetime.utcnow, index=True)


class ReportMoPnl(Base):

    __abstract__ = True

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    asset = Column(String(255), index=True)
    snapshot_date = Column(DATETIME(fsp=6), index=True)
    reference_snapshot = Column(Integer, nullable=True, index=True)

    quantity = Column(Numeric(40, 20), index=True)
    amount_usd = Column(Numeric(40, 20), index=True)
    weighted_average_price = Column(Numeric(40, 20), index=True)
    pnl = Column(Numeric(40, 20), index=True)

    record_version = Column(Integer, index=True)
    record_start_date = Column(DATETIME(fsp=6), default=datetime.datetime.utcnow, index=True)
    record_end_date = Column(DATETIME(fsp=6), nullable=True, index=True)


class ElwoodTransferCounter(Base):
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    folder = Column(String(32), index=True)
    name = Column(String(32), index=True)
    type = Column(String(32), index=True)

    portfolio = Column(String(255), index=True)
    system_record_date = Column(DATETIME(fsp=6), default=datetime.datetime.utcnow, index=True)
    enable = Column(Boolean, default=True)
    last_deal_id = Column(Integer, index=True)
    effective_date_start = Column(DATETIME(fsp=6), index=True)
    effective_date_end = Column(DATETIME(fsp=6), nullable=True, index=True)


class ElwoodService(Base):
    id = Column(BigInteger, primary_key=True, autoincrement=True, index=True)
    name = Column(String(255), nullable=True, index=True)
    last_id = Column(BigInteger, default=0)
    count = Column(BigInteger, default=0)
    start_date = Column(DATETIME(fsp=6), index=True)
    end_date = Column(DATETIME(fsp=6), nullable=True, index=True)


def _compare_float(f1, f2):
    if f1 is None and f2 is None:
        return True

    if f1 is None or f2 is None:
        return False
    return round(float(f1), 15) == round(float(f2), 15)


class ElwoodExport(Base):
    def __init__(self) -> None:
        super().__init__()

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    portfolio = Column(String(255), index=True)
    export = Column(Integer, index=True)
    deal_id = Column(Integer, index=True)
    record_date = Column(DATETIME(fsp=6), primary_key=True, default=datetime.datetime.utcnow, index=True)
    version = Column(Integer)

    trade_date_time = Column(DATETIME(fsp=6), nullable=True)
    buy_sell = Column(String(255))
    exchange = Column(String(255))
    symbol = Column(String(255))
    product_type = Column(String(255))
    base = Column(String(255))
    quote = Column(String(255))
    instrument_expiry = Column(DATETIME(fsp=6), nullable=True)
    quantity = Column(Numeric(40, 20))
    price = Column(Numeric(40, 20))
    fee = Column(Numeric(40, 20))
    fee_currency = Column(String(255))
    cacl_fee = Column(Boolean)
    book = Column(String(255))
    account = Column(String(255))
    strategy = Column(String(255))
    trader = Column(String(255))
    memo = Column(String(255))
    unique_id = Column(String(255))
    counterparty = Column(String(255))
    effective_date_start = Column(DATETIME(fsp=6), default=datetime.datetime.utcnow, index=True)
    effective_date_end = Column(DATETIME(fsp=6), nullable=True, index=True)

    def unsafe_equal_values(self, another) -> bool:
        return (
            (self.trade_date_time == another.trade_date_time) and
            (self.exchange == another.exchange) and
            (self.symbol == another.symbol) and
            (self.product_type == another.product_type) and
            (self.base == another.base) and
            (self.quote == another.quote) and
            (self.instrument_expiry == another.instrument_expiry) and
            _compare_float(self.quantity, another.quantity) and
            _compare_float(self.price, another.price) and
            _compare_float(self.fee, another.fee) and
            (self.book == another.book) and
            (self.account == another.account) and
            (self.strategy == another.strategy) and
            (self.counterparty == another.counterparty)
        )


class ElwoodSettlementExport(Base):
    def __init__(self) -> None:
        super().__init__()

    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    # portfolio = Column(String(255), index=True)
    export = Column(Integer, index=True)
    settlement_id = Column(Integer, index=True)
    record_date = Column(DATETIME(fsp=6), default=datetime.datetime.utcnow, index=True)
    version = Column(Integer)

    transaction_date_time = Column(DATETIME(fsp=6), nullable=True)
    created_date_time = Column(DATETIME(fsp=6), nullable=True)
    exchange = Column(String(255))
    symbol = Column(String(255))
    type = Column(String(255))
    quantity = Column(Numeric(40, 20))
    fee = Column(Numeric(40, 20))
    fee_reducing = Column(Boolean)
    account = Column(String(255))
    sub_account = Column(String(255))
    tx_hash = Column(String(512))
    from_address = Column(String(512))
    to_address = Column(String(512))
    note = Column(String(255))
    external_id = Column(String(255))
    strategy = Column(String(255))
    book = Column(String(255))
    effective_date_start = Column(DATETIME(fsp=6), default=datetime.datetime.utcnow, index=True)
    effective_date_end = Column(DATETIME(fsp=6), nullable=True, index=True)

    def unsafe_equal_values(self, another) -> bool:
        return (
            (self.settlement_id == another.settlement_id) and
            (self.exchange == another.exchange) and
            (self.symbol == another.symbol) and
            (self.type == another.type) and
            _compare_float(self.quantity, another.quantity) and
            _compare_float(self.fee, another.fee) and
            (self.account == another.account) and
            (self.sub_account == another.sub_account) and
            (self.tx_hash == another.tx_hash) and
            (self.from_address == another.from_address) and
            (self.to_address == another.to_address) and
            (self.note == another.note) and
            (self.external_id == another.external_id) and
            (self.strategy == another.strategy) and
            (self.book == another.book)
        )


class ReportMoPnlEntity(ReportMoPnl):
    entity = Column(String(255), index=True)


class ReportMoPnlCounterparty(ReportMoPnl):
    counterparty_ref = Column(String(255), index=True)
