import pytest
from altonomy.ace.enums import CompCode, DealProcessingStatus, FeedType
from altonomy.ace.models import FeedV2, TradeV2
from altonomy.ace.v2.feed.xalpha_feed_processor import XAlphaFeedProcessor
from altonomy.ace.v2.trade.xalpha_trade_processor import XAlphaTradeProcessor
from tests.test_helpers.utils import clear_db
from tests.v2.mocks import (mock_cashflow_deal, mock_execution_deal,
                            mock_fx_spot_deal)

FX_SPOT_COMP_CODES = (
    CompCode.FX_SPOT_BASE,
    CompCode.FX_SPOT_QUOTE,
    CompCode.FX_SPOT_FEE,
)


@pytest.mark.usefixtures("db")
@pytest.fixture
def feed_processor(db) -> XAlphaFeedProcessor:
    return XAlphaFeedProcessor(db)


@pytest.mark.usefixtures("db")
@pytest.fixture
def trade_processor(db) -> XAlphaTradeProcessor:
    return XAlphaTradeProcessor(db)


@pytest.fixture(autouse=True)
def reset_db(db) -> None:
    clear_db(db)


def assert_trade_feed(trade: TradeV2, feed: FeedV2) -> None:
    assert feed.deal_id == feed.deal_id
    assert feed.master_deal_id == feed.master_deal_id
    assert feed.portfolio == feed.portfolio
    assert feed.entity == feed.entity
    assert feed.deal_ref == feed.deal_ref
    assert feed.master_deal_ref == feed.master_deal_ref
    assert feed.product == feed.product
    assert feed.counterparty_ref == feed.counterparty_ref
    assert feed.counterparty_name == feed.counterparty_name
    assert feed.account == feed.account
    assert feed.value_date == feed.value_date
    assert feed.trade_date == feed.trade_date


def assert_trade_base(trade: TradeV2, feed: FeedV2) -> None:
    assert feed.amount == trade.base_amount
    assert feed.asset == trade.base_asset
    assert feed.asset_price == trade.base_asset_price
    assert_trade_feed(trade, feed)


def assert_trade_quote(trade: TradeV2, feed: FeedV2) -> None:
    assert feed.amount == trade.quote_amount
    assert feed.asset == trade.quote_asset
    assert feed.asset_price == trade.quote_asset_price
    assert_trade_feed(trade, feed)


def assert_trade_fee(trade: TradeV2, feed: FeedV2) -> None:
    assert feed.amount == trade.fee_amount
    assert feed.asset == trade.fee_asset
    assert feed.asset_price == trade.fee_asset_price
    assert_trade_feed(trade, feed)


def assert_trade_quote_none(trade: TradeV2) -> None:
    assert trade.quote_amount is None
    assert trade.quote_asset is None
    assert trade.quote_asset_price == 0


def assert_trade_fee_none(trade: TradeV2) -> None:
    assert trade.quote_amount is None
    assert trade.quote_asset is None
    assert trade.quote_asset_price == 0


def assert_crosscheck_none(deal: dict, feed_processor: XAlphaFeedProcessor, trade_processor: XAlphaTradeProcessor) -> None:
    deal_id = deal["deal_id"]
    trade = trade_processor.get_current_feed(deal_id)
    assert trade is None

    feeds = feed_processor.get_current_feeds_by_deal_id(deal_id)
    assert len(feeds) == 0


def assert_fx_spot_crosscheck(deal: dict, feed_processor: XAlphaFeedProcessor, trade_processor: XAlphaTradeProcessor) -> None:
    deal_id = deal["deal_id"]
    trade = trade_processor.get_current_feed(deal_id)
    assert trade is not None

    base = feed_processor.get_current_feed(deal_id, CompCode.FX_SPOT_BASE)
    assert base is not None
    assert base.comp_code == CompCode.FX_SPOT_BASE
    assert_trade_base(trade, base)

    quote = feed_processor.get_current_feed(deal_id, CompCode.FX_SPOT_QUOTE)
    assert quote is not None
    assert quote.comp_code == CompCode.FX_SPOT_QUOTE
    assert_trade_quote(trade, quote)

    fee = feed_processor.get_current_feed(deal_id, CompCode.FX_SPOT_FEE)
    assert fee is not None
    assert fee.comp_code == CompCode.FX_SPOT_FEE
    assert_trade_fee(trade, fee)

    feed_type = FeedType.Cash if all(feed.feed_type == FeedType.Cash for feed in (base, quote, fee)) else FeedType.PV
    trade_type = trade.feed_type
    assert feed_type == trade_type


def assert_execution_crosscheck(deal: dict, feed_processor: XAlphaFeedProcessor, trade_processor: XAlphaTradeProcessor) -> bool:
    deal_id = deal["deal_id"]
    trade = trade_processor.get_current_feed(deal_id)
    assert trade is not None

    start = feed_processor.get_current_feed(deal_id, CompCode.EXECUTION_START)
    assert start is not None
    assert start.comp_code == CompCode.EXECUTION_START
    assert_trade_base(trade, start)

    end = feed_processor.get_current_feed(deal_id, CompCode.EXECUTION_END)
    assert end is not None
    assert end.comp_code == CompCode.EXECUTION_END
    assert_trade_quote(trade, end)

    fee = feed_processor.get_current_feed(deal_id, CompCode.EXECUTION_FEE)
    assert fee is not None
    assert fee.comp_code == CompCode.EXECUTION_FEE
    assert_trade_fee(trade, fee)

    feed_type = FeedType.Cash if all(feed.feed_type == FeedType.Cash for feed in (start, end, fee)) else FeedType.PV
    trade_type = trade.feed_type
    assert feed_type == trade_type


def assert_cashflow_crosscheck(deal: dict, feed_processor: XAlphaFeedProcessor, trade_processor: XAlphaTradeProcessor) -> bool:
    deal_id = deal["deal_id"]
    trade = trade_processor.get_current_feed(deal_id)
    assert trade is not None

    cashflow = feed_processor.get_current_feed(deal_id, CompCode.CASHFLOW_TRANSFER)
    assert cashflow is not None
    assert_trade_base(trade, cashflow)

    assert_trade_quote_none(trade)
    assert_trade_fee_none(trade)

    assert cashflow.feed_type == trade.feed_type


def process(
    deal: dict,
    feed_processor: XAlphaFeedProcessor,
    trade_processor: XAlphaTradeProcessor,
) -> None:
    feed_processor.process_deal(deal)
    trade_processor.process_deal(deal)


class TestTradeFeedCrossCheck:

    def test_crosscheck_fx_spot(
        self,
        db,
        feed_processor: XAlphaFeedProcessor,
        trade_processor: XAlphaTradeProcessor,
    ) -> None:
        deal = mock_fx_spot_deal()
        process(deal, feed_processor, trade_processor)
        assert_fx_spot_crosscheck(deal, feed_processor, trade_processor)

    def test_crosscheck_execution(
        self,
        db,
        feed_processor: XAlphaFeedProcessor,
        trade_processor: XAlphaTradeProcessor,
    ) -> None:
        deal = mock_execution_deal()
        process(deal, feed_processor, trade_processor)
        assert_execution_crosscheck(deal, feed_processor, trade_processor)

    def test_crosscheck_cashflow(
        self,
        db,
        feed_processor: XAlphaFeedProcessor,
        trade_processor: XAlphaTradeProcessor,
    ) -> None:
        deal = mock_cashflow_deal()
        process(deal, feed_processor, trade_processor)
        assert_cashflow_crosscheck(deal, feed_processor, trade_processor)

    def test_crosscheck_settled_fx_spot(
        self,
        db,
        feed_processor: XAlphaFeedProcessor,
        trade_processor: XAlphaTradeProcessor,
    ) -> None:
        deal = mock_fx_spot_deal()
        deal["deal_processing_status"] = DealProcessingStatus.Processing
        process(deal, feed_processor, trade_processor)
        assert_fx_spot_crosscheck(deal, feed_processor, trade_processor)

        deal["deal_processing_status"] = DealProcessingStatus.Settled
        process(deal, feed_processor, trade_processor)
        assert_fx_spot_crosscheck(deal, feed_processor, trade_processor)

    def test_crosscheck_settled_execution(
        self,
        db,
        feed_processor: XAlphaFeedProcessor,
        trade_processor: XAlphaTradeProcessor,
    ) -> None:
        deal = mock_execution_deal()
        deal["deal_processing_status"] = DealProcessingStatus.Processing
        process(deal, feed_processor, trade_processor)
        assert_execution_crosscheck(deal, feed_processor, trade_processor)

        deal["deal_processing_status"] = DealProcessingStatus.Settled
        process(deal, feed_processor, trade_processor)
        assert_execution_crosscheck(deal, feed_processor, trade_processor)

    def test_crosscheck_settled_cashflow(
        self,
        db,
        feed_processor: XAlphaFeedProcessor,
        trade_processor: XAlphaTradeProcessor,
    ) -> None:
        deal = mock_cashflow_deal()
        deal["deal_processing_status"] = DealProcessingStatus.Processing
        process(deal, feed_processor, trade_processor)
        assert_cashflow_crosscheck(deal, feed_processor, trade_processor)

        deal["deal_processing_status"] = DealProcessingStatus.Settled
        process(deal, feed_processor, trade_processor)
        assert_cashflow_crosscheck(deal, feed_processor, trade_processor)

    def test_crosscheck_create_child_closes_parent(
        self,
        db,
        feed_processor: XAlphaFeedProcessor,
        trade_processor: XAlphaTradeProcessor,
    ) -> None:
        parent = mock_fx_spot_deal()
        child = mock_fx_spot_deal()
        child["master_deal_id"] = parent["deal_id"]

        process(parent, feed_processor, trade_processor)
        assert_fx_spot_crosscheck(parent, feed_processor, trade_processor)

        process(child, feed_processor, trade_processor)
        assert_crosscheck_none(parent, feed_processor, trade_processor)

    def test_crosscheck_delete_only_child_opens_parent(
        self,
        db,
        feed_processor: XAlphaFeedProcessor,
        trade_processor: XAlphaTradeProcessor,
    ) -> None:
        parent = mock_fx_spot_deal()
        child = mock_fx_spot_deal()
        child["master_deal_id"] = parent["deal_id"]

        process(parent, feed_processor, trade_processor)
        assert_fx_spot_crosscheck(parent, feed_processor, trade_processor)

        process(child, feed_processor, trade_processor)
        assert_crosscheck_none(parent, feed_processor, trade_processor)

        child["deal_processing_status"] = DealProcessingStatus.Cancelled
        process(child, feed_processor, trade_processor)
        assert_fx_spot_crosscheck(parent, feed_processor, trade_processor)

    def test_crosscheck_delete_one_child(
        self,
        db,
        feed_processor: XAlphaFeedProcessor,
        trade_processor: XAlphaTradeProcessor,
    ) -> None:
        parent = mock_fx_spot_deal()
        child_1 = mock_fx_spot_deal()
        child_1["master_deal_id"] = parent["deal_id"]
        child_2 = mock_fx_spot_deal()
        child_2["master_deal_id"] = parent["deal_id"]

        process(parent, feed_processor, trade_processor)
        assert_fx_spot_crosscheck(parent, feed_processor, trade_processor)

        process(child_1, feed_processor, trade_processor)
        process(child_2, feed_processor, trade_processor)
        assert_crosscheck_none(parent, feed_processor, trade_processor)

        child_1["deal_processing_status"] = DealProcessingStatus.Cancelled
        process(child_1, feed_processor, trade_processor)
        assert_crosscheck_none(parent, feed_processor, trade_processor)
