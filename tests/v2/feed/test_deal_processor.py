import random

import pytest
from altonomy.ace.enums import CashFlowPurpose, CompCode, DealProcessingStatus
from altonomy.ace.models import FeedV2
from altonomy.ace.v2.feed.xalpha_feed_processor import XAlphaFeedProcessor
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
def deal_processor(db) -> XAlphaFeedProcessor:
    return XAlphaFeedProcessor(db)


@pytest.fixture(autouse=True)
def reset_db(db) -> None:
    clear_db(db)


def assert_deal_feed(deal: dict, feed: FeedV2) -> bool:
    assert feed.deal_id == deal["deal_id"]
    assert feed.master_deal_id == deal["master_deal_id"]
    assert feed.portfolio == deal["portfolio_number"]
    assert feed.entity == deal["portfolio_entity"]
    assert feed.deal_ref == deal["deal_ref"]
    assert feed.master_deal_ref == deal["master_deal_ref"]
    assert feed.product == deal["deal_type"]
    assert feed.coa_code == "-1"
    assert feed.asset_price == 0
    assert feed.counterparty_ref == deal["counterparty_ref"]
    assert feed.counterparty_name == deal["counterparty_name"]
    assert feed.account == deal["account"]
    assert feed.value_date == deal["value_date"].replace(tzinfo=None)
    assert feed.trade_date == deal["trade_date"].replace(tzinfo=None)


def assert_fx_spot_feeds(deal_processor, deal) -> bool:
    for comp_code in FX_SPOT_COMP_CODES:
        feed = deal_processor.get_current_feed(deal["deal_id"], comp_code)
        assert feed is not None
        assert feed.comp_code == comp_code
        assert_deal_feed(deal, feed)


def assert_fx_spot_no_feeds(deal_processor, deal) -> bool:
    for comp_code in FX_SPOT_COMP_CODES:
        feed = deal_processor.get_current_feed(deal["deal_id"], comp_code)
        assert feed is None


def count_deal_feed_open(db, deal) -> int:
    return (
        db.query(FeedV2)
        .filter(
            FeedV2.deal_id == deal["deal_id"],
            FeedV2.effective_date_end.is_(None),
        )
    ).count()


def count_deal_feed_all(db, deal) -> int:
    return (
        db.query(FeedV2)
        .filter(FeedV2.deal_id == deal["deal_id"])
    ).count()


def count_deal_child_feed_open(db, deal) -> int:
    return (
        db.query(FeedV2)
        .filter(
            FeedV2.master_deal_id == deal["deal_id"],
            FeedV2.effective_date_end.is_(None),
        )
    ).count()


def count_deal_child_feed_all(db, deal) -> int:
    return (
        db.query(FeedV2)
        .filter(FeedV2.master_deal_id == deal["deal_id"])
    ).count()


class TestXAlphaParent:

    def test_create_child_closes_parent(self, db, deal_processor: XAlphaFeedProcessor) -> None:
        parent = mock_fx_spot_deal()
        child = mock_fx_spot_deal()
        child["master_deal_id"] = parent["deal_id"]

        deal_processor.process_deal(parent)

        assert_fx_spot_feeds(deal_processor, parent)

        deal_processor.process_deal(child)

        assert child["master_deal_id"] == parent["deal_id"]
        assert_fx_spot_no_feeds(deal_processor, parent)

    def test_delete_only_child_opens_parent(self, db, deal_processor: XAlphaFeedProcessor) -> None:
        parent = mock_fx_spot_deal()
        child = mock_fx_spot_deal()
        child["master_deal_id"] = parent["deal_id"]

        deal_processor.process_deal(parent)
        assert_fx_spot_feeds(deal_processor, parent)

        deal_processor.process_deal(child)
        assert child["master_deal_id"] == parent["deal_id"]
        assert_fx_spot_no_feeds(deal_processor, parent)

        child["deal_processing_status"] = DealProcessingStatus.Cancelled
        deal_processor.process_deal(child)
        assert_fx_spot_feeds(deal_processor, parent)

    def test_delete_one_child(self, db, deal_processor: XAlphaFeedProcessor) -> None:
        parent = mock_fx_spot_deal()
        child_1 = mock_fx_spot_deal()
        child_1["master_deal_id"] = parent["deal_id"]
        child_2 = mock_fx_spot_deal()
        child_2["master_deal_id"] = parent["deal_id"]

        deal_processor.process_deal(parent)
        assert_fx_spot_feeds(deal_processor, parent)

        deal_processor.process_deal(child_1)
        deal_processor.process_deal(child_2)
        assert child_1["master_deal_id"] == parent["deal_id"]
        assert child_2["master_deal_id"] == parent["deal_id"]
        assert_fx_spot_no_feeds(deal_processor, parent)

        child_1["deal_processing_status"] = DealProcessingStatus.Cancelled
        deal_processor.process_deal(child_1)
        assert_fx_spot_no_feeds(deal_processor, parent)

    def test_update_deleted_child_does_not_copy_parent(self, db, deal_processor: XAlphaFeedProcessor) -> None:
        parent = mock_execution_deal()
        child_1 = mock_cashflow_deal()
        child_1["master_deal_id"] = parent["deal_id"]
        child_2 = mock_cashflow_deal()
        child_2["master_deal_id"] = parent["deal_id"]

        parent["deal_processing_status"] = DealProcessingStatus.Confirmed
        child_1["deal_processing_status"] = DealProcessingStatus.Confirmed
        child_2["deal_processing_status"] = DealProcessingStatus.Pending

        deal_processor.process_deal(parent)
        assert count_deal_child_feed_open(db, parent) == 0
        assert count_deal_feed_all(db, parent) == 3
        assert count_deal_feed_open(db, parent) == 3

        deal_processor.process_deal(child_1)
        assert count_deal_feed_open(db, child_1) == 1
        assert count_deal_child_feed_open(db, parent) == 1
        assert count_deal_feed_all(db, parent) == 3
        assert count_deal_feed_open(db, parent) == 0

        deal_processor.process_deal(child_2)
        assert count_deal_feed_open(db, child_2) == 0
        assert count_deal_child_feed_open(db, parent) == 1
        assert count_deal_feed_all(db, parent) == 3
        assert count_deal_feed_open(db, parent) == 0

        parent["deal_processing_status"] = DealProcessingStatus.Cancelled
        child_1["deal_processing_status"] = DealProcessingStatus.Cancelled
        child_2["deal_processing_status"] = DealProcessingStatus.Cancelled

        deal_processor.process_deal(child_1)
        assert count_deal_feed_open(db, child_1) == 0
        assert count_deal_child_feed_open(db, parent) == 0
        assert count_deal_feed_all(db, parent) == 6
        assert count_deal_feed_open(db, parent) == 3

        deal_processor.process_deal(child_2)
        assert count_deal_feed_open(db, child_2) == 0
        assert count_deal_child_feed_open(db, parent) == 0
        assert count_deal_feed_all(db, parent) == 6
        assert count_deal_feed_open(db, parent) == 3

        deal_processor.process_deal(parent)
        assert count_deal_feed_all(db, parent) == 6
        assert count_deal_feed_open(db, parent) == 0

    def test_create_delete_create(self, db, deal_processor: XAlphaFeedProcessor) -> None:
        parent = mock_execution_deal()
        child_1 = mock_cashflow_deal()
        child_1["master_deal_id"] = parent["deal_id"]
        child_2 = mock_cashflow_deal()
        child_2["master_deal_id"] = parent["deal_id"]

        parent["deal_processing_status"] = DealProcessingStatus.Confirmed
        child_1["deal_processing_status"] = DealProcessingStatus.Confirmed
        child_2["deal_processing_status"] = DealProcessingStatus.Confirmed

        deal_processor.process_deal(parent)
        deal_processor.process_deal(child_1)
        deal_processor.process_deal(child_2)

        assert count_deal_feed_open(db, parent) == 0
        assert count_deal_feed_open(db, child_1) == 1
        assert count_deal_feed_open(db, child_2) == 1

        parent["deal_processing_status"] = DealProcessingStatus.Pending
        child_1["deal_processing_status"] = DealProcessingStatus.Pending
        child_2["deal_processing_status"] = DealProcessingStatus.Pending

        deal_processor.process_deal(child_1)
        deal_processor.process_deal(child_2)
        deal_processor.process_deal(parent)

        assert count_deal_feed_open(db, parent) == 0
        assert count_deal_feed_open(db, child_1) == 0
        assert count_deal_feed_open(db, child_2) == 0

        parent["deal_processing_status"] = DealProcessingStatus.Confirmed
        child_1["deal_processing_status"] = DealProcessingStatus.Confirmed
        child_2["deal_processing_status"] = DealProcessingStatus.Confirmed

        deal_processor.process_deal(child_1)
        deal_processor.process_deal(child_2)
        deal_processor.process_deal(parent)

        assert count_deal_feed_open(db, parent) == 0
        assert count_deal_feed_open(db, child_1) == 1
        assert count_deal_feed_open(db, child_2) == 1


class TestCashFlow:

    def test_cashflow_change_comp_code(self, db, deal_processor: XAlphaFeedProcessor) -> None:
        deal = mock_cashflow_deal()
        deal_id = deal["deal_id"]
        cashflow_purpose = random.sample(list(CashFlowPurpose), 2)

        deal["deal_type_data"]["cashflow_purpose"] = cashflow_purpose[0]
        deal_processor.process_deal(deal)
        feed = deal_processor.get_current_feed(deal_id, CompCode.CASHFLOW_TRANSFER)
        assert feed is not None
        comp_code_1 = feed.comp_code

        deal["deal_type_data"]["cashflow_purpose"] = cashflow_purpose[1]
        deal_processor.process_deal(deal)
        feed = deal_processor.get_current_feed(deal_id, CompCode.CASHFLOW_TRANSFER)
        assert feed is not None
        comp_code_2 = feed.comp_code

        assert comp_code_1 is not None
        assert comp_code_2 is not None
        assert comp_code_1 != comp_code_2
