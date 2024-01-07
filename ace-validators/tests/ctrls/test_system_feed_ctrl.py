from datetime import datetime, timedelta

import pytest
from altonomy.ace.accounting_core.comp_code import CompCode
from altonomy.ace.ctrls.system_feed_ctrl import SystemFeedCtrl
from altonomy.ace.enums import DealProcessingStatus, FeedType
from altonomy.ace.models import SystemFeed, SystemFeedError, SystemFeedToday
from tests.accounting_core.xalpha.mocks import (mock_deal, mock_execution_deal,
                                                mock_fx_spot_deal)
from tests.test_helpers.utils import clear_db, random_int, random_string


@pytest.mark.usefixtures("db")
@pytest.fixture
def system_feed_ctrl(db) -> SystemFeedCtrl:
    ctrl = SystemFeedCtrl(db)
    return ctrl


@pytest.fixture(autouse=True)
def reset_db(db) -> None:
    clear_db(db)


def count_create_cash_feeds(db):
    return db.query(SystemFeed).filter(SystemFeed.feed_type == FeedType.Cash, SystemFeed.record_type == "CREATE").count()


def count_open_create_cash_feeds(db):
    return (
        db.query(SystemFeed)
        .filter(
            SystemFeed.feed_type == FeedType.Cash,
            SystemFeed.record_type == "CREATE",
            SystemFeed.as_of_date_end.is_(None),
        )
        .count()
    )


def count_delete_cash_feeds(db):
    return db.query(SystemFeed).filter(SystemFeed.feed_type == FeedType.Cash, SystemFeed.record_type == "DELETE").count()


def count_pv_feeds(db):
    return db.query(SystemFeed).filter(SystemFeed.feed_type == FeedType.PV).count()


def count_create_cash_feeds_today(db):
    return db.query(SystemFeedToday).filter(SystemFeedToday.feed_type == FeedType.Cash, SystemFeedToday.record_type == "CREATE").count()


def count_delete_cash_feeds_today(db):
    return db.query(SystemFeedToday).filter(SystemFeedToday.feed_type == FeedType.Cash, SystemFeedToday.record_type == "DELETE").count()


def handle_xalpha_deals(system_feed_ctrl, deals, is_daily: bool = False, offset: int = 0) -> None:
    effective_date = datetime.now() + timedelta(days=offset)
    as_of_date = effective_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    system_feed_ctrl.handle_xalpha_deals(deals, effective_date, is_daily, as_of_date)


def edit_deal_trade_date(deal: dict) -> None:
    deal["trade_date"] = random_int()


def assert_fx_spot_trade(deal: dict, trade: dict) -> None:
    assert deal["deal_ref"] == trade["deal_ref"]
    assert deal["deal_type"] == trade["product"]
    assert deal["portfolio_number"] == trade["portfolio"]
    assert deal["account"] == trade["account"]
    assert datetime.utcfromtimestamp(deal["trade_date"]) == trade["trade_date"]

    deal_data = deal["deal_type_data"]
    sign = 1 if deal_data["direction"] == "buy" else -1
    assert deal_data["base_asset"] == trade["base_asset"]
    assert round(float(deal_data["base_asset_amount"]), 8) == round(float(sign * trade["base_amount"]), 8)
    assert deal_data["quote_asset"] == trade["quote_asset"]
    assert round(float(deal_data["quote_asset_amount"]), 8) == round(float(-sign * trade["quote_amount"]), 8)
    assert deal_data["fee_asset"] == trade["fee_asset"]
    assert round(float(deal_data["fee_amount"]), 8) == round(float(-1 * trade["fee_amount"]), 8)


def assert_execution_trade(deal: dict, trade: dict) -> None:
    assert deal["deal_ref"] == trade["deal_ref"]
    assert deal["deal_type"] == trade["product"]
    assert deal["portfolio_number"] == trade["portfolio"]
    assert deal["account"] == trade["account"]
    assert datetime.utcfromtimestamp(deal["trade_date"]) == trade["trade_date"]

    deal_data = deal["deal_type_data"]
    assert deal_data["start_asset"] == trade["base_asset"]
    assert round(float(deal_data["start_asset_amount"]), 8) == round(float(-1 * trade["base_amount"]), 8)
    assert deal_data["end_asset"] == trade["quote_asset"]
    assert round(float(deal_data["end_asset_amount"]), 8) == round(float(trade["quote_amount"]), 8)
    assert deal_data["fee_asset"] == trade["fee_asset"]
    assert round(float(deal_data["fee_amount"]), 8) == round(float(trade["fee_amount"]), 8)


class TestHandleXalphaDeal:

    @pytest.fixture
    def batch_date(self):
        return datetime.utcnow()

    @pytest.fixture
    def deal(self):
        deal_type = random_string(20)
        deal_type_data = {}
        return mock_deal(deal_type, deal_type_data)

    @pytest.fixture(params=[True, False])
    def is_daily(self, request):
        return request.param

    def test_initial_state(self, db) -> None:
        feed_count = db.query(SystemFeed).count()
        error_count = db.query(SystemFeedError).count()
        assert feed_count == 0
        assert error_count == 0

    def test_flow_error(self, db, system_feed_ctrl, deal, batch_date, is_daily) -> None:
        out_feeds = system_feed_ctrl.handle_xalpha_deal(deal, batch_date, is_daily)
        assert out_feeds is None

        feed_count = db.query(SystemFeed).count()
        assert feed_count == 0

        errors = db.query(SystemFeedError).all()
        error_count = len(errors)
        assert error_count == 1

        error = errors[0]
        assert error.error_type == "Flow"  # TODO: more detailed checks on error log properly created

    @pytest.mark.xfail(reason="TODO")
    def test_code_error(self, db, system_feed_ctrl, deal) -> None:
        pass

    def test_flow_success(self, db, system_feed_ctrl, deal, monkeypatch, batch_date, is_daily) -> None:

        def dummy(*args, **kwargs):
            return [SystemFeed()]

        mock_comp_code = 0

        mock_handlers = [
            (mock_comp_code, dummy),
            (mock_comp_code, dummy),
            (mock_comp_code, dummy),
        ]

        def mock_get_handler(deal_type):
            assert deal_type == deal.get("deal_type")

            def dummy(*args, **kwargs):
                return [SystemFeed()]

            return mock_handlers

        monkeypatch.setattr('altonomy.ace.ctrls.system_feed_ctrl.get_handler', mock_get_handler)

        as_of_date = datetime.utcnow()
        as_of_date = as_of_date.replace(hour=0, minute=0, second=0, microsecond=0)
        deal.update({
            "as_of_date": as_of_date
        })

        out_feeds = system_feed_ctrl.handle_xalpha_deal(deal, batch_date, is_daily)
        assert len(out_feeds) == len(mock_handlers)  # TODO: more detailed checks on feeds

    def test_deal_settled(self, db, system_feed_ctrl: SystemFeedCtrl) -> None:
        '''deal status settled'''
        deal = mock_fx_spot_deal()
        deal["deal_processing_status"] = DealProcessingStatus.Settled
        deals = [deal]

        handle_xalpha_deals(system_feed_ctrl, deals)
        assert count_create_cash_feeds(db) == 3
        assert count_pv_feeds(db) == 0

    def test_deal_processing(self, db, system_feed_ctrl: SystemFeedCtrl) -> None:
        '''deal status processing'''
        deal = mock_fx_spot_deal()
        deal["deal_processing_status"] = DealProcessingStatus.Processing
        deals = [deal]

        handle_xalpha_deals(system_feed_ctrl, deals)
        assert count_create_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 3

    def test_deal_processing_multiple_days(self, db, system_feed_ctrl: SystemFeedCtrl) -> None:
        '''deal status processing over multiple days'''
        deal = mock_fx_spot_deal()
        deal["deal_processing_status"] = DealProcessingStatus.Processing
        deals = [deal]

        handle_xalpha_deals(system_feed_ctrl, deals)
        assert count_create_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 3

        handle_xalpha_deals(system_feed_ctrl, deals, offset=1)
        assert count_create_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 6

        handle_xalpha_deals(system_feed_ctrl, deals, offset=2)
        assert count_create_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 9

    def test_deal_processing_to_settled(self, db, system_feed_ctrl: SystemFeedCtrl) -> None:
        '''deal status from processing to settled'''
        deal = mock_fx_spot_deal()
        deal["deal_processing_status"] = DealProcessingStatus.Processing
        deals = [deal]

        handle_xalpha_deals(system_feed_ctrl, deals, offset=0)
        assert count_create_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 3

        deal["deal_processing_status"] = DealProcessingStatus.Settled

        handle_xalpha_deals(system_feed_ctrl, deals, offset=1)
        assert count_create_cash_feeds(db) == 3
        assert count_pv_feeds(db) == 3

    def test_deal_settled_to_processing(self, db, system_feed_ctrl: SystemFeedCtrl) -> None:
        '''deal status from settled to processing'''
        deal = mock_fx_spot_deal()
        deal["deal_processing_status"] = DealProcessingStatus.Settled
        deals = [deal]

        handle_xalpha_deals(system_feed_ctrl, deals, offset=0)
        assert count_create_cash_feeds(db) == 3

        deal["deal_processing_status"] = DealProcessingStatus.Processing

        handle_xalpha_deals(system_feed_ctrl, deals, offset=1)

        assert count_create_cash_feeds(db) == 3
        assert count_delete_cash_feeds(db) == 3
        assert count_pv_feeds(db) == 3

        handle_xalpha_deals(system_feed_ctrl, deals, offset=2)

        assert count_create_cash_feeds(db) == 3
        assert count_delete_cash_feeds(db) == 3
        assert count_pv_feeds(db) == 6

    def test_deal_settled_today_edit_twice(self, db, system_feed_ctrl: SystemFeedCtrl) -> None:
        '''deal status settled, then edited twice today'''
        deal = mock_fx_spot_deal()
        deal["deal_processing_status"] = DealProcessingStatus.Settled
        deals = [deal]

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=0)
        assert count_create_cash_feeds(db) == 3

        edit_deal_trade_date(deal)

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=1)
        assert count_create_cash_feeds_today(db) == 3
        assert count_delete_cash_feeds_today(db) == 3

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=1)
        assert count_create_cash_feeds_today(db) == 3
        assert count_delete_cash_feeds_today(db) == 3


class TestFeed:

    def test_today_feed_when_edit_portfolio(self, db, system_feed_ctrl: SystemFeedCtrl) -> None:
        '''correct today feed generated when editing deal portfolio'''
        portfolio_1 = "1"
        portfolio_2 = "2"
        portfolios = [portfolio_1, portfolio_2]

        deal = mock_fx_spot_deal()
        deal["portfolio_number"] = portfolio_1
        deal["deal_processing_status"] = DealProcessingStatus.Settled
        deals = [deal]

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False)

        deal["portfolio_number"] = portfolio_2

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=1)

        feeds = system_feed_ctrl.get_feeds_today(portfolios)

        assert len(feeds) == 6
        assert sum(1 for feed in feeds if feed["portfolio"] == portfolio_1) == 3
        assert sum(1 for feed in feeds if feed["portfolio"] == portfolio_2) == 3

        comp_codes = (CompCode.FX_SPOT_BASE, CompCode.FX_SPOT_QUOTE, CompCode.FX_SPOT_FEE)

        for comp_code in comp_codes:
            create_feed = next((f for f in feeds if f["portfolio"] == portfolio_2 and f["comp_code"] == comp_code), None)
            delete_feed = next((f for f in feeds if f["portfolio"] == portfolio_1 and f["comp_code"] == comp_code), None)
            assert create_feed["amount"] == -1 * delete_feed["amount"]
            assert create_feed["asset"] == delete_feed["asset"]


class TestTrade:

    def test_trade_after_edit(self, db, system_feed_ctrl: SystemFeedCtrl) -> None:
        '''cash trade with multiple edits rolled together correctly'''
        deal = mock_fx_spot_deal()
        deal["deal_processing_status"] = DealProcessingStatus.Settled
        deals = [deal]

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=1)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 1
        assert_fx_spot_trade(deal, trades[0])

        edit_deal_trade_date(deal)

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=2)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 1
        assert_fx_spot_trade(deal, trades[0])


class TestHandleXalphaFxSpotDeal:

    def test_fx_spot_base_then_quote_settled(self, db, system_feed_ctrl: SystemFeedCtrl):
        '''fx spot settle base, then settle quote'''
        deal = mock_fx_spot_deal()
        deal["deal_processing_status"] = DealProcessingStatus.Processing
        deal["deal_type_data"]["base_settled"] = True
        deals = [deal]

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=0)
        assert count_open_create_cash_feeds(db) == 1
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 2

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=1)
        assert count_open_create_cash_feeds(db) == 1
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 4

        deal["deal_processing_status"] = DealProcessingStatus.Settled
        deal["deal_type_data"]["quote_settled"] = True

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=2)
        assert count_open_create_cash_feeds(db) == 3
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 4

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=3)
        assert count_open_create_cash_feeds(db) == 3
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 4

    def test_fx_spot_quote_then_base_settled(self, db, system_feed_ctrl: SystemFeedCtrl):
        '''fx spot settle quote, then settle base'''
        deal = mock_fx_spot_deal()
        deal["deal_processing_status"] = DealProcessingStatus.Processing
        deal["deal_type_data"]["quote_settled"] = True
        deals = [deal]

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=0)
        assert count_open_create_cash_feeds(db) == 1
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 2

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=1)
        assert count_open_create_cash_feeds(db) == 1
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 4

        deal["deal_processing_status"] = DealProcessingStatus.Settled
        deal["deal_type_data"]["base_settled"] = True

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=2)
        assert count_open_create_cash_feeds(db) == 3
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 4

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=3)
        assert count_open_create_cash_feeds(db) == 3
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 4

    def test_fx_spot_base_then_quote_settled_with_edit(self, db, system_feed_ctrl: SystemFeedCtrl):
        '''fx spot settle base, then settle quote with edits'''
        deal = mock_fx_spot_deal()
        deal["deal_processing_status"] = DealProcessingStatus.Processing
        deal["deal_type_data"]["base_settled"] = True
        deals = [deal]

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=0)
        assert count_open_create_cash_feeds(db) == 1
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 2

        deal["deal_type_data"]["base_asset"] = random_string(4)

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=1)
        assert count_open_create_cash_feeds(db) == 1
        assert count_create_cash_feeds(db) == 2
        assert count_delete_cash_feeds(db) == 1
        assert count_pv_feeds(db) == 4

        deal["deal_processing_status"] = DealProcessingStatus.Settled
        deal["deal_type_data"]["quote_settled"] = True

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=2)
        assert count_open_create_cash_feeds(db) == 3
        assert count_create_cash_feeds(db) == 4
        assert count_delete_cash_feeds(db) == 1
        assert count_pv_feeds(db) == 4

        deal["deal_type_data"]["base_asset"] = random_string(4)

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=3)
        assert count_open_create_cash_feeds(db) == 3
        assert count_create_cash_feeds(db) == 5
        assert count_delete_cash_feeds(db) == 2
        assert count_pv_feeds(db) == 4

    def test_fx_spot_partial_settled_trade_view_today(self, db, system_feed_ctrl: SystemFeedCtrl):
        '''fx spot partial settled trades today'''
        deal = mock_fx_spot_deal()
        portfolio = deal["portfolio_number"]
        deals = [deal]

        deal["deal_processing_status"] = DealProcessingStatus.Processing
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=0)

        trades = system_feed_ctrl.get_trade_view_today([portfolio])
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_fx_spot_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=0)

        trades = system_feed_ctrl.get_trade_view_today([portfolio])
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_fx_spot_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=1)

        trades = system_feed_ctrl.get_trade_view_today([portfolio])
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_fx_spot_trade(deal, trades[0])

        deal["deal_type_data"]["base_settled"] = True
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=1)

        trades = system_feed_ctrl.get_trade_view_today([portfolio])
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_fx_spot_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=2)

        trades = system_feed_ctrl.get_trade_view_today([portfolio])
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_fx_spot_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=3)

        trades = system_feed_ctrl.get_trade_view_today([portfolio])
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_fx_spot_trade(deal, trades[0])

        deal["deal_processing_status"] = DealProcessingStatus.Settled
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=3)

        trades = system_feed_ctrl.get_trade_view_today([portfolio])
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.Cash
        assert_fx_spot_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=4)

        trades = system_feed_ctrl.get_trade_view_today([portfolio])
        assert len(trades) == 0

    def test_fx_spot_partial_settled_trade_view_past(self, db, system_feed_ctrl: SystemFeedCtrl):
        '''fx spot partial settled trades past'''
        deal = mock_fx_spot_deal()
        deals = [deal]

        deal["deal_processing_status"] = DealProcessingStatus.Processing
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=0)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 0

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=0)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 0

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=1)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 0

        deal["deal_type_data"]["base_settled"] = True
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=1)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 0

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=2)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 0

        deal["deal_processing_status"] = DealProcessingStatus.Settled
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=2)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 0

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=3)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.Cash
        assert_fx_spot_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=4)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.Cash
        assert_fx_spot_trade(deal, trades[0])

    def test_fx_spot_partial_settled_trade_by_portfolio_product(self, db, system_feed_ctrl: SystemFeedCtrl):
        '''fx spot partial settled trades by portfolio product'''
        deal = mock_fx_spot_deal()
        portfolio = deal["portfolio_number"]
        product = deal["deal_type"]
        deals = [deal]

        deal["deal_processing_status"] = DealProcessingStatus.Processing
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=0)

        trades = system_feed_ctrl.get_trade_view_by_product_portfolio([portfolio], product)
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_fx_spot_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=0)

        trades = system_feed_ctrl.get_trade_view_by_product_portfolio([portfolio], product)
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_fx_spot_trade(deal, trades[0])

        deal["deal_type_data"]["base_settled"] = True
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=1)

        trades = system_feed_ctrl.get_trade_view_by_product_portfolio([portfolio], product)
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_fx_spot_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=1)

        trades = system_feed_ctrl.get_trade_view_by_product_portfolio([portfolio], product)
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_fx_spot_trade(deal, trades[0])

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=2)

        trades = system_feed_ctrl.get_trade_view_by_product_portfolio([portfolio], product)
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_fx_spot_trade(deal, trades[0])

        deal["deal_processing_status"] = DealProcessingStatus.Settled
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=2)

        trades = system_feed_ctrl.get_trade_view_by_product_portfolio([portfolio], product)
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.Cash
        assert_fx_spot_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=3)

        trades = system_feed_ctrl.get_trade_view_by_product_portfolio([portfolio], product)
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.Cash
        assert_fx_spot_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=4)

        trades = system_feed_ctrl.get_trade_view_by_product_portfolio([portfolio], product)
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.Cash
        assert_fx_spot_trade(deal, trades[0])


class TestHandleXalphaExecutionDeal:

    def test_client_then_fee_settled(self, db, system_feed_ctrl: SystemFeedCtrl):
        '''execution settle client, then settle end'''
        deal = mock_execution_deal()
        deal["deal_processing_status"] = DealProcessingStatus.Processing
        deal["deal_type_data"]["client_settled"] = True
        deals = [deal]

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=0)
        assert count_create_cash_feeds(db) == 2
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 1

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=1)
        assert count_create_cash_feeds(db) == 2
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 2

        deal["deal_processing_status"] = DealProcessingStatus.Settled
        deal["deal_type_data"]["fee_settled"] = True

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=2)
        assert count_create_cash_feeds(db) == 3
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 2

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=3)
        assert count_create_cash_feeds(db) == 3
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 2

    def test_end_then_client_settled(self, db, system_feed_ctrl: SystemFeedCtrl):
        '''execution settle end, then settle client'''
        deal = mock_execution_deal()
        deal["deal_processing_status"] = DealProcessingStatus.Processing
        deal["deal_type_data"]["fee_settled"] = True
        deals = [deal]

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=0)
        assert count_create_cash_feeds(db) == 1
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 2

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=1)
        assert count_create_cash_feeds(db) == 1
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 4

        deal["deal_processing_status"] = DealProcessingStatus.Settled
        deal["deal_type_data"]["client_settled"] = True

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=2)
        assert count_create_cash_feeds(db) == 3
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 4

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=3)
        assert count_create_cash_feeds(db) == 3
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 4

    def test_client_then_fee_settled_with_edit(self, db, system_feed_ctrl: SystemFeedCtrl):
        '''execution settle client, then settle end with edits'''
        deal = mock_execution_deal()
        deal["deal_processing_status"] = DealProcessingStatus.Processing
        deal["deal_type_data"]["client_settled"] = True
        deals = [deal]

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=0)
        assert count_create_cash_feeds(db) == 2
        assert count_delete_cash_feeds(db) == 0
        assert count_pv_feeds(db) == 1

        deal["deal_type_data"]["start_asset"] = random_string(4)
        deal["deal_type_data"]["end_asset"] = random_string(4)

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=1)
        assert count_create_cash_feeds(db) == 4
        assert count_delete_cash_feeds(db) == 2
        assert count_pv_feeds(db) == 2

        deal["deal_processing_status"] = DealProcessingStatus.Settled

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=2)
        assert count_create_cash_feeds(db) == 5
        assert count_delete_cash_feeds(db) == 2
        assert count_pv_feeds(db) == 2

        deal["deal_type_data"]["start_asset"] = random_string(4)
        deal["deal_type_data"]["end_asset"] = random_string(4)

        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=3)
        assert count_create_cash_feeds(db) == 7
        assert count_delete_cash_feeds(db) == 4
        assert count_pv_feeds(db) == 2

    def test_partial_settled_trade_view_today(self, db, system_feed_ctrl: SystemFeedCtrl):
        '''execution partial settled trades today'''
        deal = mock_execution_deal()
        portfolio = deal["portfolio_number"]
        deals = [deal]

        deal["deal_processing_status"] = DealProcessingStatus.Processing
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=0)

        trades = system_feed_ctrl.get_trade_view_today([portfolio])
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_execution_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=0)

        trades = system_feed_ctrl.get_trade_view_today([portfolio])
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_execution_trade(deal, trades[0])

        deal["deal_type_data"]["client_settled"] = True
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=1)

        trades = system_feed_ctrl.get_trade_view_today([portfolio])
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_execution_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=1)

        trades = system_feed_ctrl.get_trade_view_today([portfolio])
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_execution_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=2)

        trades = system_feed_ctrl.get_trade_view_today([portfolio])
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_execution_trade(deal, trades[0])

        deal["deal_processing_status"] = DealProcessingStatus.Settled
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=3)

        trades = system_feed_ctrl.get_trade_view_today([portfolio])
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.Cash
        assert_execution_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=3)

        trades = system_feed_ctrl.get_trade_view_today([portfolio])
        assert len(trades) == 0

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=4)

        trades = system_feed_ctrl.get_trade_view_today([portfolio])
        assert len(trades) == 0

    def test_partial_settled_trade_view_past(self, db, system_feed_ctrl: SystemFeedCtrl):
        '''execution partial settled trades past'''
        deal = mock_execution_deal()
        deals = [deal]

        deal["deal_processing_status"] = DealProcessingStatus.Processing
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=0)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 0

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=0)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 0

        deal["deal_type_data"]["client_settled"] = True
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=1)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 0

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=1)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 0

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=2)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 0

        deal["deal_processing_status"] = DealProcessingStatus.Settled
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=3)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 0

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=3)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.Cash
        assert_execution_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=4)

        trades = system_feed_ctrl.get_trade_view_past()
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.Cash
        assert_execution_trade(deal, trades[0])

    def test_partial_settled_trade_by_portfolio_product(self, db, system_feed_ctrl: SystemFeedCtrl):
        '''execution partial settled trades by portfolio product'''
        deal = mock_execution_deal()
        portfolio = deal["portfolio_number"]
        product = deal["deal_type"]
        deals = [deal]

        deal["deal_processing_status"] = DealProcessingStatus.Processing
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=0)

        trades = system_feed_ctrl.get_trade_view_by_product_portfolio([portfolio], product)
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_execution_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=0)

        trades = system_feed_ctrl.get_trade_view_by_product_portfolio([portfolio], product)
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_execution_trade(deal, trades[0])

        deal["deal_type_data"]["client_settled"] = True
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=1)

        trades = system_feed_ctrl.get_trade_view_by_product_portfolio([portfolio], product)
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_execution_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=1)

        trades = system_feed_ctrl.get_trade_view_by_product_portfolio([portfolio], product)
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_execution_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=2)

        trades = system_feed_ctrl.get_trade_view_by_product_portfolio([portfolio], product)
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.PV
        assert_execution_trade(deal, trades[0])

        deal["deal_processing_status"] = DealProcessingStatus.Settled
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=True, offset=3)

        trades = system_feed_ctrl.get_trade_view_by_product_portfolio([portfolio], product)
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.Cash
        assert_execution_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=3)

        trades = system_feed_ctrl.get_trade_view_by_product_portfolio([portfolio], product)
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.Cash
        assert_execution_trade(deal, trades[0])

        system_feed_ctrl.dao_today.truncate()
        handle_xalpha_deals(system_feed_ctrl, deals, is_daily=False, offset=4)

        trades = system_feed_ctrl.get_trade_view_by_product_portfolio([portfolio], product)
        assert len(trades) == 1
        assert trades[0]["feed_type"] == FeedType.Cash
        assert_execution_trade(deal, trades[0])
