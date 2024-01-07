import pytest
from altonomy.ace.accounting_core.xalpha import execution_start, execution_end, execution_fee
from tests.accounting_core.xalpha.base_tests import BaseTestXalpha
from tests.accounting_core.xalpha.mocks import mock_deal
from tests.test_helpers.utils import random_decimal, random_string


class TestXalphaRuleExecution(BaseTestXalpha):

    @pytest.fixture(params=[True, False])
    def is_complete(self, request):
        return request.param

    @pytest.fixture
    def deal(self, is_complete):
        deal_type_data = {
            "start_asset": random_string(4),
            "start_asset_amount": random_decimal(),
            "end_asset": random_string(4),
            "end_asset_amount": random_decimal(),
            "fee_asset": random_string(4),
            "fee_asset_amount": random_decimal(),
            "fee_porportion": random_decimal(),
            "is_complete": is_complete,
        }
        return mock_deal("Execution", deal_type_data)

    @pytest.fixture
    def feeds(self, context, deal):
        feeds = []
        feeds.extend(execution_start(context, deal))
        feeds.extend(execution_end(context, deal))
        feeds.extend(execution_fee(context, deal))
        return feeds

    def test_product(self, feeds):
        assert all(feed.product == "Execution" for feed in feeds)

    def test_success(self, feeds):
        assert len(feeds) > 0
