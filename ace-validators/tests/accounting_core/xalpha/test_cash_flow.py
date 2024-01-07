import pytest
from altonomy.ace.accounting_core.xalpha import cash_flow
from tests.accounting_core.xalpha.base_tests import BaseTestXalpha
from tests.accounting_core.xalpha.mocks import mock_deal
from tests.test_helpers.utils import random_decimal, random_string


class TestXalphaRuleCashflow(BaseTestXalpha):
    @pytest.fixture(
        params=[
            "transfer",
            "fee",
            "etc",
            "trade funding fee",
            "trade insurance clear fee",
        ]
    )
    def cashflow_purpose(self, request):
        return request.param

    @pytest.fixture(params=["pay", "recieve"])
    def direction(self, request):
        return request.param

    @pytest.fixture
    def deal(self, cashflow_purpose, direction):
        deal_type_data = {
            "direction": direction,
            "cashflow_purpose": cashflow_purpose,
            "amount": random_decimal(),
            "asset": random_string(4),
        }
        return mock_deal("Cash Flow", deal_type_data)

    @pytest.fixture
    def feeds(self, context, deal):
        return cash_flow(context, deal)

    def test_product(self, feeds):
        assert all(feed.product == "Cash Flow" for feed in feeds)

    def test_success(self, feeds):
        assert len(feeds) > 0
