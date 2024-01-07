from datetime import datetime

import pytest
from altonomy.ace.accounting_core.xalpha.loan_and_deposit import loan_and_deposit
from tests.accounting_core.xalpha.base_tests import BaseTestXalpha
from tests.accounting_core.xalpha.mocks import mock_deal
from tests.test_helpers.utils import (random_decimal, random_int,
                                      random_string, random_time_future,
                                      random_time_past)


class TestXalphaRuleLoanAndDeposit(BaseTestXalpha):

    @pytest.fixture(params=["loan", "deposit"])
    def direction(self, request):
        return request.param

    @pytest.fixture
    def deal(self, direction):
        deal_type_data = {
            "direction": direction,
            "asset": random_string(4),
            "amount": random_decimal(),
            "interest_rate": random_decimal(),
            "basis": 365,
            "accured_interest": random_decimal(),
            "days_accured": random_int(),
            "start_date": datetime.timestamp(random_time_past()),
            "end_date": datetime.timestamp(random_time_future()),
        }
        return mock_deal("Loan & Deposit", deal_type_data)

    @pytest.fixture
    def feeds(self, context, deal):
        return loan_and_deposit(context, deal)

    def test_product(self, feeds):
        assert all(feed.product == "Loan & Deposit" for feed in feeds)

    def test_success(self, feeds):
        assert len(feeds) > 0
