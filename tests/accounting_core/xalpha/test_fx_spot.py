import pytest
from altonomy.ace.accounting_core.xalpha import fx_spot_base, fx_spot_quote, fx_spot_fee
from altonomy.ace.accounting_core.comp_code import DealType
from tests.accounting_core.xalpha.base_tests import BaseTestXalpha
from tests.accounting_core.xalpha.mocks import mock_deal
from tests.test_helpers.utils import random_decimal, random_string


class TestXalphaRuleFxSpot(BaseTestXalpha):

    @pytest.fixture(params=["buy", "sell"])
    def direction(self, request):
        return request.param

    @pytest.fixture
    def deal(self, direction):
        deal_type_data = {
            "direction": direction,
            "base_asset": random_string(4),
            "base_asset_amount": random_decimal(),
            "quote_asset": random_string(4),
            "quote_asset_amount": random_decimal(),
            "fee_asset": random_string(4),
            "fee_amount": random_decimal(),
            "unit_price": random_decimal(),
        }
        return mock_deal(DealType.FX_SPOT, deal_type_data)

    @pytest.fixture
    def feeds(self, context, deal):
        feeds = []
        feeds.extend(fx_spot_base(context, deal))
        feeds.extend(fx_spot_quote(context, deal))
        feeds.extend(fx_spot_fee(context, deal))
        return feeds

    def test_product(self, feeds):
        assert all(feed.product == DealType.FX_SPOT for feed in feeds)

    def test_success(self, feeds):
        assert len(feeds) > 0
