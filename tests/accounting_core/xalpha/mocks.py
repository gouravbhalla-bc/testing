from random import choice, randint

from tests.test_helpers.utils import random_decimal, random_int, random_string, random_bool


def mock_deal(deal_type, deal_type_data):
    return {
        "trade_date": random_decimal(),
        "value_date": random_decimal(),
        # exclude portfolio_number 8014, >90000
        "portfolio_number": str(randint(1, 8013) if random_bool() else randint(8015, 89999)),
        "portfolio_entity": random_string(10),
        "account": str(random_int()),
        "counterparty_name": random_string(10),
        "deal_ref": random_string(10),
        "deal_type": deal_type,
        "deal_type_data": deal_type_data,
        "deal_processing_status": "processing",
    }


def mock_fx_spot_deal():
    deal_type = "FX Spot"
    direction = choice(["buy", "sell"])
    deal_type_data = {
        "direction": direction,
        "base_asset": random_string(4),
        "base_asset_amount": random_decimal(),
        "quote_asset": random_string(4),
        "quote_asset_amount": random_decimal(),
        "fee_asset": random_string(4),
        "fee_amount": random_decimal(),
        "unit_price": random_decimal(),
        "base_settled": False,
        "quote_settled": False,
    }
    return mock_deal(deal_type, deal_type_data)


def mock_execution_deal():
    deal_type = "Execution"
    deal_type_data = {
        "start_asset": random_string(4),
        "start_asset_amount": random_decimal(),
        "end_asset": random_string(4),
        "end_asset_amount": random_decimal(),
        "fee_asset": random_string(4),
        "fee_amount": random_decimal(),
        "is_complete": False,
        "start_settled": False,
        "end_settled": False,
    }
    return mock_deal(deal_type, deal_type_data)
