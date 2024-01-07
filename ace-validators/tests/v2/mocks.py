from random import choice

from altonomy.ace.enums import DealProcessingStatus, Product, CashFlowPurpose
from tests.test_helpers.utils import random_decimal, random_int, random_string, random_time_past


def mock_deal(deal_type, deal_type_data):
    return {
        "deal_id": random_int(),
        "master_deal_id": None,
        "master_deal_ref": None,
        "valid_from": random_time_past(),
        "valid_to": None,
        "trade_date": random_time_past(),
        "value_date": random_time_past(),
        "portfolio_number": str(random_int()),
        "portfolio_entity": random_string(10),
        "account": str(random_int()),
        "counterparty_ref": random_string(10),
        "counterparty_name": random_string(10),
        "deal_ref": random_string(10),
        "deal_type": deal_type,
        "deal_type_data": deal_type_data,
        "deal_processing_status": DealProcessingStatus.Processing,
        "version": 1,
    }


def mock_fx_spot_deal():
    deal_type = Product.FX_SPOT
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
    deal_type = Product.EXECUTION
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


def mock_cashflow_deal():
    deal_type = Product.CASHFLOW
    deal_type_data = {
        "asset": random_string(4),
        "amount": random_decimal(),
        "direction": choice(["pay", "receive"]),
        "cashflow_purpose": choice(list(CashFlowPurpose)),
        "cashflow_settled_date": random_time_past(),
    }
    return mock_deal(deal_type, deal_type_data)
