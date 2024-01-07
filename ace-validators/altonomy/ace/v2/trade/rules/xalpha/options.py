import math
from datetime import datetime
from altonomy.ace.enums import TransferType, DealType, FeedType, OptionExpiryStatus
from altonomy.ace.v2.trade.rules.xalpha.common import (
    create_trade_from_deal, dedup_and_create_cancel_rule,
    general_trade_type_rule, option_dedup_and_create_cancel_rule)


def option(deal_processor, deal):
    data = deal.get("deal_type_data", {})

    direction = 1 if data.get("direction") == "buy" else -1
    option_instrument = data.get("option_instrument")

    trade = create_trade_from_deal(deal)
    trade.transfer_type = TransferType.TRADE
    trade.product = DealType.OPTIONS
    trade.contract = option_instrument
    trade.feed_type = FeedType.Cash if data.get("premium_settled") else general_trade_type_rule(deal)
    trade.base_asset = data.get("base_asset")
    trade.base_amount = direction * data.get("base_asset_amount", 0)
    trade.quote_asset = data.get("premium_asset")
    trade.quote_amount = -direction * data.get("premium_asset_amount", 0)
    trade.fee_asset = data.get("option_fee_asset")
    trade.fee_amount = data.get("fee_amount", 0)
    create_trades, delete_trades = dedup_and_create_cancel_rule(deal_processor, trade, deal)
    return create_trades, delete_trades


def option_fx_spot(deal_processor, deal):
    data = deal.get("deal_type_data", {})

    # direction = 1 if data.get("direction") == "buy" else -1
    # Get Option Expiry Information
    expiry_status = data.get("expiry_status", None)
    option_instrument = data.get("option_instrument")
    effective_start_date = deal.get("valid_from")
    amount = data.get("ace_base_amount", 0)

    if math.isclose(amount, 0) or expiry_status != OptionExpiryStatus.EXERCISED:
        create_trades, delete_trades = option_dedup_and_create_cancel_rule(deal_processor, deal, None, effective_start_date)
    else:
        trade = create_trade_from_deal(deal)
        expiry = data.get("expiry")
        trade.transfer_type = TransferType.TRADE
        trade.product = DealType.FX_SPOT
        trade.contract = option_instrument
        trade.feed_type = FeedType.Cash if (data.get("ace_base_settle", False) and data.get("ace_quote_settle", False)) else general_trade_type_rule(deal)

        trade.base_asset = data.get("ace_base_asset", None)
        trade.base_amount = data.get("ace_base_amount", 0)
        trade.quote_asset = data.get("ace_quote_asset", None)
        trade.quote_amount = data.get("ace_quote_amount", 0)
        trade.fee_asset = data.get("option_fee_asset")
        trade.fee_amount = 0
        trade.trade_date = trade.value_date = datetime.fromtimestamp(expiry)

        create_trades, delete_trades = dedup_and_create_cancel_rule(deal_processor, trade, deal)
    return create_trades, delete_trades
