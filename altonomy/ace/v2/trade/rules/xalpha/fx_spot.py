from altonomy.ace.enums import TransferType
from altonomy.ace.v2.trade.rules.xalpha.common import (
    create_trade_from_deal, dedup_and_create_cancel_rule,
    general_trade_type_rule)


def fx_spot(deal_processor, deal):
    data = deal.get("deal_type_data", {})

    direction = 1 if data.get("direction") == "buy" else -1

    trade = create_trade_from_deal(deal)
    trade.transfer_type = TransferType.TRADE
    trade.feed_type = general_trade_type_rule(deal)

    trade.base_asset = data.get("base_asset")
    trade.base_amount = direction * data.get("base_asset_amount", 0)
    trade.quote_asset = data.get("quote_asset")
    trade.quote_amount = -direction * data.get("quote_asset_amount", 0)
    trade.fee_asset = data.get("fee_asset")
    trade.fee_amount = -1 * data.get("fee_amount", 0)

    create_trades, delete_trades = dedup_and_create_cancel_rule(deal_processor, trade, deal)
    return create_trades, delete_trades
