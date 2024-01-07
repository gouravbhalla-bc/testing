from altonomy.ace.enums import TransferType
from altonomy.ace.v2.trade.rules.xalpha.common import (
    create_trade_from_deal, dedup_and_create_cancel_rule,
    general_trade_type_rule)


def execution(deal_processor, deal):
    data = deal.get("deal_type_data", {})

    trade = create_trade_from_deal(deal)
    trade.transfer_type = TransferType.TRADE
    trade.feed_type = general_trade_type_rule(deal)

    trade.base_asset = data.get("start_asset")
    trade.base_amount = data.get("start_asset_amount", 0)
    trade.quote_asset = data.get("end_asset")
    trade.quote_amount = -1 * data.get("end_asset_amount", 0)
    trade.fee_asset = data.get("fee_asset")
    trade.fee_amount = data.get("fee_amount", 0)

    create_trades, delete_trades = dedup_and_create_cancel_rule(deal_processor, trade, deal)
    return create_trades, delete_trades
