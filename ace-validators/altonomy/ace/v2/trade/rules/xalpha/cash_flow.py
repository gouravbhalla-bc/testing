from altonomy.ace.enums import TransferType
from altonomy.ace.v2.trade.rules.xalpha.common import (
    create_trade_from_deal, dedup_and_create_cancel_rule,
    general_trade_type_rule)


def cash_flow(deal_processor, deal):
    data = deal.get("deal_type_data", {})

    sign = 1 if data.get("direction") == "receive" else -1

    trade = create_trade_from_deal(deal)
    trade.transfer_type = TransferType.TRANSFER
    trade.feed_type = general_trade_type_rule(deal)

    trade.base_asset = data.get("asset")
    trade.base_amount = sign * data.get("amount", 0)
    trade.quote_asset = None
    trade.quote_amount = None
    trade.fee_asset = None
    trade.fee_amount = None

    create_trades, delete_trades = dedup_and_create_cancel_rule(deal_processor, trade, deal)
    return create_trades, delete_trades
