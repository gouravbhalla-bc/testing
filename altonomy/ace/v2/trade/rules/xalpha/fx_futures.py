from altonomy.ace.enums import TransferType
from altonomy.ace.v2.trade.rules.xalpha.common import (
    create_trade_from_deal,
    dedup_and_create_cancel_rule,
    general_trade_type_rule,
)


def parse_futures_position_size(value: str):
    # LINK 0.011959270142827855
    last_space = value.rfind(' ')
    if last_space == -1:
        raise Exception('unexpected position syntax')
    amount = float(value[last_space:])
    symbol = value[0:last_space]
    return symbol, amount


def fx_futures(deal_processor, deal):
    data = deal.get("deal_type_data", {})

    trade = create_trade_from_deal(deal)
    trade.transfer_type = TransferType.TRADE
    trade.feed_type = general_trade_type_rule(deal)

    position_size_base = data.get("position_size_base")
    position_size_quote = data.get("position_size_quote")

    trade.base_asset, trade.base_amount = parse_futures_position_size(position_size_base)
    trade.quote_asset, trade.quote_amount = parse_futures_position_size(position_size_quote)
    trade.fee_asset = data.get("fee_asset")
    trade.fee_amount = -1 * float(data.get("fee_amount", 0))
    trade.contract = data.get("trading_pair")

    create_trades, delete_trades = dedup_and_create_cancel_rule(
        deal_processor, trade, deal
    )
    return create_trades, delete_trades
