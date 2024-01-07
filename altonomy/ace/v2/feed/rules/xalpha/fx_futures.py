from altonomy.ace.enums import CompCode, FeedType, TransferType
from altonomy.ace.v2.feed.rules.xalpha.common import (
    create_feed_from_deal, dedup_and_create_cancel_rule,
    general_feed_type_rule)


def parse_futures_position_size(value: str):
    # LINK 0.011959270142827855
    last_space = value.rfind(' ')
    if last_space == -1:
        raise Exception('unexpected position syntax')
    amount = float(value[last_space:])
    symbol = value[0:last_space]
    return symbol, amount


def fx_futures_quantity(deal_processor, deal):
    comp_code = CompCode.FUTURES_BASE
    data = deal.get("deal_type_data", {})

    qty = create_feed_from_deal(deal)
    qty.comp_code = comp_code
    qty.transfer_type = TransferType.TRADE

    position_size_base = data.get("position_size_base")
    qty.asset, qty.amount = parse_futures_position_size(position_size_base)
    qty.transfer_type = TransferType.TRADE
    qty.feed_type = FeedType.Cash if data.get("base_settled") else general_feed_type_rule(deal)
    qty.contract = data.get("trading_pair")

    create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, qty, deal, comp_code)
    return create_feeds, delete_feeds


def fx_futures_margin(deal_processor, deal):
    comp_code = CompCode.FUTURES_QUOTE
    data = deal.get("deal_type_data", {})

    margin = create_feed_from_deal(deal)
    margin.comp_code = comp_code

    position_size_quote = data.get("position_size_quote")
    margin.asset, margin.amount = parse_futures_position_size(position_size_quote)
    margin.transfer_type = TransferType.TRADE
    margin.feed_type = FeedType.Cash if data.get("quote_settled") else general_feed_type_rule(deal)
    margin.contract = data.get("trading_pair")

    create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, margin, deal, comp_code)
    return create_feeds, delete_feeds


def fx_futures_fee(deal_processor, deal):
    comp_code = CompCode.FUTURES_FEE
    data = deal.get("deal_type_data", {})

    fee = create_feed_from_deal(deal)
    fee.comp_code = comp_code
    fee.asset = data.get("fee_asset")
    fee.amount = -1 * float(data.get("fee_amount"))
    fee.transfer_type = TransferType.TRADE
    fee.feed_type = general_feed_type_rule(deal)
    fee.contract = data.get("trading_pair")

    create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, fee, deal, comp_code)
    return create_feeds, delete_feeds
