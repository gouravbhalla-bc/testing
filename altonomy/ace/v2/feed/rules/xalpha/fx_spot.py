from altonomy.ace.enums import CompCode, FeedType, TransferType
from altonomy.ace.v2.feed.rules.xalpha.common import (
    create_feed_from_deal, dedup_and_create_cancel_rule,
    general_feed_type_rule)


def fx_spot_base(deal_processor, deal):
    comp_code = CompCode.FX_SPOT_BASE
    data = deal.get("deal_type_data", {})

    direction = 1 if data.get("direction") == "buy" else -1

    base = create_feed_from_deal(deal)
    base.comp_code = comp_code
    base.asset = data.get("base_asset")
    base.amount = direction * data.get("base_asset_amount")
    base.transfer_type = TransferType.TRADE
    base.feed_type = FeedType.Cash if data.get("base_settled") else general_feed_type_rule(deal)

    create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, base, deal, comp_code)
    return create_feeds, delete_feeds


def fx_spot_quote(deal_processor, deal):
    comp_code = CompCode.FX_SPOT_QUOTE
    data = deal.get("deal_type_data", {})

    direction = 1 if data.get("direction") == "buy" else -1

    quote = create_feed_from_deal(deal)
    quote.comp_code = comp_code
    quote.asset = data.get("quote_asset")
    quote.amount = -direction * data.get("quote_asset_amount")
    quote.transfer_type = TransferType.TRADE
    quote.feed_type = FeedType.Cash if data.get("quote_settled") else general_feed_type_rule(deal)

    create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, quote, deal, comp_code)
    return create_feeds, delete_feeds


def fx_spot_fee(deal_processor, deal):
    comp_code = CompCode.FX_SPOT_FEE
    data = deal.get("deal_type_data", {})

    fee = create_feed_from_deal(deal)
    fee.comp_code = comp_code
    fee.asset = data.get("fee_asset")
    fee.amount = -1 * data.get("fee_amount")
    fee.transfer_type = TransferType.TRADE
    fee.feed_type = general_feed_type_rule(deal)

    create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, fee, deal, comp_code)
    return create_feeds, delete_feeds
