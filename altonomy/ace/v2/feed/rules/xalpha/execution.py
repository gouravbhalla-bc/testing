from altonomy.ace.enums import CompCode, FeedType, TransferType
from altonomy.ace.v2.feed.rules.xalpha.common import (
    create_feed_from_deal, dedup_and_create_cancel_rule,
    general_feed_type_rule)


def execution_start(deal_processor, deal):
    comp_code = CompCode.EXECUTION_START
    data = deal.get("deal_type_data", {})

    start = create_feed_from_deal(deal)
    start.comp_code = comp_code
    start.asset = data.get("start_asset")
    start.amount = data.get("start_asset_amount")
    start.transfer_type = TransferType.TRADE
    start.feed_type = FeedType.Cash if data.get("start_settled") else general_feed_type_rule(deal)

    create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, start, deal, comp_code)
    return create_feeds, delete_feeds


def execution_end(deal_processor, deal):
    comp_code = CompCode.EXECUTION_END
    data = deal.get("deal_type_data", {})

    end = create_feed_from_deal(deal)
    end.comp_code = comp_code
    end.asset = data.get("end_asset")
    end.amount = -1 * data.get("end_asset_amount")
    end.transfer_type = TransferType.TRADE
    end.feed_type = general_feed_type_rule(deal)

    create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, end, deal, comp_code)
    return create_feeds, delete_feeds


def execution_fee(deal_processor, deal):
    comp_code = CompCode.EXECUTION_FEE
    data = deal.get("deal_type_data", {})

    fee = create_feed_from_deal(deal)
    fee.comp_code = comp_code
    fee.asset = data.get("fee_asset")
    fee.amount = data.get("fee_amount")
    fee.transfer_type = TransferType.TRADE
    fee.feed_type = FeedType.Cash if data.get("fee_settled") else general_feed_type_rule(deal)

    create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, fee, deal, comp_code)
    return create_feeds, delete_feeds
