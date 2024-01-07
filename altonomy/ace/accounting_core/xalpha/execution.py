from altonomy.ace.accounting_core.comp_code import CompCode
from altonomy.ace.accounting_core.utils import (apply_general_rule,
                                                create_feed_from_deal)


@apply_general_rule
def execution_start(ctx, deal):
    data = deal.get("deal_type_data", {})

    feeds = []

    start = create_feed_from_deal(ctx, deal)
    start.comp_code = CompCode.EXECUTION_START
    start.asset = data.get("start_asset")
    start.amount = -1 * data.get("start_asset_amount")
    start.transfer_type = "trade"
    start.feed_type = "Cash" if data.get("client_settled") else None
    feeds.append(start)

    return feeds


@apply_general_rule
def execution_end(ctx, deal):
    data = deal.get("deal_type_data", {})

    feeds = []

    end = create_feed_from_deal(ctx, deal)
    end.comp_code = CompCode.EXECUTION_END
    end.asset = data.get("end_asset")
    end.amount = data.get("end_asset_amount")
    end.transfer_type = "trade"
    end.feed_type = "Cash" if data.get("client_settled") else None
    feeds.append(end)

    return feeds


@apply_general_rule
def execution_fee(ctx, deal):
    data = deal.get("deal_type_data", {})

    feeds = []

    fee = create_feed_from_deal(ctx, deal)
    fee.comp_code = CompCode.EXECUTION_FEE
    fee.asset = data.get("fee_asset")
    fee.amount = data.get("fee_amount")
    fee.transfer_type = "trade"
    fee.feed_type = "Cash" if data.get("fee_settled") else None
    feeds.append(fee)

    return feeds
