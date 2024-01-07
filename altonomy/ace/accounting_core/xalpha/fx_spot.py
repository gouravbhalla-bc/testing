from altonomy.ace.accounting_core.comp_code import CompCode
from altonomy.ace.accounting_core.utils import (apply_general_rule,
                                                create_feed_from_deal)


@apply_general_rule
def fx_spot_base(ctx, deal):
    data = deal.get("deal_type_data", {})

    direction = 1 if data.get("direction") == "buy" else -1
    feeds = []

    base = create_feed_from_deal(ctx, deal)
    base.comp_code = CompCode.FX_SPOT_BASE
    base.asset = data.get("base_asset")
    base.amount = direction * data.get("base_asset_amount")
    base.transfer_type = "trade"
    base.feed_type = "Cash" if data.get("base_settled") else None
    feeds.append(base)

    return feeds


@apply_general_rule
def fx_spot_quote(ctx, deal):
    data = deal.get("deal_type_data", {})

    direction = 1 if data.get("direction") == "buy" else -1
    feeds = []

    quote = create_feed_from_deal(ctx, deal)
    quote.comp_code = CompCode.FX_SPOT_QUOTE
    quote.asset = data.get("quote_asset")
    quote.amount = -direction * data.get("quote_asset_amount")
    quote.transfer_type = "trade"
    quote.feed_type = "Cash" if data.get("quote_settled") else None
    feeds.append(quote)

    return feeds


@apply_general_rule
def fx_spot_fee(ctx, deal):
    data = deal.get("deal_type_data", {})

    feeds = []

    fee = create_feed_from_deal(ctx, deal)
    fee.comp_code = CompCode.FX_SPOT_FEE
    fee.asset = data.get("fee_asset")
    fee.amount = -1 * data.get("fee_amount")
    fee.transfer_type = "trade"
    feeds.append(fee)

    return feeds
