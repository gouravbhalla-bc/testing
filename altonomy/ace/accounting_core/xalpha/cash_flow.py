from altonomy.ace.accounting_core.comp_code import CompCode
from altonomy.ace.accounting_core.utils import (apply_general_rule,
                                                create_feed_from_deal)


@apply_general_rule
def cash_flow(ctx, deal):
    data = deal.get("deal_type_data", {})

    sign = 1 if data.get("direction") == "receive" else -1
    feeds = []

    cash = create_feed_from_deal(ctx, deal)
    cash.comp_code = CompCode.CASHFLOW_TRANSFER
    cash.asset = data.get("asset")
    cash.amount = sign * data.get("amount")
    feeds.append(cash)

    for feed in feeds:
        feed.transfer_type = "transfer"

    return feeds
