from altonomy.ace.accounting_core.utils import apply_general_rule


@apply_general_rule
def futures(ctx, deal):
    return []
    # data = deal.get("deal_type_data", {})

    # fee = feed_from_deal(deal)
    # fee.asset = data.get("fee_asset")
    # fee.amount = data.get("fee_amount")

    # contract = feed_from_deal(deal)
    # contract.asset = data.get("futures_contract")
    # contract.amount = data.get("amount")

    # return [fee, contract]
