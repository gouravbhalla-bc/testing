from altonomy.ace.accounting_core.utils import apply_general_rule


@apply_general_rule
def options(ctx, deal):
    return []
    # data = deal.get("deal_type_data", {})

    # contract = feed_from_deal(deal)
    # contract.comp_code = CompCode.OPTIONS_PREMIUM
    # contract.asset = data.get("options_contract")
    # contract.amount = data.get("size")

    # premium = create_feed_from_deal(ctx, deal)
    # premium.comp_code = CompCode.OPTIONS_PREMIUM
    # premium.asset = data.get("premium_asset")
    # premium.amount = -1 * data.get("premium_paid")

    # return [contract, premium]
