from altonomy.ace.enums import (CashFlowPurpose, CompCode, DealType,
                                TransferType)
from altonomy.ace.v2.feed.rules.xalpha.common import (
    create_feed_from_deal, dedup_and_create_cancel_rule,
    general_feed_type_rule)


def cash_flow(deal_processor, deal):
    data = deal.get("deal_type_data", {})
    comp_code = {
        CashFlowPurpose.TRANSFER: CompCode.CASHFLOW_TRANSFER,
        CashFlowPurpose.MM_FEE: CompCode.CASHFLOW_MM_FEE,
        CashFlowPurpose.REFERRAL_FEE: CompCode.CASHFLOW_REFERRAL_FEE,
        CashFlowPurpose.TRANSACTION_FEE: CompCode.CASHFLOW_TRANSACTION_FEE,
        CashFlowPurpose.PNL_DIVIDENDING: CompCode.CASHFLOW_PNL_DIVIDENDING,
        CashFlowPurpose.MM_PROFIT_SHARE: CompCode.CASHFLOW_MM_PROFIT_SHARE,
        CashFlowPurpose.NON_TRADING_EXPENSE: CompCode.CASHFLOW_NON_TRADING_EXPENSE,
        CashFlowPurpose.INTERCO_LOAN: CompCode.CASHFLOW_INTERCO_LOAN,
        CashFlowPurpose.INTERCO_RETURN: CompCode.CASHFLOW_INTERCO_RETURN,
        CashFlowPurpose.FUNDING: CompCode.CASHFLOW_FUNDING,
        CashFlowPurpose.OTHERS: CompCode.CASHFLOW_ETC,
        CashFlowPurpose.ETC: CompCode.CASHFLOW_ETC,
        CashFlowPurpose.BUSINESS_PNL: CompCode.CASHFLOW_BUSINESS_PNL,
        CashFlowPurpose.OTHER_INCOME: CompCode.CASHFLOW_OTHER_INCOME,
        CashFlowPurpose.OTHER_EXPENSE: CompCode.CASHFLOW_OTHER_EXPENSE,
        CashFlowPurpose.INVESTMENTS: CompCode.CASHFLOW_INVESTMENTS,

        #  Execution Child Deals
        CashFlowPurpose.EXECUTION_START: CompCode.EXECUTION_CASHFLOW_START,
        CashFlowPurpose.EXECUTION_END: CompCode.EXECUTION_CASHFLOW_END,
        CashFlowPurpose.EXECUTION_FEE: CompCode.EXECUTION_CASHFLOW_FEE,
        CashFlowPurpose.EXECUTION_TRANSFER: CompCode.EXECUTION_CASHFLOW_TRANSFER,

        # Trading fees
        CashFlowPurpose.FUNDING_FEE: CompCode.CASHFLOW_FUNDING_FEE,
        CashFlowPurpose.INSURANCE_CLEAR: CompCode.CASHFLOW_INSURANCE_CLEAR,

        # NFT
        CashFlowPurpose.NFT_BID_ASK: CompCode.CASHFLOW_NFT_BID_ASK,
        CashFlowPurpose.NFT_TOKEN: CompCode.CASHFLOW_NFT_TOKEN,
        CashFlowPurpose.NFT_SERVICE_FEE: CompCode.CASHFLOW_NFT_SERVICE_FEE,

        # Variation Margin Options
        CashFlowPurpose.VARIATION_MARGIN: CompCode.VARIATION_MARGIN,
    }.get(data.get("cashflow_purpose"), CompCode.CASHFLOW_ETC)

    sign = 1 if data.get("direction") == "receive" else -1

    cash = create_feed_from_deal(deal)

    if comp_code in (
        CompCode.EXECUTION_CASHFLOW_START,
        CompCode.EXECUTION_CASHFLOW_END,
        CompCode.EXECUTION_CASHFLOW_FEE,
        CompCode.EXECUTION_CASHFLOW_TRANSFER,
    ):
        cash.product = DealType.EXECUTION
        cash.transfer_type = TransferType.TRADE
    else:
        cash.transfer_type = TransferType.TRANSFER

    cash.comp_code = comp_code
    cash.asset = data.get("asset")
    cash.amount = sign * data.get("amount")
    cash.feed_type = general_feed_type_rule(deal)

    create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, cash, deal, comp_code)
    return create_feeds, delete_feeds
