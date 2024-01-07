from altonomy.ace.accounting_core.xalpha.fx_spot import fx_spot_base, fx_spot_quote, fx_spot_fee
from altonomy.ace.accounting_core.xalpha.cash_flow import cash_flow
from altonomy.ace.accounting_core.xalpha.execution import execution_start, execution_end, execution_fee
from altonomy.ace.accounting_core.comp_code import DealType, CompCode


handlers = {
    DealType.FX_SPOT: [
        (CompCode.FX_SPOT_BASE, fx_spot_base),
        (CompCode.FX_SPOT_QUOTE, fx_spot_quote),
        (CompCode.FX_SPOT_FEE, fx_spot_fee),
    ],
    DealType.CASHFLOW: [
        (CompCode.CASHFLOW_TRANSFER, cash_flow),
    ],
    DealType.EXECUTION: [
        (CompCode.EXECUTION_START, execution_start),
        (CompCode.EXECUTION_END, execution_end),
        (CompCode.EXECUTION_FEE, execution_fee),
    ],
}


def get_handler(deal_type):
    return handlers.get(deal_type)
