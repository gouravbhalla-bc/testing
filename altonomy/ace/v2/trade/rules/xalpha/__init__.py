from altonomy.ace.v2.trade.rules.xalpha.fx_spot import fx_spot
from altonomy.ace.v2.trade.rules.xalpha.fx_futures import fx_futures
from altonomy.ace.v2.trade.rules.xalpha.execution import execution
from altonomy.ace.v2.trade.rules.xalpha.cash_flow import cash_flow
from altonomy.ace.v2.trade.rules.xalpha.options import option, option_fx_spot
from altonomy.ace.enums import DealType


handlers = {
    DealType.FX_SPOT: [fx_spot],
    DealType.EXECUTION: [execution],
    DealType.CASHFLOW: [cash_flow],
    DealType.FUTURES: [fx_futures],
    DealType.OPTIONS: [option, option_fx_spot],
}


def get_handlers(deal_type):
    return handlers.get(deal_type)
