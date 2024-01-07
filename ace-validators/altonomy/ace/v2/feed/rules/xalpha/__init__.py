from altonomy.ace.v2.feed.rules.xalpha.fx_futures import fx_futures_fee, fx_futures_margin, fx_futures_quantity
from altonomy.ace.v2.feed.rules.xalpha.fx_spot import fx_spot_base, fx_spot_quote, fx_spot_fee
from altonomy.ace.v2.feed.rules.xalpha.execution import execution_start, execution_end, execution_fee
from altonomy.ace.v2.feed.rules.xalpha.cash_flow import cash_flow
from altonomy.ace.v2.feed.rules.xalpha.options import options_premium, options_notional, options_fee, options_spot_exercise_base, options_spot_exercise_quote, options_initial_margin, options_initial_margin_out
from altonomy.ace.enums import DealType


handlers = {
    DealType.FX_SPOT: [fx_spot_base, fx_spot_quote, fx_spot_fee],
    DealType.EXECUTION: [execution_start, execution_end, execution_fee],
    DealType.CASHFLOW: [cash_flow],
    DealType.FUTURES: [fx_futures_quantity, fx_futures_margin, fx_futures_fee],
    DealType.OPTIONS: [options_premium, options_notional, options_fee, options_spot_exercise_base, options_spot_exercise_quote, options_initial_margin, options_initial_margin_out],
}


def get_handlers(deal_type):
    return handlers.get(deal_type)
