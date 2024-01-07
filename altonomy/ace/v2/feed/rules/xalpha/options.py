from datetime import datetime
import math
from altonomy.ace.enums import CompCode, FeedType, OptionExpiryStatus, TransferType, OptionInitialMarginDirection
from altonomy.ace.v2.feed.rules.xalpha.common import (create_feed_from_deal, dedup_and_create_cancel_rule, general_feed_type_rule, option_dedup_and_cancel_rule)


def options_premium(deal_processor, deal):
    comp_code = CompCode.OPTIONS_PREMIUM
    data = deal.get("deal_type_data", {})
    option_instrument = data.get("option_instrument")
    direction = -1 if data.get("direction") == "buy" else 1

    premium = create_feed_from_deal(deal)
    premium.comp_code = comp_code
    premium.asset = data.get("premium_asset")
    premium.amount = direction * data.get("premium_asset_amount")
    premium.transfer_type = TransferType.TRADE
    premium.feed_type = FeedType.Cash if data.get("premium_settled") else general_feed_type_rule(deal)
    premium.contract = option_instrument
    premium.value_date = deal.get("trade_date")

    create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, premium, deal, comp_code)
    return create_feeds, delete_feeds


def options_notional(deal_processor, deal):
    comp_code = CompCode.OPTIONS_NOTIONAL
    data = deal.get("deal_type_data", {})
    option_instrument = data.get("option_instrument")
    expiry = data.get("expiry")
    direction = 1 if data.get("direction") == "buy" else -1

    notional = create_feed_from_deal(deal)
    notional.comp_code = comp_code
    notional.asset = data.get("base_asset")
    notional.amount = direction * data.get("notional")
    notional.transfer_type = TransferType.TRADE
    notional.feed_type = FeedType.Cash if data.get("premium_settled") else general_feed_type_rule(deal)
    # Notional will use expiry date as value date.
    notional.value_date = datetime.fromtimestamp(expiry)

    notional.contract = f"{option_instrument}(N)"

    create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, notional, deal, comp_code)
    return create_feeds, delete_feeds


def options_fee(deal_processor, deal):
    comp_code = CompCode.OPTIONS_FEE
    data = deal.get("deal_type_data", {})
    option_instrument = data.get("option_instrument")
    effective_start_date = deal.get("valid_from")
    amount = data.get("fee_amount", 0)
    if math.isclose(amount, 0):
        create_feeds, delete_feeds = option_dedup_and_cancel_rule(deal_processor, deal, comp_code, None, effective_start_date)
    else:
        fee = create_feed_from_deal(deal)
        fee.comp_code = comp_code
        fee.asset = data.get("option_fee_asset")
        fee.amount = data.get("fee_amount", 0)
        # Fee Should be negative
        if fee.amount > 0:
            fee.amount = -1 * fee.amount
        fee.transfer_type = TransferType.TRADE
        fee.feed_type = FeedType.Cash if data.get("premium_settled") else general_feed_type_rule(deal)
        fee.contract = option_instrument
        fee.value_date = deal.get("trade_date")

        create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, fee, deal, comp_code)
    return create_feeds, delete_feeds


def options_spot_exercise_base(deal_processor, deal):
    comp_code = CompCode.OPTIONS_SPOT_EXERCISE_BASE
    data = deal.get("deal_type_data", {})
    effective_start_date = deal.get("valid_from")
    amount = data.get("ace_base_amount", 0)
    expiry_status = data.get("expiry_status", None)

    if math.isclose(amount, 0) or expiry_status != OptionExpiryStatus.EXERCISED:
        create_feeds, delete_feeds = option_dedup_and_cancel_rule(deal_processor, deal, comp_code, None, effective_start_date)
    else:
        option_instrument = data.get("option_instrument")
        expiry = data.get("expiry")
        exercise_base = create_feed_from_deal(deal)
        exercise_base.comp_code = comp_code
        exercise_base.asset = data.get("ace_base_asset", None)
        exercise_base.amount = data.get("ace_base_amount", 0)
        exercise_base.transfer_type = TransferType.TRADE
        exercise_base.feed_type = FeedType.Cash if data.get("ace_base_settle", False) else general_feed_type_rule(deal)
        exercise_base.trade_date = exercise_base.value_date = datetime.fromtimestamp(expiry)
        exercise_base.contract = option_instrument

        create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, exercise_base, deal, comp_code)
    return create_feeds, delete_feeds


def options_spot_exercise_quote(deal_processor, deal):
    comp_code = CompCode.OPTIONS_SPOT_EXERCISE_QUOTE
    data = deal.get("deal_type_data", {})
    effective_start_date = deal.get("valid_from")
    amount = data.get("ace_quote_amount", 0)
    expiry_status = data.get("expiry_status", None)

    if math.isclose(amount, 0) or expiry_status != OptionExpiryStatus.EXERCISED:
        create_feeds, delete_feeds = option_dedup_and_cancel_rule(deal_processor, deal, comp_code, None, effective_start_date)
    else:
        option_instrument = data.get("option_instrument")
        expiry = data.get("expiry")
        exercise_quote = create_feed_from_deal(deal)
        exercise_quote.comp_code = comp_code
        exercise_quote.asset = data.get("ace_quote_asset", None)
        exercise_quote.amount = data.get("ace_quote_amount", 0)
        exercise_quote.transfer_type = TransferType.TRADE
        exercise_quote.feed_type = FeedType.Cash if data.get("ace_quote_settle", False) else general_feed_type_rule(deal)
        exercise_quote.trade_date = exercise_quote.value_date = datetime.fromtimestamp(expiry)
        exercise_quote.contract = option_instrument

        create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, exercise_quote, deal, comp_code)
    return create_feeds, delete_feeds


def options_initial_margin(deal_processor, deal):
    data = deal.get("deal_type_data", {})
    comp_code = CompCode.INITIAL_MARGIN_IN
    effective_start_date = deal.get("valid_from")
    amount = data.get("initial_margin") if data.get("initial_margin") is not None else 0
    if math.isclose(amount, 0):
        create_feeds, delete_feeds = option_dedup_and_cancel_rule(deal_processor, deal, comp_code, None, effective_start_date)
    else:
        option_instrument = data.get("option_instrument")
        initial_margin_direction = data.get("initial_margin_direction", OptionInitialMarginDirection.RECEIVE)
        initial_margin = create_feed_from_deal(deal)
        initial_margin.comp_code = comp_code
        initial_margin.asset = data.get("initial_margin_asset") if data.get("initial_margin_asset") is not None else 'USDT'
        initial_margin.amount = amount
        initial_margin.amount = initial_margin.amount if initial_margin_direction == OptionInitialMarginDirection.RECEIVE else -(initial_margin.amount)
        initial_margin.transfer_type = TransferType.TRANSFER
        initial_margin.feed_type = FeedType.Cash if data.get("initial_margin_settled") else general_feed_type_rule(deal)
        initial_margin.contract = option_instrument
        create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, initial_margin, deal, comp_code)
    return create_feeds, delete_feeds


def options_initial_margin_out(deal_processor, deal):
    data = deal.get("deal_type_data", {})
    comp_code = CompCode.INITIAL_MARGIN_OUT
    expiry_status = data.get("expiry_status", OptionExpiryStatus.OPEN)
    effective_start_date = deal.get("valid_from")
    amount = data.get("initial_margin") if data.get("initial_margin") is not None else 0
    if math.isclose(amount, 0) or expiry_status == OptionExpiryStatus.OPEN:
        create_feeds, delete_feeds = option_dedup_and_cancel_rule(deal_processor, deal, comp_code, None, effective_start_date)
    else:
        option_instrument = data.get("option_instrument")
        expiry = data.get("expiry")
        initial_margin_direction = data.get("initial_margin_direction", OptionInitialMarginDirection.RECEIVE)
        initial_margin_out = create_feed_from_deal(deal)
        initial_margin_out.comp_code = comp_code
        initial_margin_out.asset = data.get("initial_margin_out_asset") if data.get("initial_margin_out_asset") is not None else 'USDT'
        initial_margin_amount = data.get("initial_margin") if data.get("initial_margin") is not None else 0
        initial_margin_amount = initial_margin_amount if initial_margin_direction == OptionInitialMarginDirection.RECEIVE else -(initial_margin_amount)

        initial_margin_out.amount = -(initial_margin_amount) if expiry_status in (OptionExpiryStatus.EXERCISED, OptionExpiryStatus.NOT_EXERCISED) else 0
        initial_margin_out.transfer_type = TransferType.TRANSFER
        initial_margin_out.feed_type = FeedType.Cash if data.get("initial_margin_out_settled") else general_feed_type_rule(deal)
        initial_margin_out.contract = option_instrument
        initial_margin_out.trade_date = initial_margin_out.value_date = datetime.fromtimestamp(expiry)

        create_feeds, delete_feeds = dedup_and_create_cancel_rule(deal_processor, initial_margin_out, deal, comp_code)
    return create_feeds, delete_feeds
