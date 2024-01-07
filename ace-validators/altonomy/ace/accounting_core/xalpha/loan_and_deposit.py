from datetime import datetime

from altonomy.ace.accounting_core.comp_code import CompCode
from altonomy.ace.accounting_core.utils import create_feed_from_deal


def loan_and_deposit(ctx, deal):
    data = deal.get("deal_type_data", {})

    seconds_in_day = 86400

    tday = datetime.now().timestamp() // seconds_in_day

    direction = data.get("direction")
    asset = data.get("asset")
    amount = data.get("amount")
    basis = 365  # data.get("basis")
    interest_rate = data.get("interest_rate")
    start = int(data.get("start_date")) // seconds_in_day
    end = int(data.get("end_date")) // seconds_in_day
    sign = 1 if direction == "deposit" else -1

    feeds = []

    if tday == start:
        initial = create_feed_from_deal(ctx, deal)
        initial.feed_type = "Cash"
        initial.comp_code = CompCode.DEPOSIT_INIT if direction == "deposit" else CompCode.LOAN_INIT
        initial.asset = asset
        initial.amount = sign * amount

        feeds.append(initial)

    if tday >= start and tday <= end:
        days_accured = min(end, tday) - start + 1
        feed_type = "Cash" if tday == end else "PV"

        loan = create_feed_from_deal(ctx, deal)
        loan.feed_type = feed_type
        loan.comp_code = CompCode.DEPOSIT_NOM if direction == "deposit" else CompCode.LOAN_NOM
        loan.asset = asset
        loan.amount = -sign * amount

        interest = create_feed_from_deal(ctx, deal)
        interest.feed_type = feed_type
        interest.comp_code = CompCode.DEPOSIT_INTEREST if direction == "deposit" else CompCode.LOAN_INTEREST
        interest.asset = asset
        interest.amount = -sign * amount * (interest_rate / basis) * days_accured

        feeds.append(loan)
        feeds.append(interest)

    for feed in feeds:
        feed.transfer_type = "transfer"

    return feeds
