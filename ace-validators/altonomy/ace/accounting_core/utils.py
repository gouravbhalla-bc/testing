from datetime import datetime
from altonomy.ace.enums import DealProcessingStatus


def apply_rules_first_to_last(*rules):

    def wrapper_rule(fn):

        def wrapped_rule(ctx, deal):
            ctx.new_feeds = fn(ctx, deal)
            for rule in rules:
                ctx.new_feeds = rule(ctx, deal)
            return ctx.new_feeds

        return wrapped_rule

    return wrapper_rule


def general_feed_type_rule(ctx, deal):
    is_settled = deal.get("deal_processing_status") == DealProcessingStatus.Settled
    is_cancelled = deal.get("deal_processing_status") == DealProcessingStatus.Cancelled

    for feed in ctx.new_feeds:
        if feed.feed_type is None:
            feed.feed_type = "Cash" if is_settled else "PV"

        if is_cancelled:
            feed.record_type = None

    return ctx.new_feeds


def general_feed_cancel_rule(ctx, deal):
    feeds = []
    feeds.extend(cancel_feed_from_old_feed(ctx, deal, feed) for feed in ctx.current_feeds)
    feeds.extend(ctx.new_feeds)
    return feeds


def general_feed_no_duplicate_cash_feed_rule(ctx, deal):
    to_del = [new for new in ctx.new_feeds if new.feed_type == "Cash" and any(new.unsafe_equal_values(old) for old in ctx.current_feeds)]
    ctx.current_feeds = [old for old in ctx.current_feeds if all(not d.unsafe_equal_values(old) for d in to_del)]
    return [new for new in ctx.new_feeds if all(not d.unsafe_equal_values(new) for d in to_del)]


apply_general_rule = apply_rules_first_to_last(
    general_feed_type_rule,
    general_feed_no_duplicate_cash_feed_rule,
    general_feed_cancel_rule,
)


class DealContextRule:

    def __init__(self, new_feeds, current_feeds, FeedType):
        self.new_feeds = new_feeds
        self.current_feeds = current_feeds
        self.FeedType = FeedType


def create_feed_from_deal(ctx, deal):
    record_type = "CREATE"

    transfer_type = "trade"

    deal_type = deal.get("deal_type")
    deal_ref = deal.get("deal_ref")
    master_deal_ref = deal.get("master_deal_ref")
    account = deal.get("account")
    counterparty_ref = deal.get("counterparty_ref")
    counterparty_name = deal.get("counterparty_name")

    value_date = datetime.utcfromtimestamp(deal.get("value_date")) if deal.get("value_date") is not None else None
    trade_date = datetime.utcfromtimestamp(deal.get("trade_date"))

    portfolio = str(deal.get("portfolio_number"))
    entity = deal.get("portfolio_entity")

    as_of_date = deal.get("as_of_date")

    coa_code = "-1"

    return ctx.FeedType(
        entity=entity,
        portfolio=portfolio,
        deal_ref=deal_ref,
        master_deal_ref=master_deal_ref if master_deal_ref is not None else deal_ref + "*",
        account=account,
        counterparty_ref=counterparty_ref,
        counterparty_name=counterparty_name,
        coa_code=coa_code,
        product=deal_type,
        value_date=value_date,
        trade_date=trade_date,
        record_type=record_type,
        transfer_type=transfer_type,
        as_of_date=as_of_date,
    )


def cancel_feed_from_old_feed(ctx, new_deal, old_feed):
    feed_type = "Cash"
    record_type = "DELETE"

    amount = -old_feed.amount
    comp_code = old_feed.comp_code
    asset = old_feed.asset

    entity = old_feed.entity
    product = old_feed.product
    coa_code = old_feed.coa_code
    deal_ref = old_feed.deal_ref
    master_deal_ref = old_feed.master_deal_ref
    portfolio = old_feed.portfolio
    value_date = old_feed.value_date
    trade_date = old_feed.trade_date
    account = old_feed.account
    counterparty_ref = old_feed.counterparty_ref
    counterparty_name = old_feed.counterparty_name
    transfer_type = old_feed.transfer_type
    as_of_date = new_deal.get("as_of_date")

    new_create_feed = next(
        (
            f for f in ctx.new_feeds
            if f.deal_ref == deal_ref and f.comp_code == comp_code and f.record_type == "CREATE"
        ),
        None,
    )

    as_of_date_end = new_create_feed.as_of_date if new_create_feed is not None else None

    return ctx.FeedType(
        amount=amount,
        asset=asset,
        comp_code=comp_code,
        feed_type=feed_type,

        entity=entity,
        portfolio=portfolio,
        deal_ref=deal_ref,
        master_deal_ref=master_deal_ref,
        coa_code=coa_code,
        product=product,
        record_type=record_type,
        value_date=value_date,
        trade_date=trade_date,
        account=account,
        counterparty_ref=counterparty_ref,
        counterparty_name=counterparty_name,
        transfer_type=transfer_type,
        as_of_date=as_of_date,
        as_of_date_end=as_of_date_end,
    )
