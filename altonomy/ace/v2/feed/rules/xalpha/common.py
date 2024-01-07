from typing import List, Tuple
from datetime import datetime
import math

from altonomy.ace.enums import (DealProcessingStatus, FeedType, RecordType,
                                SystemSource, TransferType)
from altonomy.ace.v2.feed.models import FeedV2


def is_feed_equal(old_feed: FeedV2, new_feed: FeedV2) -> bool:

    if old_feed is None and new_feed is None:
        return True
    elif old_feed is None or new_feed is None:
        return False

    return old_feed.unsafe_equal_values(new_feed)


def dedup_and_create_cancel_rule(
    deal_processor,
    new_feed: FeedV2,
    deal: dict,
    comp_code: str,
) -> Tuple[List[FeedV2], List[FeedV2]]:

    deal_id = deal.get("deal_id")
    old_feed = deal_processor.get_current_feed(deal_id, comp_code)
    is_cancelled = deal.get("deal_processing_status") in (DealProcessingStatus.Cancelled, DealProcessingStatus.Pending)

    create_feeds = []
    delete_feeds = []

    if not is_cancelled and not is_feed_equal(old_feed, new_feed):
        if old_feed is not None and old_feed.feed_type == FeedType.Cash:
            cancel_feed = create_cancel_feed(new_feed, old_feed)
            delete_feeds.append(cancel_feed)
        create_feeds.append(new_feed)

    return create_feeds, delete_feeds


def option_dedup_and_cancel_rule(
    deal_processor,
    deal: dict,
    comp_code: str,
    new_feed: FeedV2 = None,
    effective_date_start: datetime = None,
) -> Tuple[List[FeedV2], List[FeedV2]]:

    deal_id = deal.get("deal_id")
    old_feed = deal_processor.get_current_feed(deal_id, comp_code)
    version = deal.get("version"),

    is_cancelled = deal.get("deal_processing_status") in (DealProcessingStatus.Cancelled, DealProcessingStatus.Pending)

    create_feeds = []
    delete_feeds = []

    if not is_cancelled and not is_feed_equal(old_feed, new_feed):
        if old_feed is not None and old_feed.feed_type == FeedType.Cash:
            cancel_feed = create_cancel_feed(new_feed, old_feed, effective_date_start=effective_date_start)
            delete_feeds.append(cancel_feed)
        if old_feed is not None and new_feed is None and not math.isclose(old_feed.amount, 0):
            new_feed = create_new_feed_with_no_amount(old_feed=old_feed, effective_date_start=effective_date_start, version=version)
        if new_feed is not None:
            create_feeds.append(new_feed)
    return create_feeds, delete_feeds


def general_feed_type_rule(deal: dict) -> FeedType:
    is_settled = deal.get("deal_processing_status") == DealProcessingStatus.Settled
    return FeedType.Cash if is_settled else FeedType.PV


def create_cancel_feed(new_create_feed: FeedV2 = None, old_feed: FeedV2 = None, effective_date_start: datetime = None) -> FeedV2:
    amount = -old_feed.amount
    comp_code = old_feed.comp_code
    asset = old_feed.asset

    entity = old_feed.entity
    product = old_feed.product
    coa_code = old_feed.coa_code
    portfolio = old_feed.portfolio
    value_date = old_feed.value_date
    trade_date = old_feed.trade_date
    account = old_feed.account
    counterparty_ref = old_feed.counterparty_ref
    counterparty_name = old_feed.counterparty_name
    transfer_type = old_feed.transfer_type
    contract = old_feed.contract

    return FeedV2(
        deal_id=old_feed.deal_id,
        master_deal_id=old_feed.master_deal_id,
        system_source=SystemSource.XALPHA,
        version=old_feed.version,
        ref_id=old_feed.id,

        feed_type=FeedType.Cash,
        record_type=RecordType.DELETE,

        amount=amount,
        asset=asset,
        comp_code=comp_code,

        entity=entity,
        portfolio=portfolio,
        deal_ref=old_feed.deal_ref,
        master_deal_ref=old_feed.master_deal_ref,
        coa_code=coa_code,
        product=product,
        value_date=value_date,
        trade_date=trade_date,
        account=account,
        counterparty_ref=counterparty_ref,
        counterparty_name=counterparty_name,
        transfer_type=transfer_type,
        contract=contract,

        effective_date_start=new_create_feed.effective_date_start if new_create_feed is not None else effective_date_start,
        effective_date_end=new_create_feed.effective_date_start if new_create_feed is not None else effective_date_start,
    )


def create_new_feed_with_no_amount(old_feed: FeedV2, effective_date_start: datetime, version: int) -> FeedV2:
    amount = 0
    comp_code = old_feed.comp_code
    asset = old_feed.asset

    entity = old_feed.entity
    product = old_feed.product
    coa_code = old_feed.coa_code
    portfolio = old_feed.portfolio
    value_date = old_feed.value_date
    trade_date = old_feed.trade_date
    account = old_feed.account
    counterparty_ref = old_feed.counterparty_ref
    counterparty_name = old_feed.counterparty_name
    transfer_type = old_feed.transfer_type
    contract = old_feed.contract

    return FeedV2(
        deal_id=old_feed.deal_id,
        master_deal_id=old_feed.master_deal_id,
        system_source=SystemSource.XALPHA,
        version=version,
        ref_id=old_feed.id,

        feed_type=old_feed.feed_type,
        record_type=RecordType.CREATE,

        amount=amount,
        asset=asset,
        comp_code=comp_code,

        entity=entity,
        portfolio=portfolio,
        deal_ref=old_feed.deal_ref,
        master_deal_ref=old_feed.master_deal_ref,
        coa_code=coa_code,
        product=product,
        value_date=value_date,
        trade_date=trade_date,
        account=account,
        counterparty_ref=counterparty_ref,
        counterparty_name=counterparty_name,
        transfer_type=transfer_type,
        contract=contract,

        effective_date_start=effective_date_start,
        effective_date_end=None,
    )


def create_feed_from_deal(deal: dict) -> FeedV2:
    return FeedV2(
        deal_id=deal.get("deal_id"),
        master_deal_id=deal.get("master_deal_id") if deal.get("master_deal_id") else None,
        system_source=SystemSource.XALPHA,
        record_type=RecordType.CREATE,
        effective_date_start=deal.get("valid_from"),
        version=deal.get("version"),

        product=deal.get("deal_type"),
        coa_code="-1",
        transfer_type=TransferType.TRADE,
        contract=None,

        deal_ref=deal.get("deal_ref"),
        master_deal_ref=deal.get("master_deal_ref"),
        entity=deal.get("portfolio_entity"),
        portfolio=str(deal.get("portfolio_number")),
        account=deal.get("account"),
        counterparty_ref=deal.get("counterparty_ref"),
        counterparty_name=deal.get("counterparty_name"),
        value_date=deal.get("value_date"),
        trade_date=deal.get("trade_date"),
    )


def copy_feed(feed: FeedV2) -> FeedV2:
    return FeedV2(
        deal_id=feed.deal_id,
        master_deal_id=feed.master_deal_id,
        system_source=SystemSource.XALPHA,
        version=feed.version,
        ref_id=feed.id,

        amount=feed.amount,
        asset=feed.asset,
        comp_code=feed.comp_code,
        feed_type=feed.feed_type,

        entity=feed.entity,
        portfolio=feed.portfolio,
        deal_ref=feed.deal_ref,
        master_deal_ref=feed.master_deal_ref,
        coa_code=feed.coa_code,
        product=feed.product,
        record_type=feed.record_type,
        value_date=feed.value_date,
        trade_date=feed.trade_date,
        account=feed.account,
        counterparty_ref=feed.counterparty_ref,
        counterparty_name=feed.counterparty_name,
        transfer_type=feed.transfer_type,
        contract=feed.contract,
    )
