from typing import List, Tuple
from datetime import datetime
import math

from altonomy.ace.enums import (DealProcessingStatus, FeedType, RecordType,
                                SystemSource, TransferType, DealType)
from altonomy.ace.v2.trade.models import TradeV2


def is_trade_equal(old_trade: TradeV2, new_trade: TradeV2) -> bool:

    if old_trade is None and new_trade is None:
        return True
    elif old_trade is None or new_trade is None:
        return False

    return old_trade.unsafe_equal_values(new_trade)


def dedup_and_create_cancel_rule(
    deal_processor,
    new_trade: TradeV2,
    deal: dict,
) -> Tuple[List[TradeV2], List[TradeV2]]:

    deal_id = deal.get("deal_id")
    product = deal.get("deal_type")
    old_trade = deal_processor.get_current_feed_by_product(deal_id, product)

    is_cancelled = deal.get("deal_processing_status") in (DealProcessingStatus.Cancelled, DealProcessingStatus.Pending)

    create_trades = []
    delete_trades = []

    if not is_cancelled and not is_trade_equal(old_trade, new_trade):
        if old_trade is not None and old_trade.feed_type == FeedType.Cash:
            cancel_feed = create_cancel_trade(new_trade, old_trade)
            delete_trades.append(cancel_feed)
        create_trades.append(new_trade)

    return create_trades, delete_trades


def option_dedup_and_create_cancel_rule(
    deal_processor,
    deal: dict,
    new_trade: TradeV2 = None,
    effective_date_start: datetime = None,
) -> Tuple[List[TradeV2], List[TradeV2]]:

    deal_id = deal.get("deal_id")
    product = DealType.FX_SPOT
    old_trade = deal_processor.get_current_feed_by_product(deal_id, product)
    version = deal.get("version"),

    is_cancelled = deal.get("deal_processing_status") in (DealProcessingStatus.Cancelled, DealProcessingStatus.Pending)
    create_trades = []
    delete_trades = []

    if not is_cancelled and not is_trade_equal(old_trade, new_trade):
        if old_trade is not None and old_trade.feed_type == FeedType.Cash:
            cancel_feed = create_cancel_trade(new_trade, old_trade)
            delete_trades.append(cancel_feed)
        if old_trade is not None and new_trade is None and not math.isclose(old_trade.base_amount, 0):
            new_trade = create_new_trade_with_no_amount(old_feed=old_trade, effective_date_start=effective_date_start, version=version)
        if new_trade is not None:
            create_trades.append(new_trade)
    return create_trades, delete_trades


def general_trade_type_rule(deal: dict) -> FeedType:
    is_settled = deal.get("deal_processing_status") == DealProcessingStatus.Settled
    return FeedType.Cash if is_settled else FeedType.PV


def create_cancel_trade(new_create_feed: TradeV2, old_feed: TradeV2) -> TradeV2:
    entity = old_feed.entity
    product = old_feed.product
    portfolio = old_feed.portfolio
    value_date = old_feed.value_date
    trade_date = old_feed.trade_date
    account = old_feed.account
    counterparty_ref = old_feed.counterparty_ref
    counterparty_name = old_feed.counterparty_name
    transfer_type = old_feed.transfer_type
    contract = old_feed.contract

    return TradeV2(
        deal_id=old_feed.deal_id,
        master_deal_id=old_feed.master_deal_id,
        system_source=SystemSource.XALPHA,
        version=old_feed.version,
        ref_id=old_feed.id,

        feed_type=FeedType.Cash,
        record_type=RecordType.DELETE,

        base_amount=-old_feed.base_amount,
        base_asset=old_feed.base_asset,
        quote_amount=-old_feed.quote_amount if old_feed.quote_amount is not None else None,
        quote_asset=old_feed.quote_asset,
        fee_amount=-old_feed.fee_amount if old_feed.fee_amount is not None else None,
        fee_asset=old_feed.fee_asset,

        entity=entity,
        portfolio=portfolio,
        deal_ref=old_feed.deal_ref,
        master_deal_ref=old_feed.master_deal_ref,
        product=product,
        value_date=value_date,
        trade_date=trade_date,
        account=account,
        counterparty_ref=counterparty_ref,
        counterparty_name=counterparty_name,
        transfer_type=transfer_type,
        contract=contract,

        effective_date_start=new_create_feed.effective_date_start,
        effective_date_end=new_create_feed.effective_date_start,
    )


def create_new_trade_with_no_amount(old_feed: TradeV2, effective_date_start: datetime, version: int) -> TradeV2:
    entity = old_feed.entity
    product = old_feed.product
    portfolio = old_feed.portfolio
    value_date = old_feed.value_date
    trade_date = old_feed.trade_date
    account = old_feed.account
    counterparty_ref = old_feed.counterparty_ref
    counterparty_name = old_feed.counterparty_name
    transfer_type = old_feed.transfer_type
    contract = old_feed.contract

    return TradeV2(
        deal_id=old_feed.deal_id,
        master_deal_id=old_feed.master_deal_id,
        system_source=SystemSource.XALPHA,
        version=version,
        ref_id=old_feed.id,

        feed_type=old_feed.feed_type,
        record_type=RecordType.CREATE,

        base_amount=0,
        base_asset=old_feed.base_asset,
        quote_amount=0,
        quote_asset=old_feed.quote_asset,
        fee_amount=-old_feed.fee_amount if old_feed.fee_amount is not None else None,
        fee_asset=old_feed.fee_asset,

        entity=entity,
        portfolio=portfolio,
        deal_ref=old_feed.deal_ref,
        master_deal_ref=old_feed.master_deal_ref,
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


def create_trade_from_deal(deal: dict) -> TradeV2:
    return TradeV2(
        deal_id=deal.get("deal_id"),
        master_deal_id=deal.get("master_deal_id") if deal.get("master_deal_id") else None,
        system_source=SystemSource.XALPHA,
        record_type=RecordType.CREATE,
        effective_date_start=deal.get("valid_from"),

        product=deal.get("deal_type"),
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
        version=deal.get("version"),
    )


def copy_trade(trade: TradeV2) -> TradeV2:
    return TradeV2(
        deal_id=trade.deal_id,
        master_deal_id=trade.master_deal_id,
        system_source=SystemSource.XALPHA,
        version=trade.version,
        ref_id=trade.id,

        base_amount=trade.base_amount,
        base_asset=trade.base_asset,
        quote_amount=trade.quote_amount,
        quote_asset=trade.quote_asset,
        fee_amount=trade.fee_amount,
        fee_asset=trade.fee_asset,
        feed_type=trade.feed_type,

        entity=trade.entity,
        portfolio=trade.portfolio,
        deal_ref=trade.deal_ref,
        master_deal_ref=trade.master_deal_ref,
        product=trade.product,
        record_type=trade.record_type,
        value_date=trade.value_date,
        trade_date=trade.trade_date,
        account=trade.account,
        counterparty_ref=trade.counterparty_ref,
        counterparty_name=trade.counterparty_name,
        transfer_type=trade.transfer_type,
        contract=trade.contract
    )
