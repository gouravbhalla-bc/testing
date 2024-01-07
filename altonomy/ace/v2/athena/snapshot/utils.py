from collections import defaultdict
from datetime import datetime, timedelta
from typing import List

from altonomy.ace.models import (FeedV2, PositionSnapshot, SettlementSnapshot, SummaryV2Snapshot,
                                 SummarySnapshot, TradeV2)
from altonomy.ace.v2.athena.daos import SnapshotBaseDao
from altonomy.ace.v2.athena.snapshot import Position, Settlement, Summary, SummaryV2
from altonomy.ace.v2.feed.daos import FeedV2Dao
from altonomy.ace.v2.feed.daos.manual_feed_v2_dao import ManualFeedV2Dao
from altonomy.ace.v2.feed.models.manual_feed_v2 import ManualFeedV2
from altonomy.ace.v2.ticker.ctrls import TickerCtrl
from altonomy.ace.v2.trade.daos import TradeV2Dao
from altonomy.ace.enums import CompCode
from sqlalchemy import func
from sqlalchemy.orm import Session


def bulk_load_snapshot_items(
    item_dao,
    snapshot_dao,
    portfolios: int,
    trade_date: datetime,
    effective_date: datetime,
    snapshot_filters,
    item_filters,
    item_order_by,
    item_key_predicate,
    snapshot_key_predicate,
    manual_item_dao: ManualFeedV2Dao = None,
    manual_item_filters=None,
    manual_item_order_by=None

):
    snapshots = snapshot_dao.get_cached_snapshots_in_portfolios(
        snapshot_filters,
        portfolios,
        trade_date,
        effective_date,
        order_by=snapshot_filters,
    )

    trade_date_start = (
        min(snapshot.trade_date for snapshot in snapshots)
        if len(snapshots) > 0
        else trade_date - timedelta(days=7)  # we do not expect items beyond the first day, so this is functionally a large buffer
    )

    items = item_dao.get_filtered_at_trade_date_at_effective_date(
        item_filters,
        trade_date_start,
        trade_date,
        effective_date,
        order_by=item_order_by,
    )

    items_dict = defaultdict(list)
    for item in items:
        item_key = item_key_predicate(item)
        items_dict[item_key].append(item)

    if None not in [manual_item_dao, manual_item_filters, manual_item_order_by]:
        manual_items = manual_item_dao.get_filtered_at_trade_date_at_effective_date(
            manual_item_filters,
            trade_date_start,
            trade_date,
            effective_date,
            order_by=manual_item_order_by,
        )
        for item in manual_items:
            item_key = item_key_predicate(item)
            items_dict[item_key].append(item)

    for snapshot in snapshots:
        key = item_key_predicate(snapshot)
        snapshot_items = [
            item
            for item in
            items_dict.pop(key, [])
            if item.trade_date >= snapshot.trade_date
        ]

        yield snapshot, snapshot_items

    for items in items_dict.values():
        yield None, items


def bulk_load_position_by_portfolio(
    db: Session,
    portfolios: List[str],
    trade_date: datetime,
    effective_date: datetime,
) -> List[Position]:

    item_dao = TradeV2Dao(db, TradeV2)
    snapshot_dao = SnapshotBaseDao(db, PositionSnapshot)

    snapshot_filters = (
        func.binary(PositionSnapshot.base_asset),
        func.binary(PositionSnapshot.quote_asset),
    )

    item_filters = (
        TradeV2.portfolio.in_(portfolios),
    )

    item_order_by = (
        func.binary(TradeV2.base_asset),
        func.binary(TradeV2.quote_asset),
    )

    def item_key_predicate(item):
        return (item.portfolio, item.base_asset, item.quote_asset)

    def snapshot_key_predicate(snapshot):
        return (snapshot.portfolio, snapshot.base_asset, snapshot.quote_asset)

    snapshot_items = bulk_load_snapshot_items(
        item_dao,
        snapshot_dao,
        portfolios,
        trade_date,
        effective_date,
        snapshot_filters,
        item_filters,
        item_order_by,
        item_key_predicate,
        snapshot_key_predicate,
    )

    ticker_ctrl = TickerCtrl(db)
    tickers = ticker_ctrl.get_tickers_cached("USDT")

    for cached_snapshot, items in snapshot_items:
        if cached_snapshot is None:
            item = items[0]
            portfolio = item.portfolio
            base_asset = item.base_asset
            quote_asset = item.quote_asset
        else:
            portfolio = cached_snapshot.portfolio
            base_asset = cached_snapshot.base_asset
            quote_asset = cached_snapshot.quote_asset

        snapshot = Position(db, portfolio, base_asset, quote_asset)
        snapshot.load_from_cached(cached_snapshot)

        base_price = tickers.get(base_asset.upper() if base_asset is not None else base_asset, 0)
        snapshot.base_ticker_price = base_price

        quote_price = tickers.get(quote_asset.upper() if quote_asset is not None else quote_asset, 0)
        snapshot.quote_ticker_price = quote_price

        snapshot.process_items(items)

        yield snapshot


def bulk_load_settlement_by_portfolio(
    db: Session,
    portfolios: List[str],
    trade_date: datetime,
    effective_date: datetime,
) -> List[Settlement]:

    item_dao = FeedV2Dao(db, FeedV2)
    manual_item_dao = ManualFeedV2Dao(db, ManualFeedV2)

    snapshot_dao = SnapshotBaseDao(db, SettlementSnapshot)

    snapshot_filters = (
        func.binary(SettlementSnapshot.asset),
        SettlementSnapshot.counterparty_ref,
        SettlementSnapshot.counterparty_name,
    )

    item_filters = (
        FeedV2.portfolio.in_(portfolios),
    )

    manual_item_filters = (
        ManualFeedV2.portfolio.in_(portfolios),
    )

    item_order_by = (
        func.binary(FeedV2.asset),
        FeedV2.counterparty_ref,
        FeedV2.counterparty_name,
    )

    manual_item_order_by = (
        func.binary(ManualFeedV2.asset),
        ManualFeedV2.counterparty_ref,
        ManualFeedV2.counterparty_name,
    )

    def item_key_predicate(item):
        return (
            item.portfolio,
            item.asset,
            item.counterparty_ref,
            item.counterparty_name,
        )

    def snapshot_key_predicate(snapshot):
        return (
            snapshot.portfolio,
            snapshot.asset,
            snapshot.counterparty_ref,
            snapshot.counterparty_name,
        )

    snapshot_items = bulk_load_snapshot_items(
        item_dao,
        snapshot_dao,
        portfolios,
        trade_date,
        effective_date,
        snapshot_filters,
        item_filters,
        item_order_by,
        item_key_predicate,
        snapshot_key_predicate,
        manual_item_dao=manual_item_dao,
        manual_item_order_by=manual_item_order_by,
        manual_item_filters=manual_item_filters
    )

    ticker_ctrl = TickerCtrl(db)
    tickers = ticker_ctrl.get_tickers_cached("USDT")

    for cached_snapshot, items in snapshot_items:
        if cached_snapshot is None:
            item = items[0]
            portfolio = item.portfolio
            asset = item.asset
            cp_ref = item.counterparty_ref
            cp_name = item.counterparty_name
        else:
            portfolio = cached_snapshot.portfolio
            asset = cached_snapshot.asset
            cp_ref = cached_snapshot.counterparty_ref
            cp_name = cached_snapshot.counterparty_name

        snapshot = Settlement(db, portfolio, asset, cp_ref, cp_name)
        snapshot.load_from_cached(cached_snapshot)

        snapshot.process_items(items)

        price = tickers.get(asset.upper() if asset is not None else asset, 0)
        snapshot.net_exposure = snapshot.position * price

        yield snapshot


def bulk_load_summary_by_portfolio(
    db: Session,
    portfolios: List[str],
    trade_date: datetime,
    effective_date: datetime,
) -> List[Settlement]:

    item_dao = FeedV2Dao(db, FeedV2)
    snapshot_dao = SnapshotBaseDao(db, SummarySnapshot)
    manual_item_dao = ManualFeedV2Dao(db, ManualFeedV2)

    snapshot_filters = (
        func.binary(SummarySnapshot.asset),
    )

    item_filters = (
        FeedV2.portfolio.in_(portfolios),
    )

    manual_item_filters = (
        ManualFeedV2.portfolio.in_(portfolios),
    )

    item_order_by = (
        func.binary(FeedV2.asset),
    )

    manual_item_order_by = (
        func.binary(ManualFeedV2.asset),
        ManualFeedV2.counterparty_ref,
        ManualFeedV2.counterparty_name,
    )

    def item_key_predicate(item):
        return (
            item.portfolio,
            item.asset,
        )

    def snapshot_key_predicate(snapshot):
        return (
            snapshot.portfolio,
            snapshot.asset,
        )

    snapshot_items = bulk_load_snapshot_items(
        item_dao,
        snapshot_dao,
        portfolios,
        trade_date,
        effective_date,
        snapshot_filters,
        item_filters,
        item_order_by,
        item_key_predicate,
        snapshot_key_predicate,
        manual_item_dao=manual_item_dao,
        manual_item_filters=manual_item_filters,
        manual_item_order_by=manual_item_order_by
    )

    ticker_ctrl = TickerCtrl(db)
    tickers = ticker_ctrl.get_tickers_cached("USDT")

    for cached_snapshot, items in snapshot_items:
        if cached_snapshot is None:
            item = items[0]
            portfolio = item.portfolio
            asset = item.asset
        else:
            portfolio = cached_snapshot.portfolio
            asset = cached_snapshot.asset

        snapshot = Summary(db, portfolio, asset)
        snapshot.load_from_cached(cached_snapshot)

        snapshot.process_items(items)

        price = tickers.get(asset.upper() if asset is not None else asset, 0)
        snapshot.last_price = price
        # snapshot.change = (price - last_price) / price if price != 0 else 0

        yield snapshot


def bulk_load_summary_v2_by_portfolio(
    db: Session,
    portfolios: List[str],
    trade_date: datetime,
    effective_date: datetime,
) -> List[Settlement]:

    item_dao = FeedV2Dao(db, FeedV2)
    manual_item_dao = ManualFeedV2Dao(db, ManualFeedV2)

    snapshot_dao = SnapshotBaseDao(db, SummaryV2Snapshot)

    snapshot_filters = (
        func.binary(SummaryV2Snapshot.asset),
        SummaryV2Snapshot.contract,
        SummaryV2Snapshot.product,
    )

    item_filters = (
        FeedV2.portfolio.in_(portfolios),
        FeedV2.comp_code.not_in([CompCode.INITIAL_MARGIN_IN, CompCode.INITIAL_MARGIN_OUT, CompCode.VARIATION_MARGIN]),
    )

    manual_item_filters = (
        ManualFeedV2.portfolio.in_(portfolios),
    )

    item_order_by = (
        func.binary(FeedV2.asset),
        FeedV2.contract,
        FeedV2.product,
    )

    manual_item_order_by = (
        func.binary(ManualFeedV2.asset),
        ManualFeedV2.contract,
        ManualFeedV2.product,
    )

    def item_key_predicate(item):
        return (
            item.portfolio,
            item.asset,
            item.contract,
            item.product
        )

    def snapshot_key_predicate(snapshot):
        return (
            snapshot.portfolio,
            snapshot.asset,
            snapshot.contract,
            snapshot.product
        )

    snapshot_items = bulk_load_snapshot_items(
        item_dao,
        snapshot_dao,
        portfolios,
        trade_date,
        effective_date,
        snapshot_filters,
        item_filters,
        item_order_by,
        item_key_predicate,
        snapshot_key_predicate,
        manual_item_dao=manual_item_dao,
        manual_item_filters=manual_item_filters,
        manual_item_order_by=manual_item_order_by
    )

    ticker_ctrl = TickerCtrl(db)
    tickers = ticker_ctrl.get_tickers_cached("USDT")

    for cached_snapshot, items in snapshot_items:
        if cached_snapshot is None:
            item = items[0]
            portfolio = item.portfolio
            asset = item.asset
            product = item.product
            contract = item.contract
        else:
            portfolio = cached_snapshot.portfolio
            asset = cached_snapshot.asset
            product = cached_snapshot.product
            contract = cached_snapshot.contract
        snapshot = SummaryV2(db, portfolio, asset, product, contract)
        snapshot.load_from_cached(cached_snapshot)

        snapshot.process_items(items)

        price = tickers.get(asset.upper() if asset is not None else asset, 0)
        snapshot.last_price = price
        # snapshot.change = (price - last_price) / price if price != 0 else 0

        yield snapshot
