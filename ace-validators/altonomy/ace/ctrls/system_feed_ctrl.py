import traceback
from datetime import datetime, timedelta
from itertools import chain
from collections import defaultdict
from typing import List

from altonomy.ace import __version__
from altonomy.ace.accounting_core.comp_code import CompCode
from altonomy.ace.accounting_core.utils import DealContextRule
from altonomy.ace.accounting_core.xalpha import get_handler
from altonomy.ace.daos import (ManualFeedDao, SystemFeedDao,
                               SystemFeedErrorDao, SystemFeedTodayDao,
                               TickerFeedDao)
from altonomy.ace.enums import FeedType
# from altonomy.ace.external.nitro_client import get_asset_price
from altonomy.ace.models import (ManualFeed, SystemFeed, SystemFeedError,
                                 SystemFeedToday, TickerFeed)
from sqlalchemy.orm import Session

system_source = "XAL"


def floor_to_hour(dt: datetime) -> int:
    bucket = 3600
    return int((dt.timestamp() // bucket) * bucket)


def convert_system_feed_to_today(feeds):
    _feeds = []
    for f in feeds:
        _feed = SystemFeedToday(
            feed_type=f.feed_type,
            transfer_type=f.transfer_type,
            portfolio=f.portfolio,
            asset=f.asset,
            amount=f.amount,
            asset_price=f.asset_price,
            master_deal_ref=f.master_deal_ref,
            deal_ref=f.deal_ref,
            counterparty_ref=f.counterparty_ref,
            counterparty_name=f.counterparty_name,
            account=f.account,
            coa_code=f.coa_code,
            entity=f.entity,
            product=f.product,
            comp_code=f.comp_code,
            effective_date=f.effective_date,
            value_date=f.value_date,
            trade_date=f.trade_date,
            input_date=f.input_date,
            as_of_date=f.as_of_date + timedelta(days=1),
            system_source=f.system_source,
            record_type=f.record_type
        )
        _feeds.append(_feed)
    return _feeds


class SystemFeedCtrl(object):

    def __init__(self, db: Session):
        self.db = db
        self.dao = SystemFeedDao(db, SystemFeed)
        self.dao_today = SystemFeedTodayDao(db, SystemFeedToday)
        self.dao_manual = ManualFeedDao(db, ManualFeed)
        self.dao_error = SystemFeedErrorDao(db, SystemFeedError)
        self.dao_ticker = TickerFeedDao(db, TickerFeed)

    def handle_xalpha_deals(self, deals, effective_date, is_daily, as_of_date):
        new_feeds = []

        for deal in deals:
            deal.update({
                "as_of_date": as_of_date
            })
            _hd = self.handle_xalpha_deal(deal, effective_date, is_daily)
            if _hd is not None:
                new_feeds.extend(_hd)

        if is_daily:
            current_as_of_date = self.dao_today.get_current_as_of_date()
            if current_as_of_date != as_of_date:
                self.dao_today.truncate()  # shift to another function + change to delete based on as_of date
        else:
            self.dao.delete_feeds_by_as_of_date(as_of_date)

        if is_daily:
            del_refs = set()

            for nf in new_feeds:
                del_refs.add(nf.deal_ref)
                del_refs.add(nf.master_deal_ref)

            del_refs = list(del_refs)
            self.dao_today.delete_feeds_by_deal_refs(del_refs, as_of_date)

        merged_feeds = []
        merged_feeds.extend(new_feeds)
        merged_feeds = [f for f in merged_feeds if f.record_type is not None]  # should be handled in deal creation
        self.db.bulk_save_objects(merged_feeds)

        if not is_daily:
            # create PV in today table
            pv_feeds = [f for f in merged_feeds if f.feed_type == "PV"]
            today_pv_feeds = convert_system_feed_to_today(pv_feeds)
            self.db.bulk_save_objects(today_pv_feeds)

            # set as_of_date_end
            updated_deal_refs_by_comp_code = defaultdict(list)
            for f in merged_feeds:
                updated_deal_refs_by_comp_code[f.comp_code].append(f.deal_ref)

            open_feeds = []
            for comp_code, deal_refs in updated_deal_refs_by_comp_code.items():
                to_close = self.dao.get_all_open_by_deal_ref_and_comp_code_before_as_of_date(
                    deal_refs,
                    comp_code,
                    as_of_date,
                )
                for feed in to_close:
                    feed.as_of_date_end = as_of_date
                    open_feeds.append(feed)

            self.db.bulk_save_objects(open_feeds)

        self.db.commit()

    def handle_xalpha_deal(self, deal, effective_date, is_daily):
        deal_type = deal.get("deal_type")
        deal_ref = deal.get("deal_ref")

        as_of_date = deal.get("as_of_date")

        handlers = get_handler(deal_type)
        FeedType = SystemFeedToday if is_daily else SystemFeed

        error_type = None
        reason = None

        out_feeds = []

        if handlers:
            try:
                for comp_code, handler in handlers:
                    scan_as_of_date = as_of_date - timedelta(days=1)
                    current_feeds = self.dao.get_last_cash_records(deal_ref, comp_code, scan_as_of_date)
                    new_feeds = []

                    ctx = DealContextRule(new_feeds, current_feeds, FeedType)
                    new_feeds = handler(ctx, deal)

                    for feed in new_feeds:
                        feed.effective_date = effective_date
                        feed.system_source = system_source
                        out_feeds.append(feed)
            except Exception:
                error_type = "Code"
                reason = traceback.format_exc()
        else:
            error_type = "Flow"
            reason = f"No handlers for {deal_type}"

        if error_type:
            if "as_of_date" in deal:
                del deal["as_of_date"]
            feed_error = SystemFeedError(
                system_source=system_source,
                version=__version__,
                product=deal_type,
                error_type=error_type,
                reason=reason,
                data=deal,
            )

            self.dao_error.create(feed_error)

            return None

        return out_feeds

    def get_tickers(self):
        ticker_prices = self.dao_ticker.get_all_asset_latest_ticker_price()
        tickers = dict()
        last_tickers = dict()
        for ticker in ticker_prices:
            if ticker.base_asset not in tickers:
                tickers[ticker.base_asset] = ticker.price
                last_tickers[ticker.base_asset] = ticker.last_price
        return tickers, last_tickers

    def get_tickers_historical_by_asset(self, asset: str) -> dict:
        ticker_prices = self.dao_ticker.get_asset_ticker_price(asset)
        return {floor_to_hour(ticker.effective_date): ticker.price for ticker in ticker_prices}

    def get_feeds_past(self):
        rows = self.dao.get_all_cash_records_including_manual()
        return self.generate_feed(rows)

    def get_feeds_today(self, portfolios: List[str]):
        rows = self.dao_today.get_today_feeds_by_portfolio(portfolios)
        return self.generate_feed(rows, always_sum_feeds=True)

    def get_feeds_transfer_by_portfolio(self, portfolios: List[str]):
        daos = (self.dao, self.dao_today, self.dao_manual)

        feeds = []
        for dao in daos:
            feeds.extend(dao.get_all_transfer_by_portfolio(portfolios))

        deal_refs = list({feed.deal_ref for feed in feeds})
        for dao in daos:
            feeds.extend(dao.get_all_by_deal_ref_and_not_in_portfolio(deal_refs, portfolios))

        return self.generate_feed(feeds)

    def get_feeds_transfer_by_portfolio_time(
        self,
        portfolios: List[str],
        from_date: datetime,
        to_date: datetime,
    ) -> List[dict]:
        cutoff_date = to_date.replace(tzinfo=None)
        start_date_pv = to_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        start_date_cash = from_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        end_date = (to_date + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

        return [
            feed
            for feed in self.get_feeds_transfer_by_portfolio(portfolios)
            if feed["trade_date"] <= cutoff_date and
            (
                (feed["feed_type"] == "PV" and feed["as_of_date"] > start_date_pv) or
                (feed["feed_type"] == "Cash" and feed["as_of_date"] > start_date_cash)
            ) and
            feed["as_of_date"] <= end_date
        ]

    def format_feed(self, feed, tickers) -> dict:
        return {
            "feed_type": feed.feed_type,
            "transfer_type": feed.transfer_type,
            "portfolio": feed.portfolio,
            "asset": feed.asset,
            "amount": feed.amount,
            "asset_price": feed.asset_price,
            "master_deal_ref": feed.master_deal_ref,
            "deal_ref": feed.deal_ref,
            "counterparty_ref": feed.counterparty_ref,
            "counterparty_name": feed.counterparty_name,
            "account": feed.account,
            "coa_code": feed.coa_code,
            "entity": feed.entity,
            "product": feed.product,
            "comp_code": feed.comp_code,
            "effective_date": feed.effective_date,
            "value_date": feed.value_date,
            "trade_date": feed.trade_date,
            "input_date": feed.input_date,
            "as_of_date": feed.as_of_date,
            "ticker_price": tickers.get(floor_to_hour(feed.trade_date), 0),
            "is_today": isinstance(feed, SystemFeedToday),
        }

    def update_fx_spot_feed_trade_price(self, feed: dict) -> dict:
        comp_code = feed["comp_code"]
        if comp_code not in (CompCode.FX_SPOT_BASE, CompCode.FX_SPOT_QUOTE):
            return feed

        pair_code = CompCode.FX_SPOT_BASE if comp_code == CompCode.FX_SPOT_QUOTE else CompCode.FX_SPOT_QUOTE
        dao = self.dao_today if feed["is_today"] else self.dao
        pair_feed = dao.get_latest_by_deal_ref_and_comp_code(feed["deal_ref"], pair_code)

        if pair_feed is not None:
            pair_ticker = self.dao_ticker.get_first_asset_ticker_price(pair_feed.asset, feed["trade_date"])
            pair_ticker_price = pair_ticker.price if pair_ticker is not None else 0
            trade_price = pair_feed.amount / feed["amount"]
            trade_price = trade_price * pair_ticker_price
        else:
            trade_price = 0

        new_feed = {}
        new_feed.update(feed)
        new_feed["ticker_price"] = trade_price
        return new_feed

    def format_feeds(
        self,
        feeds: List[SystemFeed],
        feeds_today: List[SystemFeedToday],
        asset: str,
    ) -> List[dict]:
        tickers = self.get_tickers_historical_by_asset(asset)
        new_feeds = {}
        new_feeds.update({(feed.deal_ref, feed.comp_code): self.format_feed(feed, tickers) for feed in feeds})
        new_feeds.update({(feed.deal_ref, feed.comp_code): self.format_feed(feed, tickers) for feed in feeds_today})
        return new_feeds

    def get_feeds_by_portfolio_asset_entity_time(
        self,
        portfolio: str,
        asset: str,
        from_date: datetime,
        to_date: datetime,
    ) -> List[dict]:
        comp_codes = (
            CompCode.FX_SPOT_BASE,
            CompCode.FX_SPOT_QUOTE,
            CompCode.EXECUTION_FEE,
            CompCode.CASHFLOW_TRANSFER,
        )

        feeds = self.dao.get_all_by_portfolio_and_asset_between_date_with_comp_codes(
            portfolio,
            asset,
            comp_codes,
            from_date,
            to_date,
        )

        feeds_today = self.dao_today.get_all_by_portfolio_and_asset_between_date_with_comp_codes(
            portfolio,
            asset,
            comp_codes,
            from_date,
            to_date,
        )

        new_feeds = self.format_feeds(feeds, feeds_today, asset)
        new_feeds = [self.update_fx_spot_feed_trade_price(feed) for feed in new_feeds.values()]
        new_feeds.sort(key=lambda k: 0 if k["amount"] > 0 else 1)
        new_feeds.sort(key=lambda k: k["trade_date"])
        return new_feeds

    def generate_feed(self, feeds, always_sum_feeds=False):
        tickers, _last_tickers = self.get_tickers()

        feed_keys = (
            "feed_type",
            "transfer_type",
            "portfolio",
            "asset",
            "amount",
            "asset_price",
            "master_deal_ref",
            "deal_ref",
            "counterparty_ref",
            "counterparty_name",
            "account",
            "coa_code",
            "entity",
            "product",
            "comp_code",
            "effective_date",
            "value_date",
            "trade_date",
            "input_date",
            "as_of_date",
        )

        new_feeds = {}

        for feed in feeds:
            key = (feed.deal_ref, feed.asset, feed.comp_code, feed.portfolio)

            if key not in new_feeds or (feed.record_type == "CREATE" and new_feeds[key]["input_date"] < feed.input_date):
                new_feed = {k: getattr(feed, k) for k in feed_keys}
                new_feed["ticker_price"] = tickers.get(feed.asset, 0)
                new_feed["sum_amount"] = new_feeds.get(key, {}).get("sum_amount", 0)
                new_feeds[key] = new_feed

            new_feed = new_feeds[key]
            if always_sum_feeds or new_feed["feed_type"] == "Cash":
                new_feed["sum_amount"] = new_feed["sum_amount"] + feed.amount

        for key, new_feed in new_feeds.items():
            if "sum_amount" not in new_feed:
                continue

            if always_sum_feeds or new_feed["feed_type"] == "Cash":
                new_feed["amount"] = new_feed["sum_amount"]
            del new_feed["sum_amount"]

        return list(new_feeds.values())

    def with_ticker_price(self, values: List[dict], asset_key: str):
        tickers, last_tickers = self.get_tickers()

        for value in values:
            asset = value.get(asset_key)

            current = tickers.get(asset, 0)
            prev = last_tickers.get(asset, 0)
            if prev is None:
                change = 0
            else:
                change = (current - prev) / current if current != 0 else 0

            value.update({
                "Last Price": current,
                "Change": change,
            })

        return values

    def get_trade_view_past(self):
        feeds_with_partial_settled = self.dao.get_all_cash_records_including_manual()

        feed_by_deal_ref_and_portfolio = defaultdict(list)
        for feed in feeds_with_partial_settled:
            key = (feed.deal_ref, feed.portfolio)
            feed_by_deal_ref_and_portfolio[key].append(feed)

        feeds_settled = []
        for feeds in feed_by_deal_ref_and_portfolio.values():
            deal_type = feeds[0].product

            handlers = get_handler(deal_type)

            #  at least 1 of each comp_code
            if all(
                next((True for feed in feeds if feed.comp_code == comp_code), False)
                for comp_code, _handler
                in handlers
            ):
                feeds_settled.extend(feeds)

        return self.generate_trade_view(feeds_settled)

    def get_trade_view_today(self, portfolios: List[str]):
        today_feeds = self.dao_today.get_today_feeds_by_portfolio(portfolios)

        feed_by_deal_ref = defaultdict(list)
        for feed in today_feeds:
            feed_by_deal_ref[feed.deal_ref].append(feed)

        processed_feeds = []

        partial_settled_deal_refs_by_comp_code = defaultdict(list)
        for deal_ref, feeds in feed_by_deal_ref.items():
            deal_type = feeds[0].product

            delete_feeds = list(f for f in feeds if f.record_type == "DELETE")
            if len(delete_feeds) != 0:
                count_check = self.dao.count_open_create_cash_feeds_by_deal_ref(deal_ref)
            else:
                count_check = 0

            handlers = get_handler(deal_type)

            for comp_code, _handler in handlers:
                feeds_with_comp_code = list(f for f in feeds if f.comp_code == comp_code)

                if len(feeds_with_comp_code) == 0:
                    partial_settled_deal_refs_by_comp_code[comp_code].append(deal_ref)
                else:
                    if count_check != len(handlers):  # previous trade was not a "Cash" trade, so we exclude the DELETE records
                        feeds_with_comp_code = list(f for f in feeds_with_comp_code if f.record_type == "CREATE")
                    processed_feeds.extend(feeds_with_comp_code)

        for comp_code, deal_refs in partial_settled_deal_refs_by_comp_code.items():
            partial_settled_cash_feeds = self.dao.get_all_open_cash_by_deal_refs_and_comp_code(deal_refs, comp_code)
            processed_feeds.extend(partial_settled_cash_feeds)

        return self.generate_trade_view(processed_feeds)

    def get_trade_view_by_product_portfolio(self, portfolios: List[str], product: str):
        daos = (self.dao, self.dao_today, self.dao_manual)

        feeds = []
        for dao in daos:
            feeds.extend(dao.get_all_by_product_and_portfolio(product, portfolios))

        deal_refs = list({feed.deal_ref for feed in feeds})
        for dao in daos:
            feeds.extend(dao.get_all_by_deal_ref_and_not_in_portfolio(deal_refs, portfolios))

        return self.generate_trade_view(feeds)

    def get_trades_by_product_portfolio_time(
        self,
        portfolios: List[str],
        product: str,
        from_date: datetime,
        to_date: datetime,
    ) -> List[dict]:
        cutoff_date = to_date.replace(tzinfo=None)
        start_date_pv = to_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        start_date_cash = from_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        end_date = (to_date + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

        return [
            trade
            for trade in self.get_trade_view_by_product_portfolio(portfolios, product)
            if trade["trade_date"] <= cutoff_date and
            (
                (trade["feed_type"] == "PV" and trade["as_of_date"] > start_date_pv) or
                (trade["feed_type"] == "Cash" and trade["as_of_date"] > start_date_cash)
            ) and
            trade["as_of_date"] <= end_date
        ]

    def merge_trade_with_feed(self, trade: dict, feed: SystemFeed, tickers: dict) -> dict:
        this_trade = {}
        this_trade.update(trade)

        # take latest feed details
        if (
            "as_of_date" not in this_trade or
            (feed.as_of_date > this_trade["as_of_date"]) or
            (feed.as_of_date == this_trade["as_of_date"] and feed.record_type == "CREATE")
        ):
            this_trade.update({
                "feed_type": feed.feed_type,
                "transfer_type": feed.transfer_type,
                "portfolio": feed.portfolio,
                "as_of_date": feed.as_of_date,
                "trade_date": feed.trade_date,
                "counterparty_name": feed.counterparty_name,
                "account": feed.account,
                "product": feed.product,
            })

        comp_code = str(feed.comp_code)

        if comp_code in (CompCode.FX_SPOT_BASE, CompCode.EXECUTION_START, CompCode.CASHFLOW_TRANSFER):
            this_trade.update({
                "base_asset": feed.asset,
                "base_amount": feed.amount,
                "base_asset_ticker_price": tickers.get(feed.asset, 0),
                "sum_base_amount": this_trade.get("sum_base_amount", 0) + feed.amount if feed.feed_type == FeedType.Cash else 0,
            })

            if feed.as_of_date == this_trade.get("as_of_date"):
                this_trade.update({
                    "base_feed_type": feed.feed_type,
                })

        elif comp_code in (CompCode.FX_SPOT_QUOTE, CompCode.EXECUTION_END):
            this_trade.update({
                "quote_asset": feed.asset,
                "quote_amount": feed.amount,
                "quote_asset_ticker_price": tickers.get(feed.asset, 0),
                "sum_quote_amount": this_trade.get("sum_quote_amount", 0) + feed.amount if feed.feed_type == FeedType.Cash else 0,
            })

            if feed.as_of_date == this_trade.get("as_of_date"):
                this_trade.update({
                    "quote_feed_type": feed.feed_type,
                })

        elif comp_code in (CompCode.FX_SPOT_FEE, CompCode.EXECUTION_FEE):
            this_trade.update({
                "fee_asset": feed.asset,
                "fee_amount": feed.amount,
                "sum_fee_amount": this_trade.get("sum_fee_amount", 0) + feed.amount if feed.feed_type == FeedType.Cash else 0,
            })

            if feed.as_of_date == this_trade.get("as_of_date"):
                this_trade.update({
                    "fee_feed_type": feed.feed_type,
                })

        return this_trade

    def generate_trade_view(self, feeds):
        tickers, _last_tickers = self.get_tickers()

        trade_view = {}
        for feed in feeds:
            key = (feed.deal_ref, feed.portfolio)
            if key not in trade_view:
                trade_view[key] = {
                    "deal_ref": feed.deal_ref,
                }
            trade_view[key] = self.merge_trade_with_feed(trade_view[key], feed, tickers)

        sum_keys = ("sum_base_amount", "sum_quote_amount", "sum_fee_amount")
        feed_type_keys = ("base_feed_type", "quote_feed_type", "fee_feed_type")

        for trade in trade_view.values():
            trade["feed_type"] = (
                FeedType.Cash
                if all(trade.get(k, FeedType.Cash) == FeedType.Cash for k in feed_type_keys)
                else FeedType.PV
            )

            if trade["feed_type"] == "Cash":
                trade["base_amount"] = trade.get("sum_base_amount", 0)
                trade["quote_amount"] = trade.get("sum_quote_amount", 0)
                trade["fee_amount"] = trade.get("sum_fee_amount", 0)

            for k in chain(sum_keys, feed_type_keys):
                if k in trade:
                    del trade[k]

        return sorted(trade_view.values(), key=lambda k: k["trade_date"])
