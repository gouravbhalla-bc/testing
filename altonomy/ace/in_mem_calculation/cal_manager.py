from decimal import Decimal
from typing import List, Optional

from altonomy.ace.accounting_core.comp_code import CompCode
from altonomy.ace.in_mem_calculation.model.trade_snapshot import TradeSnapshot
from altonomy.ace.in_mem_calculation.cal_models import (AccountBalance,
                                                        Position, Settlement,
                                                        Summary)


class CalManager(object):

    def __init__(
        self,
        position_data: Optional[dict] = None,
        summary_data: Optional[dict] = None,
        balance_data: Optional[dict] = None,
        settlement_data: Optional[dict] = None,
    ):
        self._position_data = position_data if position_data is not None else {}
        self._summary_data = summary_data if summary_data is not None else {}
        self._balance_data = balance_data if balance_data is not None else {}
        self._settlement_data = settlement_data if settlement_data is not None else {}

    def clone(self, portfolios: List[str]):
        return CalManager(
            position_data={key: value.clone() for key, value in self._position_data.items() if key[0] in portfolios},
            summary_data={key: value.clone() for key, value in self._summary_data.items() if key[0] in portfolios},
            balance_data={key: value.clone() for key, value in self._balance_data.items() if key[0] in portfolios},
            settlement_data={key: value.clone() for key, value in self._settlement_data.items() if key[0] in portfolios},
        )

    def clear(self):
        self._position_data.clear()
        self._summary_data.clear()
        self._balance_data.clear()
        self._settlement_data.clear()

    def position_to_dict(self, position: Position):
        return {
            "Portfolio": position.portfolio,
            "Base Asset": position.base_asset,
            "Quote Asset": position.quote_asset,
            "Position": position.net_position,
            "RPL": position.realized_pnl,
            "RPL USD": position.realized_pnl_usd,
            "Market Price": position.market_price,
            "UPL": position.unrealized_pnl,
            "UPL USD": position.unrealized_pnl_usd,
            "Last Price": position.last_price,
            "Total Buy Quantity": position.total_buy_quantity,
            "Total Sell Quantity": position.total_sell_quantity,
            "Average Buy Price": position.average_buy_price,
            "Average Sell Price": position.average_sell_price,
            "Weighted Average Price": position.avg_open_price,
            "PNL Sum": position.total_pnl,
            "PNL USD Sum": position.total_pnl_usd,
        }

    def merge_positions(self, position_dicts: List[dict]):
        pairs = []
        new_positions = {}

        for pos in position_dicts:
            pair = (pos["Base Asset"], pos["Quote Asset"])

            if pair not in pairs:
                pairs.append(pair)
                new_positions[pair] = {
                    "Base Asset": pos["Base Asset"],
                    "Quote Asset": pos["Quote Asset"],
                    "Position": 0,
                    "RPL": 0,
                    "RPL USD": 0,
                    "Market Price": pos["Market Price"],
                    "UPL": 0,
                    "UPL USD": 0,
                    "Total Buy Quantity": 0,
                    "Total Sell Quantity": 0,
                    "PNL Sum": 0,
                    "PNL USD Sum": 0,
                }

            new_pos = new_positions[pair]
            new_positions[pair] = {
                "Base Asset": pos["Base Asset"],
                "Quote Asset": pos["Quote Asset"],
                "Position": new_pos["Position"] + pos["Position"],
                "RPL": new_pos["RPL"] + pos["RPL"],
                "RPL USD": new_pos["RPL USD"] + pos["RPL USD"],
                "Market Price": pos["Market Price"],
                "UPL": new_pos["UPL"] + pos["UPL"],
                "UPL USD": new_pos["UPL USD"] + pos["UPL USD"],
                "Total Buy Quantity": new_pos["Total Buy Quantity"] + pos["Total Buy Quantity"],
                "Total Sell Quantity": new_pos["Total Sell Quantity"] + pos["Total Sell Quantity"],
                "PNL Sum": new_pos["PNL Sum"] + pos["PNL Sum"],
                "PNL USD Sum": new_pos["PNL USD Sum"] + pos["PNL USD Sum"],
            }

        return list(new_positions.values())

    def merge_summaries(self, summary_dicts: List[dict]):
        new_summaries = {}

        for summary in summary_dicts:
            key = summary["Asset"]

            if key not in new_summaries:
                new_summaries[key] = {
                    "Asset": summary["Asset"],
                    "Position": 0,
                }

            new_summary = new_summaries[key]
            new_summary.update({
                "Position": new_summary["Position"] + summary["Position"],
            })

        return list(new_summaries.values())

    def merge_settlements(self, settlement_dicts: List[dict]):
        new_settlements = {}
        str_cols = ["Portfolio", "Counterparty Ref", "Counterparty Name"]

        for settlement in settlement_dicts:
            key = settlement["Counterparty Ref"]

            if key not in new_settlements:
                new_settlements[key] = {
                    "Counterparty Ref": settlement["Counterparty Ref"],
                    "Counterparty Name": settlement["Counterparty Name"],
                }

            new_settlement = new_settlements[key]
            new_settlement.update({
                c: new_settlement.get(c, 0) + settlement.get(c, 0)
                for c in settlement
                if c not in str_cols
            })

        return list(new_settlements.values())

    def merge_account_balance(self, balance_dicts: List[dict]):
        new_balances = {}
        str_cols = ["Portfolio", "Account"]

        for balance in balance_dicts:
            key = balance["Account"]

            if key not in new_balances:
                new_balances[key] = {
                    "Account": balance["Account"],
                }

            new_balance = new_balances[key]
            new_balance.update({
                c: new_balance.get(c, 0) + balance.get(c, 0)
                for c in balance
                if c not in str_cols
            })

        return list(new_balances.values())

    def update_position(
        self,
        portfolio: str,
        product: str,
        base_asset: str,
        quote_asset: str,
        base_amount: Decimal,
        quote_amount: Decimal,
        fee_asset: str,
        fee_quantity: Decimal,
        market_price: Decimal,
        quote_asset_ticker_price: Decimal,
    ):
        is_execution = product == "Execution"

        # ignore trades with base_amount = 0
        if not is_execution and base_amount == 0:
            return

        key = (portfolio, base_asset, quote_asset)

        if key not in self._position_data:
            self._position_data[key] = Position(portfolio, base_asset, quote_asset)

        position = self._position_data[key]

        if is_execution:
            position.update_by_tradefeed_execution(fee_quantity, quote_asset_ticker_price)

        else:
            position.update_by_tradefeed(
                base_amount,
                quote_amount,
                fee_asset,
                fee_quantity,
                quote_asset_ticker_price,
            )

        position.update_by_marketdata(market_price, quote_asset_ticker_price)

    def update_summary(self, feed):
        portfolio = feed["portfolio"]
        asset = feed["asset"]
        product = feed["product"]
        comp_code = feed["comp_code"]

        key = (portfolio, asset)

        if key not in self._summary_data:
            self._summary_data[key] = Summary(portfolio, asset)

        summary = self._summary_data[key]

        if product == "Execution":
            if str(comp_code) == CompCode.EXECUTION_FEE:
                summary.update(feed)
        else:
            summary.update(feed)

    def update_settlement(self, feed):
        if feed["feed_type"] != "PV":
            return

        portfolio = feed["portfolio"]
        counterparty_ref = feed["counterparty_ref"]
        counterparty_name = feed["counterparty_name"]
        product = feed["product"]
        comp_code = feed["comp_code"]

        key = (portfolio, counterparty_ref)

        if key not in self._settlement_data:
            self._settlement_data[key] = Settlement(portfolio, counterparty_ref, counterparty_name)

        settlement = self._settlement_data[key]
        if product == "Execution":
            if str(comp_code) == CompCode.EXECUTION_FEE:
                settlement.update(feed)
        else:
            settlement.update(feed)

    def update_account_balance(self, feed):
        if feed["feed_type"] != "Cash":
            return

        if feed["product"] == "Execution" and feed["comp_code"] != CompCode.EXECUTION_FEE:
            return

        portfolio = feed["portfolio"]
        account = feed["account"]

        key = (portfolio, account)

        if key not in self._balance_data:
            self._balance_data[key] = AccountBalance(portfolio, account)

        balance = self._balance_data[key]
        balance.update(feed)

    def enrich_feeds_with_pnl(self, snapshot: dict, feeds: List[dict]):
        snapshot = TradeSnapshot()

        out = []
        for feed in feeds:
            snapshot.update(feed)
            new_feed = {
                "trade_price": snapshot.traded_price,
                "sum_pnl": snapshot.sum_pnl,
                "pnl": snapshot.pnl,
                "quantity": snapshot.quantity,
                "amount_usdt": snapshot.amount_usdt,
                "weighted_average_price": snapshot.weighted_average_price,
            }
            new_feed.update(feed)
            out.append(new_feed)

        return out

    def get_position_by_portfolio(self, portfolios: List[str]):
        _list = []
        for key, value in self._position_data.items():
            key_portfolio = key[0]
            if key_portfolio in portfolios:
                _list.append(self.position_to_dict(value))
        return _list

    def get_summary_by_portfolio(self, portfolios: List[str]):
        return [
            value.as_dict()
            for key, value in self._summary_data.items()
            if key[0] in portfolios
        ]

    def get_settlement_by_portfolio(self, portfolios: List[str]):
        settlements = []
        cols = ['Portfolio', 'Counterparty Ref', 'Counterparty Name', 'Net Exposure']

        for key, value in self._settlement_data.items():
            if key[0] in portfolios:
                settlement = value.as_dict()
                for k in settlement.keys():
                    if k not in cols:
                        cols.append(k)
                settlements.append(settlement)
        return [
            {col: s.get(col, 0) for col in cols}
            for s in settlements
        ]

    def get_account_balance_by_portfolio(self, portfolios: List[str]):
        balances = []
        cols = ['Portfolio', 'Account', 'Net Exposure']

        for key, value in self._balance_data.items():
            if key[0] in portfolios and not value.is_empty():
                balance = value.as_dict()
                for k in balance.keys():
                    if k not in cols:
                        cols.append(k)
                balances.append(balance)
        return [
            {col: s.get(col, 0) for col in cols}
            for s in balances
        ]

    def process_trades(self, all_trades: List[dict]):
        trades = [x for x in all_trades if x['transfer_type'] != 'transfer']
        for trade in trades:
            market_price = Decimal(trade["base_asset_ticker_price"] / trade["quote_asset_ticker_price"]) if trade["quote_asset_ticker_price"] != 0 else Decimal(0)
            self.update_position(
                portfolio=trade["portfolio"],
                product=trade["product"],
                base_asset=trade["base_asset"],
                base_amount=trade["base_amount"],
                quote_asset=trade["quote_asset"],
                quote_amount=trade["quote_amount"],
                fee_asset=trade["fee_asset"],
                fee_quantity=trade["fee_amount"],
                market_price=market_price,
                quote_asset_ticker_price=trade["quote_asset_ticker_price"]
            )

    def process_feeds(self, all_feeds: List[dict]):
        for feed in all_feeds:
            self.update_summary(feed)
            self.update_settlement(feed)
            self.update_account_balance(feed)
