from sqlalchemy.orm import Session
import inspect
import os
import sys
from typing import Dict, List
from altonomy.ace.v2.elwood.ctrls.elwood_service import ElwoodServiceCtrl

from altonomy.ace.models import ElwoodExport, ElwoodService, ElwoodTransferCounter
from altonomy.ace.v2.elwood.daos.elwood_export_dao import ElwoodExportDao
from altonomy.ace.v2.elwood.models.elwood_deal import ElwoodDeal
from altonomy.ace.v2.log_util import get_v2_logger
from altonomy.ace.v2.trade.models.trade_v2 import TradeV2
import altonomy.ace.v2.elwood.elwood_export as elwood_export

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
parent_dir = os.path.dirname(parent_dir)
sys.path.insert(0, parent_dir)

import traceback
import datetime

from altonomy.ace.v2.elwood.ctrls.elwood_transfer_counter_ctrl import ElwoodTransferCounterCtrl
from altonomy.ace.v2.trade.ctrls.trade_v2_ctrl import TradeV2Ctrl
from altonomy.ace.v2.elwood.ctrls.deal_ctrl import DealCtrl


class ElwoodCtrl(object):

    def __init__(self, db: Session, xalpha_session: Session):
        self.elwood_transfer_counter_ctrl = ElwoodTransferCounterCtrl(db)
        self.elwood_service_ctrl = ElwoodServiceCtrl(db)
        self.trade_ctrl = TradeV2Ctrl(db)
        self.elwood_export_dao = ElwoodExportDao(db)
        self.deal_ctrl = DealCtrl(xalpha_session)
        self.logger = get_v2_logger("Elwwod Logger")

    def process(self, users: List):
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.elwood_trade_transfer(users)

    def elwood_trade_transfer(self, users: List):
        try:
            elwood_transfer_infos = self.elwood_transfer_counter_ctrl.get_all()
            for elwood_transfer_info in elwood_transfer_infos:
                if not elwood_transfer_info["enable"] or elwood_transfer_info["name"] == "settlement":
                    continue

                elwood_transfer_info["last_deal_id"]
                last_deal_id = elwood_transfer_info["last_deal_id"]
                from_date = elwood_transfer_info["effective_date_end"]
                to_date = datetime.datetime.utcnow()
                portfolios = elwood_transfer_info["portfolio"]
                portfolios = portfolios.split(",") if len(portfolios) > 0 else []

                count = 0
                fx_spot = True if elwood_transfer_info["name"] == "fx spot" else False
                if elwood_transfer_info["type"] == "trade":
                    [l, count] = self.get_formatted_trades(users, portfolios, from_date, to_date, fx_spot)
                else:
                    [l, count] = self.get_formatted_deals(users, portfolios, from_date, to_date)

                last_deal_id = l if l is not None else last_deal_id

                elwood_transfer_info.update(
                    {
                        "last_deal_id": last_deal_id,
                        "effective_date_start": from_date,
                        "effective_date_end": to_date
                    }
                )
                elwood_transfer_info = ElwoodTransferCounter(**elwood_transfer_info)
                self.elwood_transfer_counter_ctrl.update(elwood_transfer_info)

                elwood_service = {}
                elwood_service.update(
                    {
                        "name": elwood_transfer_info.name,
                        "last_id": last_deal_id,
                        "count": count,
                        "start_date": from_date,
                        "end_date": to_date
                    }
                )
                elwood_service = ElwoodService(**elwood_service)
                self.elwood_service_ctrl.create(elwood_service)
        except Exception as e:
            self.logger.error(
                f"Process transfer unhandled error: {str(e)}"
                f"\n{ traceback.format_exc() }"
            )

    def get_formatted_trades(
        self,
        users: List,
        portfolios: List[str],
        from_date: datetime,
        to_date: datetime,
        fx_spot: bool,
    ) -> List:
        last_deal_id = None
        count = 0
        it = iter(self.trade_ctrl.get_trades_by_portfolio_time(portfolios, from_date, to_date))
        while True:
            try:
                trades = next(it)
                formatted_trades = []
                current_trades = {}
                for trade in trades:
                    if trade.product == 'Cash Flow':
                        continue
                    if fx_spot and self.exclude_execution_from_fx_spot(trade):
                        continue
                    last_trade = self.elwood_export_dao.get_active_trade(trade.deal_id, portfolios)
                    last_trade = self.trade_ctrl.get_last_trade_dict(last_trade)

                    # last_trade = current_trades[trade.deal_id] if trade.deal_id in current_trades.keys() else last_trade
                    deal = self.deal_ctrl.get_last_deal(trade.deal_id, trade.effective_date_start)
                    if deal is not None:
                        deal = self.deal_ctrl.convert_deal_to_elwood_deal(deal)
                    deal_id = None
                    if '8000' == trade.portfolio:
                        # 1st, 2nd, 3rd principle export
                        for export in range(1, 4):
                            deal_id = self.trade_matching(trade, export, deal, current_trades, last_trade, formatted_trades)
                    elif '8838' == trade.portfolio:
                        # 4th principle export
                        export = 4
                        deal_id = self.trade_matching(trade, export, deal, current_trades, last_trade, formatted_trades)
                    elif '8002' == trade.portfolio:
                        # 3rd agency export
                        export = 3
                        deal_id = self.trade_matching(trade, export, deal, current_trades, last_trade, formatted_trades)
                    elif '8839' == trade.portfolio:
                        # 3rd agency export
                        export = 3
                        deal_id = self.trade_matching(trade, export, deal, current_trades, last_trade, formatted_trades)
                    last_deal_id = deal_id if deal_id is not None else last_deal_id
                print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                count = count + len(formatted_trades)
                self.add_user_name(formatted_trades, users)
                self.elwood_export_dao.create_many(formatted_trades)
                self.fix_price_from_deal(portfolios, from_date, to_date)
            except StopIteration:
                break
        print(count)
        return [last_deal_id, count]

    def get_formatted_deals(
        self,
        users: List,
        portfolios: List[int],
        from_date: datetime,
        to_date: datetime
    ) -> List:
        last_deal_id = None
        count = 0
        it = iter(self.deal_ctrl.get_deal_by_portfolio_time(portfolios, from_date, to_date))
        while True:
            try:
                trades = next(it)
                formatted_trades = []
                current_trades = {}
                for trade in trades:
                    if trade.deal_type == 'Cash Flow':
                        continue
                    trade = self.deal_ctrl.convert_deal_to_elwood_deal(trade)
                    if not self.deal_ctrl.is_valid_deal(trade):
                        continue

                    last_trade = self.elwood_export_dao.get_active_trade(trade.deal_id, portfolios)
                    last_trade = self.deal_ctrl.get_last_deal_dict(last_trade)

                    if '8002' in portfolios:
                        # 1st, 2nd agency export
                        for export in range(1, 3):
                            key = (trade.deal_id, export)
                            last = current_trades[key] if key in current_trades.keys() else None
                            if last is None:
                                last = last_trade[export] if export in last_trade.keys() else None
                            elwood_trade = elwood_export.get_agency(trade, export)
                            new_trades = self.matching(elwood_trade, last, trade.deal_processing_status)
                            for new_trade in new_trades:
                                if new_trade is None:
                                    continue
                                formatted_trades.append(new_trade)
                                current_trades[key] = new_trade
                                last_deal_id = trade.deal_id
                print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                count = count + len(formatted_trades)
                self.add_user_name(formatted_trades, users)
                self.elwood_export_dao.create_many(formatted_trades)
            except StopIteration:
                break
        return [last_deal_id, count]

    def fix_price_from_deal(
        self,
        portfolios: List[int],
        from_date: datetime,
        to_date: datetime
    ):
        if '8000' in portfolios:
            it = iter(self.deal_ctrl.get_deal_by_portfolio_time(portfolios, from_date, to_date))
            while True:
                try:
                    deals = next(it)
                    for deal in deals:
                        deal = self.deal_ctrl.convert_deal_to_elwood_deal(deal)
                        for export in range(2, 4):
                            trade = self.elwood_export_dao.get_trade_by_export(deal.deal_id, portfolios, export)
                            if trade is not None and trade.price != deal.reference_price:
                                trade.price = deal.reference_price
                                self.elwood_export_dao.update(trade)
                except StopIteration:
                    break

    def add_user_name(self, formatted_trades: List, users: List):
        def get_username_from_id(x):
            if not x or x == '0':
                return 'bots'
            try:
                return next((u["username"] for u in users if int(u["id"]) == int(x)), None)
            except ValueError:
                return x
        for trade in formatted_trades:
            if trade.version != 1:
                deal = self.deal_ctrl.get_deal_by_version(trade.deal_id)
                trade.trader = str(deal.maker_id) if deal is not None else None
            name = get_username_from_id(trade.trader)
            trade.trader = name if name is not None else trade.trader

    def trade_matching(
        self,
        trade: TradeV2,
        export: int,
        deal: ElwoodDeal,
        current_trades: Dict,
        last_trade: Dict,
        formatted_trades: List
    ) -> int:
        last_deal_id = None
        key = (trade.deal_id, export)
        last = current_trades[key] if key in current_trades.keys() else None
        if last is None:
            last = last_trade[export] if export in last_trade.keys() else None
        elwood_trade = elwood_export.get_principle(trade, deal, export)
        # For 8839, 8002 portfolio
        if trade.portfolio in ['8002', '8839']:
            elwood_trade = elwood_export.get_agency_trade(trade, deal, export)
        new_trades = self.matching(elwood_trade, last)
        for new_trade in new_trades:
            if new_trade is None:
                continue
            formatted_trades.append(new_trade)
            current_trades[key] = new_trade
            last_deal_id = trade.deal_id
        return last_deal_id

    def matching(
        self,
        current_trade: ElwoodExport,
        last_trade: ElwoodExport,
        deal_processing_status: str = None
    ) -> List[ElwoodExport]:
        # deal consider deleted if status is pending or cacelled
        if deal_processing_status in ["pending", "cancelled"]:
            current_trade.effective_date_end = datetime.datetime.utcnow()
        # If its a new trade
        if last_trade is None or last_trade.effective_date_end is not None:
            return [current_trade]
        elif not current_trade.unsafe_equal_values(last_trade):
            # Invalidate last trade
            last_trade.effective_date_end = datetime.datetime.utcnow()
            return [last_trade, current_trade]
        elif current_trade.unsafe_equal_values(last_trade) and current_trade.effective_date_end is not None:
            # Invalidate last trade
            last_trade.effective_date_end = datetime.datetime.utcnow()
            return [last_trade, current_trade]
        return []

    def exclude_execution_from_fx_spot(self, trade: TradeV2) -> bool:
        # Only take FX Spot and MFX'
        if trade.product == "FX Spot":
            return False
        return True
