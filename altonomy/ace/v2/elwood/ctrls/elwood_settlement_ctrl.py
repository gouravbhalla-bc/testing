from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from http.client import HTTPException
import traceback
from typing import List, Optional
from sqlalchemy.orm import Session
from altonomy.ace.models import ElwoodService, ElwoodTransferCounter
from altonomy.ace.v2.elwood.ctrls.elwood_service import ElwoodServiceCtrl
from altonomy.ace.v2.elwood.ctrls.elwood_transfer_counter_ctrl import ElwoodTransferCounterCtrl
from altonomy.settlement_engine.ctrls import SettlementV2Ctrl
from fastapi.logger import logger
from altonomy.settlement_engine.db.sessions import SessionLocal
from altonomy.settlement_engine.common import api_utils
from altonomy.settlement_engine.external.optimus_client import OptimusClient
import itertools
from altonomy.settlement_engine.external.txn_client import TxnClient
from itertools import islice
from altonomy.settlement_engine.endpoints.schemas import (
    SettlementV2Paginated,
    SettlementV2
)
from altonomy.settlement_engine.enums import SettlementDirection
from altonomy.ace.v2.elwood.elwood_settlement_export import get_elwood_settlement
from altonomy.ace.v2.elwood.daos.elwood_settlement_export_dao import ElwoodSettlementExportDao


class ElwoodSettlementCtrl(object):
    def __init__(self, db: Session):
        self.elwood_transfer_counter_ctrl = ElwoodTransferCounterCtrl(db)
        self.elwood_service_ctrl = ElwoodServiceCtrl(db)
        self.elwood_settlement_dao = ElwoodSettlementExportDao(db)

    def elwood_trade_transfer(self, token: str):
        try:
            elwood_transfer_infos = self.elwood_transfer_counter_ctrl.get_all()
            for elwood_transfer_info in elwood_transfer_infos:
                if not elwood_transfer_info["enable"] or elwood_transfer_info["name"] != "settlement":
                    continue
                last_deal_id = elwood_transfer_info["last_deal_id"]
                from_date = elwood_transfer_info["effective_date_end"]
                to_date = datetime.utcnow()
                count = 0
                settlements = self.list_settlements(token, 1, 1000, from_date, to_date)
                for page in range(1, settlements.pages + 1):
                    s = self.list_settlements(token, page, 1000, from_date, to_date)
                    print(len(s.items))
                    if len(s.items) != 0:
                        [c, l] = self.matching(s.items)
                        last_deal_id = l if l is not None else last_deal_id
                        count = count + c
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
            logger.error(
                f"Process transfer unhandled error: {str(e)}"
                f"\n{ traceback.format_exc() }"
            )

    def matching(
        self,
        settlements: List[SettlementV2],
    ) -> List:
        len(settlements)
        current_settlemnt = {}
        exports = []
        last_settlement_id = None
        for settlement in settlements:
            export = get_elwood_settlement(settlement)
            if settlement.is_cancelled:
                export.effective_date_end = datetime.utcnow()
            last_settlement = current_settlemnt[export.settlement_id] if export.settlement_id in current_settlemnt.keys() else None
            if last_settlement is None:
                last_settlement = self.elwood_settlement_dao.get_active_settlement(export.settlement_id)

            if last_settlement is None:
                exports.append(export)
                last_settlement_id = export.settlement_id
            elif (not export.unsafe_equal_values(last_settlement) or
                    export.unsafe_equal_values(last_settlement) and export.effective_date_end is not None):
                last_settlement.effective_date_end = datetime.utcnow()
                exports.append(last_settlement)
                exports.append(export)
                last_settlement_id = export.settlement_id

        self.elwood_settlement_dao.create_many(exports)
        return [len(exports), last_settlement_id]

    def list_settlements(
        self,
        alt_auth_token: str = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        created_on_start: Optional[datetime] = None,
        created_on_end: Optional[datetime] = None,
    ) -> SettlementV2Paginated:

        db = SessionLocal()
        ctrl = SettlementV2Ctrl(db, alt_auth_token, logger)
        settlements_page = ctrl.get_all_settlement_by_time(
            type="settlement",
            created_on_start=created_on_start,
            created_on_end=created_on_end,
            page=page,
            page_size=page_size,
        )

        settlements = settlements_page.items
        # get maker names
        err, users = api_utils.get_users(alt_auth_token)
        if err:
            raise HTTPException(403, err)

        def get_username_from_id(x):
            if not x:
                return None
            return next(
                (u.get("username") for u in users if int(u.get("id")) == int(x)), None
            )

        oc = OptimusClient(alt_auth_token)
        err, counterparties = oc.get_counterparties()
        if err:
            raise err
        err, optimus_settlements = oc.get_settlements()
        if err:
            raise err
        err, accounts = oc.get_accounts()
        if err:
            raise err

        def find(items, value, key, default={}):
            return next(
                (item for item in items if value == item[key]),
                default,
            )

        settlement_ids = [settlement.settlement_id for settlement in settlements]
        settlement_id2filled = {
            settlement.settlement_id_self: settlement.amount
            for settlement in ctrl.get_all_filled_by_date(settlement_ids)
        }

        def flatten(nested):
            return itertools.chain.from_iterable(nested)

        tx_ids = {
            *flatten(
                settlement.tx_id.split(",")
                for settlement in settlements
                if settlement.tx_id is not None
            )
        }
        txn_client = TxnClient(alt_auth_token)

        def chunk(it, size):
            it = iter(it)
            return iter(lambda: tuple(islice(it, size)), ())

        txns = []
        for chunk_ids in chunk(tx_ids, 100):
            tx_page_size = 10 * len(chunk_ids)

            try:
                txns.extend(
                    txn_client.get_exchange_txns(tx_ids=chunk_ids, page_size=tx_page_size)[
                        "txns"
                    ]
                )
            except Exception as e:
                logger.error(f"Failed to get exchange txns: {e}")

            try:
                txns.extend(
                    txn_client.get_blockchain_txns(
                        tx_ids=chunk_ids, page_size=tx_page_size
                    )["txns"]
                )
            except Exception as e:
                logger.error(f"Failed to get blockchain txns: {e}")
        tx_id2txn = defaultdict(list)
        for txn in txns:
            tx_id2txn[txn.get("tx_id")].append(txn)

        def lookup_names(settlement):
            counterparty = find(
                counterparties, settlement.counterparty_ref, "counterparty_ref"
            )
            optimus_settlement = find(
                optimus_settlements, settlement.settlement_method_id, "settlement_id"
            )
            account = find(accounts, settlement.settlement_account_id, "account_product_id")
            if settlement.tx_id:
                txns = flatten(tx_id2txn.get(tx_id, {}) for tx_id in settlement.tx_id.split(","))
                if settlement.direction == SettlementDirection.Incoming:
                    txns = [txn for txn in txns if txn.get("asset") == settlement.asset and txn.get("amount") >= 0]
                else:
                    txns = [txn for txn in txns if txn.get("asset") == settlement.asset and txn.get("amount") < 0]
            else:
                txns = []

            def format_txn_addr(addr):
                if addr is None or len(addr) == 0:
                    return ""
                return addr

            # convert to dict https://stackoverflow.com/a/19415231
            return {
                **settlement.__dict__,
                "counterparty_name": counterparty.get("nickname_internal"),
                "settlement_method_name": optimus_settlement.get(
                    "settlement_nickname_internal"
                ),
                "settlement_method_ref": optimus_settlement.get("settlement_ref"),
                "settlement_account_name": account.get("account_name"),
                "maker_username": get_username_from_id(settlement.maker_id),
                "amount_filled": -settlement_id2filled.get(
                    settlement.settlement_id, Decimal(0)
                ),
                "tx_addr_source": ",".join(
                    map(format_txn_addr, (txn.get("tx_from") for txn in txns))
                ),
                "tx_addr_destination": ",".join(
                    map(format_txn_addr, (txn.get("tx_to") for txn in txns))
                ),
            }
        return SettlementV2Paginated(
            pages=settlements_page.pages,
            total=settlements_page.total,
            page=settlements_page.page,
            page_size=settlements_page.per_page,
            items=[lookup_names(settlement) for settlement in settlements],
        )
