from datetime import datetime
from typing import Dict, List
from altonomy.ace.daos.elwood_transfer_counter_dao import ElwoodTransferCounterDao

from altonomy.ace.models import ElwoodTransferCounter
from sqlalchemy.orm import Session
from altonomy.ace.common.exceptions import (
    ResourceNotFound,
    ResourceExists,
    ResourceNotUpdated,
)


class ElwoodTransferCounterCtrl(object):

    def __init__(self, db: Session):
        self.dao = ElwoodTransferCounterDao(db)

    def get_all(self) -> List[Dict]:
        elwood_trade_counters = self.dao.get_all()
        if elwood_trade_counters is None:
            return []
        return [self.to_dict(w) for w in elwood_trade_counters]

    def get(self, name: str) -> Dict:
        elwood_trade_counter = self.dao.get(name)
        if elwood_trade_counter is None:
            raise ResourceNotFound(name="ElwoodTransferCounter", id=type)
        return self.to_dict(elwood_trade_counter)

    def create(self, elwood_trade_counter: ElwoodTransferCounter) -> Dict:
        found = self.dao.get(elwood_trade_counter.name)
        if found is not None:
            raise ResourceExists(name="ElwoodTransferCounter", existingId=found.id)
        w = self.dao.create(elwood_trade_counter)
        return self.to_dict(w)

    def update(self, elwood_trade_counter: ElwoodTransferCounter) -> Dict:
        if elwood_trade_counter is None:
            raise ResourceNotUpdated(
                name="ElwoodTransferCounter", id=type, reason="No fields applicable for update"
            )
        found = self.dao.get(elwood_trade_counter.name)
        found.last_deal_id = elwood_trade_counter.last_deal_id
        found.effective_date_start = elwood_trade_counter.effective_date_start
        found.effective_date_end = elwood_trade_counter.effective_date_end
        found.system_record_date = datetime.utcnow()
        if found is None:
            raise ResourceNotFound(name="ElwoodTransferCounter", id=type)
        w = self.dao.update(found)
        return self.to_dict(w)

    def to_dict(self, elwood_trade_counter: ElwoodTransferCounter) -> Dict:
        return {
            "id": elwood_trade_counter.id,
            "folder": elwood_trade_counter.folder,
            "name": elwood_trade_counter.name,
            "type": elwood_trade_counter.type,
            "portfolio": elwood_trade_counter.portfolio,
            "system_record_date": None,
            "enable": elwood_trade_counter.enable,
            "last_deal_id": elwood_trade_counter.last_deal_id,
            "effective_date_start": elwood_trade_counter.effective_date_start,
            "effective_date_end": elwood_trade_counter.effective_date_end
        }
