from typing import List

from altonomy.ace.common.utils import row_to_json
from altonomy.ace.models import TradeV2
from altonomy.ace.v2.common.ace_publisher import AcePublisher
from altonomy.ace.v2.trade.daos import TradeV2Dao


class AceTradeCreatePublisher(AcePublisher):

    def get_stream_key(self) -> str:
        return "STREAM:ACE:TRADEV2:CREATE"

    def get_log_name(self) -> str:
        return "ace_trade_create_publisher"

    def get_items(self, batch_size: int) -> List[dict]:
        last_id = self.get_last_payload().get("id", 0)

        with self.get_session() as db:
            trade_dao: TradeV2 = TradeV2Dao(db, TradeV2)
            trades = trade_dao.get_N_feeds_after_record_id(last_id, batch_size)

        items = []
        for trade in trades:
            item = {
                "id": trade.id,
                "json": row_to_json(trade),
            }
            items.append(item)
        return items

    def get_trim_len(self) -> int:
        return 400000  # 7 days of trade_v2 rounded up
