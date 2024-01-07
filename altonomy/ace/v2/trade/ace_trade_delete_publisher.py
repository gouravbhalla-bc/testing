import json
from datetime import datetime
from typing import List

from altonomy.ace.common.utils import row_to_json
from altonomy.ace.models import TradeV2
from altonomy.ace.v2.common.ace_publisher import AcePublisher
from altonomy.ace.v2.trade.daos import TradeV2Dao


class AceTradeDeletePublisher(AcePublisher):

    def get_stream_key(self) -> str:
        return "STREAM:ACE:TRADEV2:DELETE"

    def get_log_name(self) -> str:
        return "ace_trade_delete_publisher"

    def get_items(self, batch_size: int) -> List[dict]:
        payload_json = json.loads(self.get_last_payload().get("json", "{}"))
        last_effective_date_end = datetime.fromtimestamp(payload_json.get("effective_date_end", 0))

        with self.get_session() as db:
            trade_dao: TradeV2Dao = TradeV2Dao(db, TradeV2)
            trades = trade_dao.get_N_feeds_after_last_effective_date_end(last_effective_date_end, batch_size)

        items = []
        for feed in trades:
            item = {
                "id": feed.id,
                "json": row_to_json(feed),
            }
            items.append(item)
        return items

    def get_trim_len(self) -> int:
        return 1000  # 7 days of trade_v2 rounded up
