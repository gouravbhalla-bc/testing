from typing import List

from altonomy.ace.common.utils import row_to_json
from altonomy.ace.models import FeedV2
from altonomy.ace.v2.feed.daos import FeedV2Dao
from altonomy.ace.v2.common.ace_publisher import AcePublisher


class AceFeedCreatePublisher(AcePublisher):

    def get_stream_key(self) -> str:
        return "STREAM:ACE:FEEDV2"

    def get_log_name(self) -> str:
        return "ace_feed_create_publisher"

    def get_items(self, batch_size: int) -> List[dict]:
        last_id = self.get_last_payload().get("id", 0)

        with self.get_session() as db:
            feed_dao: FeedV2Dao = FeedV2Dao(db, FeedV2)
            feeds = feed_dao.get_N_feeds_after_record_id(last_id, batch_size)

        items = []
        for feed in feeds:
            item = {
                "id": feed.id,
                "json": row_to_json(feed),
            }
            items.append(item)
        return items

    def get_trim_len(self) -> int:
        return 1200000  # 7 days of feed_v2 rounded up
