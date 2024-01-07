import traceback
from datetime import datetime
from typing import List

from altonomy.ace.enums import DealProcessingStatus
from altonomy.ace.v2.trade.daos import TradeV2Dao
from altonomy.ace.v2.trade.models import TradeV2
from altonomy.ace.v2.trade.rules.xalpha import get_handlers
from altonomy.ace.v2.trade.rules.xalpha.common import copy_trade
from altonomy.ace.v2.log_util import get_v2_logger
from sqlalchemy.orm import Session


logger = get_v2_logger("xalpha_trade_processor")


class XAlphaTradeProcessor:

    def __init__(self, db: Session):
        self.db: Session = db
        self.trade_dao: TradeV2Dao = TradeV2Dao(db, TradeV2)

    def process_deal(self, deal: dict) -> None:

        deal_id = deal.get("deal_id")
        master_deal_id = deal.get("master_deal_id")
        deal_type = deal.get("deal_type")
        valid_from = deal.get("valid_from")

        handlers = get_handlers(deal_type)
        if not handlers:
            self.log_error("Flow", f"No handlers for {deal_type}")
            return

        new_create_trades = []
        new_delete_trades = []
        try:
            for handler in handlers:
                create_feeds, delete_feeds = handler(self, deal)
                new_create_trades.extend(create_feeds)
                new_delete_trades.extend(delete_feeds)
        except Exception:
            self.log_error("Code", traceback.format_exc())
            return

        is_cancelled = deal.get("deal_processing_status") in (DealProcessingStatus.Cancelled, DealProcessingStatus.Pending)
        if len(new_create_trades) == 0 and is_cancelled:
            prev_trades = self.get_current_feeds_by_deal_id(deal_id)
            for prev_feed in prev_trades:
                prev_feed.effective_date_end = valid_from
        else:
            prev_trades = []
            for new_create_feed in new_create_trades:
                prev_feed = self.get_current_feed_by_product(new_create_feed.deal_id, new_create_feed.product)
                if prev_feed is None:
                    continue
                new_create_feed.ref_id = prev_feed.id

                prev_feed.effective_date_end = valid_from
                prev_trades.append(prev_feed)

        parent_trades = []
        if is_cancelled:
            sibling_count = self.count_current_sibling_feeds(master_deal_id, deal_id)
            if sibling_count == 0:
                closed_parent_feeds = self.get_last_feeds(master_deal_id)
                for parent_feed in closed_parent_feeds:
                    new_parent_feed = copy_trade(parent_feed)
                    if new_parent_feed.effective_date_start != valid_from:
                        new_parent_feed.effective_date_start = valid_from
                        parent_trades.append(new_parent_feed)

        else:
            parent_trades.extend(self.get_current_feeds_by_deal_id(master_deal_id))
            for parent_feed in parent_trades:
                parent_feed.effective_date_end = valid_from

        self.db.bulk_save_objects(new_delete_trades)
        self.db.bulk_save_objects(new_create_trades)
        self.db.bulk_save_objects(prev_trades)
        self.db.bulk_save_objects(parent_trades)
        self.db.commit()

    def log_error(self, error_type: str, reason: str) -> None:
        logger.error(f"{datetime.utcnow()} | {error_type} | {reason}")
        # if "as_of_date" in deal:
        #     del deal["as_of_date"]
        # feed_error = SystemFeedError(
        #     system_source=system_source,
        #     version=__version__,
        #     product=deal_type,
        #     error_type=error_type,
        #     reason=reason,
        #     data=deal,
        # )

        # self.dao_error.create(feed_error)

    def get_last_feeds(self, deal_id: int) -> TradeV2:
        last_eff_date_end = self.get_deal_last_effective_date_end(deal_id)
        return self.trade_dao.get_all_create_by_deal_id_and_effective_date_end(deal_id, last_eff_date_end)

    def get_deal_last_effective_date_end(self, deal_id: int) -> int:
        return self.trade_dao.get_last_effective_date_end(deal_id)

    def get_current_feed(self, deal_id: int) -> TradeV2:
        return self.trade_dao.get_current_feed(deal_id)

    def get_current_feed_by_product(self, deal_id: int, product: str) -> TradeV2:
        return self.trade_dao.get_current_feed_by_product(deal_id, product)

    def get_current_feeds_by_deal_id(self, deal_id: int) -> List[TradeV2]:
        return self.trade_dao.get_current_feeds_by_deal_id(deal_id)

    def count_current_sibling_feeds(self, master_deal_id: int, exclude_deal_id: int) -> int:
        return self.trade_dao.count_current_sibling_feeds(master_deal_id, exclude_deal_id)
