import traceback
from datetime import datetime
from typing import List

from altonomy.ace.enums import DealProcessingStatus, CompCode
from altonomy.ace.v2.feed.daos import FeedV2Dao
from altonomy.ace.v2.feed.models import FeedV2
from altonomy.ace.v2.feed.rules.xalpha import get_handlers
from altonomy.ace.v2.feed.rules.xalpha.common import copy_feed
from altonomy.ace.v2.log_util import get_v2_logger
from sqlalchemy.orm import Session


logger = get_v2_logger("xalpha_feed_processor")


class XAlphaFeedProcessor:

    def __init__(self, db: Session):
        self.db: Session = db
        self.feed_dao: FeedV2Dao = FeedV2Dao(db, FeedV2)
        # self.feed_dao_error = Dao(db, FeedV2Error)

    def process_deal(self, deal: dict) -> None:

        deal_id = deal.get("deal_id")
        master_deal_id = deal.get("master_deal_id")
        deal_type = deal.get("deal_type")
        valid_from = deal.get("valid_from")

        handlers = get_handlers(deal_type)

        if not handlers:
            self.log_error("Flow", f"No handlers for {deal_type}")
            return

        new_create_feeds = []
        new_delete_feeds = []
        try:
            for handler in handlers:
                create_feeds, delete_feeds = handler(self, deal)
                new_create_feeds.extend(create_feeds)
                new_delete_feeds.extend(delete_feeds)
        except Exception:
            self.log_error("Code", traceback.format_exc())
            return

        if self.count_currrent_children_feeds(deal_id) != 0:
            for feed in new_create_feeds:
                feed.effective_date_end = feed.effective_date_start

        is_cancelled = deal.get("deal_processing_status") in (DealProcessingStatus.Cancelled, DealProcessingStatus.Pending)

        if len(new_create_feeds) == 0 and is_cancelled:
            prev_feeds = self.get_current_feeds_by_deal_id(deal_id)
            for prev_feed in prev_feeds:
                prev_feed.effective_date_end = valid_from
        else:
            prev_feeds = []
            for new_create_feed in new_create_feeds:
                prev_feed = self.get_current_feed(new_create_feed.deal_id, new_create_feed.comp_code)
                if prev_feed is None:
                    continue
                new_create_feed.ref_id = prev_feed.id

                prev_feed.effective_date_end = valid_from
                prev_feeds.append(prev_feed)

        parent_feeds = []
        if is_cancelled:
            sibling_count = self.count_current_sibling_feeds(master_deal_id, deal_id)
            if sibling_count == 0 and len(prev_feeds) != 0:
                closed_parent_feeds = self.get_last_feeds(master_deal_id)
                for parent_feed in closed_parent_feeds:
                    new_parent_feed = copy_feed(parent_feed)
                    if new_parent_feed.effective_date_start != valid_from:
                        new_parent_feed.effective_date_start = valid_from
                        parent_feeds.append(new_parent_feed)

        else:
            parent_feeds.extend(self.get_current_feeds_by_deal_id(master_deal_id))
            for parent_feed in parent_feeds:
                parent_feed.effective_date_end = valid_from

        self.db.bulk_save_objects(new_delete_feeds)
        self.db.bulk_save_objects(new_create_feeds)
        self.db.bulk_save_objects(prev_feeds)
        self.db.bulk_save_objects(parent_feeds)
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

    def get_last_feeds(self, deal_id: int) -> FeedV2:
        last_eff_date_end = self.get_deal_last_effective_date_end(deal_id)
        return self.feed_dao.get_all_create_by_deal_id_and_effective_date_end(deal_id, last_eff_date_end)

    def get_deal_last_effective_date_end(self, deal_id: int) -> int:
        return self.feed_dao.get_last_effective_date_end(deal_id)

    def get_current_feed(self, deal_id: int, comp_code: str) -> FeedV2:
        CASHFLOW_COMP_CODES = (
            CompCode.CASHFLOW_TRANSFER,
            CompCode.CASHFLOW_MM_FEE,
            CompCode.CASHFLOW_REFERRAL_FEE,
            CompCode.CASHFLOW_TRANSACTION_FEE,
            CompCode.CASHFLOW_PNL_DIVIDENDING,
            CompCode.CASHFLOW_MM_PROFIT_SHARE,
            CompCode.CASHFLOW_NON_TRADING_EXPENSE,
            CompCode.CASHFLOW_INTERCO_LOAN,
            CompCode.CASHFLOW_INTERCO_RETURN,
            CompCode.CASHFLOW_FUNDING,
            CompCode.CASHFLOW_ETC,
            CompCode.CASHFLOW_BUSINESS_PNL,
            CompCode.CASHFLOW_OTHER_INCOME,
            CompCode.CASHFLOW_OTHER_EXPENSE,
            CompCode.CASHFLOW_INVESTMENTS,

            #  Execution Child Deal
            CompCode.EXECUTION_CASHFLOW_START,
            CompCode.EXECUTION_CASHFLOW_END,
            CompCode.EXECUTION_CASHFLOW_FEE,

            # Trading fees
            CompCode.CASHFLOW_FUNDING_FEE,
            CompCode.CASHFLOW_INSURANCE_CLEAR,

            # NFT
            CompCode.CASHFLOW_NFT_BID_ASK,
            CompCode.CASHFLOW_NFT_TOKEN,
            CompCode.CASHFLOW_NFT_SERVICE_FEE,
        )

        if comp_code in CASHFLOW_COMP_CODES:
            return self.feed_dao.get_current_feed(deal_id)
        else:
            return self.feed_dao.get_current_feed_by_comp_code(deal_id, comp_code)

    def get_current_feeds_by_deal_id(self, deal_id: int) -> List[FeedV2]:
        return self.feed_dao.get_current_feeds_by_deal_id(deal_id)

    def count_current_sibling_feeds(self, master_deal_id: int, exclude_deal_id: int) -> int:
        return self.feed_dao.count_current_sibling_feeds(master_deal_id, exclude_deal_id)

    def count_currrent_children_feeds(self, deal_id: int) -> int:
        return self.feed_dao.count_current_children_feeds(deal_id)
